import { DatabricksSqlError } from "./errors.js";

export type TokenProvider = () => string | Promise<string>;

export interface DatabricksSqlClientOptions {
  readonly host: string;
  readonly warehouseId: string;
  readonly catalog?: string;
  readonly schema?: string;
  readonly tokenProvider: TokenProvider;
  readonly fetch?: typeof globalThis.fetch;
  readonly pollIntervalMs?: number;
  readonly maxPolls?: number;
  readonly timeoutMs?: number;
  readonly maxResultChunks?: number;
  readonly signal?: AbortSignal;
}

export interface StatementParameter { readonly name: string; readonly value: string; readonly type: string; }
export interface StatementRequest { readonly statement: string; readonly parameters?: readonly StatementParameter[]; }
export interface StatementResult { readonly columns: readonly string[]; readonly rows: readonly (readonly unknown[])[]; readonly statementId: string; }

interface StatementResponse {
  readonly statement_id?: unknown;
  readonly status?: { readonly state?: unknown };
  readonly manifest?: unknown;
  readonly result?: unknown;
  readonly data_array?: unknown;
  readonly next_chunk_internal_link?: unknown;
  readonly external_links?: unknown;
  readonly chunk_index?: unknown;
  readonly row_offset?: unknown;
  readonly row_count?: unknown;
}

interface ExecutionContext {
  readonly signal: AbortSignal;
  readonly isTimeout: () => boolean;
  readonly dispose: () => void;
}

interface ManifestChunk { readonly index: number; readonly offset: number; readonly count: number; }
interface ResultManifest { readonly columns: readonly string[]; readonly chunks: readonly ManifestChunk[]; readonly totalRows: number; }
interface ResultChunk { readonly index: number; readonly offset: number; readonly count: number; readonly rows: readonly (readonly unknown[])[]; readonly next: string | undefined; }

const terminalFailureStates = new Set(["FAILED", "CANCELED", "CLOSED"]);
const maxTimerMilliseconds = 2_147_483_647;
const statementIdPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;

function asObject(value: unknown, message: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new DatabricksSqlError(message);
  return value as Record<string, unknown>;
}

function positiveSafeInteger(value: number | undefined, fallback: number, label: string): number {
  const chosen = value ?? fallback;
  if (!Number.isSafeInteger(chosen) || chosen <= 0) throw new TypeError(`${label} must be a positive safe integer`);
  return chosen;
}

function timerMilliseconds(value: number | undefined, fallback: number, label: string, allowZero: boolean): number {
  const chosen = value ?? fallback;
  if (!Number.isSafeInteger(chosen) || chosen < 0 || (!allowZero && chosen === 0) || chosen > maxTimerMilliseconds) {
    throw new TypeError(`${label} must be a ${allowZero ? "non-negative" : "positive"} safe timer duration no greater than ${String(maxTimerMilliseconds)}`);
  }
  return chosen;
}

function requiredCount(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) throw new DatabricksSqlError(`Databricks result manifest has invalid ${label}`);
  return value;
}

function safeStatementId(value: unknown): string {
  if (typeof value !== "string" || !statementIdPattern.test(value)) throw new DatabricksSqlError("Databricks response did not contain a valid statement_id");
  return value;
}

export class DatabricksStatementClient {
  readonly #baseUrl: URL;
  readonly #options: DatabricksSqlClientOptions;
  readonly #fetch: typeof globalThis.fetch;

  public constructor(options: DatabricksSqlClientOptions) {
    this.#baseUrl = new URL(options.host);
    if (this.#baseUrl.protocol !== "https:" || this.#baseUrl.username || this.#baseUrl.password || this.#baseUrl.pathname !== "/" || this.#baseUrl.search || this.#baseUrl.hash) throw new TypeError("host must be a bare https workspace origin without credentials or path");
    if (options.warehouseId.length === 0) throw new TypeError("warehouseId is required");
    if (options.catalog !== undefined && options.schema === undefined) throw new TypeError("catalog requires schema");
    positiveSafeInteger(options.maxPolls, 120, "maxPolls");
    timerMilliseconds(options.pollIntervalMs, 100, "pollIntervalMs", true);
    timerMilliseconds(options.timeoutMs, 30_000, "timeoutMs", false);
    positiveSafeInteger(options.maxResultChunks, 1_000, "maxResultChunks");
    this.#options = options;
    this.#fetch = options.fetch ?? globalThis.fetch;
  }

  public async execute(request: StatementRequest): Promise<StatementResult> {
    const context = this.#startContext();
    try {
      const response = await this.#request("/api/2.0/sql/statements/", {
        method: "POST",
        body: JSON.stringify({ warehouse_id: this.#options.warehouseId, ...(this.#options.catalog === undefined ? {} : { catalog: this.#options.catalog }), ...(this.#options.schema === undefined ? {} : { schema: this.#options.schema }), statement: request.statement, ...(request.parameters === undefined ? {} : { parameters: request.parameters }), format: "JSON_ARRAY", disposition: "INLINE", wait_timeout: "5s" }),
      }, context);
      return await this.#waitForResult(response, context);
    } finally {
      context.dispose();
    }
  }

  #startContext(): ExecutionContext {
    const controller = new AbortController();
    const timeout = timerMilliseconds(this.#options.timeoutMs, 30_000, "timeoutMs", false);
    let timedOut = false;
    const onAbort = (): void => { controller.abort(); };
    this.#options.signal?.addEventListener("abort", onAbort, { once: true });
    if (this.#options.signal?.aborted) controller.abort();
    const timer = setTimeout(() => { timedOut = true; controller.abort(); }, timeout);
    return {
      signal: controller.signal,
      isTimeout: (): boolean => timedOut,
      dispose: (): void => { clearTimeout(timer); this.#options.signal?.removeEventListener("abort", onAbort); },
    };
  }

  async #waitForResult(response: StatementResponse, context: ExecutionContext): Promise<StatementResult> {
    const statementId = safeStatementId(response.statement_id);
    let current = response;
    const maxPolls = positiveSafeInteger(this.#options.maxPolls, 120, "maxPolls");
    for (let polls = 0; polls <= maxPolls; polls += 1) {
      this.#assertActive(context);
      const state = current.status?.state;
      if (state === "SUCCEEDED") return this.#collectInlineResult(statementId, current, context);
      if (typeof state === "string" && terminalFailureStates.has(state)) throw new DatabricksSqlError(`Databricks statement ${state}`);
      if (typeof state !== "string") throw new DatabricksSqlError("Databricks statement returned no status state");
      if (polls === maxPolls) throw new DatabricksSqlError(`Databricks statement exceeded ${String(maxPolls)} status polls`);
      await this.#pause(timerMilliseconds(this.#options.pollIntervalMs, 100, "pollIntervalMs", true), context);
      current = await this.#request(`/api/2.0/sql/statements/${encodeURIComponent(statementId)}`, { method: "GET" }, context);
      if (current.statement_id !== undefined && safeStatementId(current.statement_id) !== statementId) throw new DatabricksSqlError("Databricks status response referred to a different statement");
    }
    throw new DatabricksSqlError("unreachable statement polling state");
  }

  async #collectInlineResult(statementId: string, response: StatementResponse, context: ExecutionContext): Promise<StatementResult> {
    const manifest = this.#parseManifest(response.manifest);
    const maxChunks = positiveSafeInteger(this.#options.maxResultChunks, 1_000, "maxResultChunks");
    if (manifest.chunks.length > maxChunks) throw new DatabricksSqlError("Databricks result exceeds the result chunk limit");
    const rows: (readonly unknown[])[] = [];
    const seenLinks = new Set<string>();
    let current = response;
    for (let position = 0; position < manifest.chunks.length; position += 1) {
      this.#assertActive(context);
      const expected = manifest.chunks[position];
      if (expected === undefined) throw new DatabricksSqlError("Databricks result manifest is incomplete");
      const chunk = this.#parseResultChunk(current, manifest.columns.length);
      if (chunk.index !== expected.index || chunk.offset !== expected.offset || chunk.count !== expected.count || chunk.rows.length !== expected.count) {
        throw new DatabricksSqlError("Databricks result chunk did not match its manifest");
      }
      for (const row of chunk.rows) rows.push(row);
      const isLast = position === manifest.chunks.length - 1;
      if (isLast) {
        if (chunk.next !== undefined) throw new DatabricksSqlError("Databricks result contained an unexpected result chunk link");
        break;
      }
      if (chunk.next === undefined) throw new DatabricksSqlError("Databricks result chunks are incomplete");
      const safePath = this.#safeChunkPath(statementId, chunk.next);
      if (seenLinks.has(safePath)) throw new DatabricksSqlError("Databricks result chunk link cycle detected");
      seenLinks.add(safePath);
      current = await this.#request(safePath, { method: "GET" }, context);
      if (current.statement_id !== undefined && safeStatementId(current.statement_id) !== statementId) throw new DatabricksSqlError("Databricks result chunk referred to a different statement");
    }
    if (rows.length !== manifest.totalRows) throw new DatabricksSqlError("Databricks result rows are incomplete");
    return { columns: manifest.columns, rows, statementId };
  }

  #parseManifest(value: unknown): ResultManifest {
    const manifest = asObject(value, "Databricks returned an invalid result manifest");
    if (manifest.format !== "JSON_ARRAY") throw new DatabricksSqlError("Databricks result manifest was not JSON_ARRAY");
    if (manifest.truncated !== false) throw new DatabricksSqlError("Databricks result manifest was truncated or missing truncation status");
    const schema = asObject(manifest.schema, "Databricks result manifest had an invalid schema");
    if (!Array.isArray(schema.columns)) throw new DatabricksSqlError("Databricks result manifest had invalid columns");
    const columns = schema.columns.map((value) => {
      const column = asObject(value, "Databricks result manifest had an invalid column");
      if (typeof column.name !== "string" || column.name.length === 0) throw new DatabricksSqlError("Databricks result manifest had an invalid column name");
      return column.name;
    });
    if (new Set(columns).size !== columns.length) throw new DatabricksSqlError("Databricks result manifest had duplicate column names");
    const totalRows = requiredCount(manifest.total_row_count, "total_row_count");
    const totalChunks = requiredCount(manifest.total_chunk_count, "total_chunk_count");
    if (totalChunks === 0 || !Array.isArray(manifest.chunks) || manifest.chunks.length !== totalChunks) throw new DatabricksSqlError("Databricks result manifest had incomplete chunk metadata");
    let offset = 0;
    const chunks = manifest.chunks.map((value, index): ManifestChunk => {
      const chunk = asObject(value, "Databricks result manifest had an invalid chunk");
      const chunkIndex = requiredCount(chunk.chunk_index, "chunk_index");
      const rowOffset = requiredCount(chunk.row_offset, "row_offset");
      const rowCount = requiredCount(chunk.row_count, "row_count");
      if (chunkIndex !== index || rowOffset !== offset) throw new DatabricksSqlError("Databricks result manifest chunks were out of sequence");
      offset += rowCount;
      return { index: chunkIndex, offset: rowOffset, count: rowCount };
    });
    if (offset !== totalRows) throw new DatabricksSqlError("Databricks result manifest row counts were incoherent");
    return { columns, chunks, totalRows };
  }

  #parseResultChunk(response: StatementResponse, columnCount: number): ResultChunk {
    if (response.external_links !== undefined) throw new DatabricksSqlError("EXTERNAL_LINKS results are not supported; this client only accepts inline JSON results");
    const source = response.result === undefined ? response as unknown : response.result;
    const result = asObject(source, "Databricks returned an invalid JSON_ARRAY result chunk");
    if (result.external_links !== undefined) throw new DatabricksSqlError("EXTERNAL_LINKS results are not supported; this client only accepts inline JSON results");
    if (!Array.isArray(result.data_array)) throw new DatabricksSqlError("Databricks result chunk omitted JSON_ARRAY data");
    const rows = result.data_array.map((row) => {
      if (!Array.isArray(row) || row.length !== columnCount) throw new DatabricksSqlError("Databricks result chunk had an invalid JSON_ARRAY row");
      if (row.some((cell) => typeof cell !== "string" && cell !== null)) throw new DatabricksSqlError("Databricks result chunk had a non-string JSON_ARRAY cell");
      return row as readonly unknown[];
    });
    const next = result.next_chunk_internal_link;
    if (next !== undefined && typeof next !== "string") throw new DatabricksSqlError("Databricks result chunk had an unsafe result chunk link");
    return {
      index: requiredCount(result.chunk_index, "chunk_index"),
      offset: requiredCount(result.row_offset, "row_offset"),
      count: requiredCount(result.row_count, "row_count"),
      rows,
      next,
    };
  }

  #safeChunkPath(statementId: string, link: string): string {
    let url: URL;
    try {
      url = new URL(link, this.#baseUrl);
    } catch {
      throw new DatabricksSqlError("Databricks returned an unsafe result chunk link");
    }
    const expected = `/api/2.0/sql/statements/${encodeURIComponent(statementId)}/result/chunks/`;
    if (url.origin !== this.#baseUrl.origin || !url.pathname.startsWith(expected) || url.hash) throw new DatabricksSqlError("Databricks returned an unsafe result chunk link");
    return `${url.pathname}${url.search}`;
  }

  #assertActive(context: ExecutionContext): void {
    if (!context.signal.aborted) return;
    throw new DatabricksSqlError(context.isTimeout() ? "Databricks operation deadline exceeded" : "Databricks operation aborted");
  }

  async #awaitActive<T>(operation: Promise<T>, context: ExecutionContext): Promise<T> {
    this.#assertActive(context);
    return new Promise<T>((resolve, reject) => {
      const abort = (): void => { cleanup(); reject(new DatabricksSqlError(context.isTimeout() ? "Databricks operation deadline exceeded" : "Databricks operation aborted")); };
      const cleanup = (): void => { context.signal.removeEventListener("abort", abort); };
      context.signal.addEventListener("abort", abort, { once: true });
      operation.then((value) => { cleanup(); resolve(value); }, (error: unknown) => { cleanup(); reject(error instanceof Error ? error : new Error("asynchronous operation failed")); });
    });
  }

  async #pause(milliseconds: number, context: ExecutionContext): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await this.#awaitActive(new Promise<void>((resolve) => { timer = setTimeout(resolve, milliseconds); }), context);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }

  async #request(path: string, init: RequestInit, context: ExecutionContext): Promise<StatementResponse> {
    this.#assertActive(context);
    let token: string;
    try {
      token = await this.#awaitActive(Promise.resolve().then(this.#options.tokenProvider), context);
    } catch {
      if (context.signal.aborted) this.#assertActive(context);
      throw new DatabricksSqlError("Databricks token provider failed");
    }
    this.#assertActive(context);
    if (typeof token !== "string" || token.length === 0) throw new TypeError("tokenProvider returned an empty token");
    let response: Response;
    try {
      response = await this.#awaitActive(this.#fetch(new URL(path, this.#baseUrl), { ...init, signal: context.signal, redirect: "error", headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }), context);
    } catch {
      if (context.signal.aborted) this.#assertActive(context);
      throw new DatabricksSqlError("Databricks HTTP request failed");
    }
    let body: unknown;
    try {
      body = await this.#awaitActive(response.json(), context);
    } catch {
      if (context.signal.aborted) this.#assertActive(context);
      throw new DatabricksSqlError("Databricks HTTP response body was invalid");
    }
    if (!response.ok) throw new DatabricksSqlError(`Databricks HTTP ${String(response.status)} request failed`);
    return asObject(body, "Databricks returned a non-object response");
  }
}
