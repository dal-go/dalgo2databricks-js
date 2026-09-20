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
  readonly status?: { readonly state?: unknown; readonly error?: { readonly message?: unknown } };
  readonly manifest?: { readonly truncated?: unknown; readonly total_chunk_count?: unknown; readonly total_row_count?: unknown; readonly chunks?: unknown; readonly schema?: { readonly columns?: readonly { readonly name?: unknown }[] } };
  readonly data_array?: unknown;
  readonly next_chunk_internal_link?: unknown;
  readonly result?: { readonly data_array?: unknown; readonly next_chunk_internal_link?: unknown; readonly external_links?: unknown; };
}

interface ExecutionContext { readonly deadline: number; readonly signal: AbortSignal | undefined; }

const terminalFailureStates = new Set(["FAILED", "CANCELED", "CLOSED"]);

function asObject(value: unknown, message: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new DatabricksSqlError(message);
  return value as Record<string, unknown>;
}

function positiveSafeInteger(value: number | undefined, fallback: number, label: string): number {
  const chosen = value ?? fallback;
  if (!Number.isSafeInteger(chosen) || chosen <= 0) throw new TypeError(`${label} must be a positive safe integer`);
  return chosen;
}

function manifestCount(value: unknown, label: string): number | undefined {
  if (value === undefined) return undefined;
  if (!Number.isSafeInteger(value) || typeof value !== "number" || value < 0) throw new DatabricksSqlError(`Databricks result manifest has invalid ${label}`);
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
    positiveSafeInteger(options.timeoutMs, 30_000, "timeoutMs");
    positiveSafeInteger(options.maxResultChunks, 1_000, "maxResultChunks");
    this.#options = options;
    this.#fetch = options.fetch ?? globalThis.fetch;
  }

  public async execute(request: StatementRequest): Promise<StatementResult> {
    const context: ExecutionContext = { deadline: Date.now() + positiveSafeInteger(this.#options.timeoutMs, 30_000, "timeoutMs"), signal: this.#options.signal };
    const response = await this.#request("/api/2.0/sql/statements/", {
      method: "POST",
      body: JSON.stringify({ warehouse_id: this.#options.warehouseId, ...(this.#options.catalog === undefined ? {} : { catalog: this.#options.catalog }), ...(this.#options.schema === undefined ? {} : { schema: this.#options.schema }), statement: request.statement, ...(request.parameters === undefined ? {} : { parameters: request.parameters }), format: "JSON_ARRAY", disposition: "INLINE", wait_timeout: "5s" }),
    }, context);
    return this.#waitForResult(response, context);
  }

  async #waitForResult(response: StatementResponse, context: ExecutionContext): Promise<StatementResult> {
    const statementId = response.statement_id;
    if (typeof statementId !== "string" || statementId.length === 0) throw new DatabricksSqlError("Databricks response did not contain a statement_id");
    let current = response;
    const maxPolls = positiveSafeInteger(this.#options.maxPolls, 120, "maxPolls");
    for (let polls = 0; polls <= maxPolls; polls += 1) {
      this.#assertActive(context);
      const state = current.status?.state;
      if (state === "SUCCEEDED") return this.#collectInlineResult(statementId, current, context);
      if (typeof state === "string" && terminalFailureStates.has(state)) throw new DatabricksSqlError(`Databricks statement ${statementId} ${state}`);
      if (typeof state !== "string") throw new DatabricksSqlError(`Databricks statement ${statementId} returned no status state`);
      if (polls === maxPolls) throw new DatabricksSqlError(`Databricks statement ${statementId} exceeded ${String(maxPolls)} status polls`);
      await this.#pause(this.#options.pollIntervalMs ?? 100, context);
      current = await this.#request(`/api/2.0/sql/statements/${encodeURIComponent(statementId)}`, { method: "GET" }, context);
    }
    throw new DatabricksSqlError("unreachable statement polling state");
  }

  async #collectInlineResult(statementId: string, response: StatementResponse, context: ExecutionContext): Promise<StatementResult> {
    const manifest = response.manifest;
    const rawColumns = manifest?.schema?.columns?.map((column) => column.name);
    if (manifest === undefined || rawColumns === undefined || rawColumns.some((name) => typeof name !== "string")) throw new DatabricksSqlError(`Databricks statement ${statementId} returned an invalid result schema`);
    if (manifest.truncated === true) throw new DatabricksSqlError(`Databricks statement ${statementId} returned a truncated result`);
    if (manifest.chunks !== undefined && !Array.isArray(manifest.chunks)) throw new DatabricksSqlError(`Databricks statement ${statementId} returned invalid chunk metadata`);
    const expectedChunks = manifestCount(manifest.total_chunk_count, "total_chunk_count");
    const expectedRows = manifestCount(manifest.total_row_count, "total_row_count");
    const maxChunks = positiveSafeInteger(this.#options.maxResultChunks, 1_000, "maxResultChunks");
    if (expectedChunks !== undefined && expectedChunks > maxChunks) throw new DatabricksSqlError(`Databricks statement ${statementId} exceeds the result chunk limit`);
    if (expectedChunks !== undefined && manifest.chunks !== undefined && manifest.chunks.length !== expectedChunks) throw new DatabricksSqlError(`Databricks statement ${statementId} has incomplete chunk metadata`);
    const columns = rawColumns as string[];
    const rows: (readonly unknown[])[] = [];
    const seenLinks = new Set<string>();
    let current = response;
    let chunksRead = 0;
    for (;;) {
      this.#assertActive(context);
      chunksRead += 1;
      if (chunksRead > maxChunks) throw new DatabricksSqlError(`Databricks statement ${statementId} exceeds the result chunk limit`);
      if (current.result?.external_links !== undefined) throw new DatabricksSqlError("EXTERNAL_LINKS results are not supported; this client only accepts inline JSON results");
      const data = current.result?.data_array ?? current.data_array;
      if (data !== undefined) {
        if (!Array.isArray(data)) throw new DatabricksSqlError(`Databricks statement ${statementId} returned invalid JSON_ARRAY data`);
        for (const row of data as unknown[]) {
          if (!Array.isArray(row) || row.length !== columns.length) throw new DatabricksSqlError(`Databricks statement ${statementId} returned an invalid JSON_ARRAY row`);
          rows.push(row);
        }
      }
      const next = current.result?.next_chunk_internal_link ?? current.next_chunk_internal_link;
      if (next === undefined) {
        if (expectedChunks !== undefined && chunksRead !== expectedChunks) throw new DatabricksSqlError(`Databricks statement ${statementId} result chunks are incomplete`);
        if (expectedRows !== undefined && rows.length !== expectedRows) throw new DatabricksSqlError(`Databricks statement ${statementId} result rows are incomplete`);
        return { columns, rows, statementId };
      }
      if (typeof next !== "string") throw new DatabricksSqlError(`Databricks statement ${statementId} returned an unsafe result chunk link`);
      const safePath = this.#safeChunkPath(statementId, next);
      if (seenLinks.has(safePath)) throw new DatabricksSqlError(`Databricks statement ${statementId} result chunk link cycle detected`);
      seenLinks.add(safePath);
      current = await this.#request(safePath, { method: "GET" }, context);
    }
  }

  #safeChunkPath(statementId: string, link: string): string {
    const url = new URL(link, this.#baseUrl);
    const expected = `/api/2.0/sql/statements/${encodeURIComponent(statementId)}/result/chunks/`;
    if (url.origin !== this.#baseUrl.origin || !url.pathname.startsWith(expected)) throw new DatabricksSqlError(`Databricks statement ${statementId} returned an unsafe result chunk link`);
    return `${url.pathname}${url.search}`;
  }

  #assertActive(context: ExecutionContext): void {
    if (context.signal?.aborted) throw new DatabricksSqlError("Databricks operation aborted");
    if (Date.now() >= context.deadline) throw new DatabricksSqlError("Databricks operation deadline exceeded");
  }

  async #pause(milliseconds: number, context: ExecutionContext): Promise<void> {
    this.#assertActive(context);
    const wait = Math.min(milliseconds, context.deadline - Date.now());
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => { cleanup(); resolve(); }, wait);
      const cleanup = (): void => context.signal?.removeEventListener("abort", abort);
      const abort = (): void => { clearTimeout(timer); cleanup(); reject(new DatabricksSqlError("Databricks operation aborted")); };
      context.signal?.addEventListener("abort", abort, { once: true });
    });
    this.#assertActive(context);
  }

  async #request(path: string, init: RequestInit, context: ExecutionContext): Promise<StatementResponse> {
    this.#assertActive(context);
    let token: string;
    try { token = await this.#options.tokenProvider(); } catch { throw new DatabricksSqlError("Databricks token provider failed"); }
    this.#assertActive(context);
    if (token.length === 0) throw new TypeError("tokenProvider returned an empty token");
    const controller = new AbortController();
    const relayAbort = (): void => { controller.abort(); };
    context.signal?.addEventListener("abort", relayAbort, { once: true });
    const remaining = context.deadline - Date.now();
    let rejectDeadline: (reason: DatabricksSqlError) => void = (reason): never => { throw reason; };
    const deadline = new Promise<Response>((_resolve, reject) => { rejectDeadline = (reason): void => { reject(reason); }; });
    const timer = setTimeout(() => { controller.abort(); rejectDeadline(new DatabricksSqlError("Databricks operation deadline exceeded")); }, remaining);
    let rejectAbort: (reason: DatabricksSqlError) => void = (reason): never => { throw reason; };
    const aborted = new Promise<Response>((_resolve, reject) => { rejectAbort = (reason): void => { reject(reason); }; });
    const abortRace = (): void => { rejectAbort(new DatabricksSqlError("Databricks operation aborted")); };
    context.signal?.addEventListener("abort", abortRace, { once: true });
    let response: Response;
    try {
      response = await Promise.race([this.#fetch(new URL(path, this.#baseUrl), { ...init, signal: controller.signal, redirect: "error", headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }), deadline, aborted]);
    } catch (error: unknown) {
      if (error instanceof DatabricksSqlError) throw error;
      this.#assertActive(context);
      throw new DatabricksSqlError("Databricks HTTP request failed");
    } finally {
      clearTimeout(timer);
      context.signal?.removeEventListener("abort", relayAbort);
      context.signal?.removeEventListener("abort", abortRace);
    }
    this.#assertActive(context);
    const body = await response.json().catch(() => undefined) as unknown;
    if (!response.ok) throw new DatabricksSqlError(`Databricks HTTP ${String(response.status)} request failed`);
    return asObject(body, "Databricks returned a non-object response");
  }
}
