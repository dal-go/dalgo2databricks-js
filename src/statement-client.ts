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
}

export interface StatementParameter {
  readonly name: string;
  readonly value: string;
  readonly type: string;
}

export interface StatementRequest {
  readonly statement: string;
  readonly parameters?: readonly StatementParameter[];
}

export interface StatementResult {
  readonly columns: readonly string[];
  readonly rows: readonly (readonly unknown[])[];
  readonly statementId: string;
}

interface StatementResponse {
  readonly statement_id?: unknown;
  readonly status?: { readonly state?: unknown; readonly error?: { readonly message?: unknown } };
  readonly manifest?: { readonly schema?: { readonly columns?: readonly { readonly name?: unknown }[] } };
  readonly result?: {
    readonly data_array?: unknown;
    readonly next_chunk_internal_link?: unknown;
    readonly external_links?: unknown;
  };
}

const terminalFailureStates = new Set(["FAILED", "CANCELED", "CLOSED"]);

function asObject(value: unknown, message: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new DatabricksSqlError(message);
  return value as Record<string, unknown>;
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export class DatabricksStatementClient {
  readonly #baseUrl: URL;
  readonly #options: Required<Pick<DatabricksSqlClientOptions, "warehouseId" | "tokenProvider">>
    & Omit<DatabricksSqlClientOptions, "host" | "warehouseId" | "tokenProvider">;
  readonly #fetch: typeof globalThis.fetch;

  public constructor(options: DatabricksSqlClientOptions) {
    this.#baseUrl = new URL(options.host);
    if (this.#baseUrl.protocol !== "https:" || this.#baseUrl.username || this.#baseUrl.password
      || this.#baseUrl.pathname !== "/" || this.#baseUrl.search || this.#baseUrl.hash) {
      throw new TypeError("host must be a bare https workspace origin without credentials or path");
    }
    if (options.warehouseId.length === 0) throw new TypeError("warehouseId is required");
    this.#options = options;
    this.#fetch = options.fetch ?? globalThis.fetch;
  }

  public async execute(request: StatementRequest): Promise<StatementResult> {
    const response = await this.#request("/api/2.0/sql/statements/", {
      method: "POST",
      body: JSON.stringify({
        warehouse_id: this.#options.warehouseId,
        ...(this.#options.catalog === undefined ? {} : { catalog: this.#options.catalog }),
        ...(this.#options.schema === undefined ? {} : { schema: this.#options.schema }),
        statement: request.statement,
        ...(request.parameters === undefined ? {} : { parameters: request.parameters }),
        format: "JSON_ARRAY",
        disposition: "INLINE",
        wait_timeout: "5s",
      }),
    });
    return this.#waitForResult(response);
  }

  async #waitForResult(response: StatementResponse): Promise<StatementResult> {
    const statementId = response.statement_id;
    if (typeof statementId !== "string" || statementId.length === 0) {
      throw new DatabricksSqlError("Databricks response did not contain a statement_id");
    }
    let current = response;
    const maxPolls = this.#options.maxPolls ?? 120;
    for (let polls = 0; polls <= maxPolls; polls += 1) {
      const state = current.status?.state;
      if (state === "SUCCEEDED") return this.#collectInlineResult(statementId, current);
      if (typeof state === "string" && terminalFailureStates.has(state)) {
        const detail = current.status?.error?.message;
        throw new DatabricksSqlError(`Databricks statement ${statementId} ${state}${typeof detail === "string" ? `: ${detail}` : ""}`);
      }
      if (typeof state !== "string") throw new DatabricksSqlError(`Databricks statement ${statementId} returned no status state`);
      if (polls === maxPolls) throw new DatabricksSqlError(`Databricks statement ${statementId} exceeded ${String(maxPolls)} status polls`);
      await sleep(this.#options.pollIntervalMs ?? 100);
      current = await this.#request(`/api/2.0/sql/statements/${encodeURIComponent(statementId)}`, { method: "GET" });
    }
    throw new DatabricksSqlError("unreachable statement polling state");
  }

  async #collectInlineResult(statementId: string, response: StatementResponse): Promise<StatementResult> {
    const rawColumns = response.manifest?.schema?.columns?.map((column) => column.name);
    if (rawColumns === undefined || rawColumns.some((name) => typeof name !== "string")) {
      throw new DatabricksSqlError(`Databricks statement ${statementId} returned an invalid result schema`);
    }
    const columns = rawColumns as string[];
    const rows: (readonly unknown[])[] = [];
    let current = response;
    for (;;) {
      if (current.result?.external_links !== undefined) {
        throw new DatabricksSqlError("EXTERNAL_LINKS results are not supported; this client only accepts inline JSON results");
      }
      const data = current.result?.data_array;
      if (data !== undefined) {
        if (!Array.isArray(data)) {
          throw new DatabricksSqlError(`Databricks statement ${statementId} returned invalid JSON_ARRAY data`);
        }
        for (const row of data as unknown[]) {
          if (!Array.isArray(row)) throw new DatabricksSqlError(`Databricks statement ${statementId} returned invalid JSON_ARRAY data`);
          rows.push(row);
        }
      }
      const next = current.result?.next_chunk_internal_link;
      if (next === undefined) return { columns, rows, statementId };
      if (typeof next !== "string" || !this.#isSafeChunkLink(statementId, next)) {
        throw new DatabricksSqlError(`Databricks statement ${statementId} returned an unsafe result chunk link`);
      }
      current = await this.#request(next, { method: "GET" });
    }
  }

  #isSafeChunkLink(statementId: string, link: string): boolean {
    const url = new URL(link, this.#baseUrl);
    const expected = `/api/2.0/sql/statements/${encodeURIComponent(statementId)}/result/chunks/`;
    return url.origin === this.#baseUrl.origin && url.pathname.startsWith(expected);
  }

  async #request(path: string, init: RequestInit): Promise<StatementResponse> {
    const token = await this.#options.tokenProvider();
    if (token.length === 0) throw new TypeError("tokenProvider returned an empty token");
    const response = await this.#fetch(new URL(path, this.#baseUrl), {
      ...init,
      redirect: "error",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    });
    const body = await response.json().catch(() => undefined) as unknown;
    if (!response.ok) {
      const detail = body === undefined ? "non-JSON response" : JSON.stringify(body);
      throw new DatabricksSqlError(`Databricks HTTP ${String(response.status)}: ${detail}`);
    }
    return asObject(body, "Databricks returned a non-object response");
  }
}
