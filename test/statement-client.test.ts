import { describe, expect, it } from "vitest";
import { DatabricksSqlError, DatabricksStatementClient } from "../src/index.js";

function json(value: unknown): Response {
  return new Response(JSON.stringify(value), { status: 200, headers: { "Content-Type": "application/json" } });
}

function nextResponse(responses: Response[]): Response {
  const response = responses.shift();
  if (response === undefined) throw new Error("test response queue exhausted");
  return response;
}

describe("DatabricksStatementClient", () => {
  it("sends tokens only in Authorization, polls, and follows verified internal result chunks", async () => {
    const requests: Request[] = [];
    const responses = [
      json({ statement_id: "stmt-1", status: { state: "PENDING" } }),
      json({ statement_id: "stmt-1", status: { state: "SUCCEEDED" }, manifest: { schema: { columns: [{ name: "id" }] } }, result: { data_array: [["a"]], next_chunk_internal_link: "/api/2.0/sql/statements/stmt-1/result/chunks/1" } }),
      json({ statement_id: "stmt-1", status: { state: "SUCCEEDED" }, data_array: [["b"]] }),
    ];
    const fetch = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      requests.push(new Request(input, init));
      return Promise.resolve(nextResponse(responses));
    };
    const client = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "secret", fetch, pollIntervalMs: 0 });

    await expect(client.execute({ statement: "SELECT :p0", parameters: [{ name: "p0", value: "x", type: "STRING" }] }))
      .resolves.toEqual({ columns: ["id"], rows: [["a"], ["b"]], statementId: "stmt-1" });
    expect(requests).toHaveLength(3);
    expect(requests[0]?.headers.get("Authorization")).toBe("Bearer secret");
    expect(requests[0]?.headers.get("Content-Type")).toBe("application/json");
    expect(requests[0]?.redirect).toBe("error");
    expect(requests[2]?.url).toBe("https://dbc.example/api/2.0/sql/statements/stmt-1/result/chunks/1");
    const firstRequest = requests.at(0);
    if (firstRequest === undefined) throw new Error("expected first request");
    await expect(firstRequest.json()).resolves.toMatchObject({ disposition: "INLINE", format: "JSON_ARRAY", wait_timeout: "5s" });
  });

  it("never dereferences external or cross-workspace result links", async () => {
    const fetch = (): Promise<Response> => Promise.resolve(json({
      statement_id: "stmt-1", status: { state: "SUCCEEDED" }, manifest: { schema: { columns: [] } },
      result: { external_links: [{ external_link: "https://signed.example/result" }] },
    }));
    const client = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "secret", fetch });
    await expect(client.execute({ statement: "SELECT 1" })).rejects.toThrow("EXTERNAL_LINKS");

    const unsafeFetch = (): Promise<Response> => Promise.resolve(json({
      statement_id: "stmt-1", status: { state: "SUCCEEDED" }, manifest: { schema: { columns: [] } },
      result: { next_chunk_internal_link: "https://attacker.example/chunk" },
    }));
    const unsafe = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "secret", fetch: unsafeFetch });
    await expect(unsafe.execute({ statement: "SELECT 1" })).rejects.toBeInstanceOf(DatabricksSqlError);
  });

  it("does not reflect credentials or server error details in thrown errors", async () => {
    const failedStatement = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token-value",
      fetch: () => Promise.resolve(json({ statement_id: "stmt-1", status: { state: "FAILED", error: { message: "server secret: token-value" } } })),
    });
    await expect(failedStatement.execute({ statement: "SELECT 1" })).rejects.toThrow("Databricks statement stmt-1 FAILED");
    await expect(failedStatement.execute({ statement: "SELECT 1" })).rejects.not.toThrow("token-value");

    const failedHttp = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token-value",
      fetch: () => Promise.resolve(new Response(JSON.stringify({ error: "server secret: token-value" }), { status: 401 })),
    });
    await expect(failedHttp.execute({ statement: "SELECT 1" })).rejects.toThrow("Databricks HTTP 401 request failed");
    await expect(failedHttp.execute({ statement: "SELECT 1" })).rejects.not.toThrow("token-value");

    const providerFailure = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => { throw new Error("token-value"); }, fetch: () => Promise.resolve(json({})),
    });
    await expect(providerFailure.execute({ statement: "SELECT 1" })).rejects.toThrow("Databricks token provider failed");

    const fetchFailure = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token-value", fetch: () => Promise.reject(new Error("token-value")),
    });
    await expect(fetchFailure.execute({ statement: "SELECT 1" })).rejects.toThrow("Databricks HTTP request failed");
  });

  it("enforces result manifest completeness, widths, chunk bounds, and cycles", async () => {
    const incomplete = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", fetch: () => Promise.resolve(json({ statement_id: "stmt-1", status: { state: "SUCCEEDED" }, manifest: { total_chunk_count: 2, total_row_count: 2, schema: { columns: [{ name: "id" }] } }, result: { data_array: [["a"]] } })) });
    await expect(incomplete.execute({ statement: "SELECT 1" })).rejects.toThrow("result chunks are incomplete");

    const badWidth = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", fetch: () => Promise.resolve(json({ statement_id: "stmt-1", status: { state: "SUCCEEDED" }, manifest: { schema: { columns: [{ name: "id" }] } }, result: { data_array: [["a", "b"]] } })) });
    await expect(badWidth.execute({ statement: "SELECT 1" })).rejects.toThrow("invalid JSON_ARRAY row");

    const truncated = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", fetch: () => Promise.resolve(json({ statement_id: "stmt-1", status: { state: "SUCCEEDED" }, manifest: { truncated: true, schema: { columns: [{ name: "id" }] } }, result: { data_array: [] } })) });
    await expect(truncated.execute({ statement: "SELECT 1" })).rejects.toThrow("truncated result");

    const shortRows = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", fetch: () => Promise.resolve(json({ statement_id: "stmt-1", status: { state: "SUCCEEDED" }, manifest: { total_row_count: 2, schema: { columns: [{ name: "id" }] } }, result: { data_array: [["a"]] } })) });
    await expect(shortRows.execute({ statement: "SELECT 1" })).rejects.toThrow("result rows are incomplete");

    const overCap = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", maxResultChunks: 1, fetch: () => Promise.resolve(json({ statement_id: "stmt-1", status: { state: "SUCCEEDED" }, manifest: { total_chunk_count: 2, schema: { columns: [{ name: "id" }] } }, result: { data_array: [] } })) });
    await expect(overCap.execute({ statement: "SELECT 1" })).rejects.toThrow("result chunk limit");

    const cycleResponses = [
      json({ statement_id: "stmt-1", status: { state: "SUCCEEDED" }, manifest: { schema: { columns: [{ name: "id" }] } }, result: { data_array: [["a"]], next_chunk_internal_link: "/api/2.0/sql/statements/stmt-1/result/chunks/1" } }),
      json({ data_array: [["b"]], next_chunk_internal_link: "/api/2.0/sql/statements/stmt-1/result/chunks/1" }),
    ];
    const cycle = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", fetch: () => Promise.resolve(nextResponse(cycleResponses)) });
    await expect(cycle.execute({ statement: "SELECT 1" })).rejects.toThrow("cycle detected");
  });

  it("honors cancellation and an operation deadline even when fetch ignores abort", async () => {
    const aborter = new AbortController();
    aborter.abort();
    const aborted = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", signal: aborter.signal, fetch: () => Promise.resolve(json({})) });
    await expect(aborted.execute({ statement: "SELECT 1" })).rejects.toThrow("operation aborted");

    const timeout = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", timeoutMs: 1, fetch: () => new Promise<Response>(() => undefined) });
    await expect(timeout.execute({ statement: "SELECT 1" })).rejects.toThrow("operation deadline exceeded");
  });
});
