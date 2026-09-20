import { describe, expect, it } from "vitest";
import { DatabricksSqlError, DatabricksStatementClient } from "../src/index.js";

const statementId = "018f8e37-7a47-4e6d-9c2a-91fd1a13cd88";

function json(value: unknown): Response {
  return new Response(JSON.stringify(value), { status: 200, headers: { "Content-Type": "application/json" } });
}

function nextResponse(responses: Response[]): Response {
  const response = responses.shift();
  if (response === undefined) throw new Error("test response queue exhausted");
  return response;
}

function manifest(columns: readonly string[], chunks: readonly { readonly chunk_index: number; readonly row_offset: number; readonly row_count: number }[]): object {
  return {
    format: "JSON_ARRAY",
    truncated: false,
    total_chunk_count: chunks.length,
    total_row_count: chunks.reduce((total, chunk) => total + chunk.row_count, 0),
    schema: { columns: columns.map((name) => ({ name })) },
    chunks,
  };
}

function result(chunkIndex: number, rowOffset: number, data: readonly (readonly unknown[])[], next?: string): object {
  return {
    chunk_index: chunkIndex,
    row_offset: rowOffset,
    row_count: data.length,
    data_array: data,
    ...(next === undefined ? {} : { next_chunk_internal_link: next }),
  };
}

function complete(columns: readonly string[], rows: readonly (readonly unknown[])[]): object {
  return {
    statement_id: statementId,
    status: { state: "SUCCEEDED" },
    manifest: manifest(columns, [{ chunk_index: 0, row_offset: 0, row_count: rows.length }]),
    result: result(0, 0, rows),
  };
}

describe("DatabricksStatementClient", () => {
  it("sends tokens only in Authorization, polls, and follows verified complete inline chunks", async () => {
    const requests: Request[] = [];
    const chunkPath = `/api/2.0/sql/statements/${statementId}/result/chunks/1?row_offset=1`;
    const responses = [
      json({ statement_id: statementId, status: { state: "PENDING" } }),
      json({
        statement_id: statementId,
        status: { state: "SUCCEEDED" },
        manifest: manifest(["id"], [{ chunk_index: 0, row_offset: 0, row_count: 1 }, { chunk_index: 1, row_offset: 1, row_count: 1 }]),
        result: result(0, 0, [["a"]], chunkPath),
      }),
      json(result(1, 1, [["b"]])),
    ];
    const fetch = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      requests.push(new Request(input, init));
      return Promise.resolve(nextResponse(responses));
    };
    const client = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "secret", fetch, pollIntervalMs: 0 });

    await expect(client.execute({ statement: "SELECT :p0", parameters: [{ name: "p0", value: "x", type: "STRING" }] }))
      .resolves.toEqual({ columns: ["id"], rows: [["a"], ["b"]], statementId });
    expect(requests).toHaveLength(3);
    expect(requests[0]?.headers.get("Authorization")).toBe("Bearer secret");
    expect(requests[0]?.headers.get("Content-Type")).toBe("application/json");
    expect(requests[0]?.redirect).toBe("error");
    expect(requests[2]?.url).toBe(`https://dbc.example${chunkPath}`);
    const firstRequest = requests.at(0);
    if (firstRequest === undefined) throw new Error("expected first request");
    await expect(firstRequest.json()).resolves.toMatchObject({ disposition: "INLINE", format: "JSON_ARRAY", wait_timeout: "5s" });
  });

  it("refuses external links at every response level and unsafe internal links", async () => {
    const topLevelExternal = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "secret",
      fetch: () => Promise.resolve(json({ ...complete(["id"], [["a"]]), external_links: [{ external_link: "https://signed.example/result" }] })),
    });
    await expect(topLevelExternal.execute({ statement: "SELECT 1" })).rejects.toThrow("EXTERNAL_LINKS");

    const unsafeLink = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "secret",
      fetch: () => Promise.resolve(json({
        statement_id: statementId,
        status: { state: "SUCCEEDED" },
        manifest: manifest(["id"], [{ chunk_index: 0, row_offset: 0, row_count: 1 }, { chunk_index: 1, row_offset: 1, row_count: 1 }]),
        result: result(0, 0, [["a"]], "https://attacker.example/chunk"),
      })),
    });
    await expect(unsafeLink.execute({ statement: "SELECT 1" })).rejects.toThrow("unsafe result chunk link");
  });

  it("never reflects externally supplied errors or untrusted statement handles", async () => {
    const secret = "secret-token";
    const fetchFailure = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token",
      fetch: () => Promise.reject(new DatabricksSqlError(secret)),
    });
    await expect(fetchFailure.execute({ statement: "SELECT 1" })).rejects.toThrow("Databricks HTTP request failed");
    await expect(fetchFailure.execute({ statement: "SELECT 1" })).rejects.not.toThrow(secret);

    const aborter = new AbortController();
    const injectedAbort = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", signal: aborter.signal,
      fetch: () => { aborter.abort(); throw new DatabricksSqlError(secret); },
    });
    await expect(injectedAbort.execute({ statement: "SELECT 1" })).rejects.toThrow("Databricks operation aborted");
    await expect(injectedAbort.execute({ statement: "SELECT 1" })).rejects.not.toThrow(secret);

    const malformedHandle = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token",
      fetch: () => Promise.resolve(json({ statement_id: secret, status: { state: "FAILED" } })),
    });
    await expect(malformedHandle.execute({ statement: "SELECT 1" })).rejects.toThrow("valid statement_id");
    await expect(malformedHandle.execute({ statement: "SELECT 1" })).rejects.not.toThrow(secret);

    const providerFailure = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => { throw new DatabricksSqlError(secret); }, fetch: () => Promise.resolve(json({})),
    });
    await expect(providerFailure.execute({ statement: "SELECT 1" })).rejects.toThrow("Databricks token provider failed");
  });

  it("requires complete coherent JSON_ARRAY manifests and complete chunks", async () => {
    const missingData = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token",
      fetch: () => Promise.resolve(json({ ...complete(["id"], [["a"]]), result: { chunk_index: 0, row_offset: 0, row_count: 1 } })),
    });
    await expect(missingData.execute({ statement: "SELECT 1" })).rejects.toThrow("omitted JSON_ARRAY data");

    const duplicateColumns = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token",
      fetch: () => Promise.resolve(json(complete(["id", "id"], [["a", "a"]]))),
    });
    await expect(duplicateColumns.execute({ statement: "SELECT 1" })).rejects.toThrow("duplicate column names");

    const incoherentCounts = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token",
      fetch: () => Promise.resolve(json({
        statement_id: statementId, status: { state: "SUCCEEDED" },
        manifest: { ...manifest(["id"], [{ chunk_index: 0, row_offset: 0, row_count: 1 }]), total_row_count: 2 },
        result: result(0, 0, [["a"]]),
      })),
    });
    await expect(incoherentCounts.execute({ statement: "SELECT 1" })).rejects.toThrow("row counts were incoherent");

    const outOfSequence = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token",
      fetch: () => Promise.resolve(json({
        statement_id: statementId, status: { state: "SUCCEEDED" },
        manifest: manifest(["id"], [{ chunk_index: 1, row_offset: 0, row_count: 1 }]), result: result(0, 0, [["a"]]),
      })),
    });
    await expect(outOfSequence.execute({ statement: "SELECT 1" })).rejects.toThrow("out of sequence");

    const nonStringCell = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token",
      fetch: () => Promise.resolve(json(complete(["id"], [[{ unexpected: true }]]))),
    });
    await expect(nonStringCell.execute({ statement: "SELECT 1" })).rejects.toThrow("non-string JSON_ARRAY cell");
  });

  it("bounds token acquisition, fetch, and response bodies with one operation deadline", async () => {
    const pendingToken = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => new Promise<string>(() => undefined), timeoutMs: 1 });
    await expect(pendingToken.execute({ statement: "SELECT 1" })).rejects.toThrow("operation deadline exceeded");

    const pendingFetch = new DatabricksStatementClient({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", timeoutMs: 1, fetch: () => new Promise<Response>(() => undefined) });
    await expect(pendingFetch.execute({ statement: "SELECT 1" })).rejects.toThrow("operation deadline exceeded");

    const pendingBody = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", timeoutMs: 1,
      fetch: () => Promise.resolve({ ok: true, status: 200, json: () => new Promise<unknown>(() => undefined) } as Response),
    });
    await expect(pendingBody.execute({ statement: "SELECT 1" })).rejects.toThrow("operation deadline exceeded");
  });

  it("accepts a valid inline JSON_ARRAY chunk with more than 150 thousand rows", async () => {
    const rows = Array.from({ length: 150_000 }, (_, index): readonly string[] => [String(index)]);
    const client = new DatabricksStatementClient({
      host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", fetch: () => Promise.resolve(json(complete(["id"], rows))),
    });
    await expect(client.execute({ statement: "SELECT id FROM items" })).resolves.toMatchObject({ statementId, columns: ["id"] });
  });

  it("rejects unsafe timer values before making a request", () => {
    const options = { host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token" };
    for (const value of [-1, 1.5, Number.POSITIVE_INFINITY, 2_147_483_648]) {
      expect(() => new DatabricksStatementClient({ ...options, pollIntervalMs: value })).toThrow("pollIntervalMs");
      expect(() => new DatabricksStatementClient({ ...options, timeoutMs: value })).toThrow("timeoutMs");
    }
  });
});
