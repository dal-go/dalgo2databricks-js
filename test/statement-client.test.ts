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
      json({ statement_id: "stmt-1", status: { state: "SUCCEEDED" }, result: { data_array: [["b"]] } }),
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
});
