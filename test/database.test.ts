import { collection, key } from "@dal-go/dalgo";
import { describe, expect, it } from "vitest";
import { DatabricksDatabase } from "../src/index.js";

function json(value: unknown): Response { return new Response(JSON.stringify(value), { status: 200 }); }

const statementId = "018f8e37-7a47-4e6d-9c2a-91fd1a13cd88";

function complete(columns: readonly string[], rows: readonly (readonly unknown[])[]): object {
  return {
    statement_id: statementId,
    status: { state: "SUCCEEDED" },
    manifest: {
      format: "JSON_ARRAY",
      truncated: false,
      total_chunk_count: 1,
      total_row_count: rows.length,
      schema: { columns: columns.map((name) => ({ name })) },
      chunks: [{ chunk_index: 0, row_offset: 0, row_count: rows.length }],
    },
    result: { chunk_index: 0, row_offset: 0, row_count: rows.length, data_array: rows },
  };
}

function nextResponse(responses: Response[]): Response {
  const response = responses.shift();
  if (response === undefined) throw new Error("test response queue exhausted");
  return response;
}

describe("DatabricksDatabase", () => {
  it("maps point reads and query rows to DALgo records", async () => {
    const responses = [
      json(complete(["id", "done"], [["a", "true"]])),
      json(complete(["id", "done"], [["a", "true"]])),
    ];
    const database = new DatabricksDatabase({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", fetch: () => Promise.resolve(nextResponse(responses)) });
    await expect(database.get(key("items", "a"))).resolves.toMatchObject({ exists: true, key: { collection: "items", id: "a" }, data: { id: "a", done: "true" } });
    await expect(database.query(collection<{ readonly id: string; readonly done: boolean }>("items").query().build()))
      .resolves.toMatchObject({ records: [{ exists: true, key: { collection: "items", id: "a" }, data: { id: "a", done: "true" } }] });
  });

  it("rejects non-safe numeric keys and transaction callbacks asynchronously", async () => {
    const database = new DatabricksDatabase({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", fetch: () => Promise.resolve(json({})) });
    await expect(database.get(key("items", Number.MAX_SAFE_INTEGER + 1))).rejects.toThrow("non-safe numeric DALgo keys");
    const transaction = database.runReadwriteTransaction(() => Promise.resolve("never"));
    await expect(transaction).rejects.toThrow("read-write transactions");
  });
});
