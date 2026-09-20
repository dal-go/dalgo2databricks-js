import { collection, key } from "@dal-go/dalgo";
import { describe, expect, it } from "vitest";
import { DatabricksDatabase } from "../src/index.js";

function json(value: unknown): Response { return new Response(JSON.stringify(value), { status: 200 }); }

function nextResponse(responses: Response[]): Response {
  const response = responses.shift();
  if (response === undefined) throw new Error("test response queue exhausted");
  return response;
}

describe("DatabricksDatabase", () => {
  it("maps point reads and query rows to DALgo records", async () => {
    const responses = [
      json({ statement_id: "one", status: { state: "SUCCEEDED" }, manifest: { schema: { columns: [{ name: "id" }, { name: "done" }] } }, result: { data_array: [["a", true]] } }),
      json({ statement_id: "two", status: { state: "SUCCEEDED" }, manifest: { schema: { columns: [{ name: "id" }, { name: "done" }] } }, result: { data_array: [["a", true]] } }),
    ];
    const database = new DatabricksDatabase({ host: "https://dbc.example", warehouseId: "wh", tokenProvider: () => "token", fetch: () => Promise.resolve(nextResponse(responses)) });
    await expect(database.get(key("items", "a"))).resolves.toMatchObject({ exists: true, key: { collection: "items", id: "a" }, data: { id: "a", done: true } });
    await expect(database.query(collection<{ readonly id: string; readonly done: boolean }>("items").query().build()))
      .resolves.toMatchObject({ records: [{ exists: true, key: { collection: "items", id: "a" }, data: { id: "a", done: true } }] });
  });
});
