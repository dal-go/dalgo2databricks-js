import { UnsupportedError, collection, key } from "@dal-go/dalgo";
import { describe, expect, it } from "vitest";
import { compileDatabricksQuery, keyTable } from "../src/index.js";

interface Item { readonly id: string; readonly done: boolean; readonly rank: number; }

describe("compileDatabricksQuery", () => {
  it("quotes identifiers and binds values instead of interpolating them", () => {
    const query = collection<Item>("items").query().where("done", "==", false).orderBy("rank", "desc").limit(10).offset(2).build();
    expect(compileDatabricksQuery(query, "main", "app")).toEqual({
      statement: "SELECT * FROM `main`.`app`.`items` WHERE `done` = :p0 ORDER BY `rank` DESC LIMIT 10 OFFSET 2",
      parameters: [{ name: "p0", value: "false", type: "BOOLEAN" }],
    });
  });

  it("rejects DALgo semantics without a safe relational equivalent", () => {
    expect(() => compileDatabricksQuery(collection<Item>("items").query().where("id", "in", ["a"]).build())).toThrow(UnsupportedError);
    expect(() => compileDatabricksQuery(collection<Item>("items").query().startAfter("a").build())).toThrow(UnsupportedError);
    expect(() => keyTable(key("items", "i", key("spaces", "s")))).toThrow(UnsupportedError);
  });

  it("validates sort direction at runtime before it is placed in SQL", () => {
    const query = collection<Item>("items").query().orderBy("id").build();
    const unsafe = { ...query, orders: [{ field: "id", direction: "desc; DROP TABLE items" }] } as never;
    expect(() => compileDatabricksQuery(unsafe)).toThrow("order direction must be asc or desc");
  });

  it("maps null equality honestly and rejects ambiguous table and number semantics", () => {
    const nullQuery = collection<Item>("items").query().where("done", "==", null).build();
    expect(compileDatabricksQuery(nullQuery).statement).toContain("`done` IS NULL");
    expect(() => compileDatabricksQuery(collection<Item>("items").query().where("rank", ">", null).build())).toThrow(UnsupportedError);
    expect(() => compileDatabricksQuery(collection<Item>("items").query().where("rank", "==", 1.5).build())).toThrow(UnsupportedError);
    expect(() => compileDatabricksQuery(collection<Item>("items").query().build(), "main")).toThrow("catalog requires schema");
    const inheritedOperator = { ...collection<Item>("items").query().build(), filters: [{ field: "id", operator: "toString", value: "x" }] } as never;
    expect(() => compileDatabricksQuery(inheritedOperator)).toThrow(UnsupportedError);
  });
});
