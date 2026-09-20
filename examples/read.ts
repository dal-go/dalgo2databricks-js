import { collection, key } from "@dal-go/dalgo";
import { DatabricksDatabase } from "../src/index.js";

const database = new DatabricksDatabase({
  host: "https://your-workspace.cloud.databricks.com",
  warehouseId: "warehouse-id",
  // Obtain the token from a trusted runtime; never place it in a URL or source file.
  tokenProvider: () => "replace-with-a-token-from-a-trusted-runtime",
  catalog: "main",
  schema: "app",
});

const item = await database.get(key("items", "item-1"));
const openItems = await database.query(collection<{ readonly id: string; readonly done: boolean }>("items")
  .query().where("done", "==", false).limit(20).build());

console.log(item, openItems.records.length);
