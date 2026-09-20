# @dal-go/dalgo2databricks

`@dal-go/dalgo2databricks` adapts the [DALgo TypeScript](https://github.com/dal-go/dalgo-js) read and structured-query contracts to the [Databricks SQL Statement Execution API](https://docs.databricks.com/api/statement-execution/v1/statement-execution).

## Status and runtime classification

This is **HTTP-capable, not browser-ready**. It uses standard `fetch`, but callers must provide a token provider and Databricks workspace CORS/authentication policy is not treated as a browser-client contract. Keep token acquisition server-side or in a purpose-built trusted runtime.

The adapter sends an OAuth or PAT value only as an `Authorization: Bearer` header to the configured HTTPS workspace origin. It rejects redirects, never places secrets in URLs, uses `JSON_ARRAY` + `INLINE` results, and refuses `EXTERNAL_LINKS` rather than accidentally dereferencing a presigned URL. It validates internal result-chunk links are same-workspace Statement Execution paths before fetching them.

## Install

```sh
pnpm add @dal-go/dalgo @dal-go/dalgo2databricks
```

## Use

```ts
import { collection, key } from "@dal-go/dalgo";
import { DatabricksDatabase } from "@dal-go/dalgo2databricks";

const db = new DatabricksDatabase({
  host: "https://your-workspace.cloud.databricks.com",
  warehouseId: "warehouse-id",
  tokenProvider: () => process.env.DATABRICKS_TOKEN!,
  catalog: "main",
  schema: "app",
});

await db.get(key("items", "item-1"));
await db.query(collection<{ id: string; done: boolean }>("items")
  .query().where("done", "==", false).orderBy("id").limit(20).build());
```

Collection names map to single Databricks table identifiers; `catalog` and `schema` are optional. Top-level DALgo key ids map to an `id` column by default (configure `idColumn` when necessary). Identifiers are restricted to simple ASCII SQL identifiers and values travel as Statement Execution parameters, never interpolated into SQL.

## Supported DALgo surface

- `get` and `getMany` for top-level keys.
- Collection structured queries with scalar equality/comparison filters, ordering, limits, and offsets.
- Async statement polling and inline JSON result chunks.

## Intentional limitations

Databricks SQL tables and DALgo document collections do not have identical semantics. The adapter explicitly rejects nested keys/collections, collection-group queries, document-id filters or orders, cursor pagination, array and set-membership query operators, unsupported parameter values, and `runReadwriteTransaction`. There is no direct DALgo transaction or write implementation because the Statement Execution API cannot preserve DALgo's read-write callback transaction semantics. A point read also fails if its configured id column is not unique.

The adapter does not attempt to fetch `EXTERNAL_LINKS`; those URLs contain temporary access credentials and are deliberately outside this package's result contract. For very large result sets, use a dedicated export flow with its own storage and credential policy.

## Development

```sh
pnpm install
pnpm check
```

The DALgo development contract is pinned to commit `04ce7f644fc334da7e471f0be503a7b937c7025d`. The package uses pnpm 11.20.0 and TypeScript 6.
