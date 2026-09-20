# @dal-go/dalgo2databricks

`@dal-go/dalgo2databricks` adapts the [DALgo TypeScript](https://github.com/dal-go/dalgo-js) read and structured-query contracts to the [Databricks SQL Statement Execution API](https://docs.databricks.com/api/statement-execution/v1/statement-execution).

## Status and runtime classification

This is **HTTP-capable, not browser-ready**. It uses standard `fetch`, but callers must provide a token provider and Databricks workspace CORS/authentication policy is not treated as a browser-client contract. Keep token acquisition server-side or in a purpose-built trusted runtime.

The adapter sends an OAuth or PAT value only as an `Authorization: Bearer` header to the configured HTTPS workspace origin. It rejects redirects, never places secrets in URLs, uses `JSON_ARRAY` + `INLINE` results, and refuses `EXTERNAL_LINKS` rather than accidentally dereferencing a presigned URL. It validates internal result-chunk links, manifest counts, row widths, truncation, and completion before returning data. Server, token-provider, and fetch error text is deliberately not reflected in thrown errors.

## Install

This package is not yet published to npm. Consume a pinned Git revision until a release is published. pnpm 11 requires allowlisting build scripts for both the adapter Git dependency and its pinned DALgo Git development dependency. For the currently reviewed source revision, use:

```yaml
# pnpm-workspace.yaml
allowBuilds:
  "@dal-go/dalgo2databricks@https://codeload.github.com/dal-go/dalgo2databricks-js/tar.gz/REPLACE_WITH_EXACT_40_CHARACTER_ADAPTER_COMMIT_SHA": true
  "@dal-go/dalgo@https://codeload.github.com/dal-go/dalgo-js/tar.gz/04ce7f644fc334da7e471f0be503a7b937c7025d": true
```

Replace `REPLACE_WITH_EXACT_40_CHARACTER_ADAPTER_COMMIT_SHA` with the full 40-character revision shown in the adapter dependency's lockfile.

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
await db.query(collection<{ readonly id: string; readonly done: string | null }>("items")
  .query().where("done", "==", false).orderBy("id").limit(20).build());
```

Collection names map to single Databricks table identifiers. A schema may be supplied against the current catalog; supplying a catalog requires a schema. Top-level DALgo key ids map to an `id` column by default (configure `idColumn` when necessary). Identifiers are restricted to simple ASCII SQL identifiers and values travel as Statement Execution parameters, never interpolated into SQL. Numeric key ids and query values must be safe integers.

## Supported DALgo surface

- `get` and `getMany` for top-level keys.
- Collection structured queries with string, Boolean, and safe-integer filters; null equality; ordering, limits, and offsets.
- Async statement polling and bounded inline JSON result chunks. `timeoutMs`, `signal`, `maxPolls`, and `maxResultChunks` bound execution; aborting the signal cancels this client operation but does not cancel the server-side statement.

## Intentional limitations

Databricks SQL tables and DALgo document collections do not have identical semantics. The adapter explicitly rejects nested keys/collections, collection-group queries, document-id filters or orders, cursor pagination, array and set-membership query operators, non-integer numbers, unsupported parameter values, and `runReadwriteTransaction`. The transaction rejection is asynchronous so it matches the DALgo promise contract. There is no direct DALgo transaction or write implementation because the Statement Execution API cannot preserve DALgo's read-write callback transaction semantics. A point read also fails if its configured id column is not unique.

Databricks `JSON_ARRAY` values are passed through as returned. In particular, result fixtures use strings (and may contain null) rather than claiming conversion to TypeScript booleans or numbers; provide a DALgo codec when application types need conversion.

The adapter does not attempt to fetch `EXTERNAL_LINKS`; those URLs contain temporary access credentials and are deliberately outside this package's result contract. For very large result sets, use a dedicated export flow with its own storage and credential policy.

## Development

```sh
pnpm install
pnpm check
```

The DALgo development contract is pinned to Git commit `04ce7f644fc334da7e471f0be503a7b937c7025d`. The pnpm 11.20.0 workspace explicitly allowlists that pinned Git dependency's build script; review that entry when changing the pin. The package uses TypeScript 6.
