import { DOCUMENT_ID, UnsupportedError } from "@dal-go/dalgo";
import type { Key, QueryCursor, QueryFilter, StructuredQuery } from "@dal-go/dalgo";
import type { StatementParameter } from "./statement-client.js";

export interface CompiledQuery {
  readonly statement: string;
  readonly parameters: readonly StatementParameter[];
}

function identifier(value: string, label: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(value)) throw new TypeError(`${label} must be a simple SQL identifier`);
  return `\`${value}\``;
}

function safeInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || typeof value !== "number" || value < 0) {
    throw new TypeError(`${label} must be a non-negative safe integer`);
  }
  return value;
}

export function tableIdentifier(collection: string, catalog?: string, schema?: string): string {
  if (catalog !== undefined && schema === undefined) {
    throw new TypeError("catalog requires schema so the table path is unambiguous");
  }
  return [catalog, schema, collection]
    .filter((part): part is string => part !== undefined)
    .map((part) => identifier(part, "table path segment"))
    .join(".");
}

export function keyTable(key: Key, catalog?: string, schema?: string): string {
  if (key.parent !== undefined) throw new UnsupportedError("nested DALgo keys on relational tables");
  return tableIdentifier(key.collection, catalog, schema);
}

function parameter(value: unknown, number: number): StatementParameter {
  const name = `p${String(number)}`;
  if (typeof value === "string") return { name, value, type: "STRING" };
  if (typeof value === "boolean") return { name, value: String(value), type: "BOOLEAN" };
  if (typeof value === "number" && Number.isSafeInteger(value)) {
    return { name, value: String(value), type: "BIGINT" };
  }
  throw new UnsupportedError("Databricks SQL parameter value type");
}

function compileFilter<T>(filter: QueryFilter<T>, parameters: StatementParameter[]): string {
  if (filter.field === DOCUMENT_ID) throw new UnsupportedError("DALgo document-id filters on relational tables");
  const field = identifier(String(filter.field), "query field");
  const operators: Readonly<Record<string, string>> = {
    "==": "=", "!=": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">=",
  };
  if (!Object.hasOwn(operators, filter.operator)) throw new UnsupportedError(`Databricks SQL query operator ${filter.operator}`);
  const operator = operators[filter.operator];
  if (operator === undefined) throw new UnsupportedError(`Databricks SQL query operator ${filter.operator}`);
  if (filter.value === null) {
    if (filter.operator === "==") return `${field} IS NULL`;
    if (filter.operator === "!=") return `${field} IS NOT NULL`;
    throw new UnsupportedError("null comparisons other than equality");
  }
  const item = parameter(filter.value, parameters.length);
  parameters.push(item);
  return `${field} ${operator} :${item.name}`;
}

function compileCursor(cursor: QueryCursor | undefined): void {
  if (cursor !== undefined) throw new UnsupportedError("DALgo structured query cursors");
}

export function compileDatabricksQuery<T>(
  query: StructuredQuery<T>,
  catalog?: string,
  schema?: string,
): CompiledQuery {
  if (query.source.kind !== "collection") throw new UnsupportedError("DALgo collection-group queries on relational tables");
  if (query.source.parent !== undefined) throw new UnsupportedError("nested DALgo collections on relational tables");
  compileCursor(query.startAt); compileCursor(query.startAfter); compileCursor(query.endAt); compileCursor(query.endBefore);
  const parameters: StatementParameter[] = [];
  const where = query.filters.map((filter) => compileFilter(filter, parameters));
  const orders = query.orders.map((order) => {
    if (order.field === DOCUMENT_ID) throw new UnsupportedError("DALgo document-id ordering on relational tables");
    const direction: unknown = order.direction;
    if (direction !== "asc" && direction !== "desc") {
      throw new TypeError("order direction must be asc or desc");
    }
    return `${identifier(String(order.field), "order field")} ${direction.toUpperCase()}`;
  });
  const limit = query.limit === undefined ? "" : ` LIMIT ${String(safeInteger(query.limit, "limit"))}`;
  const offset = query.offset === undefined || query.offset === 0 ? "" : ` OFFSET ${String(safeInteger(query.offset, "offset"))}`;
  return {
    statement: `SELECT * FROM ${tableIdentifier(query.source.name, catalog, schema)}`
      + (where.length === 0 ? "" : ` WHERE ${where.join(" AND ")}`)
      + `${orders.length === 0 ? "" : ` ORDER BY ${orders.join(", ")}`}${limit}${offset}`,
    parameters,
  };
}
