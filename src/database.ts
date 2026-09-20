import { Key, UnsupportedError } from "@dal-go/dalgo";
import type { Codec, Database, ExistingRecord, QueryPage, ReadwriteTransaction, RecordSnapshot, StructuredQuery } from "@dal-go/dalgo";
import { DatabricksSqlError } from "./errors.js";
import { compileDatabricksQuery, keyTable } from "./query.js";
import { DatabricksStatementClient, type DatabricksSqlClientOptions, type StatementParameter } from "./statement-client.js";

export interface DatabricksDatabaseOptions extends DatabricksSqlClientOptions {
  readonly idColumn?: string;
}

function columnName(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(value)) throw new TypeError("idColumn must be a simple SQL identifier");
  return `\`${value}\``;
}

function rowObject(columns: readonly string[], row: readonly unknown[]): Record<string, unknown> {
  if (row.length !== columns.length) throw new DatabricksSqlError("Databricks result row did not match its manifest schema");
  return Object.fromEntries(columns.map((column, index) => [column, row[index]]));
}

export class DatabricksDatabase implements Database {
  readonly #client: DatabricksStatementClient;
  readonly #catalog: string | undefined;
  readonly #schema: string | undefined;
  readonly #idColumn: string;
  readonly #quotedIdColumn: string;

  public constructor(options: DatabricksDatabaseOptions) {
    this.#client = new DatabricksStatementClient(options);
    this.#catalog = options.catalog;
    this.#schema = options.schema;
    this.#idColumn = options.idColumn ?? "id";
    this.#quotedIdColumn = columnName(this.#idColumn);
  }

  public async get<T>(key: Key, codec?: Codec<T>): Promise<RecordSnapshot<T>> {
    if (typeof key.id === "number" && !Number.isSafeInteger(key.id)) {
      throw new UnsupportedError("non-safe numeric DALgo keys");
    }
    const statement = `SELECT * FROM ${keyTable(key, this.#catalog, this.#schema)} WHERE ${this.#quotedIdColumn} = :p0 LIMIT 2`;
    const parameters: readonly StatementParameter[] = [{
      name: "p0", value: String(key.id), type: typeof key.id === "number" ? "BIGINT" : "STRING",
    }];
    const result = await this.#client.execute({ statement, parameters });
    if (result.rows.length === 0) return { key, exists: false };
    if (result.rows.length > 1) throw new DatabricksSqlError(`DALgo key ${key.path} matched multiple Databricks rows`);
    const first = result.rows[0];
    if (first === undefined) throw new DatabricksSqlError("Databricks returned an empty result after reporting a row");
    const data = rowObject(result.columns, first);
    return { key, exists: true, data: codec === undefined ? data as T : codec.decode(data) };
  }

  public async getMany<T>(keys: readonly Key[], codec?: Codec<T>): Promise<readonly RecordSnapshot<T>[]> {
    return Promise.all(keys.map(async (key) => this.get(key, codec)));
  }

  public async query<T>(query: StructuredQuery<T>): Promise<QueryPage<T>> {
    const compiled = compileDatabricksQuery(query, this.#catalog, this.#schema);
    const result = await this.#client.execute(compiled);
    const records = result.rows.map((row): ExistingRecord<T> => {
      const data = rowObject(result.columns, row);
      const id = data[this.#idColumn];
      if (typeof id !== "string" && (typeof id !== "number" || !Number.isSafeInteger(id))) {
        throw new DatabricksSqlError(`query result did not include a string or numeric ${this.#idColumn} column`);
      }
      return {
        key: new Key(query.source.name, id),
        exists: true,
        data: query.source.codec === undefined ? data as T : query.source.codec.decode(data),
      };
    });
    return { records };
  }

  public runReadwriteTransaction<Result>(
    _callback: (transaction: ReadwriteTransaction) => Promise<Result>,
  ): Promise<Result> {
    return Promise.reject(new UnsupportedError("Databricks SQL Statement Execution read-write transactions"));
  }
}
