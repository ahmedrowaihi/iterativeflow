import { type Backend } from "@iterativeflow/core/backend";
import {
  type SqliteBackendOpts,
  applySchema as applySqliteSchema,
  createSqliteBackend,
} from "@iterativeflow/sqlite";
import { type SqlStorage, doStorageSql } from "#storage";

export { doStorageSql } from "#storage";
export type { SqlStorage } from "#storage";
export { ddl } from "@iterativeflow/sqlite";

/**
 * A {@link Backend} backed by a Durable Object's SQLite storage — the full `@iterativeflow/sqlite`
 * backend over `ctx.storage.sql`. Call {@link applySchema} once (e.g. in the DO constructor or a
 * migration), then drive the engine inside the DO.
 */
export const createDurableObjectBackend = (
  storage: SqlStorage,
  opts?: SqliteBackendOpts,
): Backend => createSqliteBackend(doStorageSql(storage), opts);

/** Apply the schema to a Durable Object's SQLite storage. Run once before use. DO storage manages its
 *  own durability, so the file-store PRAGMAs are skipped (`PRAGMA journal_mode` is unsupported there). */
export const applySchema = (storage: SqlStorage, prefix = ""): Promise<void> =>
  applySqliteSchema(doStorageSql(storage), prefix, { pragmas: false });
