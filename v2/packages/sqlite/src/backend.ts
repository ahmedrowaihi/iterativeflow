import { type Backend, type IdGen, createLocalWakeup, newId } from "@iterativeflow/core/backend";
import { createSqliteQueue } from "#queue";
import { type Tables, tables } from "#schema";
import type { Sql } from "#sql";
import { createSqliteStore } from "#store";
import { createSqliteTimer } from "#timer";

export interface SqliteBackendOpts {
  /** Table-name prefix, for running multiple engines in one database. Default none. */
  prefix?: string;
  /** Id generator for runs, signals, and lease tokens. Defaults to {@link newId} (RFC-4122 v4). */
  id?: IdGen;
}

/**
 * The SQLite {@link Backend}: the four ports over one {@link Sql} connection (a local file, Turso, or
 * a Durable Object's SQLite storage via {@link libsqlDb} or any adapter). Store/Queue/Timer share the
 * database, so an outbox commits in one transaction — the atomic domain the seam requires. Wakeup is
 * in-process. Run {@link applySchema} once before use.
 */
export const createSqliteBackend = (sql: Sql, opts: SqliteBackendOpts = {}): Backend => {
  const t: Tables = tables(opts.prefix ?? "");
  const id = opts.id ?? newId;
  return {
    store: createSqliteStore(sql, t, id),
    queue: createSqliteQueue(sql, t, id),
    timer: createSqliteTimer(sql, t),
    wakeup: createLocalWakeup(),
  };
};
