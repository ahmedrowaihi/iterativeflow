import { type Backend, type IdGen, createLocalWakeup, newId } from "@iterativeflow/core/backend";
import { createMysqlQueue } from "#queue";
import { type Tables, tables } from "#schema";
import type { Sql } from "#sql";
import { createMysqlStore } from "#store";
import { createMysqlTimer } from "#timer";

export interface MysqlBackendOpts {
  /** Table-name prefix, for running multiple engines in one database. Default none. */
  prefix?: string;
  /** Id generator for runs, signals, and lease tokens. Defaults to {@link newId} (RFC-4122 v4). */
  id?: IdGen;
}

/**
 * The MySQL {@link Backend}: the four ports over one {@link Sql} connection (an InnoDB database).
 * Store/Queue/Timer share the database, so an outbox commits in one transaction; `claim` uses
 * `FOR UPDATE SKIP LOCKED`. Wakeup is in-process. Run {@link applySchema} once before use.
 */
export const createMysqlBackend = (sql: Sql, opts: MysqlBackendOpts = {}): Backend => {
  const t: Tables = tables(opts.prefix ?? "");
  const id = opts.id ?? newId;
  return {
    store: createMysqlStore(sql, t, id),
    queue: createMysqlQueue(sql, t, id),
    timer: createMysqlTimer(sql, t),
    wakeup: createLocalWakeup(),
  };
};
