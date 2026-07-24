import {
  type Backend,
  type IdGen,
  type Wakeup,
  createLocalWakeup,
  newId,
} from "@iterativeflow/core/backend";
import { createPgQueue } from "#queue";
import { createPgStore } from "#store";
import type { Sql } from "#sql";
import { createPgTimer } from "#timer";

export interface PgBackendOpts {
  /** Schema the four tables live in. Default `workflow`. */
  schema?: string;
  /** Id generator for runs and lease tokens. Defaults to {@link newId} (RFC-4122 v4). */
  id?: IdGen;
  /**
   * Completion wakeup. Defaults to the process-local, connection-safe {@link createLocalWakeup}.
   * Pass `createPgListener(...).wakeup` to wake `result()` waiters across processes via `LISTEN`.
   */
  wakeup?: Wakeup;
}

/**
 * The Postgres {@link Backend}: the four ports over one connection source. Store, Queue, and
 * Timer share the same database, so an outbox commits as one `BEGIN…COMMIT` — the single
 * transactional domain the seam requires. Pass a pool via {@link pgPool} (or any {@link Sql}).
 */
export const createPgBackend = (sql: Sql, opts: PgBackendOpts = {}): Backend => {
  const schema = opts.schema ?? "workflow";
  const id = opts.id ?? newId;
  return {
    store: createPgStore(sql, schema, id),
    queue: createPgQueue(sql, schema, id),
    timer: createPgTimer(sql, schema),
    wakeup: opts.wakeup ?? createLocalWakeup(),
  };
};
