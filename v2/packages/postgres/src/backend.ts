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
   * Opt-in `LISTEN/NOTIFY` push from {@link createPgListener}. Wires BOTH seams off one object: its
   * `wakeup` wakes `result()` waiters on completion, its `waitForWork` wakes the worker loop on
   * enqueue. Omit for the poll-first default (a process-local {@link createLocalWakeup}, no push).
   */
  listener?: { wakeup: Wakeup; waitForWork(timeoutMs: number): Promise<void> };
}

/**
 * The Postgres {@link Backend}: the four ports over one connection source. Store, Queue, and
 * Timer share the same database, so an outbox commits as one `BEGIN…COMMIT` — the single
 * transactional domain the seam requires. Pass a pool via {@link pgPool} (or any {@link Sql}).
 */
export const createPgBackend = (sql: Sql, opts: PgBackendOpts = {}): Backend => {
  const schema = opts.schema ?? "workflow";
  const id = opts.id ?? newId;
  const queue = createPgQueue(sql, schema, id);
  return {
    store: createPgStore(sql, schema, id),
    queue: opts.listener ? { ...queue, waitForWork: opts.listener.waitForWork } : queue,
    timer: createPgTimer(sql, schema),
    wakeup: opts.listener?.wakeup ?? createLocalWakeup(),
  };
};
