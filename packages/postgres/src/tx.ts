import type { Backend } from "@iterativeflow/core/backend";
import type { Pool } from "pg";
import { type PgBackendOpts, createPgBackend } from "#backend";
import { type Sql, pgPool } from "#sql";

/**
 * Run `fn` inside one Postgres transaction, handing it a {@link Backend} bound to that
 * transaction plus the raw {@link Sql} for the caller's own writes. Every `submit` /
 * `startRun` / `enqueue` on that backend commits ATOMICALLY with the caller's writes on
 * `tx` — the transactional-enqueue guarantee: business work and workflow dispatch land
 * together or not at all. A throw rolls back both, so a failed request never leaves an
 * orphan run or a dangling job.
 *
 * @example
 * await inTx(pool, async (backend, tx) => {
 *   await tx.query("INSERT INTO orders(id, ...) VALUES ($1, ...)", [orderId]);
 *   await submit(backend, fulfilOrder, { orderId }); // enqueued iff the order commits
 * });
 */
export const inTx = <T>(
  pool: Pool,
  fn: (backend: Backend, tx: Sql) => Promise<T>,
  opts?: PgBackendOpts,
): Promise<T> => pgPool(pool).tx((tx) => fn(createPgBackend(tx, opts), tx));
