import type { Backend } from "@iterativeflow/core/backend";
import type { Client } from "@libsql/client";
import { type SqliteBackendOpts, createSqliteBackend } from "#backend";
import { type Sql, libsqlDb } from "#sql";

/**
 * Run `fn` inside one SQLite transaction, handing it a {@link Backend} bound to that
 * transaction plus the raw {@link Sql} for the caller's own writes. Every `submit` /
 * `startRun` / `enqueue` on that backend commits ATOMICALLY with the caller's writes on
 * `tx` — the transactional-enqueue guarantee: business work and workflow dispatch land
 * together or not at all. A throw rolls back both, so a failed request never leaves an
 * orphan run or a dangling job.
 *
 * @example
 * await inTx(client, async (backend, tx) => {
 *   await tx.query("INSERT INTO orders (id) VALUES (?)", [orderId]);
 *   await submit(backend, fulfilOrder, { orderId }); // enqueued iff the order commits
 * });
 */
export const inTx = <T>(
  client: Client,
  fn: (backend: Backend, tx: Sql) => Promise<T>,
  opts?: SqliteBackendOpts,
): Promise<T> => libsqlDb(client).tx((tx) => fn(createSqliteBackend(tx, opts), tx));
