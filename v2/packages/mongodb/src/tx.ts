import type { Backend } from "@iterativeflow/core/backend";
import type { ClientSession, MongoClient } from "mongodb";
import { type MongoBackendOpts, createMongoBackend } from "#backend";

/**
 * Run `fn` inside one MongoDB transaction, handing it a {@link Backend} bound to that transaction
 * plus the raw {@link ClientSession} for the caller's own writes. `submit` / `startRun` / `enqueue`
 * on that backend commit ATOMICALLY with the caller's writes in `session` — the transactional-enqueue
 * guarantee: business work and workflow dispatch land together or not at all. A throw rolls back both,
 * so a failed request never leaves an orphan run or a dangling job. Requires a replica set (MongoDB
 * transactions do).
 *
 * Only the write path joins the transaction: reads via the bound backend (e.g. `store.loadRun`) do
 * NOT observe the transaction's own uncommitted writes — unlike a SQL read-your-own-write.
 *
 * @example
 * await inTx(client, async (backend, session) => {
 *   await client.db().collection("orders").insertOne({ _id: orderId }, { session });
 *   await submit(backend, fulfilOrder, { orderId }); // enqueued iff the order commits
 * });
 */
export const inTx = async <T>(
  client: MongoClient,
  fn: (backend: Backend, session: ClientSession) => Promise<T>,
  opts?: MongoBackendOpts,
): Promise<T> => {
  const session = client.startSession();
  try {
    // Built once outside withTransaction — its callback re-runs on transient retries; the session is stable.
    const backend = createMongoBackend(client, opts, session);
    return await session.withTransaction(() => fn(backend, session));
  } finally {
    await session.endSession();
  }
};
