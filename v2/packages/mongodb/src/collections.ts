import type { Db } from "mongodb";

/** @internal */
export const names = (prefix: string) => ({
  runs: `${prefix}runs`,
  steps: `${prefix}steps`,
  signals: `${prefix}signals`,
  jobs: `${prefix}jobs`,
  timers: `${prefix}timers`,
  crons: `${prefix}crons`,
});

export type Names = ReturnType<typeof names>;

/**
 * Create the indexes the backend relies on. Idempotent (`createIndex` is a no-op if present). Run
 * once before use. `ord` is a per-document ObjectId giving a total insertion order (Mongo has no
 * auto-increment); the idempotency and signal-dedup indexes are `sparse` so unkeyed docs never
 * collide. `steps` use a composite string `_id` (`runId:cursorKey`) so a duplicate insert is a clean
 * first-writer-wins conflict.
 */
export const ensureIndexes = async (db: Db, prefix = ""): Promise<void> => {
  const n = names(prefix);
  await db.collection(n.runs).createIndexes([
    {
      key: { name: 1, version: 1, idempotency_key: 1 },
      unique: true,
      partialFilterExpression: { idempotency_key: { $exists: true } },
    },
    { key: { parent_run_id: 1 } },
    { key: { status: 1 } },
    { key: { created_at: 1 } },
    { key: { ord: 1 } },
  ]);
  await db.collection(n.signals).createIndexes([
    { key: { run_id: 1, ord: 1 } },
    {
      key: { run_id: 1, idem_key: 1 },
      unique: true,
      partialFilterExpression: { idem_key: { $exists: true } },
    },
  ]);
  await db.collection(n.jobs).createIndex({ priority: 1, run_at: 1 });
  await db.collection(n.timers).createIndex({ fire_at: 1 });
  await db.collection(n.crons).createIndex({ next_run_at: 1 });
};
