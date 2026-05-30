import { asc, eq, sql } from "drizzle-orm";
import type { StorageSliceDeps } from "./types";
import { RESUMABLE } from "./types";

interface ReconcileOpts {
  olderThan: Date;
  runningStuckOlderThan: Date;
  batchSize?: number;
}

/**
 * Re-enqueue runs whose status looks stuck: `pending`/`sleeping`/`retrying`
 * past their fire/expiry, `awaiting_signal` with a delivered or expired
 * signal, or `running` past the stuck threshold. Locks each candidate,
 * re-checks its status, then re-enqueues via the worker's tx-enqueue.
 *
 * @internal
 */
export const reenqueueOrphans =
  ({ db, tables, enqueue, logger }: StorageSliceDeps) =>
  async ({ olderThan, runningStuckOlderThan, batchSize = 100 }: ReconcileOpts): Promise<number> => {
    const { runs, timers, signals } = tables;
    const stale = await db
      .select({ runId: runs.id })
      .from(runs)
      .where(
        sql`(
          ${runs.updatedAt} < ${olderThan} AND (
            ${runs.status} = 'pending'
            OR (${runs.status} IN ('sleeping', 'retrying') AND EXISTS (
              SELECT 1 FROM ${timers} t
              WHERE t.run_id = ${runs.id}
                AND t.fired_at IS NULL
                AND t.fire_at <= NOW()
            ))
            OR (${runs.status} = 'awaiting_signal' AND EXISTS (
              SELECT 1 FROM ${signals} s
              WHERE s.run_id = ${runs.id}
                AND (s.delivered = true OR (s.expires_at IS NOT NULL AND s.expires_at <= NOW()))
            ))
          )
        )
        OR (${runs.status} = 'running' AND ${runs.updatedAt} < ${runningStuckOlderThan})`,
      )
      .orderBy(asc(runs.updatedAt), asc(runs.id))
      .limit(batchSize);

    if (stale.length === 0) return 0;

    let reEnqueued = 0;
    for (const { runId } of stale) {
      try {
        await db.transaction(async (tx) => {
          await tx
            .select({ id: runs.id })
            .from(runs)
            .where(eq(runs.id, runId))
            .for("update")
            .limit(1);
          const cur = await tx
            .select({ status: runs.status, updatedAt: runs.updatedAt })
            .from(runs)
            .where(eq(runs.id, runId))
            .limit(1);
          if (!cur[0]) return;
          const { status, updatedAt } = cur[0];
          if (!(RESUMABLE as ReadonlyArray<string>).includes(status)) return;
          if (status === "running" && updatedAt >= runningStuckOlderThan) return;
          await enqueue(tx, runId);
        });
        reEnqueued += 1;
      } catch (err) {
        logger.error(err instanceof Error ? err : new Error(String(err)), {
          event: "flow.reenqueue_failed",
          runId,
        });
      }
    }
    logger.info("flow.reenqueueOrphans", { scanned: stale.length, reEnqueued });
    return reEnqueued;
  };
