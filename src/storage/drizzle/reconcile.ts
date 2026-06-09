import { and, asc, eq, inArray, lt, or, sql } from "drizzle-orm";
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
 * A stuck `running` run is first reset to `retrying`: it was left `running` by
 * a crashed worker, and `claimRun` rejects `running` as "lost", so without the
 * reset the re-enqueued job could never re-claim it.
 *
 * Date params are bound through drizzle's column encoders (via `lt`) so
 * postgres-js, neon-serverless, and node-postgres all encode them
 * consistently. The EXISTS subqueries stay raw — they only reference
 * server-side `NOW()` and column refs, no JS-side values to encode.
 *
 * @internal
 */
export const reenqueueOrphans =
  ({ db, tables, enqueue, logger }: StorageSliceDeps) =>
  async ({ olderThan, runningStuckOlderThan, batchSize = 100 }: ReconcileOpts): Promise<number> => {
    const { runs, timers, signals } = tables;

    const timerDue = sql`EXISTS (
      SELECT 1 FROM ${timers} t
      WHERE t.run_id = ${runs.id}
        AND t.fired_at IS NULL
        AND t.fire_at <= NOW()
    )`;
    const signalResolved = sql`EXISTS (
      SELECT 1 FROM ${signals} s
      WHERE s.run_id = ${runs.id}
        AND (s.delivered = true OR (s.expires_at IS NOT NULL AND s.expires_at <= NOW()))
    )`;

    const stale = await db
      .select({ runId: runs.id })
      .from(runs)
      .where(
        or(
          and(
            lt(runs.updatedAt, olderThan),
            or(
              eq(runs.status, "pending"),
              and(inArray(runs.status, ["sleeping", "retrying"]), timerDue),
              and(eq(runs.status, "awaiting_signal"), signalResolved),
            ),
          ),
          and(eq(runs.status, "running"), lt(runs.updatedAt, runningStuckOlderThan)),
        ),
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
          if (status === "running") {
            await tx.update(runs).set({ status: "retrying" }).where(eq(runs.id, runId));
          }
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
