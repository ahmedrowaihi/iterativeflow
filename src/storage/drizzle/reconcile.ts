import { and, asc, eq, inArray, lt, or, sql } from "drizzle-orm";
import { notifyTerminal } from "./notify";
import { buildOps } from "./ops";
import type { StorageSliceDeps } from "./types";
import { RESUMABLE } from "./types";

interface ReconcileOpts {
  olderThan: Date;
  runningStuckOlderThan: Date;
  maxRunAttempts: number;
  batchSize?: number;
}

/**
 * Re-enqueue runs whose status looks stuck: `pending` past its fire,
 * `sleeping`/`retrying` past a due timer, `awaiting_signal` with a delivered or
 * expired signal, or `running` past the stuck threshold. Locks each candidate,
 * re-checks under the lock, then re-enqueues via the worker's tx-enqueue.
 *
 * A stuck `running` run is first reset to `retrying`: it was left `running` by
 * a crashed worker, and `claimRun` rejects `running` as "lost", so without the
 * reset the re-enqueued job could never re-claim it.
 *
 * `retrying` recovery rides the same `timerDue` path as `sleeping`: a step
 * retry arms a `workflow.timers` row at its backoff deadline (see
 * `armRetryTimer`), so a healthy backoff has a future unfired timer and is
 * skipped, while an orphan whose wake was lost has an overdue one and is
 * recovered. A `retrying` orphan whose attempts are already exhausted is taken
 * terminal (`failed`) instead of bounced back through the queue.
 *
 * Date params are bound through drizzle's column encoders (via `lt`) so
 * postgres-js, neon-serverless, and node-postgres all encode them
 * consistently. The EXISTS subqueries stay raw — they only reference
 * server-side `NOW()` and column refs, no JS-side values to encode.
 *
 * @internal
 */
export const reenqueueOrphans =
  (deps: StorageSliceDeps) =>
  async ({
    olderThan,
    runningStuckOlderThan,
    maxRunAttempts,
    batchSize = 100,
  }: ReconcileOpts): Promise<number> => {
    const { db, tables, enqueue, logger } = deps;
    const { runs, timers, signals } = tables;
    const notify = notifyTerminal(deps);

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
    const failedRunIds: string[] = [];
    for (const { runId } of stale) {
      try {
        const outcome = await db.transaction(async (tx) => {
          const [cur] = await tx
            .select({
              name: runs.name,
              status: runs.status,
              updatedAt: runs.updatedAt,
              attempts: runs.attempts,
            })
            .from(runs)
            .where(eq(runs.id, runId))
            .for("update")
            .limit(1);
          if (!cur) return "skip";
          const { name, status, updatedAt, attempts } = cur;
          if (!(RESUMABLE as ReadonlyArray<string>).includes(status)) return "skip";

          if (status === "running") {
            if (updatedAt >= runningStuckOlderThan) return "skip";
            await tx.update(runs).set({ status: "retrying" }).where(eq(runs.id, runId));
            await enqueue(tx, runId);
            return "reenqueued";
          }

          // A retrying orphan whose next claim would bump attempts past the cap
          // and fail on arrival — fail it here instead of bouncing a doomed run.
          if (status === "retrying" && attempts >= maxRunAttempts) {
            const ops = buildOps({ db: tx, tables, enqueue }).ops;
            await ops.markFailed(runId, {
              code: "RUN_ATTEMPTS_EXHAUSTED",
              message: `Run "${name}" exceeded maxRunAttempts=${maxRunAttempts}`,
            });
            await ops.recordEvent({ runId, type: "failed" });
            return "failed";
          }

          await enqueue(tx, runId);
          return "reenqueued";
        });

        if (outcome === "reenqueued") reEnqueued += 1;
        else if (outcome === "failed") failedRunIds.push(runId);
      } catch (err) {
        logger.error(err instanceof Error ? err : new Error(String(err)), {
          event: "flow.reenqueue_failed",
          runId,
        });
      }
    }

    // Wake result() waiters (and any parent invoke) for runs taken terminal.
    for (const runId of failedRunIds) {
      await notify(runId).catch((err) => {
        logger.error(err instanceof Error ? err : new Error(String(err)), {
          event: "flow.reconcile_notify_failed",
          runId,
        });
      });
    }

    logger.info("flow.reenqueueOrphans", {
      scanned: stale.length,
      reEnqueued,
      failed: failedRunIds.length,
    });
    return reEnqueued + failedRunIds.length;
  };
