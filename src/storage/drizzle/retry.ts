import { and, eq } from "drizzle-orm";
import type { RetryResult } from "../types";
import type { StorageSliceDeps } from "./types";

/**
 * Atomic retry of a failed run: lock the row, verify status, delete the
 * `failed_terminal` step row(s) so the failing step re-executes, reset
 * the run to `pending` with `attempts=0`, record a `resumed` event, and
 * enqueue. Ok step results stay memoized — this is replay, not restart.
 *
 * @internal
 */
export const retryRun =
  ({ db, tables, enqueue }: StorageSliceDeps) =>
  async (runId: string): Promise<RetryResult> =>
    db.transaction(async (tx) => {
      const { runs, steps, events } = tables;
      const locked = await tx
        .select({ id: runs.id, status: runs.status })
        .from(runs)
        .where(eq(runs.id, runId))
        .for("update")
        .limit(1);
      if (locked.length === 0) return { kind: "missing" } satisfies RetryResult;
      const row = locked[0];
      if (row.status !== "failed") {
        return { kind: "not_failed", status: row.status } satisfies RetryResult;
      }
      await tx
        .delete(steps)
        .where(and(eq(steps.runId, runId), eq(steps.status, "failed_terminal")));
      await tx
        .update(runs)
        .set({
          status: "pending" as const,
          error: null,
          attempts: 0,
          completedAt: null,
        })
        .where(eq(runs.id, runId));
      await tx.insert(events).values({ runId, type: "resumed" as const });
      await enqueue(tx, runId);
      return { kind: "queued" } satisfies RetryResult;
    });
