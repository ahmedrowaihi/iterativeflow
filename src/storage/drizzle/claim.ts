import { and, eq, inArray, sql } from "drizzle-orm";
import type { WorkflowDb } from "../db";
import { runs, signals, steps, timers } from "../schema";
import type { ClaimResult, RunSnapshot } from "../types";
import { asTx, TERMINAL } from "./types";

const loadSnapshotIn = async (tx: WorkflowDb, runId: string): Promise<RunSnapshot> => {
  const [stepRows, timerRows, signalRows] = await Promise.all([
    tx.select().from(steps).where(eq(steps.runId, runId)),
    tx.select().from(timers).where(eq(timers.runId, runId)),
    tx.select().from(signals).where(eq(signals.runId, runId)),
  ]);
  return {
    steps: new Map(stepRows.map((s) => [s.cursorKey, s])),
    timers: new Map(timerRows.map((t) => [t.cursorKey, t])),
    signals: new Map(signalRows.map((h) => [h.cursorKey, h])),
  };
};

/**
 * Atomic claim: lock the row, decide claim/missing/terminal/lost, transition
 * to `running`, bump `attempts`, load the snapshot — all in one transaction.
 * Closes the prior race window between `markRunning` and `loadSnapshot`.
 *
 * @internal
 */
export const claimRun =
  (db: WorkflowDb) =>
  async (runId: string): Promise<ClaimResult> =>
    db.transaction(async (raw) => {
      const tx = asTx(raw);
      const locked = await tx.select().from(runs).where(eq(runs.id, runId)).for("update").limit(1);
      if (locked.length === 0) return { kind: "missing" } satisfies ClaimResult;
      const row = locked[0];
      if ((TERMINAL as ReadonlyArray<string>).includes(row.status)) {
        return { kind: "terminal", status: row.status } satisfies ClaimResult;
      }
      if (row.status === "running") {
        return { kind: "lost" } satisfies ClaimResult;
      }
      const resumed = row.status !== "pending";
      const updated = await tx
        .update(runs)
        .set({
          status: "running" as const,
          startedAt: sql`COALESCE(${runs.startedAt}, NOW())`,
          attempts: sql`${runs.attempts} + 1`,
        })
        .where(
          and(
            eq(runs.id, runId),
            inArray(runs.status, ["pending", "sleeping", "awaiting_signal", "retrying"]),
          ),
        )
        .returning({ id: runs.id });
      if (updated.length === 0) {
        return { kind: "lost" } satisfies ClaimResult;
      }
      const snapshot = await loadSnapshotIn(tx, runId);
      return {
        kind: "claimed",
        claim: {
          run: { ...row, status: "running", attempts: row.attempts + 1 },
          snapshot,
          resumed,
        },
      } satisfies ClaimResult;
    });
