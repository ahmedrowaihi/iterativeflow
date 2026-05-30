import { and, eq, sql } from "drizzle-orm";
import type { WorkflowDb } from "../db";
import { events, runs, signals, steps, timers, type StepRow } from "../schema";
import type { AtomicStorage, StorageOps } from "../types";
import type { TxEnqueue } from "./types";

/**
 * Build the row-level CRUD ops over `(db, enqueue)`. Same body works for the
 * root connection (when no transaction is active) and for a tx-scoped
 * `WorkflowDb` (via `Storage.transaction`).
 *
 * @internal
 */
export const buildOps = (
  db: WorkflowDb,
  enqueue: TxEnqueue,
): {
  ops: StorageOps;
  lockRun: AtomicStorage["lockRun"];
  enqueue: AtomicStorage["enqueue"];
} => {
  const ops: StorageOps = {
    async createRun(opt) {
      const values = {
        name: opt.name,
        version: opt.version,
        input: opt.input as object,
        idempotencyKey: opt.idempotencyKey ?? null,
        tags: opt.tags ? [...opt.tags] : null,
        parentRunId: opt.parentRunId ?? null,
        parentCursorKey: opt.parentCursorKey ?? null,
        status: "pending" as const,
      };

      if (!opt.idempotencyKey) {
        const inserted = await db
          .insert(runs)
          .values(values)
          .returning({ id: runs.id, status: runs.status });
        return { runId: inserted[0].id, status: inserted[0].status, created: true };
      }

      const inserted = await db
        .insert(runs)
        .values(values)
        .onConflictDoNothing({
          target: [runs.name, runs.version, runs.idempotencyKey],
          where: sql`${runs.idempotencyKey} IS NOT NULL`,
        })
        .returning({ id: runs.id, status: runs.status });

      if (inserted.length > 0) {
        return { runId: inserted[0].id, status: inserted[0].status, created: true };
      }
      const existing = await db
        .select({ id: runs.id, status: runs.status })
        .from(runs)
        .where(
          and(
            eq(runs.name, opt.name),
            eq(runs.version, opt.version),
            eq(runs.idempotencyKey, opt.idempotencyKey),
          ),
        )
        .limit(1);
      if (existing.length === 0) {
        throw new Error("createRun: conflict but row not found");
      }
      return { runId: existing[0].id, status: existing[0].status, created: false };
    },

    async loadRun(runId) {
      const rows = await db.select().from(runs).where(eq(runs.id, runId)).limit(1);
      return rows[0];
    },

    async loadSnapshot(runId) {
      const [stepRows, timerRows, signalRows] = await Promise.all([
        db.select().from(steps).where(eq(steps.runId, runId)),
        db.select().from(timers).where(eq(timers.runId, runId)),
        db.select().from(signals).where(eq(signals.runId, runId)),
      ]);
      return {
        steps: new Map(stepRows.map((s) => [s.cursorKey, s])),
        timers: new Map(timerRows.map((t) => [t.cursorKey, t])),
        signals: new Map(signalRows.map((h) => [h.cursorKey, h])),
      };
    },

    async markRunning(runId) {
      await db
        .update(runs)
        .set({
          status: "running" as const,
          startedAt: sql`COALESCE(${runs.startedAt}, NOW())`,
          attempts: sql`${runs.attempts} + 1`,
        })
        .where(eq(runs.id, runId));
    },

    async markSleeping(runId) {
      await db
        .update(runs)
        .set({ status: "sleeping" as const })
        .where(eq(runs.id, runId));
    },

    async markAwaitingSignal(runId) {
      await db
        .update(runs)
        .set({ status: "awaiting_signal" as const })
        .where(eq(runs.id, runId));
    },

    async markRetrying(runId) {
      await db
        .update(runs)
        .set({ status: "retrying" as const })
        .where(eq(runs.id, runId));
    },

    async markCompleted(runId, output) {
      await db
        .update(runs)
        .set({
          status: "done" as const,
          output: output as object,
          completedAt: new Date(),
        })
        .where(and(eq(runs.id, runId), sql`${runs.status} != 'canceled'`));
    },

    async markFailed(runId, error) {
      await db
        .update(runs)
        .set({
          status: "failed" as const,
          error,
          completedAt: new Date(),
        })
        .where(and(eq(runs.id, runId), sql`${runs.status} != 'canceled'`));
    },

    async markCanceled(runId, reason) {
      await db
        .update(runs)
        .set({
          status: "canceled" as const,
          error: reason ? { code: "RUN_CANCELED", message: reason } : null,
          completedAt: new Date(),
        })
        .where(eq(runs.id, runId));
    },

    async loadStep(runId, cursorKey) {
      const rows = await db
        .select()
        .from(steps)
        .where(and(eq(steps.runId, runId), eq(steps.cursorKey, cursorKey)))
        .limit(1);
      return rows[0];
    },

    async startStep(runId, cursorKey, attempts) {
      const startedAt = new Date();
      const insertRow: StepRow = {
        runId,
        cursorKey,
        status: "running",
        result: null,
        error: null,
        attempts,
        startedAt,
        completedAt: null,
      };
      await db
        .insert(steps)
        .values(insertRow)
        .onConflictDoUpdate({
          target: [steps.runId, steps.cursorKey],
          set: {
            status: "running" as const,
            attempts,
            startedAt,
            completedAt: null,
          },
        });
    },

    async finishStep(opt) {
      await db
        .update(steps)
        .set({
          status: opt.status,
          result: opt.result === undefined ? null : (opt.result as object),
          error: opt.error ?? null,
          attempts: opt.attempts,
          completedAt: new Date(),
        })
        .where(and(eq(steps.runId, opt.runId), eq(steps.cursorKey, opt.cursorKey)));
      if (opt.status === "ok" || opt.status === "failed_terminal") {
        await db.execute(
          sql`SELECT pg_notify('flow_progress', ${`step:${opt.runId}:${opt.cursorKey}`})`,
        );
      }
    },

    async loadTimer(runId, cursorKey) {
      const rows = await db
        .select()
        .from(timers)
        .where(and(eq(timers.runId, runId), eq(timers.cursorKey, cursorKey)))
        .limit(1);
      return rows[0];
    },

    async createTimer(runId, cursorKey, fireAt) {
      await db
        .insert(timers)
        .values({ runId, cursorKey, fireAt })
        .onConflictDoNothing({ target: [timers.runId, timers.cursorKey] });
    },

    async fireTimer(runId, cursorKey) {
      await db
        .update(timers)
        .set({ firedAt: new Date() })
        .where(and(eq(timers.runId, runId), eq(timers.cursorKey, cursorKey)));
    },

    async loadSignal(runId, cursorKey) {
      const rows = await db
        .select()
        .from(signals)
        .where(and(eq(signals.runId, runId), eq(signals.cursorKey, cursorKey)))
        .limit(1);
      return rows[0];
    },

    async preDeliverSignal(runId, cursorKey, payload) {
      const inserted = await db
        .insert(signals)
        .values({
          runId,
          cursorKey,
          delivered: true,
          deliveredAt: new Date(),
          payload: payload === undefined ? null : (payload as object),
        })
        .onConflictDoNothing({ target: [signals.runId, signals.cursorKey] })
        .returning({ runId: signals.runId });
      return inserted.length > 0;
    },

    async recordEvent(opt) {
      await db.insert(events).values({
        runId: opt.runId,
        type: opt.type,
        cursorKey: opt.cursorKey ?? null,
        payload: opt.payload === undefined ? null : (opt.payload as object),
      });
    },
  };

  return {
    ops,
    lockRun: async (runId) => {
      const rows = await db.select().from(runs).where(eq(runs.id, runId)).for("update").limit(1);
      return rows[0];
    },
    enqueue: async (runId, opts) => {
      await enqueue(db, runId, opts);
    },
  };
};
