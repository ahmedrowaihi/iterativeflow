import { and, eq, sql } from "drizzle-orm";
import type {
  ArmResult,
  AtomicStorage,
  EnqueueOpts,
  Logger,
  RunDetail,
  SignalResult,
  Storage,
  StorageOps,
} from "../engine/types";
import type { WorkflowDb } from "./db";
import { events, hooks, runs, steps, timers, type StepRow } from "./schema";

export type TxEnqueue = (tx: WorkflowDb, runId: string, opts?: EnqueueOpts) => Promise<void>;

export interface DrizzleStorageOpts {
  db: WorkflowDb;
  logger: Logger;
  enqueue: TxEnqueue;
}

const RESUMABLE = ["pending", "sleeping", "waiting", "running"] as const;

const buildOps = (
  db: WorkflowDb,
  enqueue: TxEnqueue,
  logger: Logger,
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
          target: [runs.name, runs.idempotencyKey],
          where: sql`${runs.idempotencyKey} IS NOT NULL`,
        })
        .returning({ id: runs.id, status: runs.status });

      if (inserted.length > 0) {
        return { runId: inserted[0].id, status: inserted[0].status, created: true };
      }
      const existing = await db
        .select({ id: runs.id, status: runs.status })
        .from(runs)
        .where(and(eq(runs.name, opt.name), eq(runs.idempotencyKey, opt.idempotencyKey)))
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
      const [stepRows, timerRows, hookRows] = await Promise.all([
        db.select().from(steps).where(eq(steps.runId, runId)),
        db.select().from(timers).where(eq(timers.runId, runId)),
        db.select().from(hooks).where(eq(hooks.runId, runId)),
      ]);
      return {
        steps: new Map(stepRows.map((s) => [s.stepKey, s])),
        timers: new Map(timerRows.map((t) => [t.stepKey, t])),
        hooks: new Map(hookRows.map((h) => [h.hookKey, h])),
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

    async markWaiting(runId) {
      await db
        .update(runs)
        .set({ status: "waiting" as const })
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
        .where(eq(runs.id, runId));
    },

    async markFailed(runId, error) {
      await db
        .update(runs)
        .set({
          status: "failed" as const,
          error,
          completedAt: new Date(),
        })
        .where(eq(runs.id, runId));
    },

    async markCanceled(runId, reason) {
      await db
        .update(runs)
        .set({
          status: "canceled" as const,
          error: reason ? { code: "CANCELED", message: reason } : null,
          completedAt: new Date(),
        })
        .where(eq(runs.id, runId));
    },

    async loadStep(runId, stepKey) {
      const rows = await db
        .select()
        .from(steps)
        .where(and(eq(steps.runId, runId), eq(steps.stepKey, stepKey)))
        .limit(1);
      return rows[0];
    },

    async startStep(runId, stepKey, attempts) {
      const startedAt = new Date();
      const insertRow: StepRow = {
        runId,
        stepKey,
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
          target: [steps.runId, steps.stepKey],
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
        .where(and(eq(steps.runId, opt.runId), eq(steps.stepKey, opt.stepKey)));
    },

    async loadTimer(runId, stepKey) {
      const rows = await db
        .select()
        .from(timers)
        .where(and(eq(timers.runId, runId), eq(timers.stepKey, stepKey)))
        .limit(1);
      return rows[0];
    },

    async createTimer(runId, stepKey, fireAt) {
      await db
        .insert(timers)
        .values({ runId, stepKey, fireAt })
        .onConflictDoNothing({ target: [timers.runId, timers.stepKey] });
    },

    async fireTimer(runId, stepKey) {
      await db
        .update(timers)
        .set({ firedAt: new Date() })
        .where(and(eq(timers.runId, runId), eq(timers.stepKey, stepKey)));
    },

    async loadHook(runId, hookKey) {
      const rows = await db
        .select()
        .from(hooks)
        .where(and(eq(hooks.runId, runId), eq(hooks.hookKey, hookKey)))
        .limit(1);
      return rows[0];
    },

    async preDeliverHook(runId, hookKey, payload) {
      const inserted = await db
        .insert(hooks)
        .values({
          runId,
          hookKey,
          delivered: true,
          deliveredAt: new Date(),
          payload: payload === undefined ? null : (payload as object),
        })
        .onConflictDoNothing({ target: [hooks.runId, hooks.hookKey] })
        .returning({ runId: hooks.runId });
      return inserted.length > 0;
    },

    async recordEvent(opt) {
      try {
        await db.insert(events).values({
          runId: opt.runId,
          type: opt.type,
          stepKey: opt.stepKey ?? null,
          payload: opt.payload === undefined ? null : (opt.payload as object),
        });
      } catch (err) {
        logger.error(err instanceof Error ? err : new Error(String(err)), {
          event: "workflow.recordEvent.failed",
          runId: opt.runId,
          type: opt.type,
          stepKey: opt.stepKey,
        });
      }
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

export const createDrizzleStorage = (opt: DrizzleStorageOpts): Storage => {
  const { db, enqueue, logger } = opt;

  const root = buildOps(db, enqueue, logger);

  const lockRunIn = async (tx: WorkflowDb, runId: string) => {
    await tx.select({ id: runs.id }).from(runs).where(eq(runs.id, runId)).for("update").limit(1);
  };

  const storage: Storage = {
    ...root.ops,

    async transaction(fn) {
      return db.transaction(async (tx) => {
        const txDb = tx as unknown as WorkflowDb;
        const inner = buildOps(txDb, enqueue, logger);
        const atomic: AtomicStorage = {
          ...inner.ops,
          lockRun: inner.lockRun,
          enqueue: inner.enqueue,
        };
        return fn(atomic);
      });
    },

    async signalHook(runId, hookName, payload) {
      const canonicalKey = `hook:${hookName}`;
      return db.transaction(async (raw) => {
        const tx = raw as unknown as WorkflowDb;
        await lockRunIn(tx, runId);

        const armed = await tx
          .select({ hookKey: hooks.hookKey })
          .from(hooks)
          .where(
            and(
              eq(hooks.runId, runId),
              eq(hooks.delivered, false),
              sql`(${hooks.hookKey} = ${canonicalKey} OR ${hooks.hookKey} LIKE ${`${canonicalKey}:%`})`,
            ),
          )
          .limit(1);

        if (armed.length > 0) {
          const targetKey = armed[0].hookKey;
          const delivered = await tx
            .update(hooks)
            .set({
              delivered: true,
              deliveredAt: new Date(),
              payload: payload === undefined ? null : (payload as object),
            })
            .where(
              and(eq(hooks.runId, runId), eq(hooks.hookKey, targetKey), eq(hooks.delivered, false)),
            )
            .returning({ runId: hooks.runId });

          if (delivered.length === 0) {
            return { kind: "duplicate" } satisfies SignalResult;
          }

          await tx.insert(events).values({
            runId,
            type: "hook_resolved",
            stepKey: targetKey,
            payload: { hookKey: targetKey } as object,
          });
          await enqueue(tx, runId);
          return { kind: "delivered", hookKey: targetKey } satisfies SignalResult;
        }

        const inserted = await tx
          .insert(hooks)
          .values({
            runId,
            hookKey: canonicalKey,
            delivered: true,
            deliveredAt: new Date(),
            payload: payload === undefined ? null : (payload as object),
          })
          .onConflictDoNothing({ target: [hooks.runId, hooks.hookKey] })
          .returning({ runId: hooks.runId });

        if (inserted.length === 0) {
          return { kind: "duplicate" } satisfies SignalResult;
        }

        await tx.insert(events).values({
          runId,
          type: "hook_resolved",
          stepKey: canonicalKey,
          payload: { hookKey: canonicalKey, buffered: true } as object,
        });
        return { kind: "buffered", hookKey: canonicalKey } satisfies SignalResult;
      });
    },

    async armOrConsumeHook(runId, hookKey, expiresAt) {
      return db.transaction(async (raw) => {
        const tx = raw as unknown as WorkflowDb;
        await lockRunIn(tx, runId);

        const existing = await tx
          .select()
          .from(hooks)
          .where(and(eq(hooks.runId, runId), eq(hooks.hookKey, hookKey)))
          .limit(1);

        if (existing[0]?.delivered) {
          return {
            kind: "consumed",
            payload: existing[0].payload,
          } satisfies ArmResult;
        }

        if (!existing[0]) {
          await tx
            .insert(hooks)
            .values({
              runId,
              hookKey,
              expiresAt: expiresAt ?? null,
              delivered: false,
            })
            .onConflictDoNothing({ target: [hooks.runId, hooks.hookKey] });
          await tx.insert(events).values({
            runId,
            type: "hook_armed",
            stepKey: hookKey,
            payload: { expiresAt: expiresAt ?? null } as object,
          });
        }
        return { kind: "armed" } satisfies ArmResult;
      });
    },

    async loadRunDetail(runId) {
      const detail = await Promise.all([
        db.select().from(runs).where(eq(runs.id, runId)).limit(1),
        db.select().from(steps).where(eq(steps.runId, runId)),
        db.select().from(timers).where(eq(timers.runId, runId)),
        db.select().from(hooks).where(eq(hooks.runId, runId)),
      ]);
      const run = detail[0][0];
      if (!run) return undefined;
      return {
        run,
        steps: detail[1],
        timers: detail[2],
        hooks: detail[3],
      } satisfies RunDetail;
    },

    async loadOutput(runId) {
      const row = await db
        .select({ output: runs.output, status: runs.status })
        .from(runs)
        .where(eq(runs.id, runId))
        .limit(1);
      if (row.length === 0 || row[0].status !== "done") return undefined;
      return row[0].output;
    },

    async reenqueueOrphans({ olderThan, runningStuckOlderThan, batchSize = 100 }) {
      const stuckCutoff = runningStuckOlderThan ?? new Date(0);
      const stale = await db
        .select({ runId: runs.id })
        .from(runs)
        .where(
          sql`(
            ${runs.updatedAt} < ${olderThan} AND (
              ${runs.status} = 'pending'
              OR (${runs.status} = 'sleeping' AND EXISTS (
                SELECT 1 FROM workflow.timers t
                WHERE t.run_id = ${runs.id}
                  AND t.fired_at IS NULL
                  AND t.fire_at <= NOW()
              ))
              OR (${runs.status} = 'waiting' AND EXISTS (
                SELECT 1 FROM workflow.hooks h
                WHERE h.run_id = ${runs.id}
                  AND (h.delivered = true OR (h.expires_at IS NOT NULL AND h.expires_at <= NOW()))
              ))
            )
          )
          OR (${runs.status} = 'running' AND ${runs.updatedAt} < ${stuckCutoff})`,
        )
        .limit(batchSize);

      if (stale.length === 0) return 0;

      let reEnqueued = 0;
      for (const { runId } of stale) {
        try {
          await db.transaction(async (raw) => {
            const tx = raw as unknown as WorkflowDb;
            await lockRunIn(tx, runId);
            const cur = await tx
              .select({ status: runs.status, updatedAt: runs.updatedAt })
              .from(runs)
              .where(eq(runs.id, runId))
              .limit(1);
            if (!cur[0]) return;
            const { status, updatedAt } = cur[0];
            if (!(RESUMABLE as ReadonlyArray<string>).includes(status)) return;
            if (
              status === "running" &&
              (!runningStuckOlderThan || updatedAt >= runningStuckOlderThan)
            )
              return;
            await enqueue(tx, runId);
          });
          reEnqueued += 1;
        } catch (err) {
          logger.error(err instanceof Error ? err : new Error(String(err)), {
            event: "workflow.reenqueue_failed",
            runId,
          });
        }
      }
      logger.info("workflow.reenqueueOrphans", {
        scanned: stale.length,
        reEnqueued,
      });
      return reEnqueued;
    },

    async pruneEvents({ olderThan, batchSize = 1000 }) {
      const result = (await db.execute(sql`
        WITH del AS (
          SELECT id FROM workflow.events
          WHERE at < ${olderThan}
          ORDER BY id
          LIMIT ${batchSize}
        )
        DELETE FROM workflow.events
        WHERE id IN (SELECT id FROM del)
        RETURNING id
      `)) as unknown as { rows: unknown[] };
      return result.rows.length;
    },

    async pruneRuns({ olderThan, status = ["done", "failed", "canceled"], batchSize = 1000 }) {
      const statusList = sql.join(
        status.map((s) => sql`${s}`),
        sql`, `,
      );
      const result = (await db.execute(sql`
        WITH del AS (
          SELECT id FROM workflow.runs
          WHERE updated_at < ${olderThan}
            AND status IN (${statusList})
          ORDER BY updated_at
          LIMIT ${batchSize}
        )
        DELETE FROM workflow.runs
        WHERE id IN (SELECT id FROM del)
        RETURNING id
      `)) as unknown as { rows: unknown[] };
      return result.rows.length;
    },
  };

  return storage;
};

export const noopEnqueue: TxEnqueue = async () => {};
