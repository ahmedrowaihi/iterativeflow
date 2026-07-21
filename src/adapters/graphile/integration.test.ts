import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import { makeWorkerUtils } from "graphile-worker";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { flow } from "../../builder/flow";
import { createEngine, type Engine } from "../../engine/engine";
import { reapOrphanedCronJobs } from "./cron";
import type { FlowHandle, Logger } from "../../engine/types";
import { applyFlowSchema, dropFlowSchema } from "../../storage/setup";
import type { WorkflowDb } from "../../storage/db";
import { acquireTestDb, makePool, pgUnavailable } from "./pg-test-db";

const silent: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

interface Harness {
  pool: Pool;
  db: WorkflowDb;
  cleanup: () => Promise<void>;
}

const setup = async (): Promise<Harness> => {
  const testDb = await acquireTestDb();
  const pool = makePool(testDb.url);
  return {
    pool,
    db: drizzle({ client: pool }) as unknown as WorkflowDb,
    cleanup: async () => {
      await pool.end();
      await testDb.close();
    },
  };
};

const waitFor = async <T>(
  fn: () => Promise<T | undefined> | T | undefined,
  { timeoutMs = 10_000, intervalMs = 100 }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<T> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = await fn();
    if (v !== undefined) return v;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
};

describe.skipIf(pgUnavailable)("real-pg integration", () => {
  let h: Harness;

  beforeAll(async () => {
    h = await setup();
    await dropFlowSchema(h.db).catch(() => undefined);
    await applyFlowSchema(h.db);
    const utils = await makeWorkerUtils({ pgPool: h.pool });
    await utils.release();
  }, 120_000);

  afterAll(async () => {
    if (!h) return;
    await dropFlowSchema(h.db).catch(() => undefined);
    await h.cleanup();
  }, 60_000);

  it("start() inserts a graphile_worker.add_job inside the outbox txn", async () => {
    const engine = createEngine({
      db: h.db,
      pool: h.pool,
      logger: silent,
      reconciler: false,
    });

    const handle = engine.register(
      flow("smoke")
        .step("noop", () => "ok")
        .build(),
    );

    const { runId } = await handle.start({});

    const { rows } = await h.pool.query<{ key: string; task_identifier: string }>(
      "SELECT key, task_identifier FROM graphile_worker.jobs WHERE key = $1",
      [`flow:${runId}`],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].key).toBe(`flow:${runId}`);
    expect(rows[0].task_identifier).toBe("flow:run:smoke@1");
  });

  it("reapOrphanedCronJobs purges cron:* jobs with no registered task", async () => {
    const utils = await makeWorkerUtils({ pgPool: h.pool });
    await utils.addJob("cron:orphan", {}, { jobKey: "cron:orphan" });
    await utils.release();

    const before = await h.pool.query<{ n: string }>(
      "SELECT count(*) AS n FROM graphile_worker.jobs WHERE task_identifier = $1",
      ["cron:orphan"],
    );
    expect(Number(before.rows[0].n)).toBe(1);

    await reapOrphanedCronJobs(h.db, "graphile_worker", [], silent);

    const after = await h.pool.query<{ n: string }>(
      "SELECT count(*) AS n FROM graphile_worker.jobs WHERE task_identifier = $1",
      ["cron:orphan"],
    );
    expect(Number(after.rows[0].n)).toBe(0);
  });

  describe("engine lifecycle on real pg", () => {
    let engine: Engine;
    // Register every flow before listen() — the worker's task list is fixed at
    // listen() time, so a flow registered later would never be claimed.
    const handles: Record<string, FlowHandle<unknown, unknown>> = {};

    beforeAll(async () => {
      engine = createEngine({
        db: h.db,
        pool: h.pool,
        logger: silent,
        reconciler: false,
        worker: { concurrency: 2, pollInterval: 200 },
      });

      handles.e2e = engine.register(
        flow("e2e")
          .step("compute", () => 42)
          .sleep("50ms")
          .step("after-sleep", ({ input }) => input)
          .output(({ input }) => input)
          .build(),
      );
      handles.idempoV1 = engine.register(
        flow("idempo")
          .version(1)
          .step("noop", () => "v1")
          .build(),
      );
      handles.idempoV2 = engine.register(
        flow("idempo")
          .version(2)
          .step("noop", () => "v2")
          .build(),
      );
      handles.cancelMe = engine.register(
        flow("cancel-me")
          .sleep("10s")
          .step("never", () => "never-runs")
          .build(),
      );
      handles.waitSignal = engine.register(
        flow("wait-signal")
          .signal("go")
          .output(() => "done")
          .build(),
      );
      handles.waitStep = engine.register(
        flow("wait-step")
          .step("compute", () => 7)
          .sleep("200ms")
          .step("after", ({ input }) => input)
          .build(),
      );
      handles.waitImmediate = engine.register(
        flow("wait-immediate")
          .step("once", () => "ok")
          .build(),
      );
      handles.warmListen = engine.register(
        flow("warm-listen")
          .signal("never")
          .output(() => "done")
          .build(),
      );
      handles.hookExpire = engine.register(
        flow("hook-expire")
          .signal("ping", { timeout: "200ms" })
          .output(() => "done")
          .build(),
      );

      await engine.listen();
    });

    afterAll(async () => {
      await engine.stop();
    });

    it("runs a workflow end-to-end with sleep + step replay", async () => {
      const { runId } = await handles.e2e.start({});

      const output = await waitFor(() => handles.e2e.output(runId), { timeoutMs: 15_000 });
      expect(output).toBe(42);
    }, 30_000);

    it("idempotency key is scoped by (name, version, key)", async () => {
      const a = await handles.idempoV1.start({}, { idempotencyKey: "shared" });
      const b = await handles.idempoV2.start({}, { idempotencyKey: "shared" });

      expect(a.runId).not.toBe(b.runId);
    });

    it("cancel marks the run canceled", async () => {
      const { runId } = await handles.cancelMe.start({});
      await new Promise((r) => setTimeout(r, 200));
      await engine.cancel(runId, "test");

      const status = await engine.status(runId);
      expect(status?.run.status).toBe("canceled");
    });

    it("handle.wait({ until: { signal } }) unblocks when engine.signal arrives", async () => {
      const { runId } = await handles.waitSignal.start({});

      const waitP = handles.waitSignal.wait(runId, { until: { signal: "go" }, timeoutMs: 10_000 });
      await new Promise((r) => setTimeout(r, 200));
      await engine.signal(runId, "go", { ok: true });

      await expect(waitP).resolves.toBeUndefined();
    }, 30_000);

    it("handle.wait({ until: { step } }) unblocks when the step row is persisted", async () => {
      const { runId } = await handles.waitStep.start({});

      await expect(
        handles.waitStep.wait(runId, { until: { step: "compute" }, timeoutMs: 10_000 }),
      ).resolves.toBeUndefined();
    }, 30_000);

    it("handle.wait({ until: { step } }) returns immediately when the step is already finished", async () => {
      const { runId } = await handles.waitImmediate.start({});
      await waitFor(() => handles.waitImmediate.output(runId), { timeoutMs: 10_000 });

      const start = Date.now();
      await handles.waitImmediate.wait(runId, { until: { step: "once" }, timeoutMs: 1_000 });
      expect(Date.now() - start).toBeLessThan(500);
    });

    it("LISTEN client reconnects after pg_terminate_backend kills its backend", async () => {
      // Force ensureListen by starting a waiter — handle.wait calls it.
      const handle = handles.warmListen;
      const { runId } = await handle.start({});
      const probeKey = "signal:warm";
      const initialWait = handle.wait(runId, { until: { signal: "warm" }, timeoutMs: 5_000 });
      // Give LISTEN a beat to subscribe.
      await waitFor(async () => ((await engine.health()).listen ? true : undefined), {
        timeoutMs: 10_000,
        intervalMs: 100,
      });
      // Manual NOTIFY proves the listener wires through pre-kill.
      await h.pool.query(`SELECT pg_notify('flow_progress', $1)`, [`signal:${runId}:${probeKey}`]);
      await expect(initialWait).resolves.toBeUndefined();

      // Kill the LISTEN backend.
      const before = await h.pool.query<{ pid: number }>(
        `SELECT pid FROM pg_stat_activity
         WHERE query ILIKE 'LISTEN flow_%'
         ORDER BY backend_start DESC LIMIT 1`,
      );
      expect(before.rows.length).toBeGreaterThan(0);
      const killedPid = before.rows[0].pid;
      await h.pool.query(`SELECT pg_terminate_backend($1)`, [killedPid]);

      // Wait for reconnect — state must return to "listening", AND the new
      // backend must be a DIFFERENT pid than the one we killed.
      await waitFor(
        async () => {
          const health = await engine.health();
          if (!health.listen) return undefined;
          const after = await h.pool.query<{ pid: number }>(
            `SELECT pid FROM pg_stat_activity
             WHERE query ILIKE 'LISTEN flow_%'
             ORDER BY backend_start DESC LIMIT 1`,
          );
          if (after.rows.length === 0 || after.rows[0].pid === killedPid) return undefined;
          return after.rows[0].pid;
        },
        { timeoutMs: 15_000, intervalMs: 200 },
      );

      // Round-trip a fresh NOTIFY through the reconnected listener.
      const afterWait = handle.wait(runId, {
        until: { signal: "post-reconnect" },
        timeoutMs: 10_000,
      });
      // Small delay so the waiter is definitely registered before the NOTIFY fires.
      await new Promise((r) => setTimeout(r, 100));
      await h.pool.query(`SELECT pg_notify('flow_progress', $1)`, [
        `signal:${runId}:signal:post-reconnect`,
      ]);
      await expect(afterWait).resolves.toBeUndefined();
    }, 60_000);

    it("hook timeout marks the run failed with SIGNAL_TIMEOUT and rejects late signal", async () => {
      const { runId } = await handles.hookExpire.start({});

      const failed = await waitFor(
        async () => {
          const s = await engine.status(runId);
          return s && s.run.status === "failed" ? s : undefined;
        },
        { timeoutMs: 15_000 },
      );
      expect(failed.run.error?.code).toBe("SIGNAL_TIMEOUT");

      await engine.signal(runId, "ping", { late: true });
      const after = await engine.status(runId);
      expect(after?.run.status).toBe("failed");
    }, 30_000);
  });
});
