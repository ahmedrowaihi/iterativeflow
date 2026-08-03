import { randomUUID } from "node:crypto";
import {
  claimFilterConformance,
  cronConformance,
  engineConformance,
  outboxConformance,
  queueConformance,
  reconcileConformance,
  signalConformance,
  storeConformance,
  timerConformance,
  wakeupConformance,
} from "@iterativeflow/conformance";
import { type Backend, defineFlow, registry, submit, tickOnce } from "@iterativeflow/core";
import { type Pool, createPool } from "mysql2/promise";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createMysqlBackend } from "#backend";
import { applySchema, tables } from "#schema";
import { type Sql, mysqlPool } from "#sql";
import { inTx } from "#tx";

const skip = process.env.SKIP_TESTCONTAINERS === "1";
const t = tables("");

describe.skipIf(skip)("mysql backend", () => {
  let container: StartedTestContainer;
  let pool: Pool;

  beforeAll(async () => {
    container = await new GenericContainer("mysql:8")
      .withEnvironment({ MYSQL_ROOT_PASSWORD: "test", MYSQL_DATABASE: "iflow" })
      .withExposedPorts(3306)
      .withWaitStrategy(Wait.forLogMessage(/ready for connections/, 2))
      .withStartupTimeout(180_000)
      .start();
    pool = createPool({
      host: container.getHost(),
      port: container.getMappedPort(3306),
      user: "root",
      password: "test",
      database: "iflow",
    });
    // MySQL logs "ready" during its init temp-server phase then restarts, so the first connections
    // can drop — ping until the real server is stable before applying the schema.
    for (let i = 0; i < 40; i++) {
      try {
        await pool.query("SELECT 1");
        break;
      } catch {
        await new Promise((r) => setTimeout(r, 2000));
      }
    }
    await applySchema(mysqlPool(pool));
  }, 240_000);

  afterAll(async () => {
    await pool?.end().catch(() => undefined);
    await container?.stop().catch(() => undefined);
  });

  const makeBackend = async (): Promise<Backend> => {
    for (const table of Object.values(t)) await pool.query(`TRUNCATE TABLE ${table}`);
    return createMysqlBackend(mysqlPool(pool));
  };

  storeConformance("mysql", async () => (await makeBackend()).store);
  queueConformance("mysql", async () => (await makeBackend()).queue);
  claimFilterConformance("mysql", () => makeBackend());
  timerConformance("mysql", async () => (await makeBackend()).timer);
  wakeupConformance("mysql", async () => (await makeBackend()).wakeup);
  outboxConformance("mysql", () => makeBackend());
  signalConformance("mysql", () => makeBackend());
  reconcileConformance("mysql", () => makeBackend());
  cronConformance("mysql", async () => (await makeBackend()).store);
  engineConformance("mysql", () => makeBackend());

  it("transactional enqueue: caller's write + submit commit atomically (and roll back together)", async () => {
    const backend = await makeBackend();
    await pool.query("CREATE TABLE IF NOT EXISTS app_orders (id VARCHAR(64) PRIMARY KEY)");
    await pool.query("TRUNCATE app_orders");
    const flow = defineFlow<Record<string, never>, number>({
      name: "fulfil",
      version: 1,
      run: async () => 1,
    });
    const orderCount = async (): Promise<number> => {
      const [rows] = await pool.query("SELECT count(*) AS n FROM app_orders");
      return Number((rows as { n: number }[])[0].n);
    };
    const runCount = async (): Promise<number> =>
      (await backend.store.listRuns({}, { limit: 10 })).runs.length;

    // Rollback: a throw after submit persists neither the order nor the run/job.
    await expect(
      inTx(pool, async (b, tx) => {
        await tx.query("INSERT INTO app_orders (id) VALUES ('o1')");
        await submit(b, flow, {});
        throw new Error("boom after submit");
      }),
    ).rejects.toThrow();
    expect(await orderCount()).toBe(0);
    expect(await runCount()).toBe(0);

    // Commit: the order AND the enqueued run land together.
    const id = await inTx(pool, async (b, tx) => {
      await tx.query("INSERT INTO app_orders (id) VALUES ('o2')");
      return submit(b, flow, {});
    });
    expect(await orderCount()).toBe(1);
    expect(await backend.store.loadRunRow(id)).toBeDefined();
  });

  describe("atomicity + concurrency", () => {
    it("concurrent first-writer-wins: N parallel checkpoints on one key spawn exactly one child", async () => {
      const backend = await makeBackend();
      const { runId } = await backend.store.startRun({ name: "p", version: 1, input: {} });
      const childIds = Array.from({ length: 8 }, () => randomUUID());

      const results = await Promise.all(
        childIds.map((cid) =>
          backend.store
            .checkpointStep(
              { runId, cursorKey: "spawn", status: "ok", result: cid, attempts: 1 },
              { spawn: [{ runId: cid, spec: { name: "c", version: 1, input: {} } }] },
            )
            .then((o) => o.result as string),
        ),
      );

      expect(new Set(results).size).toBe(1);
      const created = (await Promise.all(childIds.map((cid) => backend.store.loadRun(cid)))).filter(
        Boolean,
      );
      expect(created).toHaveLength(1);
      expect(created[0]?.run.id).toBe(results[0]);
    });

    it("a failing outbox rolls back the step — the write is atomic, not torn", async () => {
      await makeBackend();
      const clean = createMysqlBackend(mysqlPool(pool));
      const { runId } = await clean.store.startRun({ name: "f", version: 1, input: {} });

      const fault = (base: Sql, marker: string): Sql => ({
        query: (text, params) =>
          text.includes(marker)
            ? Promise.reject(new Error("injected fault"))
            : base.query(text, params),
        exec: (text, params) =>
          text.includes(marker)
            ? Promise.reject(new Error("injected fault"))
            : base.exec(text, params),
        tx: (fn) => base.tx((tt) => fn(fault(tt, marker))),
      });
      const faulty = createMysqlBackend(fault(mysqlPool(pool), t.job));

      await expect(
        faulty.store.checkpointStep(
          { runId, cursorKey: "x", status: "ok", result: 1, attempts: 1 },
          { enqueue: [{ runId }] },
        ),
      ).rejects.toThrow();
      const snap = await clean.store.loadRun(runId);
      expect(snap?.steps.has("x")).toBe(false);
    });

    it("concurrent claims lease each run to exactly one worker (SKIP LOCKED)", async () => {
      const backend = await makeBackend();
      const ids: string[] = [];
      for (let i = 0; i < 20; i++) {
        const { runId } = await backend.store.startRun({ name: "f", version: 1, input: { i } });
        ids.push(runId);
      }
      for (const runId of ids) await backend.queue.enqueue(runId);
      const now = new Date("2030-01-01T00:00:00Z");
      const claimed: string[] = [];
      // SKIP LOCKED + LIMIT counts skipped rows against the limit, so concurrent limited claims
      // lease only the head per round — drain across rounds; the invariant is no run leased twice.
      for (let round = 0; round < 10 && claimed.length < ids.length; round++) {
        const batches = await Promise.all(
          Array.from({ length: 4 }, () =>
            backend.queue.claim({ limit: 10, leaseMs: 600_000, now }),
          ),
        );
        claimed.push(...batches.flat().map((l) => l.runId));
      }
      expect(new Set(claimed).size).toBe(claimed.length);
      expect(new Set(claimed)).toEqual(new Set(ids));
    });
  });

  describe("engine end-to-end", () => {
    const TERMINAL = new Set(["done", "failed", "canceled"]);
    const drive = async (
      backend: Backend,
      flows: ReturnType<typeof registry>,
      runId: string,
    ): Promise<{ status: string; output: unknown }> => {
      let clock = new Date("2030-01-01T00:00:00Z");
      const now = (): Date => clock;
      for (let i = 0; i < 100; i++) {
        await tickOnce(backend, flows, { batchMax: 16, leaseMs: 600_000, now });
        const run = (await backend.store.loadRun(runId))?.run;
        if (run && TERMINAL.has(run.status)) return { status: run.status, output: run.output };
        clock = new Date(clock.getTime() + 2_000);
      }
      throw new Error("run did not settle");
    };

    it("invokes a child flow across the outbox and resumes with its output", async () => {
      const backend = await makeBackend();
      const child = defineFlow<{ n: number }, number>({
        name: "m-child",
        version: 1,
        run: async (_ctx, input) => input.n * 10,
      });
      const parent = defineFlow<{ n: number }, number>({
        name: "m-parent",
        version: 1,
        run: async (ctx, input) => (await ctx.invoke(child, { n: input.n })) + 1,
      });
      const flows = registry([parent, child]);
      const runId = await submit(backend, parent, { n: 5 });
      expect(await drive(backend, flows, runId)).toMatchObject({ status: "done", output: 51 });
    });
  });
});
