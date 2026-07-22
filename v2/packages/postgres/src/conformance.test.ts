import { randomUUID } from "node:crypto";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import {
  cronConformance,
  outboxConformance,
  queueConformance,
  reconcileConformance,
  signalConformance,
  storeConformance,
  timerConformance,
  wakeupConformance,
} from "@iterativeflow/conformance";
import { type Backend, builder, defineFlow, registry, submit, tickOnce } from "@iterativeflow/core";
import { type Sql, applySchema, createPgBackend, inTx, pgPool } from "@iterativeflow/postgres";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const skip = process.env.SKIP_TESTCONTAINERS === "1";

describe.skipIf(skip)("postgres backend", () => {
  let container: StartedPostgreSqlContainer;
  let pool: Pool;

  beforeAll(async () => {
    container = await new PostgreSqlContainer("postgres:16-alpine").start();
    pool = new Pool({ connectionString: container.getConnectionUri() });
    // 57P01 on shutdown surfaces as an idle-client error event; swallow so teardown stays green.
    pool.on("error", () => undefined);
    await applySchema(pgPool(pool));
  }, 180_000);

  afterAll(async () => {
    await pool?.end().catch(() => undefined);
    await container?.stop().catch(() => undefined);
  });

  const sql = () => pgPool(pool);

  /** A clean backend per test — real Postgres, truncated between cases. */
  const makeBackend = async (): Promise<Backend> => {
    await pool.query(
      'TRUNCATE "workflow".step, "workflow".job, "workflow".timer, "workflow".signal, "workflow".event, "workflow".cron, "workflow".run CASCADE',
    );
    return createPgBackend(sql());
  };

  // The exact same suites the memory backend passes — one spec, every implementation.
  storeConformance("postgres", async () => (await makeBackend()).store);
  queueConformance("postgres", async () => (await makeBackend()).queue);
  timerConformance("postgres", async () => (await makeBackend()).timer);
  wakeupConformance("postgres", async () => (await makeBackend()).wakeup);
  outboxConformance("postgres", () => makeBackend());
  signalConformance("postgres", () => makeBackend());
  reconcileConformance("postgres", () => makeBackend());
  cronConformance("postgres", async () => (await makeBackend()).store);

  // These need REAL multi-connection concurrency — memory (single-threaded) can't prove them.
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

      expect(new Set(results).size).toBe(1); // every racer agrees on ONE winner
      const winner = results[0];
      const created = (await Promise.all(childIds.map((cid) => backend.store.loadRun(cid)))).filter(
        Boolean,
      );
      expect(created).toHaveLength(1); // losers' childIds were never created
      expect(created[0]?.run.id).toBe(winner);
    });

    it("a failing outbox rolls back the step — the write is atomic, not torn", async () => {
      await makeBackend(); // truncate
      const clean = createPgBackend(sql());
      const { runId } = await clean.store.startRun({ name: "f", version: 1, input: {} });

      // Inject a fault on the enqueue INSIDE the checkpoint transaction — the step insert has
      // already run, so a non-atomic backend would leak the step. Atomic → the whole tx rolls back.
      const fault = (base: Sql, marker: string): Sql => ({
        query: (text, params) =>
          text.includes(marker)
            ? Promise.reject(new Error("injected fault"))
            : base.query(text, params),
        tx: (fn) => base.tx((t) => fn(fault(t, marker))),
      });
      const faulty = createPgBackend(fault(sql(), '"workflow".job'));

      await expect(
        faulty.store.checkpointStep(
          { runId, cursorKey: "x", status: "ok", result: 1, attempts: 1 },
          { enqueue: [{ runId }] },
        ),
      ).rejects.toThrow();
      const snap = await clean.store.loadRun(runId);
      expect(snap?.steps.has("x")).toBe(false); // step did NOT leak past the failed outbox
    });

    it("startManyRuns is atomic — a mid-batch failure lands none of the runs", async () => {
      await makeBackend();
      let inserts = 0;
      const faultOnSecond = (base: Sql): Sql => ({
        query: (text, params) => {
          if (text.includes('INSERT INTO "workflow".run')) {
            inserts += 1;
            if (inserts === 2) return Promise.reject(new Error("injected fault on 2nd insert"));
          }
          return base.query(text, params);
        },
        tx: (fn) => base.tx((t) => fn(faultOnSecond(t))),
      });
      const faulty = createPgBackend(faultOnSecond(sql()));
      await expect(
        faulty.store.startManyRuns([
          { name: "a", version: 1, input: {} },
          { name: "b", version: 1, input: {} },
        ]),
      ).rejects.toThrow();
      const rows = await pool.query('SELECT count(*)::int AS n FROM "workflow".run');
      expect(rows.rows[0].n).toBe(0); // the first insert rolled back with the batch
    });

    it("transactional enqueue: caller's write + submit commit atomically (and roll back together)", async () => {
      await makeBackend();
      await pool.query("CREATE TABLE IF NOT EXISTS app_orders (id text PRIMARY KEY)");
      await pool.query("TRUNCATE app_orders");
      const flow = defineFlow<Record<string, never>, number>({
        name: "fulfil",
        version: 1,
        run: async () => 1,
      });
      const runCount = async (id?: string) =>
        (
          await pool.query(
            id
              ? 'SELECT count(*)::int AS n FROM "workflow".run WHERE id = $1'
              : 'SELECT count(*)::int AS n FROM "workflow".run',
            id ? [id] : [],
          )
        ).rows[0].n as number;
      const jobCount = async (id: string) =>
        (await pool.query('SELECT count(*)::int AS n FROM "workflow".job WHERE run_id = $1', [id]))
          .rows[0].n as number;
      const orderCount = async () =>
        (await pool.query("SELECT count(*)::int AS n FROM app_orders")).rows[0].n as number;

      // Rollback path: the caller throws AFTER submit — neither the order nor the run/job persists.
      await expect(
        inTx(pool, async (backend, tx) => {
          await tx.query("INSERT INTO app_orders (id) VALUES ('o1')");
          await submit(backend, flow, {});
          throw new Error("boom after submit");
        }),
      ).rejects.toThrow();
      expect(await orderCount()).toBe(0);
      expect(await runCount()).toBe(0);

      // Commit path: the order AND the enqueued run land together.
      const id = await inTx(pool, async (backend, tx) => {
        await tx.query("INSERT INTO app_orders (id) VALUES ('o2')");
        return submit(backend, flow, {});
      });
      expect(await orderCount()).toBe(1);
      expect(await runCount(id)).toBe(1);
      expect(await jobCount(id)).toBe(1);
    });

    it("concurrent claims lease each run to exactly one worker (SKIP LOCKED)", async () => {
      const backend = await makeBackend();
      for (let i = 0; i < 20; i++) {
        await backend.store.startRun({ name: "f", version: 1, input: { i } });
      }
      const jobs = await pool.query('SELECT id FROM "workflow".run');
      for (const r of jobs.rows) await backend.queue.enqueue(r.id);
      const now = new Date("2030-01-01T00:00:00Z");
      // Four workers claim concurrently; no run may be leased twice.
      const batches = await Promise.all(
        Array.from({ length: 4 }, () => backend.queue.claim({ max: 10, leaseMs: 1000, now })),
      );
      const claimed = batches.flat().map((l) => l.runId);
      expect(claimed).toHaveLength(20); // all leased, none dropped
      expect(new Set(claimed).size).toBe(20); // and none leased twice
    });
  });

  // The whole durable executor, driven on real Postgres — steps, sleep, retry, invoke.
  describe("engine end-to-end", () => {
    const TERMINAL = new Set(["done", "failed", "canceled"]);

    const driveToSettle = async (
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

    it("runs a builder flow with a durable sleep to completion", async () => {
      const backend = await makeBackend();
      const flow = builder<{ x: number }>("pg-sleep", 1)
        .step("doubled", (acc) => acc.input.x * 2)
        .step("nap", async (_acc, ctx) => {
          await ctx.sleep(5_000);
          return "rested";
        })
        .output((acc) => ({ doubled: acc.doubled, nap: acc.nap }));
      const flows = registry([flow]);
      const runId = await submit(backend, flow, { x: 21 });
      const settled = await driveToSettle(backend, flows, runId);
      expect(settled).toMatchObject({ status: "done", output: { doubled: 42, nap: "rested" } });
    });

    it("invokes a child flow across the outbox and resumes with its output", async () => {
      const backend = await makeBackend();
      const child = defineFlow<{ n: number }, number>({
        name: "pg-child",
        version: 1,
        run: async (_ctx, input) => input.n * 10,
      });
      const parent = defineFlow<{ n: number }, number>({
        name: "pg-parent",
        version: 1,
        run: async (ctx, input) => (await ctx.invoke(child, { n: input.n })) + 1,
      });
      const flows = registry([parent, child]);
      const runId = await submit(backend, parent, { n: 5 });
      const settled = await driveToSettle(backend, flows, runId);
      expect(settled).toMatchObject({ status: "done", output: 51 }); // (5*10)+1
    });
  });
});
