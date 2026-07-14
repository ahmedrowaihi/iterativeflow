import { sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import { makeWorkerUtils } from "graphile-worker";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { z } from "zod";
import { defineContract } from "../../builder/contract";
import { flow } from "../../builder/flow";
import { createEngine, type Engine } from "../../engine/engine";
import type { Logger } from "../../engine/types";
import { applyFlowSchema, dropFlowSchema } from "../../storage/setup";
import type { WorkflowDb } from "../../storage/db";
import { acquireTestDb, pgUnavailable } from "./pg-test-db";

const silent: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

const waitFor = async <T>(
  fn: () => Promise<T | undefined> | T | undefined,
  { timeoutMs = 15_000, intervalMs = 100 }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<T> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = await fn();
    if (v !== undefined) return v;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
};

const provisionContract = defineContract<{ subjectId: string }, { status: "done" }>({
  name: "provision-subject",
  version: 1,
  input: z.object({ subjectId: z.string() }),
});

const provisionFlow = flow(provisionContract)
  .step("work", ({ input }) => ({ provisioned: input.subjectId }))
  .output(() => ({ status: "done" as const }))
  .build();

interface Harness {
  url: string;
  pools: Pool[];
  db: (pool: Pool) => WorkflowDb;
  cleanup: () => Promise<void>;
}

const setup = async (): Promise<Harness> => {
  const testDb = await acquireTestDb();
  const pools: Pool[] = [];
  return {
    url: testDb.url,
    pools,
    db: (pool) => drizzle({ client: pool }) as unknown as WorkflowDb,
    cleanup: async () => {
      for (const p of pools) await p.end();
      await testDb.close();
    },
  };
};

describe.skipIf(pgUnavailable)("transactional start (StartOpts.tx)", () => {
  let h: Harness;

  const newPool = (): Pool => {
    const p = new Pool({ connectionString: h.url });
    h.pools.push(p);
    return p;
  };

  beforeAll(async () => {
    h = await setup();
    const seed = newPool();
    await dropFlowSchema(h.db(seed)).catch(() => undefined);
    await applyFlowSchema(h.db(seed));
    const utils = await makeWorkerUtils({ pgPool: seed });
    await utils.release();
  }, 120_000);

  afterAll(async () => {
    if (!h) return;
    const first = h.pools[0];
    if (first) await dropFlowSchema(h.db(first)).catch(() => undefined);
    await h.cleanup();
  }, 60_000);

  it("a caller rollback discards the run row and leaves no job", async () => {
    const pool = newPool();
    const db = h.db(pool);
    const engine: Engine = createEngine({ db, pool, logger: silent, reconciler: false });
    engine.register(provisionFlow);
    const handle = engine.enqueueHandle(provisionFlow);

    let runId = "";
    await db
      .transaction(async (tx) => {
        ({ runId } = await handle.start({ subjectId: "s-1" }, { tx: tx as unknown as WorkflowDb }));
        throw new Error("caller rolls back after enqueue");
      })
      .catch(() => undefined);

    expect(await engine.status(runId)).toBeUndefined();
    const { rows } = await pool.query("SELECT 1 FROM graphile_worker.jobs WHERE key = $1", [
      `flow:${runId}`,
    ]);
    expect(rows).toHaveLength(0);
  }, 60_000);

  it("a caller commit makes the run and its job visible together and it runs", async () => {
    const pool = newPool();
    const db = h.db(pool);
    const engine: Engine = createEngine({
      db,
      pool,
      logger: silent,
      reconciler: false,
      worker: { concurrency: 2, pollInterval: 200 },
    });
    engine.register(provisionFlow);
    const handle = engine.enqueueHandle(provisionFlow);
    await engine.listen();

    try {
      const { runId } = await db.transaction(async (tx) =>
        handle.start({ subjectId: "s-2" }, { tx: tx as unknown as WorkflowDb }),
      );

      // Committed in the caller's tx → the run row is readable immediately.
      expect((await engine.status(runId))?.run.status).toBeDefined();
      const out = await handle.result(runId, { timeoutMs: 15_000 });
      expect(out).toEqual({ status: "done" });
    } finally {
      await engine.stop().catch(() => undefined);
    }
  }, 60_000);

  it("split topology: an enqueue-only API starts inside its own tx with a business row, and the worker runs it", async () => {
    const apiPool = newPool();
    const workerPool = newPool();
    await apiPool.query("CREATE TABLE IF NOT EXISTS subjects_scratch (id text PRIMARY KEY)");

    const apiDb = h.db(apiPool);
    // API engine: knows only the contract, registers no body, never listen()s.
    const api: Engine = createEngine({
      db: apiDb,
      pool: apiPool,
      logger: silent,
      reconciler: false,
    });
    // Worker engine: registers the body + listens; the only process that can claim the job.
    const worker: Engine = createEngine({
      db: h.db(workerPool),
      pool: workerPool,
      logger: silent,
      reconciler: false,
      worker: { concurrency: 2, pollInterval: 200 },
    });
    worker.register(provisionFlow);
    await worker.listen();
    const handle = api.enqueueHandle(provisionContract);

    try {
      // Rollback in the API's tx discards the business row AND the run together.
      let orphan = "";
      await apiDb
        .transaction(async (tx) => {
          await tx.execute(sql`INSERT INTO subjects_scratch (id) VALUES ('s-rollback')`);
          ({ runId: orphan } = await handle.start(
            { subjectId: "s-rollback" },
            { tx: tx as unknown as WorkflowDb },
          ));
          throw new Error("API rolls back the whole unit");
        })
        .catch(() => undefined);
      expect(await worker.status(orphan)).toBeUndefined();
      const rolled = await apiPool.query("SELECT 1 FROM subjects_scratch WHERE id = 's-rollback'");
      expect(rolled.rows).toHaveLength(0);

      // Commit persists the business row and the worker claims + runs the flow.
      const { runId } = await apiDb.transaction(async (tx) => {
        await tx.execute(sql`INSERT INTO subjects_scratch (id) VALUES ('s-commit')`);
        return handle.start({ subjectId: "s-commit" }, { tx: tx as unknown as WorkflowDb });
      });
      const done = await waitFor(
        async () => {
          const s = await worker.status(runId);
          return s?.run.status === "done" ? s : undefined;
        },
        { timeoutMs: 15_000 },
      );
      expect(done.run.status).toBe("done");
      const committed = await apiPool.query("SELECT 1 FROM subjects_scratch WHERE id = 's-commit'");
      expect(committed.rows).toHaveLength(1);
    } finally {
      await api.stop().catch(() => undefined);
      await worker.stop().catch(() => undefined);
      await apiPool.query("DROP TABLE IF EXISTS subjects_scratch").catch(() => undefined);
    }
  }, 60_000);

  it("startMany dispatches a batch atomically and the worker runs every run", async () => {
    const pool = newPool();
    const engine: Engine = createEngine({
      db: h.db(pool),
      pool,
      logger: silent,
      reconciler: false,
      worker: { concurrency: 4, pollInterval: 200 },
    });
    engine.register(provisionFlow);
    const handle = engine.enqueueHandle(provisionContract);
    await engine.listen();

    try {
      const items = Array.from({ length: 5 }, (_, i) => ({ input: { subjectId: `b-${i}` } }));
      const started = await handle.startMany(items);
      expect(started).toHaveLength(5);
      expect(new Set(started.map((s) => s.runId)).size).toBe(5);

      // One job per run landed under the per-flow identifier.
      const { rows } = await pool.query<{ n: string }>(
        "SELECT count(*) AS n FROM graphile_worker.jobs WHERE task_identifier = $1",
        ["flow:run:provision-subject@1"],
      );
      expect(Number(rows[0].n)).toBe(5);

      for (const { runId } of started) {
        const out = await handle.result(runId, { timeoutMs: 15_000 });
        expect(out).toEqual({ status: "done" });
      }
    } finally {
      await engine.stop().catch(() => undefined);
    }
  }, 60_000);

  it("a caller rollback discards the whole batch — no runs, no jobs", async () => {
    const pool = newPool();
    const db = h.db(pool);
    const engine: Engine = createEngine({ db, pool, logger: silent, reconciler: false });
    engine.register(provisionFlow);
    const handle = engine.enqueueHandle(provisionContract);

    let ids: string[] = [];
    await db
      .transaction(async (tx) => {
        const started = await handle.startMany(
          [{ input: { subjectId: "r-0" } }, { input: { subjectId: "r-1" } }],
          { tx: tx as unknown as WorkflowDb },
        );
        ids = started.map((s) => s.runId);
        throw new Error("caller rolls back the batch");
      })
      .catch(() => undefined);

    expect(ids).toHaveLength(2);
    for (const id of ids) expect(await engine.status(id)).toBeUndefined();
    const { rows } = await pool.query("SELECT 1 FROM graphile_worker.jobs WHERE key = ANY($1)", [
      ids.map((id) => `flow:${id}`),
    ]);
    expect(rows).toHaveLength(0);
  }, 60_000);

  it("startMany dedupes idempotent keys — within a batch and across calls", async () => {
    const pool = newPool();
    const engine: Engine = createEngine({
      db: h.db(pool),
      pool,
      logger: silent,
      reconciler: false,
    });
    engine.register(provisionFlow);
    const handle = engine.enqueueHandle(provisionContract);

    // A duplicate key inside one batch collapses to a single run.
    const within = await handle.startMany([
      { input: { subjectId: "d-a" }, idempotencyKey: "dup" },
      { input: { subjectId: "d-b" }, idempotencyKey: "dup" },
    ]);
    expect(within[0].runId).toBe(within[1].runId);

    // A later batch reusing a key returns the original run; fresh keys are new.
    const first = await handle.startMany([{ input: { subjectId: "e-a" }, idempotencyKey: "k1" }]);
    const again = await handle.startMany([
      { input: { subjectId: "e-a" }, idempotencyKey: "k1" },
      { input: { subjectId: "e-c" }, idempotencyKey: "k2" },
    ]);
    expect(again[0].runId).toBe(first[0].runId);
    expect(again[1].runId).not.toBe(first[0].runId);
  }, 60_000);

  it("startMany chunks a large batch across the per-statement bind-param ceiling", async () => {
    const pool = newPool();
    const engine: Engine = createEngine({
      db: h.db(pool),
      pool,
      logger: silent,
      reconciler: false,
    });
    engine.register(provisionFlow);
    const handle = engine.enqueueHandle(provisionContract);

    // 1500 > the 1000-row insert chunk → multiple statements per phase, one tx.
    const items = Array.from({ length: 1500 }, (_, i) => ({ input: { subjectId: `big-${i}` } }));
    const started = await handle.startMany(items);

    expect(started).toHaveLength(1500);
    expect(new Set(started.map((s) => s.runId)).size).toBe(1500);
    // Rows from the first and last chunk both persisted (no partial commit).
    expect(await engine.status(started[0].runId)).toBeDefined();
    expect(await engine.status(started[1499].runId)).toBeDefined();
    const { rows } = await pool.query<{ n: string }>(
      "SELECT count(*) AS n FROM graphile_worker.jobs WHERE task_identifier = $1",
      ["flow:run:provision-subject@1"],
    );
    expect(Number(rows[0].n)).toBeGreaterThanOrEqual(1500);
  }, 60_000);
});
