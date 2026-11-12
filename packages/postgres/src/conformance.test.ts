import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { backendConformance } from "@iterativeflow/conformance";
import { type Backend, defineFlow, submit } from "@iterativeflow/core";
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
  backendConformance("postgres", () => makeBackend(), { concurrentWriters: true });

  // These need REAL multi-connection concurrency — memory (single-threaded) can't prove them.
  describe("atomicity + concurrency", () => {
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
          {
            spawn: [
              {
                runId: "44444444-4444-4444-4444-444444444444",
                spec: { name: "c", version: 1, input: {} },
              },
            ],
          },
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
      const backend = await makeBackend();
      await pool.query("CREATE TABLE IF NOT EXISTS app_orders (id text PRIMARY KEY)");
      await pool.query("TRUNCATE app_orders");
      const flow = defineFlow<Record<string, never>, number>({
        name: "fulfil",
        version: 1,
        run: async () => 1,
      });
      const orderCount = async (): Promise<number> =>
        (await pool.query<{ n: number }>("SELECT count(*)::int AS n FROM app_orders")).rows[0].n;
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

      // Commit: the order AND the enqueued run land together (same tx ⇒ the run existing proves the job did).
      const id = await inTx(pool, async (b, tx) => {
        await tx.query("INSERT INTO app_orders (id) VALUES ('o2')");
        return submit(b, flow, {});
      });
      expect(await orderCount()).toBe(1);
      expect(await backend.store.loadRunRow(id)).toBeDefined();
    });
  });
});
