import { backendConformance } from "@iterativeflow/conformance";
import { type Backend, defineFlow, submit } from "@iterativeflow/core";
import type { Pool } from "mysql2/promise";
import type { StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createMysqlBackend } from "#backend";
import { int } from "#codec";
import { tables } from "#schema";
import { type Sql, mysqlPool } from "#sql";
import { inTx } from "#tx";
import { startMysql, stopMysql } from "#test-container";

const skip = process.env.SKIP_TESTCONTAINERS === "1";
const t = tables("");

describe.skipIf(skip)("mysql backend", () => {
  let container: StartedTestContainer;
  let pool: Pool;

  beforeAll(async () => {
    ({ container, pool } = await startMysql());
  }, 240_000);

  afterAll(() => stopMysql({ container, pool }));

  const makeBackend = async (): Promise<Backend> => {
    for (const table of Object.values(t)) await pool.query(`TRUNCATE TABLE ${table}`);
    return createMysqlBackend(mysqlPool(pool));
  };

  backendConformance("mysql", () => makeBackend(), { concurrentWriters: true });

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
      const [row] = await mysqlPool(pool).query("SELECT count(*) AS n FROM app_orders");
      return int(row, "n");
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
      expect(snap?.steps.has("x")).toBe(false);
    });
  });
});
