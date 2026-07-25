import {
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
import { mysqlPool } from "#sql";

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
  timerConformance("mysql", async () => (await makeBackend()).timer);
  wakeupConformance("mysql", async () => (await makeBackend()).wakeup);
  outboxConformance("mysql", () => makeBackend());
  signalConformance("mysql", () => makeBackend());
  reconcileConformance("mysql", () => makeBackend());
  cronConformance("mysql", async () => (await makeBackend()).store);
  engineConformance("mysql", () => makeBackend());

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
