import { type Pool, createPool } from "mysql2/promise";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createMysqlBackend } from "#backend";
import { applySchema } from "#schema";
import { mysqlPool } from "#sql";

const skip = process.env.SKIP_TESTCONTAINERS === "1";

describe.skipIf(skip)("mysql pending_work() — autoscaling backlog", () => {
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

  beforeEach(async () => {
    for (const table of ["run", "step", "job", "timer", "signal", "cron"]) {
      await pool.query(`TRUNCATE TABLE \`${table}\``);
    }
  });

  const pendingAt = async (asOf: number, names?: readonly string[]): Promise<number> => {
    const [rows] = await pool.query("SELECT `pending_work`(?, ?) AS pending", [
      names ? JSON.stringify(names) : null,
      asOf,
    ]);
    return Number((rows as unknown as { pending: number }[])[0].pending);
  };

  it("agrees with the summed port methods at a fixed instant, filtered by flow name", async () => {
    const be = createMysqlBackend(mysqlPool(pool));
    const asOf = new Date("2030-06-01T00:00:00Z");
    const past = new Date(asOf.getTime() - 60_000);
    const future = new Date(asOf.getTime() + 3_600_000);
    const start = (name: string) =>
      be.store.startRun({ name, version: 1, input: {} }).then((r) => r.runId);

    const leased = await start("a");
    await be.queue.enqueue(leased);
    await be.queue.claim({ limit: 1, leaseMs: 600_000, now: asOf });
    const a1 = await start("a");
    await be.queue.enqueue(a1);
    const b1 = await start("b");
    await be.queue.enqueue(b1);
    const sleeping = await start("a");
    await be.timer.schedule(sleeping, past);
    await be.timer.schedule("orphan-run", past);
    await be.store.upsertCron({
      name: "cron-a",
      schedule: "* * * * *",
      flowName: "a",
      flowVersion: 1,
      input: {},
      nextRunAt: past,
    });
    const futureJob = await start("a");
    await be.queue.enqueue(futureJob, { runAt: future });

    for (const names of [undefined, ["a"], ["b"], []] as const) {
      const ports =
        (await be.queue.depth(asOf, names)).claimable +
        (await be.timer.dueCount(asOf, names)) +
        (await be.store.dueCronCount(asOf, names));
      expect(await pendingAt(asOf.getTime(), names)).toBe(ports);
    }
  });
});
