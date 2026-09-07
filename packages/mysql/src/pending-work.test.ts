import type { Pool } from "mysql2/promise";
import type { StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createMysqlBackend } from "#backend";
import { mysqlPool } from "#sql";
import { startMysql, stopMysql } from "#test-container";

const skip = process.env.SKIP_TESTCONTAINERS === "1";

describe.skipIf(skip)("mysql pending_work() — autoscaling backlog", () => {
  let container: StartedTestContainer;
  let pool: Pool;

  beforeAll(async () => {
    ({ container, pool } = await startMysql());
  }, 240_000);

  afterAll(() => stopMysql({ container, pool }));

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
