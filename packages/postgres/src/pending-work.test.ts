import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { drainTimers, runDueCrons } from "@iterativeflow/core";
import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createPgBackend } from "#backend";
import { applySchema } from "#schema";
import { pgPool } from "#sql";

const skip = process.env.SKIP_TESTCONTAINERS === "1";

describe.skipIf(skip)("workflow.pending_work() — autoscaling backlog", () => {
  let container: StartedPostgreSqlContainer;
  let pool: Pool;

  beforeAll(async () => {
    container = await new PostgreSqlContainer("postgres:16-alpine").start();
    pool = new Pool({ connectionString: container.getConnectionUri() });
    pool.on("error", () => undefined);
    await applySchema(pgPool(pool));
  }, 180_000);

  afterAll(async () => {
    await pool?.end().catch(() => undefined);
    await container?.stop().catch(() => undefined);
  });

  beforeEach(async () => {
    await pool.query(
      'TRUNCATE "workflow".run, "workflow".job, "workflow".timer, "workflow".cron,' +
        ' "workflow".step, "workflow".signal, "workflow".event RESTART IDENTITY CASCADE',
    );
  });

  const pending = async (names?: readonly string[]): Promise<number> => {
    const { rows } = await pool.query<{ pending: string }>(
      'SELECT "workflow".pending_work($1) AS pending',
      [names ?? null],
    );
    return Number(rows[0].pending);
  };

  const pendingAt = async (asOf: Date, names?: readonly string[]): Promise<number> => {
    const { rows } = await pool.query<{ pending: string }>(
      'SELECT "workflow".pending_work($1, $2) AS pending',
      [names ?? null, asOf],
    );
    return Number(rows[0].pending);
  };

  const start = (be: ReturnType<typeof createPgBackend>, name: string, version = 1) =>
    be.store.startRun({ name, version, input: {} }).then((r) => r.runId);

  it("counts claimable jobs + due timers + due crons, and filters by flow name", async () => {
    const be = createPgBackend(pgPool(pool));
    const past = new Date(Date.now() - 60_000);
    const future = new Date(Date.now() + 3_600_000);

    const leased = await start(be, "a");
    await be.queue.enqueue(leased);
    await be.queue.claim({ limit: 1, leaseMs: 600_000 });

    const claimableA = await start(be, "a");
    await be.queue.enqueue(claimableA);
    const sleepingA = await start(be, "a");
    await be.timer.schedule(sleepingA, past);
    await be.store.upsertCron({
      name: "cron-a",
      schedule: "* * * * *",
      flowName: "a",
      flowVersion: 1,
      input: {},
      nextRunAt: past,
    });
    const claimableB = await start(be, "b");
    await be.queue.enqueue(claimableB);
    const futureA = await start(be, "a");
    await be.queue.enqueue(futureA, { runAt: future });

    expect(await pending()).toBe(4);
    expect(await pending(["a"])).toBe(3);
    expect(await pending(["b"])).toBe(1);
  });

  it("tracks the tick: draining and firing keep work counted, claiming clears it, the future is excluded", async () => {
    const be = createPgBackend(pgPool(pool));
    const past = new Date(Date.now() - 60_000);
    const future = new Date(Date.now() + 3_600_000);

    const job = await start(be, "q");
    await be.queue.enqueue(job);
    const sleeperDue = await start(be, "q");
    await be.timer.schedule(sleeperDue, past);
    const sleeperFuture = await start(be, "q");
    await be.timer.schedule(sleeperFuture, future);
    await be.store.upsertCron({
      name: "cron-q",
      schedule: "* * * * *",
      flowName: "q",
      flowVersion: 1,
      input: {},
      nextRunAt: past,
    });

    expect(await pending()).toBe(3);
    await drainTimers(be, { limit: 10 });
    expect(await pending()).toBe(3);
    await runDueCrons(be, () => new Date());
    expect(await pending()).toBe(3);
    for (let i = 0; i < 5; i++) {
      if ((await be.queue.claim({ limit: 10, leaseMs: 600_000 })).length === 0) break;
    }
    expect(await pending()).toBe(0);
  });

  it("the SQL function agrees with the summed port methods at a fixed instant", async () => {
    const be = createPgBackend(pgPool(pool));
    const asOf = new Date("2030-06-01T00:00:00Z");
    const past = new Date(asOf.getTime() - 60_000);
    const future = new Date(asOf.getTime() + 3_600_000);

    const leased = await start(be, "a");
    await be.queue.enqueue(leased);
    await be.queue.claim({ limit: 1, leaseMs: 600_000, now: asOf });
    const claimableA = await start(be, "a");
    await be.queue.enqueue(claimableA);
    const sleepingA = await start(be, "a");
    await be.timer.schedule(sleepingA, past);
    await be.timer.schedule("orphan-run", past);
    await be.store.upsertCron({
      name: "cron-a",
      schedule: "* * * * *",
      flowName: "a",
      flowVersion: 1,
      input: {},
      nextRunAt: past,
    });
    const claimableB = await start(be, "b");
    await be.queue.enqueue(claimableB);
    const futureA = await start(be, "a");
    await be.queue.enqueue(futureA, { runAt: future });

    for (const names of [undefined, ["a"], ["b"], []] as const) {
      const ports =
        (await be.queue.depth(asOf, names)).claimable +
        (await be.timer.dueCount(asOf, names)) +
        (await be.store.dueCronCount(asOf, names));
      expect(await pendingAt(asOf, names)).toBe(ports);
    }
  });

  it("filters by flow name, not version — consistent with claim's name-only shard", async () => {
    const be = createPgBackend(pgPool(pool));
    const v1 = await start(be, "tc", 1);
    await be.queue.enqueue(v1);
    const v2 = await start(be, "tc", 2);
    await be.queue.enqueue(v2);
    expect(await pending(["tc"])).toBe(2);
  });
});
