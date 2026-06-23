import { sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { flow } from "../../builder/flow";
import { createEngine, type Engine } from "../../engine/engine";
import type { Logger } from "../../engine/types";
import { applyFlowSchema } from "../../storage/setup";
import type { WorkflowDb } from "../../storage/db";
import { createServerlessDispatcher } from "../serverless";
import { createPgmqEnqueue, createPgmqQueue, drainAndRunPgmq } from "./pgmq";

const externalUrl = process.env.ITERATIVE_PG_URL;
const skipContainers = process.env.SKIP_TESTCONTAINERS === "1";

const silent: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

const sleepReal = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe.skipIf(skipContainers)("pgmq serverless execution (real pgmq queue)", () => {
  let container: StartedPostgreSqlContainer | undefined;
  let pool: Pool;
  let db: WorkflowDb;
  let engine: Engine;
  let stepRuns: { a: number; b: number };

  beforeAll(async () => {
    let url: string;
    if (externalUrl) {
      url = externalUrl;
    } else {
      container = await new PostgreSqlContainer("ghcr.io/pgmq/pg16-pgmq:latest").start();
      url = container.getConnectionUri();
    }
    pool = new Pool({ connectionString: url, max: 6 });
    db = drizzle({ client: pool }) as unknown as WorkflowDb;
    await applyFlowSchema(db);
    await db.execute(sql`CREATE EXTENSION IF NOT EXISTS pgmq`);
    await createPgmqQueue(db);
  }, 120_000);

  afterAll(async () => {
    await pool?.end();
    await container?.stop();
  });

  beforeEach(async () => {
    // Fresh run state + drained queue per test.
    await db.execute(sql`DELETE FROM workflow.runs`);
    await db.execute(sql`SELECT pgmq.purge_queue('iterativeflow_wakes')`);
    stepRuns = { a: 0, b: 0 };
    engine = createEngine({
      db,
      pool,
      logger: silent,
      worker: { enqueue: createPgmqEnqueue() },
      dispatcher: createServerlessDispatcher(),
      results: "poll",
      reconciler: { graceMs: 0, runningStuckMs: 0 },
    });
    engine.register(
      flow("quick")
        .step("only", () => "ok")
        .output(({ input }) => input)
        .build(),
    );
    engine.register(
      flow("ship")
        .step("a", () => {
          stepRuns.a++;
          return "a-done";
        })
        .sleep("1s")
        .step("b", () => {
          stepRuns.b++;
          return "b-done";
        })
        .signal("approve")
        .output(() => "shipped")
        .build(),
    );
  });

  const tick = async (): Promise<string[]> => (await drainAndRunPgmq(engine, db, { vt: 30 })).ran;
  const statusOf = async (runId: string) => (await engine.status(runId))?.run.status;

  it("advances across a pgmq-delayed sleep and a signal, memoizing steps", async () => {
    const { runId } = await engine.enqueue("ship", 1, {});

    // Cycle 1: step a, then suspend on sleep (sent to pgmq with a ~1s delay).
    expect(await tick()).toEqual([runId]);
    expect(await statusOf(runId)).toBe("sleeping");

    // The delayed message is still invisible.
    expect(await tick()).toEqual([]);

    await sleepReal(1300);

    // Cycle 2: delay elapsed, step b runs, suspend awaiting the signal.
    expect(await tick()).toEqual([runId]);
    expect(await statusOf(runId)).toBe("awaiting_signal");

    expect((await engine.signal(runId, "approve")).kind).toBe("delivered");

    // Cycle 3: signal satisfied → terminal.
    expect(await tick()).toEqual([runId]);
    const final = await engine.status(runId);
    expect(final?.run.status).toBe("done");
    expect(final?.run.output).toBe("shipped");
    expect(stepRuns).toEqual({ a: 1, b: 1 });
  }, 30_000);

  it("reconcile() re-enqueues an orphaned run onto pgmq", async () => {
    const { runId } = await engine.enqueue("quick", 1, {});

    // Drain the wake off pgmq without running it (simulated crash).
    await db.execute(sql`SELECT pgmq.read('iterativeflow_wakes', 0, 10)`);
    await db.execute(sql`SELECT pgmq.purge_queue('iterativeflow_wakes')`);
    expect(await statusOf(runId)).toBe("pending");

    await sleepReal(5);
    expect((await engine.reconcile()).reEnqueued).toBe(1);

    expect(await tick()).toEqual([runId]);
    expect(await statusOf(runId)).toBe("done");
  }, 30_000);
});
