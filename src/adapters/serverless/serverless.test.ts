import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createEngine } from "../../engine/engine";
import { silentLogger } from "../../engine/test-helpers";
import type { Engine } from "../../engine/engine";
import type { WorkflowDb } from "../../storage/db";
import { applyFlowSchema } from "../../storage/setup";
import { flow } from "../../builder/flow";
import { createOutboxEnqueue, createWakeOutboxTable, drainDueWakes } from "./outbox";
import { createServerlessDispatcher } from "./dispatcher";
import { drainAndRun } from "./runner";

// A serverless engine never connects to a pg Pool: it drives runs via
// handleRun + drainDueWakes and reads status() — no listen(), no LISTEN.
const fakePool = { options: { max: 5 } } as never;

const sleepReal = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("serverless execution (outbox + handleRun, no resident worker)", () => {
  let client: PGlite;
  let db: WorkflowDb;
  let engine: Engine;
  let stepRuns: { a: number; b: number };

  beforeEach(async () => {
    client = new PGlite();
    await client.waitReady;
    db = drizzle({ client }) as unknown as WorkflowDb;
    await applyFlowSchema(db);
    await createWakeOutboxTable(db);

    stepRuns = { a: 0, b: 0 };
    engine = createEngine({
      db,
      pool: fakePool,
      logger: silentLogger,
      worker: { enqueue: createOutboxEnqueue() },
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

  afterEach(async () => {
    await client.close();
  });

  /** Drain every due wake and advance each run one cycle. Returns ids advanced. */
  const tick = async (): Promise<string[]> => (await drainAndRun(engine, db)).ran;

  const statusOf = async (runId: string) => (await engine.status(runId))?.run.status;

  it("advances a run across sleep + signal suspends, memoizing steps", async () => {
    const { runId } = await engine.enqueue("ship", 1, {});

    // Cycle 1: step a runs, then the run suspends on sleep.
    expect(await tick()).toEqual([runId]);
    expect(await statusOf(runId)).toBe("sleeping");

    // Sleep is in the future — nothing is due yet.
    expect(await tick()).toEqual([]);

    await sleepReal(1300);

    // Cycle 2: sleep elapsed, step b runs, run suspends awaiting the signal.
    expect(await tick()).toEqual([runId]);
    expect(await statusOf(runId)).toBe("awaiting_signal");

    // Deliver the signal — this re-arms the wake through the same outbox.
    const delivery = await engine.signal(runId, "approve");
    expect(delivery.kind).toBe("delivered");

    // Cycle 3: signal satisfied, run reaches terminal.
    expect(await tick()).toEqual([runId]);

    const final = await engine.status(runId);
    expect(final?.run.status).toBe("done");
    expect(final?.run.output).toBe("shipped");
    // Each step body ran exactly once despite three replays.
    expect(stepRuns).toEqual({ a: 1, b: 1 });
  });

  it("drainDueWakes returns nothing once a run is terminal", async () => {
    const { runId } = await engine.enqueue("ship", 1, {});
    await tick();
    await sleepReal(1300);
    await tick();
    await engine.signal(runId, "approve");
    await tick();
    expect(await statusOf(runId)).toBe("done");
    expect(await drainDueWakes(db, { now: new Date() })).toEqual([]);
  });

  it("reconcile() recovers an orphaned run lost between drain and handleRun", async () => {
    const { runId } = await engine.enqueue("quick", 1, {});

    // Simulate a crash: the wake is drained off the outbox but never run.
    expect(await drainDueWakes(db, { now: new Date() })).toEqual([runId]);
    expect(await statusOf(runId)).toBe("pending");

    await sleepReal(5); // clear the grace window (graceMs: 0)
    const { reEnqueued } = await engine.reconcile();
    expect(reEnqueued).toBe(1);

    // The re-enqueued run now drains and completes.
    expect(await tick()).toEqual([runId]);
    expect(await statusOf(runId)).toBe("done");
  });

  it("handle.result() on a non-terminal run throws under results: 'poll'", async () => {
    const handle = engine.register(
      flow("blocker")
        .signal("never")
        .output(() => "x")
        .build(),
    );
    const { runId } = await handle.start({});
    await expect(handle.result(runId)).rejects.toThrow(/poll engine\.status/);
  });
});
