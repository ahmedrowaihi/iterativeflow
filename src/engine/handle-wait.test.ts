import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import type { Pool } from "pg";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { flow } from "../builder/flow";
import type { WorkflowDb } from "../storage/db";
import { applyFlowSchema } from "../storage/setup";
import { createEngine, type Engine } from "./engine";
import { playRunAttempt } from "./run-lifecycle";
import { FlowRegistry } from "./registry";
import { createDrizzleStorage } from "../storage/drizzle";
import { silentLogger, baseRunnerDeps } from "./test-helpers";

describe("handle.wait — immediate-return path (pglite)", () => {
  let client: PGlite;
  let db: WorkflowDb;
  let engine: Engine;

  beforeEach(async () => {
    client = new PGlite();
    await client.waitReady;
    db = drizzle({ client }) as unknown as WorkflowDb;
    await applyFlowSchema(db);
    engine = createEngine({
      db,
      pool: {} as unknown as Pool,
      logger: silentLogger,
      reconciler: false,
      worker: { enqueue: async () => undefined },
    });
  });
  afterEach(async () => {
    await client.close();
  });

  it("resolves immediately when the step has already finished", async () => {
    const def = flow("done")
      .step("once", () => "x")
      .build();
    const handle = engine.register(def);
    const { runId } = await handle.start({});

    // Drive the run with the lifecycle directly (no graphile worker in pglite).
    const registry = new FlowRegistry();
    registry.register({ name: def.name, version: def.version, run: def.body });
    const storage = createDrizzleStorage({
      db,
      logger: silentLogger,
      enqueue: async () => undefined,
    });
    await playRunAttempt({ ...baseRunnerDeps(), registry, storage }, runId);

    await expect(
      handle.wait(runId, { until: { step: "once" }, timeoutMs: 500 }),
    ).resolves.toBeUndefined();
  });

  it("times out when the step hasn't finished and no NOTIFY is forthcoming", async () => {
    const def = flow("pending")
      .step("never-runs", () => "x")
      .build();
    const handle = engine.register(def);
    const { runId } = await handle.start({});

    await expect(
      handle.wait(runId, { until: { step: "never-runs" }, timeoutMs: 200 }),
    ).rejects.toThrow(/handle.wait timed out/);
  });
});
