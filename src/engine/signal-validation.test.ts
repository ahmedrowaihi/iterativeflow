import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import type { Pool } from "pg";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { flow } from "../builder/flow";
import type { WorkflowDb } from "../storage/db";
import { applyFlowSchema } from "../storage/setup";
import { createEngine, type Engine } from "./engine";
import { silentLogger } from "./test-helpers";

describe("engine.signal — delivery-time schema validation", () => {
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
      disableReconciler: true,
      enqueue: async () => undefined,
    });
  });
  afterEach(async () => {
    await client.close();
  });

  it("returns kind: 'invalid_payload' when payload fails the declared schema", async () => {
    const ApproveSchema = z.object({ approverId: z.string(), approved: z.boolean() });
    const handle = engine.register(
      flow("approval")
        .step("queue", () => "queued")
        .signal("approve", { schema: ApproveSchema })
        .build(),
    );
    const { runId } = await handle.start({});

    const result = await engine.signal(runId, "approve", { approverId: 42 });

    expect(result.kind).toBe("invalid_payload");
    if (result.kind === "invalid_payload") {
      expect(result.issues.length).toBeGreaterThan(0);
      expect(result.issues[0].message).toBeTruthy();
    }
  });

  it("delivers normally when payload passes the declared schema", async () => {
    const ApproveSchema = z.object({ ok: z.boolean() });
    const handle = engine.register(
      flow("approval2")
        .step("queue", () => "queued")
        .signal("approve", { schema: ApproveSchema })
        .build(),
    );
    const { runId } = await handle.start({});

    const result = await engine.signal(runId, "approve", { ok: true });

    expect(result.kind).toBe("buffered");
  });

  it("skips validation when no schema is declared for the signal", async () => {
    const handle = engine.register(
      flow("approval3")
        .step("queue", () => "queued")
        .signal("approve")
        .build(),
    );
    const { runId } = await handle.start({});

    const result = await engine.signal(runId, "approve", { anything: "goes" });

    expect(result.kind).toBe("buffered");
  });

  it("rejects two divergent schemas for the same signal name at build time", () => {
    const A = z.object({ a: z.string() });
    const B = z.object({ b: z.number() });
    expect(() =>
      flow("conflict").signal("x", { schema: A }).signal("x", { schema: B }).build(),
    ).toThrow(/two different schemas/);
  });
});
