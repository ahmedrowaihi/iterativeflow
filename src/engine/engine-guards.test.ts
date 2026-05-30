import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { sql } from "drizzle-orm";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Pool } from "pg";
import type { Logger } from "./types";
import type { WorkflowDb } from "../storage/db";
import { applyFlowSchema } from "../storage/setup";
import { createEngine } from "./engine";

const silent: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

const stubEngine = () =>
  createEngine({
    db: {} as unknown as WorkflowDb,
    pool: {} as unknown as Pool,
    logger: silent,
  });

describe("engine guards", () => {
  it("rejects user crons that collide with the reserved reconciler name", () => {
    const engine = stubEngine();
    expect(() =>
      engine.defineCron({
        name: "__iterativeflow_reconcile",
        schedule: "* * * * *",
        run: async () => undefined,
      }),
    ).toThrow(/reserved by the engine/);
  });

  it("accepts a normal cron name", () => {
    const engine = stubEngine();
    expect(() =>
      engine.defineCron({
        name: "user-cron",
        schedule: "* * * * *",
        run: async () => undefined,
      }),
    ).not.toThrow();
  });

  it("rejects an invalid cron pattern at defineCron time", () => {
    const engine = stubEngine();
    expect(() =>
      engine.defineCron({
        name: "broken",
        schedule: "not a cron",
        run: async () => undefined,
      }),
    ).toThrow(/Invalid cron pattern/);
  });

  describe("boot validators", () => {
    it("throws when the logger is missing a method", () => {
      expect(() =>
        createEngine({
          db: {} as unknown as WorkflowDb,
          pool: {} as unknown as Pool,
          logger: {
            debug: () => undefined,
            info: () => undefined,
            warn: () => undefined,
          } as unknown as Logger,
        }),
      ).toThrow(/logger\.error must be a function/);
    });

    it("throws when retention durations are invalid", () => {
      expect(() =>
        createEngine({
          db: {} as unknown as WorkflowDb,
          pool: {} as unknown as Pool,
          logger: silent,
          retention: { eventsOlderThan: "-5m" },
        }),
      ).toThrow(/Duration must be non-negative/);
    });

    it("warns when concurrency exceeds pool.max", () => {
      const warned: { msg: string; payload?: Record<string, unknown> }[] = [];
      const noisyLogger: Logger = {
        debug: () => undefined,
        info: () => undefined,
        warn: (msg, payload) => warned.push({ msg, payload }),
        error: () => undefined,
      };
      createEngine({
        db: {} as unknown as WorkflowDb,
        pool: { options: { max: 5 } } as unknown as Pool,
        logger: noisyLogger,
        concurrency: 20,
      });
      expect(warned).toEqual([
        { msg: "flow.config.pool_too_small", payload: { concurrency: 20, poolMax: 5 } },
      ]);
    });

    it("warns when runningStuckMs is smaller than defaultStepTimeoutMs", () => {
      const warned: { msg: string; payload?: Record<string, unknown> }[] = [];
      const noisyLogger: Logger = {
        debug: () => undefined,
        info: () => undefined,
        warn: (msg, payload) => warned.push({ msg, payload }),
        error: () => undefined,
      };
      createEngine({
        db: {} as unknown as WorkflowDb,
        pool: {} as unknown as Pool,
        logger: noisyLogger,
        runningStuckMs: 60_000,
        defaultStepTimeoutMs: 30 * 60_000,
      });
      const hit = warned.find((w) => w.msg === "flow.config.stuck_shorter_than_step_timeout");
      expect(hit).toBeDefined();
      expect(hit?.payload).toMatchObject({
        runningStuckMs: 60_000,
        defaultStepTimeoutMs: 30 * 60_000,
      });
    });

    it("does NOT warn when defaultStepTimeoutMs is unset", () => {
      const warned: { msg: string; payload?: Record<string, unknown> }[] = [];
      const noisyLogger: Logger = {
        debug: () => undefined,
        info: () => undefined,
        warn: (msg, payload) => warned.push({ msg, payload }),
        error: () => undefined,
      };
      createEngine({
        db: {} as unknown as WorkflowDb,
        pool: {} as unknown as Pool,
        logger: noisyLogger,
        runningStuckMs: 60_000,
      });
      expect(
        warned.find((w) => w.msg === "flow.config.stuck_shorter_than_step_timeout"),
      ).toBeUndefined();
    });
  });

  describe("schema fingerprint", () => {
    let client: PGlite;
    let db: WorkflowDb;

    beforeEach(async () => {
      client = new PGlite();
      await client.waitReady;
      db = drizzle({ client }) as unknown as WorkflowDb;
    });
    afterEach(async () => {
      await client.close();
    });

    it("handle.start throws SCHEMA_MISMATCH when the schema is not applied", async () => {
      const engine = createEngine({
        db,
        pool: {} as unknown as Pool,
        logger: silent,
        disableReconciler: true,
      });
      const handle = engine.register({ name: "f", version: 1, body: () => "ok" });
      await expect(handle.start({})).rejects.toThrow(/SCHEMA_MISMATCH/);
    });

    it("handle.start throws SCHEMA_MISMATCH when an engine-required table is missing", async () => {
      await applyFlowSchema(db);
      await db.execute(sql`DROP TABLE workflow.signals CASCADE`);
      const engine = createEngine({
        db,
        pool: {} as unknown as Pool,
        logger: silent,
        disableReconciler: true,
      });
      const handle = engine.register({ name: "f", version: 1, body: () => "ok" });
      await expect(handle.start({})).rejects.toThrow(/SCHEMA_MISMATCH/);
    });

    it("handle.start succeeds when the schema is at v2", async () => {
      await applyFlowSchema(db);
      const engine = createEngine({
        db,
        pool: {} as unknown as Pool,
        logger: silent,
        disableReconciler: true,
        enqueue: async () => undefined,
      });
      const handle = engine.register({ name: "f", version: 1, body: () => "ok" });
      const { runId } = await handle.start({});
      expect(runId).toBeTruthy();
    });
  });
});
