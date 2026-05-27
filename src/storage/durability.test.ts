import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { eq, sql } from "drizzle-orm";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Logger, Storage } from "../engine/types";
import { createDrizzleStorage, type TxEnqueue } from "./drizzle";
import type { WorkflowDb } from "./db";
import { events, hooks, runs, timers } from "./schema";
import { applyWorkflowSchema } from "./setup";

const silent: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

interface Recorded {
  runId: string;
  runAt?: Date;
}

interface Harness {
  db: WorkflowDb;
  storage: Storage;
  enqueues: Recorded[];
  close: () => Promise<void>;
}

const setup = async (): Promise<Harness> => {
  const client = new PGlite();
  await client.waitReady;
  const db = drizzle({ client }) as unknown as WorkflowDb;
  await applyWorkflowSchema(db);
  const enqueues: Recorded[] = [];
  const enqueue: TxEnqueue = async (_tx, runId, opts) => {
    enqueues.push({ runId, runAt: opts?.runAt });
  };
  const storage = createDrizzleStorage({ db, logger: silent, enqueue });
  return { db, storage, enqueues, close: () => client.close() };
};

const ageRun = async (db: WorkflowDb, runId: string, ageMs: number) => {
  const stamp = new Date(Date.now() - ageMs);
  await db.execute(sql`UPDATE workflow.runs SET updated_at = ${stamp} WHERE id = ${runId}::uuid`);
};

describe("storage durability", () => {
  let h: Harness;
  beforeEach(async () => {
    h = await setup();
  });
  afterEach(async () => {
    await h.close();
  });

  describe("signalHook", () => {
    it("delivers + enqueues atomically when a hook is armed", async () => {
      const { runId } = await h.storage.createRun({
        name: "delivered",
        version: 1,
        input: {},
      });
      const armed = await h.storage.armOrConsumeHook(runId, "hook:s", undefined);
      expect(armed.kind).toBe("armed");

      const result = await h.storage.signalHook(runId, "s", { v: 1 });
      expect(result).toEqual({ kind: "delivered", hookKey: "hook:s" });

      const hook = await h.storage.loadHook(runId, "hook:s");
      expect(hook?.delivered).toBe(true);
      expect(hook?.payload).toEqual({ v: 1 });
      expect(h.enqueues).toEqual([{ runId, runAt: undefined }]);
    });

    it("buffers without enqueueing when no hook is armed yet", async () => {
      const { runId } = await h.storage.createRun({
        name: "buffered",
        version: 1,
        input: {},
      });

      const result = await h.storage.signalHook(runId, "s", { v: 2 });
      expect(result).toEqual({ kind: "buffered", hookKey: "hook:s" });

      const hook = await h.storage.loadHook(runId, "hook:s");
      expect(hook?.delivered).toBe(true);
      expect(hook?.payload).toEqual({ v: 2 });
      expect(h.enqueues).toEqual([]);
    });

    it("returns duplicate on second signal for the same hook", async () => {
      const { runId } = await h.storage.createRun({
        name: "dup",
        version: 1,
        input: {},
      });
      const first = await h.storage.signalHook(runId, "s", "v1");
      const second = await h.storage.signalHook(runId, "s", "v2");
      expect(first.kind).toBe("buffered");
      expect(second.kind).toBe("duplicate");
      const hook = await h.storage.loadHook(runId, "hook:s");
      expect(hook?.payload).toBe("v1");
    });

    it("rolls back deliver + enqueue together when enqueue throws", async () => {
      const { runId } = await h.storage.createRun({
        name: "rollback",
        version: 1,
        input: {},
      });
      await h.storage.armOrConsumeHook(runId, "hook:s", undefined);

      const flaky: TxEnqueue = async () => {
        throw new Error("queue down");
      };
      const flakyStorage = createDrizzleStorage({
        db: h.db,
        logger: silent,
        enqueue: flaky,
      });

      await expect(flakyStorage.signalHook(runId, "s", { v: 1 })).rejects.toThrow("queue down");

      const hook = await h.storage.loadHook(runId, "hook:s");
      expect(hook?.delivered).toBe(false);
    });
  });

  describe("armOrConsumeHook", () => {
    it("arms a new hook and records the event", async () => {
      const { runId } = await h.storage.createRun({
        name: "arm",
        version: 1,
        input: {},
      });
      const result = await h.storage.armOrConsumeHook(runId, "hook:x", undefined);
      expect(result).toEqual({ kind: "armed" });
      const hook = await h.storage.loadHook(runId, "hook:x");
      expect(hook?.delivered).toBe(false);
    });

    it("consumes a buffered hook instead of arming (deliver-vs-arm race)", async () => {
      const { runId } = await h.storage.createRun({
        name: "race",
        version: 1,
        input: {},
      });
      await h.storage.preDeliverHook(runId, "hook:x", { value: "raced" });

      const result = await h.storage.armOrConsumeHook(runId, "hook:x", undefined);
      expect(result).toEqual({ kind: "consumed", payload: { value: "raced" } });
    });
  });

  describe("reenqueueOrphans", () => {
    it("re-enqueues a stale pending run", async () => {
      const { runId } = await h.storage.createRun({
        name: "orphan-pending",
        version: 1,
        input: {},
      });
      await ageRun(h.db, runId, 5 * 60_000);

      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
      });
      expect(n).toBe(1);
      expect(h.enqueues).toEqual([{ runId, runAt: undefined }]);
    });

    it("re-enqueues a stale waiting run with a delivered hook", async () => {
      const { runId } = await h.storage.createRun({
        name: "orphan-waiting",
        version: 1,
        input: {},
      });
      await h.storage.markWaiting(runId);
      await h.storage.preDeliverHook(runId, "hook:x", { score: 7 });
      await ageRun(h.db, runId, 5 * 60_000);

      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
      });
      expect(n).toBe(1);
      expect(h.enqueues[0]?.runId).toBe(runId);
    });

    it("re-enqueues a sleeping run whose timer is past due", async () => {
      const { runId } = await h.storage.createRun({
        name: "orphan-sleeping-due",
        version: 1,
        input: {},
      });
      await h.storage.markSleeping(runId);
      await h.storage.createTimer(runId, "sleep", new Date(Date.now() - 1_000));
      await ageRun(h.db, runId, 5 * 60_000);

      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
      });
      expect(n).toBe(1);
      expect(h.enqueues[0]?.runId).toBe(runId);
      expect(h.enqueues[0]?.runAt).toBeUndefined();
    });

    it("skips a sleeping run whose timer is still in the future", async () => {
      const { runId } = await h.storage.createRun({
        name: "orphan-sleeping-future",
        version: 1,
        input: {},
      });
      await h.storage.markSleeping(runId);
      await h.storage.createTimer(runId, "sleep", new Date(Date.now() + 60_000));
      await ageRun(h.db, runId, 5 * 60_000);

      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
      });
      expect(n).toBe(0);
      expect(h.enqueues.length).toBe(0);
    });

    it("re-enqueues a waiting run whose hook expired without delivery", async () => {
      const { runId } = await h.storage.createRun({
        name: "orphan-waiting-timeout",
        version: 1,
        input: {},
      });
      await h.storage.markWaiting(runId);
      await h.storage.armOrConsumeHook(runId, "hook:to", new Date(Date.now() - 1_000));
      await ageRun(h.db, runId, 5 * 60_000);

      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
      });
      expect(n).toBe(1);
      expect(h.enqueues[0]?.runId).toBe(runId);
    });

    it("skips runs touched inside the grace window", async () => {
      await h.storage.createRun({ name: "fresh", version: 1, input: {} });
      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
      });
      expect(n).toBe(0);
      expect(h.enqueues.length).toBe(0);
    });

    it("re-enqueues a stuck running run whose updated_at is past the stuck cutoff", async () => {
      const { runId } = await h.storage.createRun({
        name: "stuck-running",
        version: 1,
        input: {},
      });
      await h.storage.markRunning(runId);
      await ageRun(h.db, runId, 15 * 60_000);

      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
        runningStuckOlderThan: new Date(Date.now() - 10 * 60_000),
      });
      expect(n).toBe(1);
      expect(h.enqueues[0]?.runId).toBe(runId);
    });

    it("skips a running run whose heartbeat is still fresh", async () => {
      const { runId } = await h.storage.createRun({
        name: "active-running",
        version: 1,
        input: {},
      });
      await h.storage.markRunning(runId);
      await ageRun(h.db, runId, 2 * 60_000);

      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
        runningStuckOlderThan: new Date(Date.now() - 10 * 60_000),
      });
      expect(n).toBe(0);
      expect(h.enqueues.length).toBe(0);
    });

    it("skips running runs entirely when no stuck cutoff is supplied", async () => {
      const { runId } = await h.storage.createRun({
        name: "no-stuck-check",
        version: 1,
        input: {},
      });
      await h.storage.markRunning(runId);
      await ageRun(h.db, runId, 24 * 60 * 60_000);

      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
      });
      expect(n).toBe(0);
      expect(runId).toBeTruthy();
    });

    it("ignores terminal runs", async () => {
      const { runId } = await h.storage.createRun({
        name: "terminal",
        version: 1,
        input: {},
      });
      await h.storage.markCompleted(runId, "done");
      await ageRun(h.db, runId, 5 * 60_000);

      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
      });
      expect(n).toBe(0);
      expect(h.enqueues.length).toBe(0);
      const rows = await h.db.select({ status: runs.status }).from(runs).where(eq(runs.id, runId));
      expect(rows[0]?.status).toBe("done");
    });
  });

  describe("loadRunDetail + loadOutput", () => {
    it("loadRunDetail returns the run with steps/timers/hooks", async () => {
      const { runId } = await h.storage.createRun({
        name: "detail",
        version: 1,
        input: { v: 1 },
      });
      await h.storage.startStep(runId, "s1", 1);
      await h.storage.finishStep({
        runId,
        stepKey: "s1",
        status: "ok",
        attempts: 1,
        result: "ok",
      });
      await h.storage.createTimer(runId, "sleep", new Date());
      await h.storage.armOrConsumeHook(runId, "hook:x", undefined);

      const detail = await h.storage.loadRunDetail(runId);
      expect(detail?.run.name).toBe("detail");
      expect(detail?.steps.length).toBe(1);
      expect(detail?.timers.length).toBe(1);
      expect(detail?.hooks.length).toBe(1);
    });

    it("loadRunDetail returns undefined for an unknown run", async () => {
      const detail = await h.storage.loadRunDetail("00000000-0000-0000-0000-000000000000");
      expect(detail).toBeUndefined();
    });

    it("loadOutput returns undefined for non-terminal runs", async () => {
      const { runId } = await h.storage.createRun({
        name: "pending-out",
        version: 1,
        input: {},
      });
      expect(await h.storage.loadOutput(runId)).toBeUndefined();
    });

    it("loadOutput returns the output once the run is done", async () => {
      const { runId } = await h.storage.createRun({
        name: "done-out",
        version: 1,
        input: {},
      });
      await h.storage.markCompleted(runId, { final: true });
      expect(await h.storage.loadOutput(runId)).toEqual({ final: true });
    });
  });

  describe("prune", () => {
    it("pruneEvents deletes only events older than the cutoff", async () => {
      const { runId } = await h.storage.createRun({
        name: "old",
        version: 1,
        input: {},
      });
      await h.storage.recordEvent({ runId, type: "started" });
      await h.storage.recordEvent({ runId, type: "completed" });
      await h.db.execute(sql`UPDATE workflow.events SET at = NOW() - INTERVAL '10 minutes'`);
      const fresh = await h.storage.createRun({
        name: "fresh",
        version: 1,
        input: {},
      });
      await h.storage.recordEvent({ runId: fresh.runId, type: "started" });

      const n = await h.storage.pruneEvents({
        olderThan: new Date(Date.now() - 5 * 60_000),
      });
      expect(n).toBe(2);

      const remaining = await h.db.select({ id: events.id }).from(events);
      expect(remaining.length).toBe(1);
    });

    it("pruneRuns cascades to steps/timers/hooks/events", async () => {
      const { runId } = await h.storage.createRun({
        name: "doomed",
        version: 1,
        input: {},
      });
      await h.storage.startStep(runId, "s1", 1);
      await h.storage.finishStep({
        runId,
        stepKey: "s1",
        status: "ok",
        attempts: 1,
        result: 42,
      });
      await h.storage.createTimer(runId, "sleep", new Date());
      await h.storage.armOrConsumeHook(runId, "hook:x", undefined);
      await h.storage.recordEvent({ runId, type: "started" });

      await h.storage.markCompleted(runId, "done");
      await h.db.execute(
        sql`UPDATE workflow.runs SET updated_at = NOW() - INTERVAL '10 minutes' WHERE id = ${runId}::uuid`,
      );

      const n = await h.storage.pruneRuns({
        olderThan: new Date(Date.now() - 5 * 60_000),
      });
      expect(n).toBe(1);

      const rs = await h.db.select().from(runs).where(eq(runs.id, runId));
      expect(rs.length).toBe(0);

      const ev = await h.db.select().from(events).where(eq(events.runId, runId));
      expect(ev.length).toBe(0);

      const tm = await h.db.select().from(timers).where(eq(timers.runId, runId));
      expect(tm.length).toBe(0);

      const hk = await h.db.select().from(hooks).where(eq(hooks.runId, runId));
      expect(hk.length).toBe(0);
    });

    it("pruneRuns ignores non-terminal runs by default", async () => {
      const live = await h.storage.createRun({
        name: "alive",
        version: 1,
        input: {},
      });
      await h.storage.markRunning(live.runId);
      await h.db.execute(
        sql`UPDATE workflow.runs SET updated_at = NOW() - INTERVAL '10 minutes' WHERE id = ${live.runId}::uuid`,
      );

      const n = await h.storage.pruneRuns({
        olderThan: new Date(Date.now() - 5 * 60_000),
      });
      expect(n).toBe(0);

      const rs = await h.db.select().from(runs).where(eq(runs.id, live.runId));
      expect(rs.length).toBe(1);
    });
  });
});
