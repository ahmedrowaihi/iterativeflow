import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { eq, sql } from "drizzle-orm";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Logger, Storage } from "../engine/types";
import { createDrizzleStorage, type TxEnqueue } from "./drizzle";
import type { WorkflowDb } from "./db";
import { events, runs, signals, timers } from "./schema";
import { applyFlowSchema } from "./setup";

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
  await applyFlowSchema(db);
  const enqueues: Recorded[] = [];
  const enqueue: TxEnqueue = async (_tx, job, opts) => {
    enqueues.push({ runId: job.runId, runAt: opts?.runAt });
  };
  const storage = createDrizzleStorage({ db, logger: silent, enqueue });
  return { db, storage, enqueues, close: () => client.close() };
};

const ageRun = async (db: WorkflowDb, runId: string, ageMs: number) => {
  const stamp = new Date(Date.now() - ageMs);
  await db.execute(sql`UPDATE workflow.runs SET updated_at = ${stamp} WHERE id = ${runId}::uuid`);
};

const stuckCutoff = (): Date => new Date(Date.now() - 10 * 60_000);

const setAttempts = async (db: WorkflowDb, runId: string, attempts: number) => {
  await db.execute(sql`UPDATE workflow.runs SET attempts = ${attempts} WHERE id = ${runId}::uuid`);
};

const eventTypes = async (db: WorkflowDb, runId: string): Promise<string[]> => {
  const rows = await db.select({ type: events.type }).from(events).where(eq(events.runId, runId));
  return rows.map((r) => r.type);
};

describe("storage durability", () => {
  let h: Harness;
  beforeEach(async () => {
    h = await setup();
  });
  afterEach(async () => {
    await h.close();
  });

  describe("claimRun", () => {
    it("claims a pending run: bumps attempts, marks running, returns snapshot", async () => {
      const { runId } = await h.storage.createRun({
        name: "claim-fresh",
        version: 1,
        input: { v: 1 },
      });
      const result = await h.storage.claimRun(runId);
      expect(result.kind).toBe("claimed");
      if (result.kind !== "claimed") return;
      expect(result.claim.run.status).toBe("running");
      expect(result.claim.run.attempts).toBe(1);
      expect(result.claim.resumed).toBe(false);
      expect(result.claim.snapshot.steps.size).toBe(0);
    });

    it("returns 'lost' when a second claim arrives while the first is still running", async () => {
      const { runId } = await h.storage.createRun({
        name: "claim-double",
        version: 1,
        input: {},
      });
      const first = await h.storage.claimRun(runId);
      expect(first.kind).toBe("claimed");
      const second = await h.storage.claimRun(runId);
      expect(second.kind).toBe("lost");
    });

    it("returns 'terminal' for a completed run", async () => {
      const { runId } = await h.storage.createRun({
        name: "claim-terminal",
        version: 1,
        input: {},
      });
      await h.storage.markCompleted(runId, "ok");
      const result = await h.storage.claimRun(runId);
      expect(result.kind).toBe("terminal");
    });

    it("returns 'missing' for an unknown run", async () => {
      const result = await h.storage.claimRun("00000000-0000-0000-0000-000000000000");
      expect(result.kind).toBe("missing");
    });

    it("marks 'resumed' when claiming a sleeping run and includes timer snapshot", async () => {
      const { runId } = await h.storage.createRun({
        name: "claim-resume",
        version: 1,
        input: {},
      });
      await h.storage.markSleeping(runId);
      await h.storage.createTimer(runId, "sleep", new Date(Date.now() - 1_000));
      const result = await h.storage.claimRun(runId);
      expect(result.kind).toBe("claimed");
      if (result.kind !== "claimed") return;
      expect(result.claim.resumed).toBe(true);
      expect(result.claim.snapshot.timers.has("sleep")).toBe(true);
    });
  });

  describe("getSchemaVersion", () => {
    it("returns 2 when every consumer-supplied table is present", async () => {
      expect(await h.storage.getSchemaVersion()).toBe(2);
    });

    it("returns 0 when an engine-required table is missing", async () => {
      await h.db.execute(sql`DROP TABLE workflow.signals CASCADE`);
      expect(await h.storage.getSchemaVersion()).toBe(0);
    });

    it("returns 0 when the schema is not applied at all", async () => {
      await h.db.execute(sql`DROP SCHEMA workflow CASCADE`);
      expect(await h.storage.getSchemaVersion()).toBe(0);
    });
  });

  describe("child runs (invokeBudget, findChildRun)", () => {
    it("invokeBudget reports depth + direct child count in one call", async () => {
      const root = await h.storage.createRun({ name: "root", version: 1, input: {} });
      const c1 = await h.storage.createRun({
        name: "c1",
        version: 1,
        input: {},
        parentRunId: root.runId,
        parentCursorKey: "invoke:c1@1",
      });
      const c2 = await h.storage.createRun({
        name: "c2",
        version: 1,
        input: {},
        parentRunId: c1.runId,
        parentCursorKey: "invoke:c2@1",
      });
      for (let i = 0; i < 2; i++) {
        await h.storage.createRun({
          name: `sibling${i}`,
          version: 1,
          input: {},
          parentRunId: root.runId,
          parentCursorKey: `invoke:sibling${i}@1`,
        });
      }

      expect(await h.storage.invokeBudget(root.runId)).toEqual({ depth: 1, childCount: 3 });
      expect(await h.storage.invokeBudget(c1.runId)).toEqual({ depth: 2, childCount: 1 });
      expect(await h.storage.invokeBudget(c2.runId)).toEqual({ depth: 3, childCount: 0 });
    });

    it("findChildRun locates the child by (parentRunId, parentCursorKey)", async () => {
      const root = await h.storage.createRun({ name: "root", version: 1, input: {} });
      const child = await h.storage.createRun({
        name: "c",
        version: 1,
        input: {},
        parentRunId: root.runId,
        parentCursorKey: "invoke:c@1",
      });
      const found = await h.storage.findChildRun(root.runId, "invoke:c@1");
      expect(found?.id).toBe(child.runId);
      const missing = await h.storage.findChildRun(root.runId, "invoke:nope@1");
      expect(missing).toBeUndefined();
    });
  });

  describe("signalHook", () => {
    it("delivers + enqueues atomically when a hook is armed", async () => {
      const { runId } = await h.storage.createRun({
        name: "delivered",
        version: 1,
        input: {},
      });
      const armed = await h.storage.armOrConsumeSignal(runId, "signal:s", undefined);
      expect(armed.kind).toBe("armed");

      const result = await h.storage.deliverSignal(runId, "s", { v: 1 });
      expect(result).toEqual({ kind: "delivered", cursorKey: "signal:s" });

      const hook = await h.storage.loadSignal(runId, "signal:s");
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

      const result = await h.storage.deliverSignal(runId, "s", { v: 2 });
      expect(result).toEqual({ kind: "buffered", cursorKey: "signal:s" });

      const hook = await h.storage.loadSignal(runId, "signal:s");
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
      const first = await h.storage.deliverSignal(runId, "s", "v1");
      const second = await h.storage.deliverSignal(runId, "s", "v2");
      expect(first.kind).toBe("buffered");
      expect(second.kind).toBe("duplicate");
      const hook = await h.storage.loadSignal(runId, "signal:s");
      expect(hook?.payload).toBe("v1");
    });

    it("returns 'expired' when the armed hook's expiresAt has passed", async () => {
      const { runId } = await h.storage.createRun({
        name: "expired-hook",
        version: 1,
        input: {},
      });
      await h.storage.armOrConsumeSignal(runId, "signal:s", new Date(Date.now() - 1_000));

      const result = await h.storage.deliverSignal(runId, "s", { late: true });
      expect(result).toEqual({ kind: "expired", cursorKey: "signal:s" });

      const hook = await h.storage.loadSignal(runId, "signal:s");
      expect(hook?.delivered).toBe(false);
      expect(hook?.payload).toBeNull();
    });

    it("rolls back deliver + enqueue together when enqueue throws", async () => {
      const { runId } = await h.storage.createRun({
        name: "rollback",
        version: 1,
        input: {},
      });
      await h.storage.armOrConsumeSignal(runId, "signal:s", undefined);

      const flaky: TxEnqueue = async () => {
        throw new Error("queue down");
      };
      const flakyStorage = createDrizzleStorage({
        db: h.db,
        logger: silent,
        enqueue: flaky,
      });

      await expect(flakyStorage.deliverSignal(runId, "s", { v: 1 })).rejects.toThrow("queue down");

      const hook = await h.storage.loadSignal(runId, "signal:s");
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
      const result = await h.storage.armOrConsumeSignal(runId, "signal:x", undefined);
      expect(result).toEqual({ kind: "armed" });
      const hook = await h.storage.loadSignal(runId, "signal:x");
      expect(hook?.delivered).toBe(false);
    });

    it("consumes a buffered hook instead of arming (deliver-vs-arm race)", async () => {
      const { runId } = await h.storage.createRun({
        name: "race",
        version: 1,
        input: {},
      });
      await h.storage.preDeliverSignal(runId, "signal:x", { value: "raced" });

      const result = await h.storage.armOrConsumeSignal(runId, "signal:x", undefined);
      expect(result).toMatchObject({ kind: "consumed", payload: { value: "raced" } });
      if (result.kind === "consumed") {
        expect(result.row.delivered).toBe(true);
        expect(result.row.payload).toEqual({ value: "raced" });
      }
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
        runningStuckOlderThan: stuckCutoff(),
        maxRunAttempts: 100,
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
      await h.storage.markAwaitingSignal(runId);
      await h.storage.preDeliverSignal(runId, "signal:x", { score: 7 });
      await ageRun(h.db, runId, 5 * 60_000);

      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
        runningStuckOlderThan: stuckCutoff(),
        maxRunAttempts: 100,
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
        runningStuckOlderThan: stuckCutoff(),
        maxRunAttempts: 100,
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
        runningStuckOlderThan: stuckCutoff(),
        maxRunAttempts: 100,
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
      await h.storage.markAwaitingSignal(runId);
      await h.storage.armOrConsumeSignal(runId, "signal:to", new Date(Date.now() - 1_000));
      await ageRun(h.db, runId, 5 * 60_000);

      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
        runningStuckOlderThan: stuckCutoff(),
        maxRunAttempts: 100,
      });
      expect(n).toBe(1);
      expect(h.enqueues[0]?.runId).toBe(runId);
    });

    it("skips runs touched inside the grace window", async () => {
      await h.storage.createRun({ name: "fresh", version: 1, input: {} });
      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
        runningStuckOlderThan: stuckCutoff(),
        maxRunAttempts: 100,
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
        maxRunAttempts: 100,
      });
      expect(n).toBe(1);
      expect(h.enqueues[0]?.runId).toBe(runId);
    });

    it("resets a stuck running run to retrying so it can be re-claimed (crash recovery)", async () => {
      const { runId } = await h.storage.createRun({
        name: "crash-recover",
        version: 1,
        input: {},
      });
      expect((await h.storage.claimRun(runId)).kind).toBe("claimed");
      await ageRun(h.db, runId, 15 * 60_000);

      await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
        runningStuckOlderThan: new Date(Date.now() - 10 * 60_000),
        maxRunAttempts: 100,
      });

      expect((await h.storage.claimRun(runId)).kind).toBe("claimed");
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
        maxRunAttempts: 100,
      });
      expect(n).toBe(0);
      expect(h.enqueues.length).toBe(0);
    });

    it("respects a far-future stuck cutoff (treats every running row as fresh)", async () => {
      const { runId } = await h.storage.createRun({
        name: "no-stuck-check",
        version: 1,
        input: {},
      });
      await h.storage.markRunning(runId);
      await ageRun(h.db, runId, 24 * 60 * 60_000);

      const n = await h.storage.reenqueueOrphans({
        olderThan: new Date(Date.now() - 60_000),
        runningStuckOlderThan: new Date(0),
        maxRunAttempts: 100,
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
        runningStuckOlderThan: stuckCutoff(),
        maxRunAttempts: 100,
      });
      expect(n).toBe(0);
      expect(h.enqueues.length).toBe(0);
      const rows = await h.db.select({ status: runs.status }).from(runs).where(eq(runs.id, runId));
      expect(rows[0]?.status).toBe("done");
    });

    const reconcileOpts = (maxRunAttempts = 100) => ({
      olderThan: new Date(Date.now() - 60_000),
      runningStuckOlderThan: stuckCutoff(),
      maxRunAttempts,
    });

    // A retrying run whose backoff timer is overdue and unfired: its wake was
    // lost (fired then the worker died before re-dispatch). This is the orphan.
    const seedRetryOrphan = async (name: string) => {
      const { runId } = await h.storage.createRun({ name, version: 1, input: {} });
      await h.storage.markRetrying(runId);
      await h.storage.armRetryTimer(runId, new Date(Date.now() - 5 * 60_000));
      await ageRun(h.db, runId, 15 * 60_000);
      return runId;
    };

    it("re-enqueues a retrying run whose backoff timer is overdue (orphaned wake)", async () => {
      const runId = await seedRetryOrphan("retry-orphan");
      const n = await h.storage.reenqueueOrphans(reconcileOpts());
      expect(n).toBe(1);
      expect(h.enqueues.map((e) => e.runId)).toEqual([runId]);
      expect((await h.storage.claimRun(runId)).kind).toBe("claimed");
    });

    it("skips a retrying run whose backoff timer is still in the future (healthy backoff)", async () => {
      const { runId } = await h.storage.createRun({ name: "retry-healthy", version: 1, input: {} });
      await h.storage.markRetrying(runId);
      await h.storage.armRetryTimer(runId, new Date(Date.now() + 30 * 60_000));
      await ageRun(h.db, runId, 15 * 60_000);

      const n = await h.storage.reenqueueOrphans(reconcileOpts());
      expect(n).toBe(0);
      expect(h.enqueues.length).toBe(0);
    });

    it("fails a stuck retrying orphan whose attempts are exhausted instead of re-enqueuing", async () => {
      const runId = await seedRetryOrphan("retry-doomed");
      await setAttempts(h.db, runId, 3);

      const n = await h.storage.reenqueueOrphans(reconcileOpts(3));
      expect(n).toBe(1);
      expect(h.enqueues.length).toBe(0);
      const [row] = await h.db
        .select({ status: runs.status, error: runs.error })
        .from(runs)
        .where(eq(runs.id, runId));
      expect(row?.status).toBe("failed");
      expect((row?.error as { code?: string } | null)?.code).toBe("RUN_ATTEMPTS_EXHAUSTED");
      expect(await eventTypes(h.db, runId)).toContain("failed");
    });
  });

  describe("retry timer", () => {
    it("armRetryTimer upserts one __retry timer, moving fire_at forward and clearing fired_at", async () => {
      const { runId } = await h.storage.createRun({ name: "arm", version: 1, input: {} });
      const first = new Date(Date.now() + 60_000);
      await h.storage.armRetryTimer(runId, first);
      let t = await h.storage.loadTimer(runId, "__retry");
      expect(t?.fireAt.getTime()).toBe(first.getTime());

      const second = new Date(Date.now() + 5 * 60_000);
      await h.storage.armRetryTimer(runId, second);
      const all = await h.db.select().from(timers).where(eq(timers.runId, runId));
      expect(all).toHaveLength(1); // upserted, not duplicated
      expect(all[0]?.fireAt.getTime()).toBe(second.getTime());
      expect(all[0]?.firedAt).toBeNull();
    });

    it("claiming a retrying run fires its __retry timer so it can't read as overdue later", async () => {
      const { runId } = await h.storage.createRun({ name: "consume", version: 1, input: {} });
      await h.storage.markRetrying(runId);
      await h.storage.armRetryTimer(runId, new Date(Date.now() - 1_000));

      expect((await h.storage.claimRun(runId)).kind).toBe("claimed");
      const t = await h.storage.loadTimer(runId, "__retry");
      expect(t?.firedAt).not.toBeNull();
    });
  });

  describe("loadRunDetail + loadOutput", () => {
    it("loadRunDetail.signals", async () => {
      const { runId } = await h.storage.createRun({
        name: "detail",
        version: 1,
        input: { v: 1 },
      });
      await h.storage.startStep(runId, "s1", 1);
      await h.storage.finishStep({
        runId,
        cursorKey: "s1",
        status: "ok",
        attempts: 1,
        result: "ok",
      });
      await h.storage.createTimer(runId, "sleep", new Date());
      await h.storage.armOrConsumeSignal(runId, "signal:x", undefined);

      const detail = await h.storage.loadRunDetail(runId);
      expect(detail?.run.name).toBe("detail");
      expect(detail?.steps.length).toBe(1);
      expect(detail?.timers.length).toBe(1);
      expect(detail?.signals.length).toBe(1);
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
        cursorKey: "s1",
        status: "ok",
        attempts: 1,
        result: 42,
      });
      await h.storage.createTimer(runId, "sleep", new Date());
      await h.storage.armOrConsumeSignal(runId, "signal:x", undefined);
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

      const hk = await h.db.select().from(signals).where(eq(signals.runId, runId));
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
