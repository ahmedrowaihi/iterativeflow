import type { Backend } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";
import { at } from "#clock";

/**
 * The transactional-outbox seam as executable invariants: a durable Store write and the
 * side-effects it carries commit together, and — critically — the outbox fires exactly
 * ONCE even when the write is replayed. Every backend (memory, Postgres, DynamoDB) must
 * satisfy this; it is the property that makes crash recovery correct rather than just
 * plausible.
 */
export const outboxConformance = (
  label: string,
  makeBackend: () => Backend | Promise<Backend>,
): void => {
  describe(`Outbox conformance — ${label}`, () => {
    it("checkpointStep commits its enqueue atomically — the run is claimable after", async () => {
      const { store, queue } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await store.checkpointStep(
        { runId, cursorKey: "a", status: "ok", result: 1, attempts: 1 },
        { enqueue: [{ runId }] },
      );
      const leases = await queue.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      expect(leases.map((l) => l.runId)).toEqual([runId]);
    });

    it("checkpointStep spawns a child run + enqueues it atomically", async () => {
      const { store, queue } = await makeBackend();
      const { runId } = await store.startRun({ name: "parent", version: 1, input: {} });
      const childId = "11111111-1111-1111-1111-111111111111";
      await store.checkpointStep(
        { runId, cursorKey: "spawn", status: "ok", result: childId, attempts: 1 },
        { spawn: [{ runId: childId, spec: { name: "child", version: 1, input: { n: 1 } } }] },
      );
      expect((await store.loadRun(childId))?.run.input).toEqual({ n: 1 });
      const leases = await queue.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      expect(leases.map((l) => l.runId)).toContain(childId);
    });

    it("a replayed checkpoint does NOT re-fire the outbox — the child spawns exactly once", async () => {
      const { store, queue } = await makeBackend();
      const { runId } = await store.startRun({ name: "parent", version: 1, input: {} });
      const childId = "22222222-2222-2222-2222-222222222222";
      const spawn = {
        spawn: [{ runId: childId, spec: { name: "child", version: 1, input: {} } }],
      };
      await store.checkpointStep(
        { runId, cursorKey: "spawn", status: "ok", result: childId, attempts: 1 },
        spawn,
      );
      // Replay: same cursorKey, first-writer-wins → the outbox must be skipped entirely.
      await store.checkpointStep(
        { runId, cursorKey: "spawn", status: "ok", result: childId, attempts: 2 },
        spawn,
      );
      const leases = await queue.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      expect(leases.filter((l) => l.runId === childId)).toHaveLength(1); // not two enqueues
    });

    it("suspendRun(sleeping) sets status + wake timer atomically", async () => {
      const { store, timer } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await store.markRunning(runId);
      await store.suspendRun(runId, "sleeping", { timers: [{ runId, fireAt: at(5000) }] });
      expect((await store.loadRun(runId))?.run.status).toBe("sleeping");
      expect(await timer.dueBatch({ now: at(4999), limit: 10 })).toEqual([]);
      expect(await timer.dueBatch({ now: at(5000), limit: 10 })).toEqual([runId]);
    });

    it("suspendRun is a no-op on a terminal run — no zombie wake timer", async () => {
      const { store, timer } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await store.markTerminal(runId, { status: "done", output: 1 });
      await store.suspendRun(runId, "sleeping", { timers: [{ runId, fireAt: at(5000) }] });
      expect((await store.loadRun(runId))?.run.status).toBe("done");
      expect(await timer.dueBatch({ now: at(5000), limit: 10 })).toEqual([]); // outbox skipped
    });

    it("markTerminal wakes a suspended parent atomically (enqueue in the same write)", async () => {
      const { store, queue } = await makeBackend();
      const parent = await store.startRun({ name: "parent", version: 1, input: {} });
      const child = await store.startRun({
        name: "child",
        version: 1,
        input: {},
        parentRunId: parent.runId,
      });
      await store.markTerminal(
        child.runId,
        { status: "done", output: 7 },
        {
          enqueue: [{ runId: parent.runId }],
        },
      );
      const leases = await queue.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      expect(leases.map((l) => l.runId)).toContain(parent.runId);
    });

    it("markTerminal cancels a pending timer atomically (no stale wake after completion)", async () => {
      const { store, timer } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await store.markRunning(runId);
      await store.suspendRun(runId, "awaiting_signal", { timers: [{ runId, fireAt: at(5000) }] });
      await store.markTerminal(runId, { status: "done", output: 1 }, { cancelTimers: [runId] });
      expect(await timer.dueBatch({ now: at(5000), limit: 10 })).toEqual([]); // timeout was cleared
    });

    it("a canceled run's markTerminal is sticky AND skips its outbox", async () => {
      const { store, queue } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await store.markTerminal(runId, { status: "canceled" });
      await store.markTerminal(runId, { status: "done", output: 1 }, { enqueue: [{ runId }] });
      expect((await store.loadRun(runId))?.run.status).toBe("canceled");
      expect(await queue.claim({ limit: 10, leaseMs: 1000, now: at(0) })).toEqual([]);
    });
  });
};
