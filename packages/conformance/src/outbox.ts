import type { Backend } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";
import { at } from "#clock";

/**
 * The transactional-outbox seam as executable invariants: a durable Store write and the
 * side-effects it carries commit together, and — critically — the outbox fires exactly
 * ONCE even when the write is replayed. Every backend must satisfy this; it is the property
 * that makes crash recovery correct rather than just plausible.
 */
export const outboxConformance = (
  label: string,
  makeBackend: () => Backend | Promise<Backend>,
): void => {
  describe(`Outbox conformance — ${label}`, () => {
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
      const replayId = "44444444-4444-4444-4444-444444444444";
      const spawnOf = (id: string) => ({
        spawn: [{ runId: id, spec: { name: "child", version: 1, input: {} } }],
      });
      await store.checkpointStep(
        { runId, cursorKey: "spawn", status: "ok", result: childId, attempts: 1 },
        spawnOf(childId),
      );
      // Replay: same cursorKey, first-writer-wins → the outbox must be skipped entirely.
      await store.checkpointStep(
        { runId, cursorKey: "spawn", status: "ok", result: replayId, attempts: 2 },
        spawnOf(replayId),
      );
      expect(await store.loadRunRow(replayId)).toBeUndefined();
      const leases = await queue.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      expect(leases.map((l) => l.runId)).toEqual([childId]);
    });

    it("a spawn of an existing run id leaves that run untouched — the first spawn wins", async () => {
      const { store } = await makeBackend();
      const { runId } = await store.startRun({ name: "parent", version: 1, input: {} });
      const childId = "33333333-3333-3333-3333-333333333333";
      const spawnAt = (cursorKey: string, n: number) =>
        store.checkpointStep(
          { runId, cursorKey, status: "ok", result: childId, attempts: 1 },
          { spawn: [{ runId: childId, spec: { name: "child", version: 1, input: { n } } }] },
        );
      await spawnAt("first", 1);
      await store.markTerminal(childId, { status: "done", output: "kept" });
      const second = await spawnAt("second", 2);
      expect(second.committed).not.toBe(false);
      expect((await store.loadRun(runId))?.steps.has("second")).toBe(true);
      expect(await store.loadRunRow(childId)).toMatchObject({
        status: "done",
        output: "kept",
        input: { n: 1 },
      });
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
      expect(await timer.dueBatch({ now: at(5000), limit: 10 })).toEqual([]);
    });

    it("markTerminal cancels a pending timer atomically (no stale wake after completion)", async () => {
      const { store, timer } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await store.markRunning(runId);
      await store.suspendRun(runId, "awaiting_signal", { timers: [{ runId, fireAt: at(5000) }] });
      await store.markTerminal(runId, { status: "done", output: 1 }, { cancelTimers: [runId] });
      expect(await timer.dueBatch({ now: at(5000), limit: 10 })).toEqual([]);
    });

    it("a canceled run's markTerminal is sticky AND skips its outbox", async () => {
      const { store, timer } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await store.markTerminal(runId, { status: "canceled" });
      await store.markTerminal(
        runId,
        { status: "done", output: 1 },
        { timers: [{ runId, fireAt: at(0) }] },
      );
      expect((await store.loadRun(runId))?.run.status).toBe("canceled");
      expect(await timer.dueBatch({ now: at(0), limit: 10 })).toEqual([]);
    });
  });
};
