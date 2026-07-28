import type { Backend } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";
import { at } from "#clock";

/**
 * The durable signal inbox as executable invariants: a delivered signal lands in `loadRun`,
 * wakes the run (re-enqueue), is idempotent on its key, and is consumed exactly once by a
 * checkpoint's `consumeSignals` outbox.
 */
export const signalConformance = (
  label: string,
  makeBackend: () => Backend | Promise<Backend>,
): void => {
  describe(`Signal conformance — ${label}`, () => {
    it("postSignal delivers into the inbox and wakes the run (re-enqueue)", async () => {
      const { store, queue } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      const res = await store.postSignal(runId, "approve", { ok: true });
      expect(res.delivered).toBe(true);
      const snap = await store.loadRun(runId);
      expect(snap?.signals.map((s) => ({ name: s.name, payload: s.payload }))).toEqual([
        { name: "approve", payload: { ok: true } },
      ]);
      const leases = await queue.claim({ limit: 10, leaseMs: 1000, now: at(0) });
      expect(leases.map((l) => l.runId)).toEqual([runId]); // woken
    });

    it("postSignal is idempotent on its key — a retried delivery lands once", async () => {
      const { store } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      const a = await store.postSignal(runId, "x", 1, { idempotencyKey: "k" });
      const b = await store.postSignal(runId, "x", 1, { idempotencyKey: "k" });
      expect(a.delivered).toBe(true);
      expect(b.delivered).toBe(false);
      expect((await store.loadRun(runId))?.signals).toHaveLength(1);
    });

    it("consumeSignals drains exactly the referenced signal, atomically with the checkpoint", async () => {
      const { store } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await store.postSignal(runId, "a", "first");
      await store.postSignal(runId, "b", "second");
      const before = await store.loadRun(runId);
      const target = before?.signals.find((s) => s.name === "a");
      await store.checkpointStep(
        { runId, cursorKey: "wait", status: "ok", result: "first", attempts: 1 },
        { consumeSignals: [target!.id] },
      );
      const after = await store.loadRun(runId);
      expect(after?.signals.map((s) => s.name)).toEqual(["b"]); // only "a" consumed
      expect(after?.steps.get("wait")?.result).toBe("first"); // and the memo committed with it
    });

    it("requireVersion commits while the job version is unchanged, refuses once a racing signal moves it", async () => {
      const { store, queue } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await queue.enqueue(runId);
      const [lease] = await queue.claim({ limit: 1, leaseMs: 1000, now: at(0) });
      // version unchanged since claim → the guarded signal-timeout resolution commits
      const committed = await store.checkpointStep(
        { runId, cursorKey: "res", status: "ok", result: { received: false }, attempts: 1 },
        { requireVersion: lease!.version },
      );
      expect(committed.committed).not.toBe(false);
      expect((await store.loadRun(runId))?.steps.get("res")?.result).toEqual({ received: false });
      // a racing signal delivery bumps the dispatch version → a stale-version guard is refused
      await store.postSignal(runId, "go", "payload");
      const blocked = await store.checkpointStep(
        { runId, cursorKey: "res2", status: "ok", result: { received: false }, attempts: 1 },
        { requireVersion: lease!.version },
      );
      expect(blocked.committed).toBe(false);
      expect((await store.loadRun(runId))?.steps.get("res2")).toBeUndefined(); // nothing written
    });

    it("distinct signals to a run preserve delivery order in the inbox", async () => {
      const { store } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await store.postSignal(runId, "s", 1);
      await store.postSignal(runId, "s", 2);
      await store.postSignal(runId, "s", 3);
      expect((await store.loadRun(runId))?.signals.map((s) => s.payload)).toEqual([1, 2, 3]);
    });
  });
};
