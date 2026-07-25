import type { Backend } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";
import { at } from "#clock";

/**
 * `orphanedRuns` as executable invariants — what the reconciler re-drives. A run is an orphan
 * only when it truly can't make progress on its own: non-terminal but off the queue with no
 * pending timer, OR an `awaiting_signal` parent whose child already finished (lost wake). A
 * run that is queued, timing, terminal, or legitimately waiting on an external signal is NOT.
 */
export const reconcileConformance = (
  label: string,
  makeBackend: () => Backend | Promise<Backend>,
): void => {
  describe(`Reconcile conformance — ${label}`, () => {
    it("a non-terminal run with no job and no timer is an orphan", async () => {
      const { store } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      expect(await store.orphanedRuns(10)).toContain(runId); // pending, never enqueued
    });

    it("a queued run is not an orphan", async () => {
      const { store, queue } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await queue.enqueue(runId);
      expect(await store.orphanedRuns(10)).not.toContain(runId);
    });

    it("a sleeping run with a pending timer is not an orphan", async () => {
      const { store } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await store.markRunning(runId);
      await store.suspendRun(runId, "sleeping", { timers: [{ runId, fireAt: at(10_000) }] });
      expect(await store.orphanedRuns(10)).not.toContain(runId);
    });

    it("an awaiting_signal run is not an orphan (legitimately waiting on an external signal)", async () => {
      const { store } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await store.markRunning(runId);
      await store.suspendRun(runId, "awaiting_signal");
      expect(await store.orphanedRuns(10)).not.toContain(runId);
    });

    it("an awaiting_child parent whose child finished IS an orphan (lost parent-wake)", async () => {
      const { store } = await makeBackend();
      const parent = await store.startRun({ name: "p", version: 1, input: {} });
      const child = await store.startRun({
        name: "c",
        version: 1,
        input: {},
        parentRunId: parent.runId,
      });
      await store.markRunning(parent.runId);
      await store.suspendRun(parent.runId, "awaiting_child");
      await store.markTerminal(child.runId, { status: "done", output: 1 }); // child done, wake lost
      expect(await store.orphanedRuns(10)).toContain(parent.runId);
    });

    it("a fan-out parent with a still-running child is NOT an orphan (join not resolved)", async () => {
      const { store } = await makeBackend();
      const parent = await store.startRun({ name: "p", version: 1, input: {} });
      const kids = await store.startManyRuns([
        { name: "c", version: 1, input: {}, parentRunId: parent.runId },
        { name: "c", version: 1, input: {}, parentRunId: parent.runId },
      ]);
      await store.markRunning(parent.runId);
      await store.suspendRun(parent.runId, "awaiting_child");
      await store.markTerminal(kids[0].runId, { status: "done", output: 1 }); // 1 of 2 done, 1 running
      expect(await store.orphanedRuns(10)).not.toContain(parent.runId);
    });

    it("a fan-out parent with a FAILED child IS an orphan even while siblings run (fast-fail)", async () => {
      const { store } = await makeBackend();
      const parent = await store.startRun({ name: "p", version: 1, input: {} });
      const kids = await store.startManyRuns([
        { name: "c", version: 1, input: {}, parentRunId: parent.runId },
        { name: "c", version: 1, input: {}, parentRunId: parent.runId },
      ]);
      await store.markRunning(parent.runId);
      await store.suspendRun(parent.runId, "awaiting_child");
      await store.markTerminal(kids[0].runId, {
        status: "failed",
        error: { code: "X", message: "boom" },
      }); // one failed, sibling still running — parent must fast-fail
      expect(await store.orphanedRuns(10)).toContain(parent.runId);
    });

    it("a terminal run is never an orphan", async () => {
      const { store } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await store.markTerminal(runId, { status: "done", output: 1 });
      expect(await store.orphanedRuns(10)).not.toContain(runId);
    });
  });
};
