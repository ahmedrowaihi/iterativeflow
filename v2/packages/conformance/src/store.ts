import type { Store } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";

/**
 * The Store contract as executable invariants. EVERY backend (in-memory, Postgres,
 * DynamoDB) is run against this exact suite — a backend is not "done" until green here.
 * The guarantee is a test, not a comment.
 */
export const storeConformance = (label: string, makeStore: () => Store | Promise<Store>): void => {
  describe(`Store conformance — ${label}`, () => {
    it("startRun creates a fresh pending run", async () => {
      const s = await makeStore();
      const r = await s.startRun({ name: "f", version: 1, input: { a: 1 } });
      expect(r.created).toBe(true);
      expect(r.status).toBe("pending");
      const snap = await s.loadRun(r.runId);
      expect(snap?.run.status).toBe("pending");
      expect(snap?.run.input).toEqual({ a: 1 });
      expect(snap?.run.attempts).toBe(0);
    });

    it("startManyRuns creates the whole batch, results aligned with the specs", async () => {
      const s = await makeStore();
      const specs = [
        { name: "f", version: 1, input: { i: 0 } },
        { name: "g", version: 2, input: { i: 1 } },
        { name: "h", version: 1, input: { i: 2 } },
      ];
      const results = await s.startManyRuns(specs);
      expect(results).toHaveLength(3);
      expect(results.every((r) => r.created)).toBe(true);
      for (let i = 0; i < specs.length; i++) {
        const snap = await s.loadRun(results[i].runId);
        expect(snap?.run.input).toEqual({ i });
        expect(snap?.run.name).toBe(specs[i].name);
      }
    });

    it("startManyRuns honours per-spec idempotency — a batch can mix created and existing", async () => {
      const s = await makeStore();
      const first = await s.startRun({ name: "f", version: 1, input: {}, idempotencyKey: "dup" });
      const results = await s.startManyRuns([
        { name: "f", version: 1, input: {}, idempotencyKey: "dup" }, // hits the existing run
        { name: "f", version: 1, input: {}, idempotencyKey: "new" }, // fresh
      ]);
      expect(results[0]).toMatchObject({ runId: first.runId, created: false });
      expect(results[1].created).toBe(true);
      expect(results[1].runId).not.toBe(first.runId);
    });

    it("startRun is idempotent on the idempotency key — no duplicate run", async () => {
      const s = await makeStore();
      const a = await s.startRun({ name: "f", version: 1, input: {}, idempotencyKey: "k" });
      const b = await s.startRun({ name: "f", version: 1, input: {}, idempotencyKey: "k" });
      expect(a.created).toBe(true);
      expect(b.created).toBe(false);
      expect(b.runId).toBe(a.runId);
    });

    it("idempotency key is scoped by (name, version)", async () => {
      const s = await makeStore();
      const a = await s.startRun({ name: "f", version: 1, input: {}, idempotencyKey: "k" });
      const b = await s.startRun({ name: "f", version: 2, input: {}, idempotencyKey: "k" });
      expect(b.runId).not.toBe(a.runId);
    });

    it("loadRun returns undefined for an unknown run", async () => {
      const s = await makeStore();
      expect(await s.loadRun("00000000-0000-0000-0000-000000000000")).toBeUndefined();
    });

    it("loadRunRow returns just the run row (no step/signal maps), undefined if gone", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "f", version: 1, input: { a: 1 } });
      await s.checkpointStep({ runId, cursorKey: "a", status: "ok", result: 1, attempts: 1 });
      const row = await s.loadRunRow(runId);
      expect(row).toMatchObject({ id: runId, status: "pending" });
      expect(row).not.toHaveProperty("steps");
      expect(row).not.toHaveProperty("signals");
      expect(await s.loadRunRow("00000000-0000-0000-0000-000000000000")).toBeUndefined();
    });

    it("loadRunRows returns rows aligned to the input ids, undefined where gone", async () => {
      const s = await makeStore();
      const a = await s.startRun({ name: "f", version: 1, input: { n: 1 } });
      const b = await s.startRun({ name: "f", version: 1, input: { n: 2 } });
      const missing = "00000000-0000-0000-0000-000000000000";
      const rows = await s.loadRunRows([a.runId, missing, b.runId]);
      expect(rows).toHaveLength(3);
      expect(rows[0]?.id).toBe(a.runId);
      expect(rows[1]).toBeUndefined();
      expect(rows[2]?.id).toBe(b.runId);
      expect(await s.loadRunRows([])).toEqual([]);
    });

    it("arriveAtJoin decrements the armed join countdown, and sentinels a gone parent", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "p", version: 1, input: {} });
      await s.checkpointStep(
        { runId, cursorKey: "s0", status: "ok", result: [], attempts: 1 },
        { joinTarget: { runId, count: 2 } },
      );
      expect(await s.arriveAtJoin(runId)).toBe(1);
      expect(await s.arriveAtJoin(runId)).toBe(0);
      expect(await s.arriveAtJoin("00000000-0000-0000-0000-000000000000")).toBeGreaterThan(
        1_000_000,
      );
    });

    it("checkpointStep persists a completed step into the memo", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "f", version: 1, input: {} });
      await s.checkpointStep({ runId, cursorKey: "a", status: "ok", result: 42, attempts: 1 });
      const snap = await s.loadRun(runId);
      expect(snap?.steps.get("a")).toMatchObject({ status: "ok", result: 42, attempts: 1 });
    });

    it("checkpointStep round-trips the shape tag (drift-guard evidence)", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "f", version: 1, input: {} });
      await s.checkpointStep({
        runId,
        cursorKey: "a",
        status: "ok",
        result: 1,
        attempts: 1,
        shape: "step:charge",
      });
      expect((await s.loadRun(runId))?.steps.get("a")?.shape).toBe("step:charge");
    });

    it("checkpointStep is first-writer-wins — a second write does NOT overwrite (exactly-once memo)", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "f", version: 1, input: {} });
      const first = await s.checkpointStep({
        runId,
        cursorKey: "a",
        status: "ok",
        result: "first",
        attempts: 1,
      });
      const second = await s.checkpointStep({
        runId,
        cursorKey: "a",
        status: "ok",
        result: "SECOND",
        attempts: 2,
      });
      expect(first.result).toBe("first");
      expect(second.result).toBe("first"); // returns the stored first outcome, not the retry's
      expect((await s.loadRun(runId))?.steps.get("a")?.result).toBe("first");
    });

    it("markRunning increments attempts (bounded retries survive crashes)", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "f", version: 1, input: {} });
      expect(await s.markRunning(runId)).toBe(1);
      expect(await s.markRunning(runId)).toBe(2);
      expect((await s.loadRun(runId))?.run.attempts).toBe(2);
    });

    it("resetAttempts zeroes the dispatch counter (only the poison-pill cap counts no-progress)", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "f", version: 1, input: {} });
      await s.markRunning(runId);
      await s.markRunning(runId);
      await s.resetAttempts(runId);
      expect((await s.loadRunRow(runId))?.attempts).toBe(0);
      expect(await s.markRunning(runId)).toBe(1);
    });

    it("markRunning never resurrects a terminal run (a late re-dispatch can't undo a cancel)", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "f", version: 1, input: {} });
      await s.markTerminal(runId, { status: "canceled" });
      await s.markRunning(runId); // e.g. a stale timer re-enqueued and re-claimed it
      expect((await s.loadRun(runId))?.run.status).toBe("canceled");
    });

    it("markTerminal sets status + output", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "f", version: 1, input: {} });
      await s.markTerminal(runId, { status: "done", output: "out" });
      const snap = await s.loadRun(runId);
      expect(snap?.run.status).toBe("done");
      expect(snap?.run.output).toBe("out");
    });

    it("markTerminal never overrides a canceled run", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "g", version: 1, input: {} });
      await s.markTerminal(runId, { status: "canceled" });
      await s.markTerminal(runId, { status: "done", output: "late" });
      expect((await s.loadRun(runId))?.run.status).toBe("canceled");
    });

    it("checkpointStep round-trips a failed_terminal outcome with its error", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "f", version: 1, input: {} });
      await s.checkpointStep({
        runId,
        cursorKey: "a",
        status: "failed_terminal",
        error: { code: "BOOM", message: "boom" },
        attempts: 1,
      });
      expect((await s.loadRun(runId))?.steps.get("a")).toMatchObject({
        status: "failed_terminal",
        error: { code: "BOOM", message: "boom" },
      });
    });

    it("first-writer-wins holds across status — a later ok cannot overwrite a failed_terminal", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "f", version: 1, input: {} });
      await s.checkpointStep({
        runId,
        cursorKey: "a",
        status: "failed_terminal",
        error: { code: "BOOM", message: "boom" },
        attempts: 1,
      });
      const later = await s.checkpointStep({
        runId,
        cursorKey: "a",
        status: "ok",
        result: 1,
        attempts: 2,
      });
      expect(later.status).toBe("failed_terminal");
      expect((await s.loadRun(runId))?.steps.get("a")?.status).toBe("failed_terminal");
    });

    it("checkpointStep on an unknown run is rejected (no orphan step)", async () => {
      const s = await makeStore();
      await expect(
        s.checkpointStep({ runId: "nope", cursorKey: "a", status: "ok", result: 1, attempts: 1 }),
      ).rejects.toThrow();
    });

    it("startRun on an idempotency hit returns the run's CURRENT status", async () => {
      const s = await makeStore();
      const a = await s.startRun({ name: "f", version: 1, input: {}, idempotencyKey: "k" });
      await s.markTerminal(a.runId, { status: "done", output: 1 });
      const b = await s.startRun({ name: "f", version: 1, input: {}, idempotencyKey: "k" });
      expect(b.created).toBe(false);
      expect(b.status).toBe("done");
    });

    it("tags round-trip", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "f", version: 1, input: {}, tags: ["x", "y"] });
      expect((await s.loadRun(runId))?.run.tags).toEqual(["x", "y"]);
    });

    it("mutating a snapshot's run/step VALUES can't corrupt the store", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "f", version: 1, input: { n: 1 } });
      await s.checkpointStep({
        runId,
        cursorKey: "a",
        status: "ok",
        result: { v: 1 },
        attempts: 1,
      });
      const snap = await s.loadRun(runId);
      (snap!.run.input as { n: number }).n = 999;
      (snap!.steps.get("a")!.result as { v: number }).v = 999;
      (snap!.steps as Map<string, unknown>).clear();
      const again = await s.loadRun(runId);
      expect((again!.run.input as { n: number }).n).toBe(1);
      expect((again!.steps.get("a")!.result as { v: number }).v).toBe(1);
    });

    it("caller mutating the input after startRun can't reach the stored run", async () => {
      const s = await makeStore();
      const input = { n: 1 };
      const { runId } = await s.startRun({ name: "f", version: 1, input });
      input.n = 999;
      expect((await s.loadRun(runId))?.run.input).toEqual({ n: 1 });
    });

    it("listRuns returns newest-first and filters by status, name, and tag", async () => {
      const s = await makeStore();
      const a = await s.startRun({ name: "alpha", version: 1, input: {}, tags: ["t1"] });
      const b = await s.startRun({ name: "beta", version: 1, input: {}, tags: ["t2"] });
      await s.startRun({ name: "alpha", version: 1, input: {}, tags: ["t1"] });
      await s.markTerminal(b.runId, { status: "done", output: 1 });

      const all = await s.listRuns({}, { limit: 10 });
      expect(all.runs).toHaveLength(3);
      expect(all.runs[0].id).not.toBe(a.runId); // newest first — `a` was inserted first

      const byName = await s.listRuns({ name: "alpha" }, { limit: 10 });
      expect(byName.runs.map((r) => r.name)).toEqual(["alpha", "alpha"]);

      const byStatus = await s.listRuns({ status: "done" }, { limit: 10 });
      expect(byStatus.runs.map((r) => r.id)).toEqual([b.runId]);

      const byTag = await s.listRuns({ tag: "t2" }, { limit: 10 });
      expect(byTag.runs.map((r) => r.id)).toEqual([b.runId]);
    });

    it("listRuns paginates with a cursor", async () => {
      const s = await makeStore();
      for (let i = 0; i < 5; i++) await s.startRun({ name: "f", version: 1, input: { i } });
      const first = await s.listRuns({}, { limit: 2 });
      expect(first.runs).toHaveLength(2);
      expect(first.cursor).toBeDefined();
      const second = await s.listRuns({}, { limit: 2, cursor: first.cursor });
      expect(second.runs).toHaveLength(2);
      const ids = new Set([...first.runs, ...second.runs].map((r) => r.id));
      expect(ids.size).toBe(4); // no overlap between pages
    });

    it("childrenOf returns the runs spawned under a parent", async () => {
      const s = await makeStore();
      const parent = await s.startRun({ name: "p", version: 1, input: {} });
      const c1 = await s.startRun({ name: "c", version: 1, input: {}, parentRunId: parent.runId });
      const c2 = await s.startRun({ name: "c", version: 1, input: {}, parentRunId: parent.runId });
      await s.startRun({ name: "unrelated", version: 1, input: {} });
      const kids = await s.childrenOf(parent.runId);
      expect(new Set(kids.map((r) => r.id))).toEqual(new Set([c1.runId, c2.runId]));
    });

    it("retryRun re-drives a failed run and keeps its ok step memos", async () => {
      const s = await makeStore();
      const { runId } = await s.startRun({ name: "f", version: 1, input: {} });
      await s.checkpointStep({ runId, cursorKey: "a", status: "ok", result: 7, attempts: 1 });
      await s.markTerminal(runId, { status: "failed", error: { code: "X", message: "boom" } });

      const res = await s.retryRun(runId);
      expect(res.retried).toBe(true);
      const snap = await s.loadRun(runId);
      expect(snap?.run.status).toBe("pending");
      expect(snap?.run.error).toBeUndefined();
      expect(snap?.steps.get("a")?.result).toBe(7); // memo preserved

      const again = await s.retryRun(runId); // not failed anymore → no-op
      expect(again.retried).toBe(false);
    });

    it("runStats counts runs per status", async () => {
      const s = await makeStore();
      const a = await s.startRun({ name: "f", version: 1, input: {} });
      await s.startRun({ name: "f", version: 1, input: {} });
      await s.markTerminal(a.runId, { status: "done", output: 1 });
      const stats = await s.runStats();
      expect(stats.pending).toBe(1);
      expect(stats.done).toBe(1);
      expect(stats.failed).toBe(0);
    });
  });
};
