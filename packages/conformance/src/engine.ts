import {
  type Backend,
  type Flow,
  type RetryPolicy,
  builder,
  cancelRun,
  defineFlow,
  reconcile,
  registry,
  runTick,
  serverlessTick,
  signalRun,
  submit,
  tickOnce,
  signalType,
} from "@iterativeflow/core";
import { isTerminal } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";

/**
 * Engine-behavior conformance: the composed durable behaviors (retry/dead-letter, signal
 * resume, cancel cascade, drift) run against every backend, not just memory — so a real
 * serialization/consistency regression can't hide behind the port-conformance suites.
 */
export const engineConformance = (
  label: string,
  makeBackend: () => Backend | Promise<Backend>,
): void => {
  describe(`engine conformance (${label})`, () => {
    const base = { batchMax: 16, leaseMs: 600_000 };

    const drive = async (
      backend: Backend,
      flows: ReturnType<typeof registry>,
      runId: string,
      retry?: RetryPolicy,
      start?: Date,
    ): Promise<{ status: string; output?: unknown; error?: { code?: string } }> => {
      let clock = start ?? new Date("2030-01-01T00:00:00Z");
      const now = (): Date => clock;
      for (let i = 0; i < 200; i++) {
        await tickOnce(backend, flows, { ...base, now, retry });
        const run = await backend.store.loadRunRow(runId);
        if (run && isTerminal(run.status)) return run;
        clock = new Date(clock.getTime() + 60_000);
      }
      throw new Error("run did not settle");
    };

    it("a run dispatched far more than maxAttempts times (many sleeps) completes", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({
        name: "poller",
        version: 1,
        run: async (ctx): Promise<string> => {
          for (let i = 0; i < 6; i++) await ctx.sleep(1000);
          return "settled";
        },
      });
      const runId = await submit(backend, flow, {});
      const run = await drive(backend, registry([flow]), runId, {
        maxAttempts: 3,
        baseDelayMs: 1,
        maxDelayMs: 1,
      });
      expect(run).toMatchObject({ status: "done", output: "settled" });
    });

    it("runs a builder flow with a durable sleep to completion", async () => {
      const backend = await makeBackend();
      const flow = builder<{ x: number }>(`${label}-sleep`, 1)
        .step("doubled", (acc) => acc.input.x * 2)
        .step("nap", async (_acc, ctx) => {
          await ctx.sleep(5_000);
          return "rested";
        })
        .output((acc) => ({ doubled: acc.doubled, nap: acc.nap }));
      const runId = await submit(backend, flow, { x: 21 });
      const run = await drive(backend, registry([flow]), runId);
      expect(run).toMatchObject({ status: "done", output: { doubled: 42, nap: "rested" } });
    });

    it("a genuinely failing step still dead-letters after its retry budget", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({
        name: "always-throws",
        version: 1,
        run: async (ctx): Promise<number> =>
          ctx.step("boom", () => {
            throw new Error("nope");
          }),
      });
      const runId = await submit(backend, flow, {});
      const run = await drive(backend, registry([flow]), runId, {
        maxAttempts: 2,
        baseDelayMs: 1,
        maxDelayMs: 1,
      });
      expect(run.status).toBe("failed");
    });

    it("dead-letters a poison-pill (uncatchable-crash) run once attempts exceed the cap", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({ name: "poison", version: 1, run: async () => 1 });
      const runId = await submit(backend, flow, {});
      for (let i = 0; i < 5; i++) await backend.store.markRunning(runId);
      await tickOnce(backend, registry([flow]), {
        ...base,
        now: () => new Date("2030-01-01T00:00:00Z"),
        retry: { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 1 },
      });
      const run = await backend.store.loadRunRow(runId);
      expect(run?.status).toBe("failed");
      expect(run?.error?.code).toBe("RUN_ATTEMPTS_EXHAUSTED");
    });

    it("awaits an external signal, parks, and resumes with its payload", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({
        name: "approve",
        version: 1,
        signals: { go: signalType<{ ok: boolean }>() },
        run: async (ctx): Promise<string> => {
          const d = await ctx.signal("go");
          return d.ok ? "shipped" : "held";
        },
      });
      const flows = registry([flow]);
      const runId = await submit(backend, flow, {});
      await tickOnce(backend, flows, { ...base, now: () => new Date("2030-01-01T00:00:00Z") });
      expect((await backend.store.loadRunRow(runId))?.status).toBe("awaiting_signal");
      await signalRun(backend, runId, "go", { ok: true });
      const run = await drive(backend, flows, runId);
      expect(run).toMatchObject({ status: "done", output: "shipped" });
    });

    it("cancels a run and cascades to grandchildren", async () => {
      const backend = await makeBackend();
      const grandchild = defineFlow({
        name: "gc",
        version: 1,
        run: async (ctx): Promise<string> => {
          await ctx.sleep(10_000_000);
          return "gc";
        },
      });
      const child = defineFlow({
        name: "c",
        version: 1,
        run: async (ctx): Promise<string> => ctx.invoke(grandchild, {}),
      });
      const parent = defineFlow({
        name: "p",
        version: 1,
        run: async (ctx): Promise<string> => ctx.invoke(child, {}),
      });
      const flows = registry([parent, child, grandchild]);
      const parentId = await submit(backend, parent, {});
      let clock = new Date("2030-01-01T00:00:00Z");
      for (let i = 0; i < 10; i++) {
        await tickOnce(backend, flows, { ...base, now: () => clock });
        clock = new Date(clock.getTime() + 1000);
      }
      await cancelRun(backend, parentId);
      const kids = await backend.store.childrenOf(parentId);
      expect(kids.length).toBeGreaterThan(0);
      const childId = kids[0].id;
      const grandkids = await backend.store.childrenOf(childId);
      expect((await backend.store.loadRunRow(parentId))?.status).toBe("canceled");
      expect((await backend.store.loadRunRow(childId))?.status).toBe("canceled");
      if (grandkids[0]) {
        expect((await backend.store.loadRunRow(grandkids[0].id))?.status).toBe("canceled");
      }
    });

    it("a child of an already-terminated parent cancels itself on dispatch (crash-safe cascade)", async () => {
      const backend = await makeBackend();
      const orphan = defineFlow({
        name: "orphan",
        version: 1,
        run: async (ctx): Promise<number> => {
          await ctx.sleep(1000);
          return 1;
        },
      });
      const { runId: parentId } = await backend.store.startRun({
        name: "p",
        version: 1,
        input: {},
      });
      await backend.store.markTerminal(parentId, {
        status: "failed",
        error: { code: "X", message: "parent died" },
      });
      const { runId: childId } = await backend.store.startRun({
        name: "orphan",
        version: 1,
        input: {},
        parentRunId: parentId,
        parentCursorKey: "s0",
      });
      await backend.queue.enqueue(childId);
      await tickOnce(backend, registry([orphan]), {
        ...base,
        now: () => new Date("2030-01-01T00:00:00Z"),
      });
      expect((await backend.store.loadRunRow(childId))?.status).toBe("canceled");
    });

    it("reconcile cancels a sleeping child left behind by a dead parent", async () => {
      const backend = await makeBackend();
      const now = (): Date => new Date("2030-01-01T00:00:00Z");
      const orphan = defineFlow({
        name: "orphan2",
        version: 1,
        run: async (ctx): Promise<number> => {
          await ctx.sleep(10_000_000);
          return 1;
        },
      });
      const { runId: parentId } = await backend.store.startRun({
        name: "p",
        version: 1,
        input: {},
      });
      const { runId: childId } = await backend.store.startRun({
        name: "orphan2",
        version: 1,
        input: {},
        parentRunId: parentId,
        parentCursorKey: "s0",
      });
      await backend.queue.enqueue(childId);
      await tickOnce(backend, registry([orphan]), { ...base, now });
      expect((await backend.store.loadRunRow(childId))?.status).toBe("sleeping");

      await backend.store.markTerminal(parentId, {
        status: "failed",
        error: { code: "X", message: "died" },
      });
      await reconcile(backend, { limit: 16 });
      await tickOnce(backend, registry([orphan]), { ...base, now });
      expect((await backend.store.loadRunRow(childId))?.status).toBe("canceled");
    });

    it("fans out children in parallel and joins their outputs in order", async () => {
      const backend = await makeBackend();
      const dbl = defineFlow({
        name: "dbl",
        version: 1,
        run: async (_ctx, n: number): Promise<number> => n * 2,
      });
      const parent = defineFlow({
        name: "fan",
        version: 1,
        run: async (ctx): Promise<readonly number[]> =>
          ctx.invoke([
            { flow: dbl, input: 1 },
            { flow: dbl, input: 2 },
            { flow: dbl, input: 3 },
          ]),
      });
      const runId = await submit(backend, parent, {});
      const run = await drive(backend, registry([parent, dbl]), runId);
      expect(run).toMatchObject({ status: "done", output: [2, 4, 6] });
    });

    it("rejects a fan-out beyond the cap", async () => {
      const backend = await makeBackend();
      const noop = defineFlow({ name: "noop", version: 1, run: async () => 1 });
      const parent = defineFlow({
        name: "toobig",
        version: 1,
        run: async (ctx): Promise<readonly number[]> =>
          ctx.invoke(Array.from({ length: 10_001 }, () => ({ flow: noop, input: {} }))),
      });
      const runId = await submit(backend, parent, {});
      await tickOnce(backend, registry([parent, noop]), {
        ...base,
        now: () => new Date("2030-01-01T00:00:00Z"),
        retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1 },
      });
      expect((await backend.store.loadRunRow(runId))?.status).toBe("failed");
    });

    it("caps invoke recursion at the depth limit and persists each run's depth", async () => {
      const backend = await makeBackend();
      // Always invokes itself — unbounded recursion without the cap.
      const recurse: Flow<number, number> = defineFlow({
        name: "recurse",
        version: 1,
        policy: { maxDepth: 3 },
        run: async (ctx, n: number): Promise<number> => ctx.invoke(recurse, n + 1),
      });
      const runId = await submit(backend, recurse, 0);
      const top = await drive(backend, registry([recurse]), runId, {
        maxAttempts: 1,
        baseDelayMs: 1,
        maxDelayMs: 1,
      });
      expect(top.status).toBe("failed");
      // The tree stops growing at the cap: the deepest descendant is the one that hit it.
      let cur = runId;
      let deepest = await backend.store.loadRunRow(cur);
      for (;;) {
        const kids = await backend.store.childrenOf(cur);
        if (kids.length === 0) break;
        deepest = kids[0];
        cur = kids[0].id;
      }
      expect(deepest?.depth).toBe(3);
      expect(deepest?.error?.message).toContain("depth");
    });

    it("fans out a batch larger than one spawn chunk (crosses chunk boundaries)", async () => {
      const backend = await makeBackend();
      const inc = defineFlow({
        name: "inc",
        version: 1,
        run: async (_ctx, n: number): Promise<number> => n + 1,
      });
      const parent = defineFlow({
        name: "wide",
        version: 1,
        run: async (ctx): Promise<readonly number[]> =>
          ctx.invoke(Array.from({ length: 45 }, (_v, n) => ({ flow: inc, input: n }))),
      });
      const runId = await submit(backend, parent, {});
      const run = await drive(backend, registry([parent, inc]), runId);
      expect(run.status).toBe("done");
      expect(run.output).toEqual(Array.from({ length: 45 }, (_v, n) => n + 1));
    });

    it("fast-fails a fan-out and cancels the running siblings when one child fails", async () => {
      const backend = await makeBackend();
      const boom = defineFlow({
        name: "boom",
        version: 1,
        run: async (): Promise<number> => {
          throw new Error("boom");
        },
      });
      const slow = defineFlow({
        name: "slow",
        version: 1,
        run: async (ctx): Promise<number> => {
          await ctx.sleep(10_000_000);
          return 1;
        },
      });
      const parent = defineFlow({
        name: "fanfail",
        version: 1,
        run: async (ctx): Promise<readonly number[]> =>
          ctx.invoke([
            { flow: boom, input: {} },
            { flow: slow, input: {} },
          ]),
      });
      const runId = await submit(backend, parent, {});
      const run = await drive(backend, registry([parent, boom, slow]), runId, {
        maxAttempts: 1,
        baseDelayMs: 1,
        maxDelayMs: 1,
      });
      expect(run.status).toBe("failed");
      const kids = await backend.store.childrenOf(runId);
      expect(kids).toHaveLength(2);
      expect(kids.every((k) => isTerminal(k.status))).toBe(true);
      expect(kids.some((k) => k.status === "canceled")).toBe(true);
    });

    it("a claimed run for an unregistered flow version parks and resumes once that version deploys", async () => {
      const backend = await makeBackend();
      const v1 = defineFlow({ name: "wf", version: 1, run: async (): Promise<string> => "v1" });
      const v2 = defineFlow({ name: "wf", version: 2, run: async (): Promise<string> => "v2" });
      const fast: RetryPolicy = { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 1 };
      const runId = await submit(backend, v2, {});
      let clock = new Date("2030-01-01T00:00:00Z");
      const now = (): Date => clock;
      // A worker running only v1 shares the "wf" name, so it claims the v2 run but can't advance it.
      await tickOnce(backend, registry([v1]), { ...base, now, retry: fast });
      expect((await backend.store.loadRunRow(runId))?.status).toBe("retrying");
      // The deploy-skew window must not dead-letter the run: repeated stale-worker ticks keep it parked.
      for (let i = 0; i < 4; i++) {
        clock = new Date(clock.getTime() + 60_000);
        await tickOnce(backend, registry([v1]), { ...base, now, retry: fast });
        expect((await backend.store.loadRunRow(runId))?.status).toBe("retrying");
      }
      const run = await drive(backend, registry([v2]), runId, fast, clock);
      expect(run).toMatchObject({ status: "done", output: "v2" });
    });

    it("a signal delivered at the timeout deadline wins the race — never lost to the timeout", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({
        name: "raced",
        version: 1,
        signals: { go: signalType<string>() },
        run: async (ctx): Promise<string> => {
          const r = await ctx.signal("go", { timeoutMs: 5_000 });
          return r.received ? `signal:${r.payload}` : "timeout";
        },
      });
      const flows = registry([flow]);
      const t0 = new Date("2030-01-01T00:00:00Z");
      const runId = await submit(backend, flow, {});
      await tickOnce(backend, flows, { ...base, now: () => t0 });
      expect((await backend.store.loadRunRow(runId))?.status).toBe("awaiting_signal");
      // Collision: at the exact deadline the timeout timer is due AND a signal sits in the inbox.
      const deadline = new Date(t0.getTime() + 5_000);
      await signalRun(backend, runId, "go", "ok");
      const run = await drive(backend, flows, runId, undefined, deadline);
      expect(run).toMatchObject({ status: "done", output: "signal:ok" });
    });

    it("a bounded signal wait commits { received: false } when nothing arrives before the deadline", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({
        name: "waits-out",
        version: 1,
        signals: { go: signalType<string>() },
        run: async (ctx): Promise<string> => {
          const r = await ctx.signal("go", { timeoutMs: 5_000 });
          return r.received ? "signal" : "timeout";
        },
      });
      const runId = await submit(backend, flow, {});
      await tickOnce(backend, registry([flow]), {
        ...base,
        now: () => new Date("2030-01-01T00:00:00Z"),
      });
      const run = await drive(backend, registry([flow]), runId);
      expect(run).toMatchObject({ status: "done", output: "timeout" });
    });

    it("memoizes a completed step across a crash-retry — the step fn runs once, the run still completes", async () => {
      const backend = await makeBackend();
      let stepCalls = 0;
      let invocations = 0;
      const flow = defineFlow({
        name: "flaky",
        version: 1,
        run: async (ctx): Promise<number> => {
          const v = await ctx.step("charge", () => {
            stepCalls += 1;
            return 42;
          });
          invocations += 1;
          if (invocations === 1) throw new Error("crash after the charge committed");
          return v;
        },
      });
      const runId = await submit(backend, flow, {});
      const run = await drive(backend, registry([flow]), runId, {
        maxAttempts: 5,
        baseDelayMs: 1,
        maxDelayMs: 1,
      });
      expect(run).toMatchObject({ status: "done", output: 42 });
      expect(stepCalls).toBe(1);
    });

    it("reconcile re-drives a run stranded off the queue (crash between start and enqueue)", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({
        name: "stranded",
        version: 1,
        run: async (): Promise<string> => "recovered",
      });
      const flows = registry([flow]);
      const { runId } = await backend.store.startRun({ name: "stranded", version: 1, input: {} });
      let clock = new Date("2030-01-01T00:00:00Z");
      const now = (): Date => clock;
      for (let i = 0; i < 10; i++) {
        await reconcile(backend, { limit: 16 });
        await tickOnce(backend, flows, { ...base, now });
        const r = await backend.store.loadRunRow(runId);
        if (r && isTerminal(r.status)) break;
        clock = new Date(clock.getTime() + 60_000);
      }
      expect(await backend.store.loadRunRow(runId)).toMatchObject({
        status: "done",
        output: "recovered",
      });
    });

    it("a reclaimed run fences the crashed worker — its stale ack and duplicate commit don't corrupt the new owner", async () => {
      const { store, queue } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await queue.enqueue(runId);
      const t0 = new Date("2030-01-01T00:00:00Z");
      const [a] = await queue.claim({ limit: 1, leaseMs: 1_000, now: t0 });
      // A's lease expires; B reclaims and commits real progress.
      const t1 = new Date(t0.getTime() + 2_000);
      const [b] = await queue.claim({ limit: 1, leaseMs: 600_000, now: t1 });
      expect(b?.runId).toBe(runId);
      await store.checkpointStep({
        runId,
        cursorKey: "s0",
        status: "ok",
        result: "B",
        attempts: 1,
      });
      // A wakes stale: its duplicate step is first-writer-fenced, its ack is a no-op.
      const late = await store.checkpointStep({
        runId,
        cursorKey: "s0",
        status: "ok",
        result: "A",
        attempts: 1,
      });
      expect(late.result).toBe("B");
      await queue.ack(a!, { now: new Date(t1.getTime() + 1_000) });
      expect((await store.loadRun(runId))?.steps.get("s0")?.result).toBe("B");
      // B's lease survived A's stale ack — a heartbeat still renews it.
      await expect(
        queue.heartbeat(b!, { leaseMs: 600_000, now: new Date(t1.getTime() + 1_000) }),
      ).resolves.toBeDefined();
    });

    it("a cancel landing while a worker holds a live lease still cancels — the in-flight tick doesn't resurrect it", async () => {
      const backend = await makeBackend();
      let bodyRan = 0;
      const flow = defineFlow({
        name: "cancelable",
        version: 1,
        run: async (ctx): Promise<string> => {
          bodyRan += 1;
          await ctx.sleep(10_000);
          return "done";
        },
      });
      const flows = registry([flow]);
      const { store, queue } = backend;
      const { runId } = await store.startRun({ name: "cancelable", version: 1, input: {} });
      await queue.enqueue(runId);
      const t0 = new Date("2030-01-01T00:00:00Z");
      const [lease] = await queue.claim({ limit: 1, leaseMs: 600_000, now: t0 });
      // Cancel arrives while the worker holds a live lease, before its tick observes the run.
      await cancelRun(backend, runId);
      const result = await runTick(backend, flows, lease!, { ...base, now: () => t0 });
      expect(result.status).toBe("already_terminal");
      expect((await store.loadRunRow(runId))?.status).toBe("canceled");
      expect(bodyRan).toBe(0);
      const after = await queue.claim({
        limit: 1,
        leaseMs: 1_000,
        now: new Date(t0.getTime() + 1_000),
      });
      expect(after).toHaveLength(0);
    });

    it("two disjoint worker registries drive their own flows to completion against one backend", async () => {
      const backend = await makeBackend();
      const a = defineFlow({ name: "sa", version: 1, run: async (): Promise<string> => "a-done" });
      const b = defineFlow({ name: "sb", version: 1, run: async (): Promise<string> => "b-done" });
      const podA = registry([a]);
      const podB = registry([b]);
      const idA = await submit(backend, a, {});
      const idB = await submit(backend, b, {});
      let clock = new Date("2030-01-01T00:00:00Z");
      const now = (): Date => clock;
      for (let i = 0; i < 10; i++) {
        // Two registries share one backend; each claims only the flow it registered (name shard).
        await tickOnce(backend, podA, { ...base, now });
        await tickOnce(backend, podB, { ...base, now });
        const ra = await backend.store.loadRunRow(idA);
        const rb = await backend.store.loadRunRow(idB);
        if (ra && rb && isTerminal(ra.status) && isTerminal(rb.status)) break;
        clock = new Date(clock.getTime() + 60_000);
      }
      expect(await backend.store.loadRunRow(idA)).toMatchObject({
        status: "done",
        output: "a-done",
      });
      expect(await backend.store.loadRunRow(idB)).toMatchObject({
        status: "done",
        output: "b-done",
      });
    });

    it("serverlessTick advances a durable sleep and reports the next wake horizon (no resident loop)", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({
        name: "napper",
        version: 1,
        run: async (ctx): Promise<string> => {
          await ctx.sleep(60_000);
          return "woke";
        },
      });
      const flows = registry([flow]);
      let clock = new Date("2030-01-01T00:00:00Z");
      const now = (): Date => clock;
      const runId = await submit(backend, flow, {});
      const s1 = await serverlessTick(backend, flows, { ...base, now });
      expect(s1.results.map((r) => r.status)).toContain("sleeping");
      expect(s1.nextWakeAt?.getTime()).toBe(clock.getTime() + 60_000);
      // One invocation after the deadline resumes the sleep — nothing ran between the two.
      clock = new Date(clock.getTime() + 60_000);
      await serverlessTick(backend, flows, { ...base, now });
      expect(await backend.store.loadRunRow(runId)).toMatchObject({
        status: "done",
        output: "woke",
      });
    });
  });
};
