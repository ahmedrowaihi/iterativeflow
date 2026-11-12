import {
  type AnyFlow,
  type Backend,
  type Flow,
  type RetryPolicy,
  cancelRun,
  cronTag,
  defineFlow,
  StepFailedError,
  reconcile,
  registerCron,
  retryRun,
  registry,
  runDueCrons,
  runTick,
  serverlessTick,
  signalRun,
  submit,
  tickOnce,
  type SignalSchema,
} from "@iterativeflow/core";
import { isTerminal } from "@iterativeflow/core/backend";
import { describe, expect, it } from "vitest";
import { T0, at } from "#clock";

const okSchema: SignalSchema<{ ok: boolean }> = {
  "~standard": {
    version: 1,
    vendor: "conformance",
    validate: (v) =>
      v instanceof Object && "ok" in v
        ? { value: { ok: v.ok === true } }
        : { issues: [{ message: "ok is required" }] },
  },
};

const textSchema: SignalSchema<string> = {
  "~standard": {
    version: 1,
    vendor: "conformance",
    validate: (v) =>
      v === String(v) ? { value: String(v) } : { issues: [{ message: "not text" }] },
  },
};

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

    const fastRetry = (maxAttempts: number): RetryPolicy => ({
      maxAttempts,
      baseDelayMs: 1,
      maxDelayMs: 1,
    });

    const tickUntil = async (
      backend: Backend,
      flows: ReturnType<typeof registry>,
      done: () => Promise<boolean>,
      retry?: RetryPolicy,
      start: Date = T0,
    ): Promise<void> => {
      let clock = start;
      const now = (): Date => clock;
      for (let i = 0; i < 200; i++) {
        await tickOnce(backend, flows, { ...base, now, retry });
        if (await done()) return;
        clock = new Date(clock.getTime() + 60_000);
      }
      throw new Error("condition never held");
    };

    const drive = async (
      backend: Backend,
      flows: ReturnType<typeof registry>,
      runId: string,
      retry?: RetryPolicy,
      start?: Date,
    ): Promise<{ status: string; output?: unknown; error?: { code?: string } }> => {
      let run = await backend.store.loadRunRow(runId);
      const settled = async (): Promise<boolean> => {
        run = await backend.store.loadRunRow(runId);
        return run !== undefined && isTerminal(run.status);
      };
      await tickUntil(backend, flows, settled, retry, start);
      if (!run) throw new Error(`run ${runId} vanished`);
      return run;
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
      const run = await drive(backend, registry([flow]), runId, fastRetry(3));
      expect(run).toMatchObject({ status: "done", output: "settled" });
    });

    it("a flow with a durable sleep between steps resumes to completion", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({
        name: `${label}-sleep`,
        version: 1,
        run: async (ctx, input: { x: number }) => {
          const doubled = await ctx.step("doubled", () => input.x * 2);
          await ctx.sleep(5_000);
          const nap = await ctx.step("nap", () => "rested");
          return { doubled, nap };
        },
      });
      const runId = await submit(backend, flow, { x: 21 });
      const run = await drive(backend, registry([flow]), runId);
      expect(run).toMatchObject({ status: "done", output: { doubled: 42, nap: "rested" } });
    });

    it("a genuinely failing step still dead-letters after its retry budget", async () => {
      const backend = await makeBackend();
      let calls = 0;
      const flow = defineFlow({
        name: "always-throws",
        version: 1,
        run: async (ctx): Promise<number> =>
          ctx.step("boom", () => {
            calls += 1;
            throw new Error("nope");
          }),
      });
      const runId = await submit(backend, flow, {});
      const run = await drive(backend, registry([flow]), runId, fastRetry(2));
      expect(run.status).toBe("failed");
      expect(calls).toBe(2);
    });

    it("dead-letters a poison-pill (uncatchable-crash) run once attempts exceed the cap", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({ name: "poison", version: 1, run: async () => 1 });
      const runId = await submit(backend, flow, {});
      for (let i = 0; i < 5; i++) await backend.store.markRunning(runId);
      await tickOnce(backend, registry([flow]), { ...base, now: () => T0, retry: fastRetry(3) });
      const run = await backend.store.loadRunRow(runId);
      expect(run?.status).toBe("failed");
      expect(run?.error?.code).toBe("RUN_ATTEMPTS_EXHAUSTED");
    });

    it("awaits an external signal, parks, and resumes with its payload", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({
        name: "approve",
        version: 1,
        signals: { go: okSchema },
        run: async (ctx): Promise<string> => {
          const d = await ctx.signal("go");
          return d.ok ? "shipped" : "held";
        },
      });
      const flows = registry([flow]);
      const runId = await submit(backend, flow, {});
      await tickOnce(backend, flows, { ...base, now: () => T0 });
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
      const grandkidSleeping = async (): Promise<boolean> => {
        const [kid] = await backend.store.childrenOf(parentId);
        const [grandkid] = kid ? await backend.store.childrenOf(kid.id) : [];
        return grandkid?.status === "sleeping";
      };
      await tickUntil(backend, flows, grandkidSleeping);
      await cancelRun(backend, parentId);
      const kids = await backend.store.childrenOf(parentId);
      expect(kids).toHaveLength(1);
      const grandkids = await backend.store.childrenOf(kids[0].id);
      expect(grandkids).toHaveLength(1);
      expect((await backend.store.loadRunRow(parentId))?.status).toBe("canceled");
      expect((await backend.store.loadRunRow(kids[0].id))?.status).toBe("canceled");
      expect((await backend.store.loadRunRow(grandkids[0].id))?.status).toBe("canceled");
    });

    it("a retried parent whose finished child was purged fails the join instead of waiting forever", async () => {
      const backend = await makeBackend();
      let fail = true;
      const child = defineFlow<Record<string, never>, number>({
        name: "gone-child",
        version: 1,
        run: async () => 1,
      });
      const parent = defineFlow<Record<string, never>, number>({
        name: "gone-parent",
        version: 1,
        run: async (ctx) => {
          const v = await ctx.invoke(child, {});
          if (fail) throw new StepFailedError("BOOM", "fail after the child finished");
          return v;
        },
      });
      const flows = registry([parent, child]);
      const runId = await submit(backend, parent, {});
      expect(await drive(backend, flows, runId)).toMatchObject({ status: "failed" });
      expect(await backend.store.deleteRuns({ name: "gone-child" }, 10)).toBe(1);
      fail = false;
      expect(await retryRun(backend, runId)).toBe(true);
      expect(await drive(backend, flows, runId)).toMatchObject({
        status: "failed",
        error: { code: "CHILD_GONE" },
      });
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
      await tickOnce(backend, registry([orphan]), { ...base, now: () => T0 });
      expect((await backend.store.loadRunRow(childId))?.status).toBe("canceled");
    });

    it("reconcile cancels a sleeping child left behind by a dead parent", async () => {
      const backend = await makeBackend();
      const now = (): Date => T0;
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

    it("rejects a fan-out beyond the flow's maxFanOut and spawns none of it", async () => {
      const backend = await makeBackend();
      const noop = defineFlow({ name: "noop", version: 1, run: async () => 1 });
      const parent = defineFlow({
        name: "toobig",
        version: 1,
        policy: { maxFanOut: 3 },
        run: async (ctx): Promise<readonly number[]> =>
          ctx.invoke(Array.from({ length: 4 }, () => ({ flow: noop, input: {} }))),
      });
      const runId = await submit(backend, parent, {});
      await tickOnce(backend, registry([parent, noop]), {
        ...base,
        now: () => T0,
        retry: fastRetry(1),
      });
      expect((await backend.store.loadRunRow(runId))?.status).toBe("failed");
      expect(await backend.store.childrenOf(runId)).toHaveLength(0);
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
      const top = await drive(backend, registry([recurse]), runId, fastRetry(1));
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

    // Wide enough that the engine spawns it across several checkpoints; the chunk tests below
    // assert they actually observed a later chunk, so a chunk size >= WIDE fails them loudly.
    const WIDE = 100;
    const inc = defineFlow({
      name: "inc",
      version: 1,
      run: async (_ctx, n: number): Promise<number> => n + 1,
    });
    const wide = defineFlow({
      name: "wide",
      version: 1,
      run: async (ctx): Promise<readonly number[]> =>
        ctx.invoke(Array.from({ length: WIDE }, (_v, n) => ({ flow: inc, input: n }))),
    });
    const wideOutput = Array.from({ length: WIDE }, (_v, n) => n + 1);

    type Commit = ReturnType<Backend["store"]["checkpointStep"]>;
    const onFinalSpawnChunk = (real: Backend, around: (commit: () => Commit) => Commit) => {
      let spawned = 0;
      let fired = false;
      const backend: Backend = {
        ...real,
        store: {
          ...real.store,
          checkpointStep: async (c, fx) => {
            const commit = (): Commit => real.store.checkpointStep(c, fx);
            const n = fx?.spawn?.length ?? 0;
            if (fired || n === 0) return commit();
            spawned += n;
            // `spawned > n`: an earlier chunk already committed, so this one is a later chunk.
            if (spawned < WIDE || spawned === n) return commit();
            fired = true;
            return around(commit);
          },
        },
      };
      return { backend, fired: () => fired };
    };

    it("fans out a batch spanning several spawn checkpoints and joins it in order", async () => {
      const backend = await makeBackend();
      const runId = await submit(backend, wide, {});
      const run = await drive(backend, registry([wide, inc]), runId);
      expect(run).toMatchObject({ status: "done", output: wideOutput });
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
      const run = await drive(backend, registry([parent, boom, slow]), runId, fastRetry(1));
      expect(run.status).toBe("failed");
      const kids = await backend.store.childrenOf(runId);
      expect(kids).toHaveLength(2);
      expect(kids.every((k) => isTerminal(k.status))).toBe(true);
      expect(kids.some((k) => k.status === "canceled")).toBe(true);
    });

    it("a settled fan-out returns every child's result and cancels no sibling", async () => {
      const backend = await makeBackend();
      const ok = defineFlow({
        name: "settle-ok",
        version: 1,
        run: async (_ctx, n: number): Promise<number> => n,
      });
      const boom = defineFlow({
        name: "settle-boom",
        version: 1,
        run: async (): Promise<number> => {
          throw new Error("boom");
        },
      });
      const parent = defineFlow({
        name: "settle-fan",
        version: 1,
        run: async (ctx) =>
          ctx.invoke(
            [
              { flow: ok, input: 1 },
              { flow: boom, input: {} },
              { flow: ok, input: 2 },
            ],
            { onChildFailure: "settle" },
          ),
      });
      const runId = await submit(backend, parent, {});
      const run = await drive(backend, registry([parent, ok, boom]), runId, fastRetry(1));
      expect(run).toMatchObject({
        status: "done",
        output: [
          { status: "done", output: 1 },
          { status: "failed", error: { message: "boom" } },
          { status: "done", output: 2 },
        ],
      });
      const kids = await backend.store.childrenOf(runId);
      expect(kids.filter((k) => k.status === "canceled")).toHaveLength(0);
    });

    const waiter = defineFlow({
      name: "waits-forever",
      version: 1,
      run: async (ctx): Promise<number> => {
        await ctx.signal("never");
        return 1;
      },
    });

    const parkOnChild = async (backend: Backend, parent: AnyFlow) => {
      const flows = registry([parent, waiter]);
      const runId = await submit(backend, parent, {});
      const now = () => T0;
      await tickOnce(backend, flows, { ...base, now });
      await tickOnce(backend, flows, { ...base, now });
      expect((await backend.store.loadRunRow(runId))?.status).toBe("awaiting_child");
      const [child] = await backend.store.childrenOf(runId);
      return { runId, childId: child.id, tick: () => tickOnce(backend, flows, { ...base, now }) };
    };

    it("canceling a child wakes its fail-fast parent without waiting for reconcile", async () => {
      const backend = await makeBackend();
      const parent = defineFlow({
        name: "waits-on-child",
        version: 1,
        run: async (ctx): Promise<number> => ctx.invoke(waiter, {}),
      });
      const { runId, childId, tick } = await parkOnChild(backend, parent);
      await cancelRun(backend, childId);
      await tick();
      expect((await backend.store.loadRunRow(runId))?.status).toBe("failed");
    });

    it("a settled parent reports a canceled child as canceled and completes", async () => {
      const backend = await makeBackend();
      const parent = defineFlow({
        name: "settles-on-child",
        version: 1,
        run: async (ctx) => ctx.invoke(waiter, {}, { onChildFailure: "settle" }),
      });
      const { runId, childId, tick } = await parkOnChild(backend, parent);
      await cancelRun(backend, childId);
      await tick();
      expect(await backend.store.loadRunRow(runId)).toMatchObject({
        status: "done",
        output: { status: "canceled" },
      });
    });

    it("a crash right after a later spawn chunk commits spawns no child twice", async () => {
      const real = await makeBackend();
      const { backend, fired } = onFinalSpawnChunk(real, async (commit) => {
        await commit();
        throw new Error("worker died after the commit");
      });
      const runId = await submit(backend, wide, {});
      const run = await drive(backend, registry([wide, inc]), runId);
      expect(fired()).toBe(true);
      expect(run.output).toEqual(wideOutput);
      expect(await backend.store.childrenOf(runId)).toHaveLength(WIDE);
    });

    it("a parent canceled between spawn chunks leaves no child running", async () => {
      const real = await makeBackend();
      const runId = await submit(real, wide, {});
      const { backend, fired } = onFinalSpawnChunk(real, async (commit) => {
        await cancelRun(real, runId);
        return commit();
      });
      const kidsSettled = async (): Promise<boolean> => {
        const kids = await backend.store.childrenOf(runId);
        return kids.length > 0 && kids.every((k) => isTerminal(k.status));
      };
      await tickUntil(backend, registry([wide, inc]), kidsSettled);
      expect(fired()).toBe(true);
      expect((await backend.store.loadRunRow(runId))?.status).toBe("canceled");
      const kids = await backend.store.childrenOf(runId);
      expect(kids).toHaveLength(WIDE);
      expect(kids.every((k) => k.status === "canceled")).toBe(true);
    });

    it("a child that finishes while its parent is mid-replay still wakes the parent", async () => {
      const real = await makeBackend();
      const quick = defineFlow({
        name: "quick-child",
        version: 1,
        run: async (): Promise<number> => 7,
      });
      const parent = defineFlow({
        name: "replaying-parent",
        version: 1,
        run: async (ctx): Promise<number> => ctx.invoke(quick, {}),
      });
      const flows = registry([parent, quick]);
      const now = () => T0;
      let raced = false;
      // The parent reads its child as still pending, and the child finishes (waking the leased
      // parent) before the parent parks — the wake must survive the parent's own ack.
      const backend: Backend = {
        ...real,
        store: {
          ...real.store,
          loadRunRow: async (id) => {
            const row = await real.store.loadRunRow(id);
            if (!raced && row?.name === "quick-child") {
              raced = true;
              await tickOnce(real, flows, { ...base, now, names: ["quick-child"] });
            }
            return row;
          },
        },
      };
      const runId = await submit(backend, parent, {});
      await tickOnce(backend, flows, { ...base, now, names: ["replaying-parent"] });
      expect(raced).toBe(true);
      expect((await real.store.loadRunRow(runId))?.status).toBe("awaiting_child");
      const run = await drive(real, flows, runId);
      expect(run).toMatchObject({ status: "done", output: 7 });
    });

    it("a re-enqueue with no priority keeps the run's own priority", async () => {
      const backend = await makeBackend();
      const low = (await backend.store.startRun({ name: "p", version: 1, input: {}, priority: 10 }))
        .runId;
      const urgent = (
        await backend.store.startRun({ name: "p", version: 1, input: {}, priority: -10 })
      ).runId;
      // Every wake re-enqueues like this: no priority. `urgent` is due later, so without its stored
      // priority the claim would order by `run_at` and pick `low` — deterministically, not by chance.
      await backend.queue.enqueueMany([
        { runId: low, opts: { runAt: at(0) } },
        { runId: urgent, opts: { runAt: at(1000) } },
      ]);
      const [first] = await backend.queue.claim({ limit: 1, leaseMs: 60_000, now: at(2000) });
      expect(first.runId).toBe(urgent);
    });

    it("a signal delivery, a retry and a bulk retry keep the run's own priority", async () => {
      const backend = await makeBackend();
      const start = async (priority?: number) =>
        (await backend.store.startRun({ name: "p", version: 1, input: {}, priority })).runId;
      const claimFirst = async () =>
        (await backend.queue.claim({ limit: 1, leaseMs: 60_000 }))[0]?.runId;
      // `rival` is enqueued at -5, so a woken run that lost its -10 (falling back to 0) loses to it.
      const rival = async () => backend.queue.enqueue(await start(), { priority: -5 });

      const signalled = await start(-10);
      await rival();
      await backend.store.postSignal(signalled, "go", {});
      expect(await claimFirst()).toBe(signalled);

      const retried = await start(-10);
      await backend.store.markTerminal(retried, {
        status: "failed",
        error: { code: "X", message: "x" },
      });
      await rival();
      await backend.store.retryRun(retried);
      expect(await claimFirst()).toBe(retried);

      const bulk = await start(-10);
      await backend.store.markTerminal(bulk, {
        status: "failed",
        error: { code: "X", message: "x" },
      });
      await rival();
      await backend.store.retryRuns({ status: "failed" }, 100);
      expect(await claimFirst()).toBe(bulk);
    });

    it("a claimed run for an unregistered flow version parks and resumes once that version deploys", async () => {
      const backend = await makeBackend();
      const v1 = defineFlow({ name: "wf", version: 1, run: async (): Promise<string> => "v1" });
      const v2 = defineFlow({ name: "wf", version: 2, run: async (): Promise<string> => "v2" });
      const fast = fastRetry(3);
      const runId = await submit(backend, v2, {});
      let clock = T0;
      const now = (): Date => clock;
      // A worker running only v1 shares the "wf" name, so it claims the v2 run but can't advance it.
      await tickOnce(backend, registry([v1]), { ...base, now, retry: fast });
      expect((await backend.store.loadRunRow(runId))?.status).toBe("parked");
      // The deploy-skew window must not dead-letter the run: repeated stale-worker ticks keep it parked.
      for (let i = 0; i < 4; i++) {
        clock = new Date(clock.getTime() + 60_000);
        await tickOnce(backend, registry([v1]), { ...base, now, retry: fast });
        expect((await backend.store.loadRunRow(runId))?.status).toBe("parked");
      }
      const run = await drive(backend, registry([v2]), runId, fast, clock);
      expect(run).toMatchObject({ status: "done", output: "v2" });
    });

    it("a signal delivered at the timeout deadline wins the race — never lost to the timeout", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({
        name: "raced",
        version: 1,
        signals: { go: textSchema },
        run: async (ctx): Promise<string> => {
          const r = await ctx.signal("go", { timeoutMs: 5_000 });
          return r.received ? `signal:${r.payload}` : "timeout";
        },
      });
      const flows = registry([flow]);
      const runId = await submit(backend, flow, {});
      await tickOnce(backend, flows, { ...base, now: () => T0 });
      expect((await backend.store.loadRunRow(runId))?.status).toBe("awaiting_signal");
      // Collision: at the exact deadline the timeout timer is due AND a signal sits in the inbox.
      await signalRun(backend, runId, "go", "ok");
      const run = await drive(backend, flows, runId, undefined, at(5_000));
      expect(run).toMatchObject({ status: "done", output: "signal:ok" });
    });

    it("a bounded signal wait commits { received: false } when nothing arrives before the deadline", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({
        name: "waits-out",
        version: 1,
        signals: { go: textSchema },
        run: async (ctx): Promise<string> => {
          const r = await ctx.signal("go", { timeoutMs: 5_000 });
          return r.received ? "signal" : "timeout";
        },
      });
      const runId = await submit(backend, flow, {});
      await tickOnce(backend, registry([flow]), { ...base, now: () => T0 });
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
      const run = await drive(backend, registry([flow]), runId, fastRetry(5));
      expect(run).toMatchObject({ status: "done", output: 42 });
      expect(stepCalls).toBe(1);
      expect(invocations).toBe(2);
    });

    it("reconcile re-drives a run stranded off the queue (crash between start and enqueue)", async () => {
      const backend = await makeBackend();
      const flow = defineFlow({
        name: "stranded",
        version: 1,
        run: async (): Promise<string> => "recovered",
      });
      const { runId } = await backend.store.startRun({ name: "stranded", version: 1, input: {} });
      await reconcile(backend, { limit: 16 });
      const run = await drive(backend, registry([flow]), runId);
      expect(run).toMatchObject({ status: "done", output: "recovered" });
    });

    it("a reclaimed run fences the crashed worker — its stale ack and duplicate commit don't corrupt the new owner", async () => {
      const { store, queue } = await makeBackend();
      const { runId } = await store.startRun({ name: "f", version: 1, input: {} });
      await queue.enqueue(runId);
      const [a] = await queue.claim({ limit: 1, leaseMs: 1_000, now: T0 });
      // A's lease expires; B reclaims and commits real progress.
      const [b] = await queue.claim({ limit: 1, leaseMs: 600_000, now: at(2_000) });
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
      await queue.ack(a!, { now: at(3_000) });
      expect((await store.loadRun(runId))?.steps.get("s0")?.result).toBe("B");
      // B's lease survived A's stale ack — a heartbeat still renews it.
      await expect(
        queue.heartbeat(b!, { leaseMs: 600_000, now: at(3_000) }),
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
      const [lease] = await queue.claim({ limit: 1, leaseMs: 600_000, now: T0 });
      // Cancel arrives while the worker holds a live lease, before its tick observes the run.
      await cancelRun(backend, runId);
      const result = await runTick(backend, flows, lease!, { ...base, now: () => T0 });
      expect(result.status).toBe("already_terminal");
      expect((await store.loadRunRow(runId))?.status).toBe("canceled");
      expect(bodyRan).toBe(0);
      expect(await queue.claim({ limit: 1, leaseMs: 1_000, now: at(1_000) })).toHaveLength(0);
    });

    it("two disjoint worker registries drive their own flows to completion against one backend", async () => {
      const backend = await makeBackend();
      const a = defineFlow({ name: "sa", version: 1, run: async (): Promise<string> => "a-done" });
      const b = defineFlow({ name: "sb", version: 1, run: async (): Promise<string> => "b-done" });
      const idA = await submit(backend, a, {});
      const idB = await submit(backend, b, {});
      expect(await drive(backend, registry([a]), idA)).toMatchObject({
        status: "done",
        output: "a-done",
      });
      expect((await backend.store.loadRunRow(idB))?.status).toBe("pending");
      expect(await drive(backend, registry([b]), idB)).toMatchObject({
        status: "done",
        output: "b-done",
      });
    });

    it("a skip-overlap cron starts no second run while its first is still live", async () => {
      const backend = await makeBackend();
      const slow = defineFlow<Record<string, never>, string>({
        name: "cron-slow",
        version: 1,
        run: async (ctx) => {
          await ctx.sleep(3_600_000);
          return "ok";
        },
      });
      const def = { name: "every-min", schedule: "* * * * *", flow: slow, input: {} };
      await registerCron(backend, { ...def, overlap: "skip" }, () => T0);
      expect(await runDueCrons(backend, () => at(60_000))).toBe(1);
      await tickOnce(backend, registry([slow]), {
        batchMax: 10,
        leaseMs: 60_000,
        now: () => at(60_000),
      });
      await runDueCrons(backend, () => at(120_000));
      const { runs } = await backend.store.listRuns({ tag: cronTag("every-min") }, { limit: 10 });
      expect(runs.map((r) => r.status)).toEqual(["sleeping"]);
      const [cron] = await backend.store.listCrons();
      expect(cron?.nextRunAt.getTime()).toBeGreaterThan(at(120_000).getTime());
    });

    it("serverlessTick reports wake-now while due timers are left over", async () => {
      const backend = await makeBackend();
      const nap = defineFlow<Record<string, never>, string>({
        name: "overdue-nap",
        version: 1,
        run: async (ctx) => {
          await ctx.sleep(1_000);
          return "woke";
        },
      });
      const flows = registry([nap]);
      for (let i = 0; i < 3; i++) await submit(backend, nap, {});
      await tickOnce(backend, flows, { batchMax: 10, leaseMs: 60_000, now: () => T0 });
      const swept = await serverlessTick(backend, flows, {
        batchMax: 1,
        leaseMs: 60_000,
        now: () => at(5_000),
      });
      expect(swept.nextWakeAt?.getTime()).toBe(at(5_000).getTime());
    });

    it("serverlessTick reports the next cron fire as its wake horizon", async () => {
      const backend = await makeBackend();
      const noop = defineFlow<Record<string, never>, number>({
        name: "cron-noop",
        version: 1,
        run: async () => 1,
      });
      const def = { name: "hourly", schedule: "0 * * * *", flow: noop, input: {} };
      await registerCron(backend, def, () => T0);
      const swept = await serverlessTick(backend, registry([noop]), {
        batchMax: 10,
        leaseMs: 60_000,
        now: () => T0,
      });
      expect(swept.nextWakeAt?.getTime()).toBe(at(3_600_000).getTime());
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
      let clock = T0;
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
