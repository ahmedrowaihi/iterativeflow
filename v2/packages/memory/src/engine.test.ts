import {
  type Backend,
  type FlowEvent,
  type Metrics,
  cancelRun,
  defineFlow,
  reconcile,
  registerCron,
  registry,
  result,
  retryRun,
  runDueCrons,
  serverlessTick,
  signalRun,
  submit,
  submitMany,
  tickOnce,
  signalType,
} from "@iterativeflow/core";
import { describe, expect, it } from "vitest";
import { createMemoryBackend } from "#index";

const TERMINAL = new Set(["done", "failed", "canceled"]);

const driveToSettle = async (
  backend: Backend,
  flows: ReturnType<typeof registry>,
  runId: string,
): Promise<{ status: string; output: unknown; error: unknown }> => {
  let clock = new Date("2030-01-01T00:00:00Z");
  const now = (): Date => clock;
  for (let i = 0; i < 100; i++) {
    // leaseMs is huge so a slow tick never races a re-claim in these single-worker tests.
    await tickOnce(backend, flows, { batchMax: 16, leaseMs: 600_000, now });
    const run = (await backend.store.loadRun(runId))?.run;
    if (run && TERMINAL.has(run.status))
      return { status: run.status, output: run.output, error: run.error };
    clock = new Date(clock.getTime() + 2_000); // fire sleep/retry timers
  }
  throw new Error("run did not settle");
};

describe("engine — end to end on the memory backend", () => {
  it("runs a multi-step flow to done, memoizing each step exactly once", async () => {
    let aCalls = 0;
    const flow = defineFlow<{ x: number }, number>({
      name: "greet",
      version: 1,
      run: async (ctx, input) => {
        const a = await ctx.step("a", () => {
          aCalls += 1;
          return input.x + 1;
        });
        const b = await ctx.step("b", () => a * 2);
        return b;
      },
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, { x: 1 });
    const settled = await driveToSettle(backend, flows, runId);
    expect(settled).toMatchObject({ status: "done", output: 4 });
    expect(aCalls).toBe(1);
  });

  it("memoizes a completed step across a crash-retry — the step fn does NOT re-run", async () => {
    let stepCalls = 0;
    let invocations = 0;
    const flow = defineFlow<Record<string, never>, number>({
      name: "flaky",
      version: 1,
      run: async (ctx) => {
        const v = await ctx.step("charge", () => {
          stepCalls += 1;
          return 42;
        });
        invocations += 1;
        if (invocations === 1) throw new Error("crash after the charge committed");
        return v;
      },
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, {});
    const settled = await driveToSettle(backend, flows, runId);
    expect(settled).toMatchObject({ status: "done", output: 42 });
    expect(stepCalls).toBe(1); // charged once despite the retry — exactly-once memo
    expect(invocations).toBe(2); // fn re-ran (at-least-once) but the step short-circuited
  });

  it("fails a run terminally once retries are exhausted", async () => {
    const flow = defineFlow<Record<string, never>, never>({
      name: "always-boom",
      version: 1,
      run: async () => {
        throw new Error("nope");
      },
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, {});
    let clock = new Date("2030-01-01T00:00:00Z");
    const now = (): Date => clock;
    for (let i = 0; i < 20; i++) {
      await tickOnce(backend, flows, {
        batchMax: 16,
        leaseMs: 600_000,
        now,
        retry: { maxAttempts: 3, baseDelayMs: 1_000, maxDelayMs: 1_000 },
      });
      const run = (await backend.store.loadRun(runId))?.run;
      if (run && TERMINAL.has(run.status)) break;
      clock = new Date(clock.getTime() + 2_000);
    }
    const run = (await backend.store.loadRun(runId))?.run;
    expect(run?.status).toBe("failed");
    expect(run?.attempts).toBe(3);
    expect(run?.error?.message).toContain("nope");
  });

  it("suspends on sleep and resumes after the deadline", async () => {
    const flow = defineFlow<Record<string, never>, string>({
      name: "napper",
      version: 1,
      run: async (ctx) => {
        await ctx.step("before", () => "ready");
        await ctx.sleep(5_000);
        return "awake";
      },
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, {});
    const settled = await driveToSettle(backend, flows, runId);
    expect(settled).toMatchObject({ status: "done", output: "awake" });
  });

  it("invokes a child flow and resumes with its output", async () => {
    const child = defineFlow<{ n: number }, number>({
      name: "double",
      version: 1,
      run: async (_ctx, input) => input.n * 2,
    });
    const parent = defineFlow<{ n: number }, number>({
      name: "parent",
      version: 1,
      run: async (ctx, input) => {
        const doubled = await ctx.invoke(child, { n: input.n });
        return doubled + 1;
      },
    });
    const backend = createMemoryBackend();
    const flows = registry([parent, child]);
    const runId = await submit(backend, parent, { n: 5 });
    const settled = await driveToSettle(backend, flows, runId);
    expect(settled).toMatchObject({ status: "done", output: 11 }); // (5*2)+1
  });

  it("submitMany dispatches a batch that all run to done", async () => {
    const flow = defineFlow<{ n: number }, number>({
      name: "square",
      version: 1,
      run: async (ctx, input) => ctx.step("sq", () => input.n * input.n),
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const ids = await submitMany(
      backend,
      [1, 2, 3, 4].map((n) => ({ flow, input: { n } })),
    );
    expect(ids).toHaveLength(4);
    for (const id of ids) await driveToSettle(backend, flows, id);
    const outputs = await Promise.all(
      ids.map(async (id) => (await backend.store.loadRun(id))?.run.output),
    );
    expect(outputs).toEqual([1, 4, 9, 16]);
  });

  it("waits for an external signal and resumes with its payload", async () => {
    const flow = defineFlow({
      name: "approval",
      version: 1,
      signals: { review: signalType<{ approved: boolean }>() },
      run: async (ctx): Promise<string> => {
        const decision = await ctx.signal("review");
        return decision.approved ? "shipped" : "rejected";
      },
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, {});

    let clock = new Date("2030-01-01T00:00:00Z");
    const now = (): Date => clock;
    await tickOnce(backend, flows, { batchMax: 16, leaseMs: 600_000, now });
    expect((await backend.store.loadRun(runId))?.run.status).toBe("awaiting_signal");

    await signalRun(backend, runId, "review", { approved: true });
    const settled = await driveToSettle(backend, flows, runId);
    expect(settled).toMatchObject({ status: "done", output: "shipped" });
  });

  it("cancelRun cancels the run and cascades to its children", async () => {
    const backend = createMemoryBackend();
    const parent = await backend.store.startRun({ name: "p", version: 1, input: {} });
    const child = await backend.store.startRun({
      name: "c",
      version: 1,
      input: {},
      parentRunId: parent.runId,
    });
    await cancelRun(backend, parent.runId);
    expect((await backend.store.loadRun(parent.runId))?.run.status).toBe("canceled");
    expect((await backend.store.loadRun(child.runId))?.run.status).toBe("canceled");
  });

  it("reconcile re-drives a run stranded off the queue (crash between start and enqueue)", async () => {
    const flow = defineFlow<Record<string, never>, string>({
      name: "stranded",
      version: 1,
      run: async () => "recovered",
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    // Simulate a crash after startRun but before enqueue: the run exists, no job.
    const { runId } = await backend.store.startRun({ name: "stranded", version: 1, input: {} });
    let settled = false;
    let clock = new Date("2030-01-01T00:00:00Z");
    const now = (): Date => clock;
    for (let i = 0; i < 5 && !settled; i++) {
      await reconcile(backend, { limit: 16 });
      await tickOnce(backend, flows, { batchMax: 16, leaseMs: 600_000, now });
      settled = (await backend.store.loadRun(runId))?.run.status === "done";
      clock = new Date(clock.getTime() + 2_000);
    }
    expect((await backend.store.loadRun(runId))?.run.output).toBe("recovered");
  });

  it("dead-letters a poison-pill run once attempts exceed the cap", async () => {
    const flow = defineFlow<Record<string, never>, number>({
      name: "poison",
      version: 1,
      run: async () => 1,
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, {});
    // Simulate prior dispatches that crashed the worker uncatchably (attempts bumped, no terminal).
    for (let i = 0; i < 5; i++) await backend.store.markRunning(runId);
    await tickOnce(backend, flows, {
      batchMax: 16,
      leaseMs: 600_000,
      now: () => new Date("2030-01-01T00:00:00Z"),
      retry: { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 1 },
    });
    const run = (await backend.store.loadRun(runId))?.run;
    expect(run?.status).toBe("failed");
    expect(run?.error?.code).toBe("RUN_ATTEMPTS_EXHAUSTED");
  });

  it("retryRun recovers a failed run, re-running only the work after the failure", async () => {
    let firstStep = 0;
    let attempts = 0;
    const flow = defineFlow<Record<string, never>, string>({
      name: "recoverable",
      version: 1,
      run: async (ctx) => {
        await ctx.step("charge", () => {
          firstStep += 1;
          return "charged";
        });
        attempts += 1;
        if (attempts === 1) throw new Error("downstream outage");
        return "ok";
      },
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, {});
    await tickOnce(backend, flows, {
      batchMax: 16,
      leaseMs: 600_000,
      now: () => new Date("2030-01-01T00:00:00Z"),
      retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1 }, // fail immediately, no auto-retry
    });
    expect((await backend.store.loadRun(runId))?.run.status).toBe("failed");

    await retryRun(backend, runId);
    await driveToSettle(backend, flows, runId);
    expect((await backend.store.loadRun(runId))?.run.output).toBe("ok");
    expect(firstStep).toBe(1); // the completed step did NOT re-run — memo preserved
  });

  it("result() returns a completed run's terminal outcome", async () => {
    const flow = defineFlow<Record<string, never>, number>({
      name: "quick",
      version: 1,
      run: async () => 99,
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, {});
    await driveToSettle(backend, flows, runId);
    const res = await result(backend, runId, { timeoutMs: 1_000 });
    expect(res).toMatchObject({ status: "done", output: 99 });
  });

  it("a step classified permanent fails fast — no in-invocation or run-level retry", async () => {
    let calls = 0;
    const flow = defineFlow<Record<string, never>, string>({
      name: "permfail",
      version: 1,
      run: async (ctx) => {
        await ctx.step(
          "validate",
          () => {
            calls += 1;
            throw new Error("bad request");
          },
          { retries: 5, classify: () => "permanent" },
        );
        return "ok";
      },
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, {});
    await tickOnce(backend, flows, {
      batchMax: 16,
      leaseMs: 600_000,
      now: () => new Date("2030-01-01T00:00:00Z"),
      retry: { maxAttempts: 10, baseDelayMs: 1, maxDelayMs: 1 },
    });
    expect((await backend.store.loadRun(runId))?.run.status).toBe("failed");
    expect(calls).toBe(1); // classify permanent short-circuited both retry tiers
  });

  it("a step timeout aborts the step's signal", async () => {
    let aborted = false;
    const flow = defineFlow<Record<string, never>, string>({
      name: "timesout",
      version: 1,
      run: async (ctx) => {
        await ctx.step(
          "slow",
          (arg) =>
            new Promise<void>((resolve) => {
              arg.signal.addEventListener("abort", () => {
                aborted = true;
              });
              setTimeout(resolve, 200);
            }),
          { timeoutMs: 10 },
        );
        return "done";
      },
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, {});
    await tickOnce(backend, flows, {
      batchMax: 16,
      leaseMs: 600_000,
      now: () => new Date("2030-01-01T00:00:00Z"),
      retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1 },
    });
    expect(aborted).toBe(true);
    expect((await backend.store.loadRun(runId))?.run.status).toBe("failed");
  });

  it("emits durable events and fires metrics hooks; level gates step events", async () => {
    const flow = defineFlow<Record<string, never>, string>({
      name: "observed",
      version: 1,
      run: async (ctx) => {
        await ctx.step("one", () => 1);
        await ctx.step("two", () => 2);
        return "ok";
      },
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);

    // level "all" — run + step events, and metrics hooks fire.
    const events: FlowEvent[] = [];
    const stepCalls: string[] = [];
    const metrics: Metrics = { stepFinished: (_r, k) => stepCalls.push(k) };
    const runId = await submit(backend, flow, {});
    await tickOnce(backend, flows, {
      batchMax: 16,
      leaseMs: 600_000,
      now: () => new Date("2030-01-01T00:00:00Z"),
      observe: { sink: { record: (e) => void events.push(e) }, level: "all", metrics },
    });
    const types = events.map((e) => e.type);
    expect(types).toContain("run.started");
    expect(types).toContain("step.finished");
    expect(types).toContain("run.completed");
    expect(stepCalls).toHaveLength(2);

    // level "lifecycle" — no step events.
    const lifecycle: FlowEvent[] = [];
    const runId2 = await submit(backend, flow, {});
    await tickOnce(backend, flows, {
      batchMax: 16,
      leaseMs: 600_000,
      now: () => new Date("2030-01-01T00:00:00Z"),
      observe: { sink: { record: (e) => void lifecycle.push(e) }, level: "lifecycle" },
    });
    expect(lifecycle.map((e) => e.type)).not.toContain("step.finished");
    expect(lifecycle.map((e) => e.type)).toContain("run.completed");
    expect(runId2).not.toBe(runId);
  });

  it("ctx.log emits a durable line once — suppressed while replaying the durable prefix", async () => {
    const flow = defineFlow<Record<string, never>, string>({
      name: "logger",
      version: 1,
      run: async (ctx) => {
        ctx.log("before sleep", { n: 1 });
        await ctx.sleep(1000);
        ctx.log("after sleep");
        return "ok";
      },
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const events: FlowEvent[] = [];
    const opts = {
      batchMax: 16,
      leaseMs: 600_000,
      observe: { sink: { record: (e: FlowEvent) => void events.push(e) }, level: "all" as const },
    };
    const runId = await submit(backend, flow, {});
    await tickOnce(backend, flows, { ...opts, now: () => new Date("2030-01-01T00:00:00Z") }); // logs "before", parks
    await tickOnce(backend, flows, { ...opts, now: () => new Date("2030-01-01T00:01:00Z") }); // drains timer, replays (no re-log), logs "after"

    expect((await backend.store.loadRunRow(runId))?.status).toBe("done");
    const logs = events.filter((e) => e.type === "run.log");
    // "before sleep" must appear exactly once despite the body re-running on the wake replay.
    expect(logs.map((l) => (l.data as { message: string }).message)).toEqual([
      "before sleep",
      "after sleep",
    ]);
    expect((logs[0].data as { data: unknown }).data).toEqual({ n: 1 });
  });

  it("a registered cron fires a run when due, exactly once per occurrence", async () => {
    const flow = defineFlow<{ kind: string }, string>({
      name: "report",
      version: 1,
      run: async (_ctx, input) => `ran ${input.kind}`,
    });
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    // Register at 08:30; next daily-midnight fire is the following 00:00.
    let clock = new Date("2030-03-15T08:30:00Z");
    const now = (): Date => clock;
    await registerCron(
      backend,
      { name: "nightly", schedule: "0 0 * * *", flow, input: { kind: "daily" } },
      now,
    );
    expect(await runDueCrons(backend, now)).toBe(0); // not due yet

    clock = new Date("2030-03-16T00:00:00Z"); // reach the fire time
    expect(await runDueCrons(backend, now)).toBe(1);
    expect(await runDueCrons(backend, now)).toBe(0); // same occurrence doesn't double-fire

    const runs = await backend.store.listRuns({ tag: "cron:nightly" }, { limit: 10 });
    expect(runs.runs).toHaveLength(1);
    await driveToSettle(backend, flows, runs.runs[0].id);
    expect((await backend.store.loadRun(runs.runs[0].id))?.run.output).toBe("ran daily");
  });

  it("cron overlap:skip skips while a prior run waits on a child (regression: awaiting_child in ACTIVE)", async () => {
    const flow = defineFlow<Record<string, never>, number>({
      name: "nightly-job",
      version: 1,
      run: async () => 1,
    });
    const backend = createMemoryBackend();
    let clock = new Date("2030-03-15T00:00:00Z");
    const now = (): Date => clock;
    await registerCron(
      backend,
      { name: "c", schedule: "0 0 * * *", flow, input: {}, overlap: "skip" },
      now,
    );
    clock = new Date("2030-03-16T00:00:00Z");
    expect(await runDueCrons(backend, now)).toBe(1); // first occurrence fires

    // That run parks waiting on a sub-workflow (awaiting_child).
    const [run] = (await backend.store.listRuns({ tag: "cron:c" }, { limit: 10 })).runs;
    await backend.store.markRunning(run.id);
    await backend.store.suspendRun(run.id, "awaiting_child");

    clock = new Date("2030-03-17T00:00:00Z");
    expect(await runDueCrons(backend, now)).toBe(0); // overlap:skip — must NOT start a second
    expect((await backend.store.listRuns({ tag: "cron:c" }, { limit: 10 })).runs).toHaveLength(1);
  });

  it("serverlessTick advances a sleep and fires a cron with no resident loop (cron-Lambda model)", async () => {
    const napper = defineFlow<Record<string, never>, string>({
      name: "napper2",
      version: 1,
      run: async (ctx) => {
        await ctx.sleep(60_000);
        return "woke";
      },
    });
    const nightly = defineFlow<Record<string, never>, number>({
      name: "nightly2",
      version: 1,
      run: async () => 1,
    });
    const backend = createMemoryBackend();
    const flows = registry([napper, nightly]);
    let clock = new Date("2030-03-15T00:00:00Z");
    const now = (): Date => clock;
    const tickOpts = { batchMax: 16, leaseMs: 600_000, now };

    await registerCron(
      backend,
      { name: "c2", schedule: "0 0 * * *", flow: nightly, input: {} },
      now,
    );
    const runId = await submit(backend, napper, {});

    const s1 = await serverlessTick(backend, flows, tickOpts);
    expect(s1.results).toContain("sleeping");
    expect((await backend.store.loadRun(runId))?.run.status).toBe("sleeping");

    // One invocation a day later fires the cron and resumes the sleep — no loop between them.
    clock = new Date("2030-03-16T00:00:00Z");
    const s2 = await serverlessTick(backend, flows, tickOpts);
    expect(s2.fired).toBe(1);
    expect((await backend.store.loadRun(runId))?.run.output).toBe("woke");

    const cronRuns = await backend.store.listRuns({ tag: "cron:c2" }, { limit: 10 });
    expect(cronRuns.runs).toHaveLength(1);
  });

  it("propagates a child failure into the parent", async () => {
    const child = defineFlow<Record<string, never>, never>({
      name: "bad-child",
      version: 1,
      run: async () => {
        throw new Error("child exploded");
      },
    });
    const parent = defineFlow<Record<string, never>, string>({
      name: "parent-of-bad",
      version: 1,
      run: async (ctx) => {
        await ctx.invoke(child, {});
        return "unreachable";
      },
    });
    const backend = createMemoryBackend();
    const flows = registry([parent, child]);
    const runId = await submit(backend, parent, {});
    let clock = new Date("2030-01-01T00:00:00Z");
    const now = (): Date => clock;
    for (let i = 0; i < 60; i++) {
      await tickOnce(backend, flows, {
        batchMax: 16,
        leaseMs: 600_000,
        now,
        retry: { maxAttempts: 2, baseDelayMs: 1_000, maxDelayMs: 1_000 },
      });
      const run = (await backend.store.loadRun(runId))?.run;
      if (run && TERMINAL.has(run.status)) break;
      clock = new Date(clock.getTime() + 2_000);
    }
    const run = (await backend.store.loadRun(runId))?.run;
    expect(run?.status).toBe("failed");
  });

  it("a flow's policy.maxFanOut overrides the default fan-out cap", async () => {
    const child = defineFlow({ name: "c", version: 1, run: async () => 1 });
    const parent = defineFlow({
      name: "capped",
      version: 1,
      policy: { maxFanOut: 2 },
      run: async (ctx): Promise<readonly number[]> =>
        ctx.invoke([
          { flow: child, input: {} },
          { flow: child, input: {} },
          { flow: child, input: {} },
        ]),
    });
    const backend = createMemoryBackend();
    const runId = await submit(backend, parent, {});
    await tickOnce(backend, registry([parent, child]), {
      batchMax: 16,
      leaseMs: 600_000,
      now: () => new Date("2030-01-01T00:00:00Z"),
      retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1 },
    });
    const run = await backend.store.loadRunRow(runId);
    expect(run?.status).toBe("failed");
    expect(run?.error?.message).toMatch(/exceeds the 2 cap/);
  });

  it("arriveAtJoin decrements the armed countdown to zero on the last arrival", async () => {
    const b = createMemoryBackend();
    const { runId: parent } = await b.store.startRun({ name: "p", version: 1, input: {} });
    await b.store.checkpointStep(
      { runId: parent, cursorKey: "s0", status: "ok", result: [], attempts: 1 },
      { joinTarget: { runId: parent, count: 3 } },
    );
    expect(await b.store.arriveAtJoin(parent)).toBe(2);
    expect(await b.store.arriveAtJoin(parent)).toBe(1);
    expect(await b.store.arriveAtJoin(parent)).toBe(0); // last arrival — executor wakes the parent
  });

  it("arriveAtJoin returns undefined for a gone parent (nothing to wake)", async () => {
    const b = createMemoryBackend();
    expect(await b.store.arriveAtJoin("00000000-0000-0000-0000-000000000000")).toBeUndefined();
  });
});
