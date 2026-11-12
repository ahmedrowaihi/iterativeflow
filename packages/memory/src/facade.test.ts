import { type Contract, createEngine, defineFlow } from "@iterativeflow/core";
import { isTerminal } from "@iterativeflow/core/backend";
import { describe, expect, it, vi } from "vitest";
import { createMemoryBackend } from "#index";

describe("createEngine — the cohesive facade", () => {
  it("drives submit → tick → status/result through one object", async () => {
    const flow = defineFlow<{ x: number }, number>({
      name: "double",
      version: 1,
      run: async (ctx, input) => ctx.step("d", () => input.x * 2),
    });
    const engine = createEngine(createMemoryBackend(), [flow], {
      now: () => new Date("2030-01-01T00:00:00Z"),
    });

    const runId = await engine.submit(flow, { x: 21 });
    for (let i = 0; i < 5; i++) {
      const snap = await engine.status(runId);
      if (snap && isTerminal(snap.run.status)) break;
      await engine.tick();
    }
    const res = await engine.result(runId, { timeoutMs: 1_000 });
    expect(res).toMatchObject({ status: "done", output: 42 });

    const health = await engine.health();
    expect(health.done).toBe(1);
    const listed = await engine.listRuns({ status: "done" }, { limit: 10 });
    expect(listed.runs.map((r) => r.id)).toEqual([runId]);

    const live = await engine.liveness();
    expect(live.runs.done).toBe(1);
    expect(live.queue.claimable).toBe(0); // nothing left to dispatch once the run is done
    expect(live.queue.leased).toBe(0);
    expect(live.queue.oldestClaimableAgeMs).toBeNull();
  });

  it("rejects a submit whose input fails the flow's Standard-Schema validator", async () => {
    // A minimal Standard-Schema validator: require { n: number }.
    const flow = defineFlow<{ n: number }, number>({
      name: "guarded",
      version: 1,
      run: async (_ctx, input) => input.n,
      input: {
        "~standard": {
          version: 1,
          vendor: "test",
          validate: (v) =>
            v instanceof Object && "n" in v && Number.isFinite(v.n)
              ? { value: { n: Number(v.n) } }
              : { issues: [{ message: "n must be a number" }] },
        },
      },
    });
    const engine = createEngine(createMemoryBackend(), [flow]);
    // A caller holding a looser contract than the worker's schema, as an untyped client would.
    const loose: Contract<{ n: number | string }, number> = {
      name: flow.name,
      version: flow.version,
      input: flow.input,
    };
    await expect(engine.submit(loose, { n: "oops" })).rejects.toThrow(/n must be a number/);
    const ok = await engine.submit(flow, { n: 5 });
    expect(ok).toEqual(expect.any(String));
  });

  it("rejects a submit whose payload exceeds maxPayloadBytes", async () => {
    const flow = defineFlow<{ blob: string }, number>({
      name: "big",
      version: 1,
      run: async () => 1,
    });
    const engine = createEngine(createMemoryBackend(), [flow], { maxPayloadBytes: 64 });
    await expect(engine.submit(flow, { blob: "x".repeat(500) })).rejects.toThrow(/maxPayloadBytes/);
    await expect(engine.submit(flow, { blob: "ok" })).resolves.toBeTruthy();
  });

  it("a throwing background tick surfaces to tickError, never an unhandled rejection (real event loop)", async () => {
    const flow = defineFlow<Record<string, never>, string>({
      name: "x",
      version: 1,
      run: async () => "ok",
    });
    const base = createMemoryBackend();
    const faulty = {
      ...base,
      queue: { ...base.queue, claim: () => Promise.reject(new Error("backend blip")) },
    };
    const errors: unknown[] = [];
    const engine = createEngine(faulty, [flow], {
      observe: { metrics: { tickError: (e) => errors.push(e) } },
    });

    const unhandled: unknown[] = [];
    const onRej: NodeJS.UnhandledRejectionListener = (reason) => {
      unhandled.push(reason);
    };
    process.on("unhandledRejection", onRej);
    try {
      const stop = engine.run({ tickMs: 5, maintenanceMs: 1_000 });
      await vi.waitFor(() => expect(errors.length).toBeGreaterThan(0), { timeout: 2_000 });
      await stop();
    } finally {
      process.off("unhandledRejection", onRej);
    }
    expect(unhandled).toHaveLength(0);
    expect(errors.length).toBeGreaterThan(0);
  });

  it("onDuplicate policy: 'reuse' returns the existing handle, 'error' throws", async () => {
    const flow = defineFlow<{ n: number }, number>({
      name: "idem",
      version: 1,
      run: async (_ctx, input) => input.n,
    });
    const engine = createEngine(createMemoryBackend(), [flow]);
    const h1 = await engine.submit(flow, { n: 1 }, { idempotencyKey: "k" });
    const h2 = await engine.submit(flow, { n: 1 }, { idempotencyKey: "k", onDuplicate: "reuse" });
    expect(h2).toBe(h1);
    await expect(
      engine.submit(flow, { n: 1 }, { idempotencyKey: "k", onDuplicate: "error" }),
    ).rejects.toThrow(/already exists/);
  });

  it("exposes cancel and retry through the facade", async () => {
    let fail = true;
    const flow = defineFlow<Record<string, never>, string>({
      name: "flaky",
      version: 1,
      run: async (ctx) =>
        ctx.step("s", () => {
          if (fail) throw new Error("boom");
          return "done";
        }),
    });
    const engine = createEngine(createMemoryBackend(), [flow], {
      retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1 },
    });
    const canceled = await engine.submit(flow, {});
    await engine.cancel(canceled);
    expect((await engine.status(canceled))?.run.status).toBe("canceled");
    expect(await engine.retry(canceled)).toBe(false);

    const failed = await engine.submit(flow, {});
    await engine.tick();
    expect((await engine.status(failed))?.run.status).toBe("failed");
    fail = false;
    expect(await engine.retry(failed)).toBe(true);
    await engine.tick();
    expect(await engine.result(failed, { timeoutMs: 1_000 })).toMatchObject({
      status: "done",
      output: "done",
    });
  });

  it("the resident loop drains a backlog larger than one batch before its first idle wait elapses", async () => {
    const flow = defineFlow<{ n: number }, number>({
      name: "sq",
      version: 1,
      run: async (ctx, input) => ctx.step("s", () => input.n * input.n),
    });
    vi.useFakeTimers({ toFake: ["setTimeout", "setInterval", "Date"] });
    try {
      const engine = createEngine(createMemoryBackend(), [flow], { batchMax: 3 });
      const handles = await Promise.all(
        Array.from({ length: 12 }, (_v, n) => engine.submit(flow, { n })),
      );
      const stop = engine.run({ tickMs: 50 });
      await vi.advanceTimersByTimeAsync(49);
      const runs = await Promise.all(handles.map(async (h) => (await engine.status(h))?.run));
      await stop();
      expect(runs.map((r) => r?.output)).toEqual(Array.from({ length: 12 }, (_v, n) => n * n));
    } finally {
      vi.useRealTimers();
    }
  });
});
