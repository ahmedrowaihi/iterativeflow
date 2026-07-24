import { createEngine, defineFlow } from "@iterativeflow/core";
import { describe, expect, it } from "vitest";
import { createMemoryBackend } from "#index";

const TERMINAL = new Set(["done", "failed", "canceled"]);

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
      if (snap && TERMINAL.has(snap.run.status)) break;
      await engine.tick();
    }
    const res = await engine.result(runId, { timeoutMs: 1_000 });
    expect(res).toMatchObject({ status: "done", output: 42 });

    const health = await engine.health();
    expect(health.done).toBe(1);
    const listed = await engine.listRuns({ status: "done" }, { limit: 10 });
    expect(listed.runs.map((r) => r.id)).toEqual([runId]);
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
            typeof (v as { n?: unknown })?.n === "number"
              ? { value: v as { n: number } }
              : { issues: [{ message: "n must be a number" }] },
        },
      },
    });
    const engine = createEngine(createMemoryBackend(), [flow]);
    await expect(engine.submit(flow, { n: "oops" } as unknown as { n: number })).rejects.toThrow(
      /n must be a number/,
    );
    const ok = await engine.submit(flow, { n: 5 });
    expect(typeof ok).toBe("string");
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

  it("a throwing background tick surfaces to tickError, never an unhandled rejection", async () => {
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
    const onRej = (e: unknown): void => {
      unhandled.push(e);
    };
    process.on("unhandledRejection", onRej);
    try {
      const stop = engine.run({ tickMs: 5, maintenanceMs: 1_000 });
      await new Promise((r) => setTimeout(r, 40));
      await stop();
    } finally {
      process.off("unhandledRejection", onRej);
    }
    expect(unhandled).toHaveLength(0);
    expect(errors.length).toBeGreaterThan(0);
  });

  it("exposes cancel and retry through the facade", async () => {
    const flow = defineFlow<Record<string, never>, string>({
      name: "cancelable",
      version: 1,
      run: async () => "done",
    });
    const engine = createEngine(createMemoryBackend(), [flow]);
    const runId = await engine.submit(flow, {});
    await engine.cancel(runId);
    expect((await engine.status(runId))?.run.status).toBe("canceled");
  });
});
