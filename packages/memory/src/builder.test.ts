import { type Backend, builder, registry, submit, tickOnce } from "@iterativeflow/core";
import { describe, expect, it } from "vitest";
import { createMemoryBackend } from "#index";

const TERMINAL = new Set(["done", "failed", "canceled"]);

const driveToSettle = async (
  backend: Backend,
  flows: ReturnType<typeof registry>,
  runId: string,
): Promise<{ status: string; output: unknown }> => {
  let clock = new Date("2030-01-01T00:00:00Z");
  const now = (): Date => clock;
  for (let i = 0; i < 100; i++) {
    await tickOnce(backend, flows, { batchMax: 16, leaseMs: 600_000, now });
    const run = (await backend.store.loadRun(runId))?.run;
    if (run && TERMINAL.has(run.status)) return { status: run.status, output: run.output };
    clock = new Date(clock.getTime() + 2_000);
  }
  throw new Error("run did not settle");
};

describe("builder — typed accumulator authoring", () => {
  it("threads each step's typed result into later steps and projects an output", async () => {
    const flow = builder<{ x: number }>("pipeline", 1)
      .step("doubled", (acc) => acc.input.x * 2)
      .step("plusTen", (acc) => acc.doubled + 10)
      .output((acc) => ({ x: acc.input.x, doubled: acc.doubled, plusTen: acc.plusTen }));

    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, { x: 3 });
    const settled = await driveToSettle(backend, flows, runId);
    expect(settled).toMatchObject({
      status: "done",
      output: { x: 3, doubled: 6, plusTen: 16 },
    });
  });

  it("build() returns the whole accumulator as output", async () => {
    const flow = builder<{ name: string }>("greet", 1)
      .step("hello", (acc) => `hello ${acc.input.name}`)
      .build();
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, { name: "ada" });
    const settled = await driveToSettle(backend, flows, runId);
    expect(settled).toMatchObject({
      status: "done",
      output: { input: { name: "ada" }, hello: "hello ada" },
    });
  });

  it("per-step retries recover a transient failure WITHOUT a full-flow replay", async () => {
    let calls = 0;
    const flow = builder<Record<string, never>>("flaky-step", 1)
      .step(
        "charge",
        () => {
          calls += 1;
          if (calls < 3) throw new Error("transient");
          return "charged";
        },
        { retries: 3 },
      )
      .build();
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, {});
    const settled = await driveToSettle(backend, flows, runId);
    expect(settled.status).toBe("done");
    expect(calls).toBe(3); // retried in place
    // run-level attempts stayed at 1 — the step absorbed the failures, no full replay.
    expect((await backend.store.loadRun(runId))?.run.attempts).toBe(1);
  });

  it("a step timeout counts against the run once its retry budget is spent", async () => {
    const flow = builder<Record<string, never>>("slow-step", 1)
      .step("slow", () => new Promise((resolve) => setTimeout(resolve, 100)), { timeoutMs: 10 })
      .build();
    const backend = createMemoryBackend();
    const flows = registry([flow]);
    const runId = await submit(backend, flow, {});
    let clock = new Date("2030-01-01T00:00:00Z");
    const now = (): Date => clock;
    for (let i = 0; i < 10; i++) {
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
    expect(run?.error?.message).toContain("timeout");
  });
});
