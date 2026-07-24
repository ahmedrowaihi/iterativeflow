import { defineFlow, registry, submit, tickOnce } from "@iterativeflow/core";
import { describe, expect, it } from "vitest";
import { createMemoryBackend } from "#index";

// Same name+version, different call shape at s0 — i.e. a flow body refactored without a version bump
// while a run was parked mid-flight. The resume tick replays against the old memo and detects drift.
const parked = defineFlow({
  name: "wf",
  version: 1,
  run: async (ctx): Promise<string> => {
    await ctx.step("a", () => 1);
    await ctx.sleep(1000);
    return "original";
  },
});
const refactored = defineFlow({
  name: "wf",
  version: 1,
  run: async (ctx): Promise<string> => {
    await ctx.step("RENAMED", () => 1); // s0 shape changed: step:a -> step:RENAMED
    await ctx.sleep(1000);
    return "refactored";
  },
});

const opts = { batchMax: 8, leaseMs: 60_000 };

/** Run `parked` to its sleep, then resume against `resume` a step later. Returns the resume tick. */
const driveToDrift = async (driftPolicy?: "park" | "fail", resume = refactored) => {
  const backend = createMemoryBackend();
  let clock = new Date("2030-01-01T00:00:00Z");
  const now = (): Date => clock;
  const runId = await submit(backend, parked, {});
  await tickOnce(backend, registry([parked]), { ...opts, now });
  clock = new Date(clock.getTime() + 2000); // past the sleep deadline
  const results = await tickOnce(backend, registry([resume]), { ...opts, now, driftPolicy });
  const run = (await backend.store.loadRun(runId))?.run;
  return { results, run };
};

describe("flow drift", () => {
  it("parks the run (recoverable) by default when the flow shape changed under it", async () => {
    const { results, run } = await driveToDrift();
    expect(results).toContain("flow_drift");
    expect(run?.status).toBe("retrying");
  });

  it("hard-fails the run with FLOW_DRIFT when driftPolicy is 'fail'", async () => {
    const { run } = await driveToDrift("fail");
    expect(run?.status).toBe("failed");
    expect(run?.error?.code).toBe("FLOW_DRIFT");
  });

  it("a flow's own driftPolicy overrides the engine default", async () => {
    // Engine default is park (no driftPolicy in opts); the resuming flow forces fail.
    const critical = defineFlow({
      name: "wf",
      version: 1,
      policy: { drift: "fail" },
      run: async (ctx): Promise<string> => {
        await ctx.step("RENAMED", () => 1);
        await ctx.sleep(1000);
        return "critical";
      },
    });
    const { run } = await driveToDrift(undefined, critical);
    expect(run?.status).toBe("failed");
    expect(run?.error?.code).toBe("FLOW_DRIFT");
  });

  it("does NOT flag drift when the flow body is unchanged", async () => {
    const { run } = await driveToDrift(undefined, parked); // resume against the same body
    expect(run).toMatchObject({ status: "done", output: "original" });
  });
});
