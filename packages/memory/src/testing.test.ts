import { defineFlow, signalType } from "@iterativeflow/core";
import { createTestHarness } from "@iterativeflow/core/testing";
import { describe, expect, it } from "vitest";
import { createMemoryBackend } from "#index";

const DAY = 24 * 60 * 60_000;

describe("test harness — virtual time over a real engine", () => {
  it("settles a flow that sleeps for three days, without waiting", async () => {
    const onboard = defineFlow({
      name: "onboard",
      version: 1,
      signals: { survey: signalType<{ score: number }>() },
      run: async (ctx, input: { userId: string }): Promise<{ score: number }> => {
        await ctx.step("create-account", () => input.userId);
        await ctx.sleep(3 * DAY);
        const survey = await ctx.signal("survey");
        return { score: survey.score };
      },
    });
    const t = createTestHarness(createMemoryBackend(), [onboard]);
    const startedAt = t.now().getTime();

    const handle = await t.engine.submit(onboard, { userId: "u_1" });
    expect(await t.advanceToNextWake()).toBe(true); // the sleep
    expect(t.now().getTime() - startedAt).toBe(3 * DAY);

    await t.engine.signal(handle, "survey", { score: 9 });
    expect(await t.settle(handle)).toMatchObject({ status: "done", output: { score: 9 } });
  });

  it("settle jumps every sleep and retry backoff on its own", async () => {
    let attempts = 0;
    const flow = defineFlow<Record<string, never>, string>({
      name: "flaky-then-slow",
      version: 1,
      run: async (ctx) => {
        await ctx.step("call-vendor", () => {
          attempts += 1;
          if (attempts < 3) throw new Error("vendor down");
          return "ok";
        });
        await ctx.sleep(7 * DAY);
        return "settled";
      },
    });
    const t = createTestHarness(createMemoryBackend(), [flow]);
    const handle = await t.engine.submit(flow, {});

    expect(await t.settle(handle)).toMatchObject({ status: "done", output: "settled" });
    expect(attempts).toBe(3); // the harness waited out both backoffs
    expect(t.now().getTime()).toBeGreaterThanOrEqual(
      new Date("2030-01-01T00:00:00Z").getTime() + 7 * DAY,
    );
  });

  it("settle names what a run is parked on instead of hanging", async () => {
    const flow = defineFlow<Record<string, never>, unknown>({
      name: "needs-approval",
      version: 1,
      run: (ctx) => ctx.signal("approve"),
    });
    const t = createTestHarness(createMemoryBackend(), [flow]);
    const handle = await t.engine.submit(flow, {});

    await expect(t.settle(handle)).rejects.toThrow(/awaiting a signal.*engine\.signal/s);
  });

  it("the clock only moves when asked, and never backwards", async () => {
    const flow = defineFlow<Record<string, never>, string>({
      name: "sleeper",
      version: 1,
      run: async (ctx) => {
        await ctx.sleep(DAY);
        return "up";
      },
    });
    const t = createTestHarness(createMemoryBackend(), [flow], {
      startAt: new Date("2031-06-01T00:00:00Z"),
    });
    const handle = await t.engine.submit(flow, {});

    await t.drain(); // executes up to the sleep; time must not move
    expect(t.now().toISOString()).toBe("2031-06-01T00:00:00.000Z");
    expect((await t.engine.status(handle))?.run.status).toBe("sleeping");

    await t.advance(2 * DAY); // overshooting the deadline still resumes it
    expect(await t.engine.result(handle)).toMatchObject({ status: "done", output: "up" });
    expect(t.now().toISOString()).toBe("2031-06-03T00:00:00.000Z");
  });

  it("advanceToNextWake reports when nothing is scheduled", async () => {
    const t = createTestHarness(createMemoryBackend(), []);
    expect(await t.advanceToNextWake()).toBe(false);
  });
});
