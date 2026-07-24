import {
  type Backend,
  type RetryPolicy,
  cancelRun,
  defineFlow,
  registry,
  signalRun,
  submit,
  tickOnce,
  type,
} from "@iterativeflow/core";
import { describe, expect, it } from "vitest";

const TERMINAL = new Set(["done", "failed", "canceled"]);

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
    ): Promise<{ status: string; output?: unknown; error?: { code?: string } }> => {
      let clock = new Date("2030-01-01T00:00:00Z");
      const now = (): Date => clock;
      for (let i = 0; i < 200; i++) {
        await tickOnce(backend, flows, { ...base, now, retry });
        const run = await backend.store.loadRunRow(runId);
        if (run && TERMINAL.has(run.status)) return run;
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
        signals: { go: type<{ ok: boolean }>() },
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
      expect(kids.every((k) => TERMINAL.has(k.status))).toBe(true);
      expect(kids.some((k) => k.status === "canceled")).toBe(true);
    });
  });
};
