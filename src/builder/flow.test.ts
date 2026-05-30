import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { beforeEach, afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { RuntimeFlowContext } from "../engine/context";
import { FlowRegistry } from "../engine/registry";
import { playRunAttempt } from "../engine/run-lifecycle";
import { baseContextDeps, baseRunnerDeps, silentLogger } from "../engine/test-helpers";
import type { Storage } from "../engine/types";
import { createDrizzleStorage, noopEnqueue } from "../storage/drizzle";
import type { WorkflowDb } from "../storage/db";
import { applyFlowSchema } from "../storage/setup";
import type { FlowDefinition } from "./types";
import { flow } from "./flow";

const silent = silentLogger;

interface Harness {
  storage: Storage;
  registry: FlowRegistry;
  ctxFor: (runId: string) => Promise<RuntimeFlowContext>;
  register: (def: FlowDefinition<unknown, unknown>) => void;
  run: (runId: string) => Promise<{ status: string }>;
  close: () => Promise<void>;
}

const setup = async (): Promise<Harness> => {
  const client = new PGlite();
  await client.waitReady;
  const db = drizzle({ client }) as unknown as WorkflowDb;
  await applyFlowSchema(db);
  const storage = createDrizzleStorage({
    db,
    logger: silent,
    enqueue: noopEnqueue,
  });
  const registry = new FlowRegistry();
  return {
    storage,
    registry,
    ctxFor: async (runId) =>
      new RuntimeFlowContext({
        ...baseContextDeps(),
        runId,
        attempt: 1,
        storage,
        snapshot: await storage.loadSnapshot(runId),
      }),
    register: (def) =>
      registry.register({
        name: def.name,
        version: def.version,
        run: def.body,
        inputSchema: def.input,
        nodes: def.nodes,
      }),
    run: async (runId) => {
      const r = await playRunAttempt({ ...baseRunnerDeps(), registry, storage }, runId);
      return { status: r.status };
    },
    close: () => client.close(),
  };
};

describe("flow builder", () => {
  let h: Harness;
  beforeEach(async () => {
    h = await setup();
  });
  afterEach(async () => {
    await h.close();
  });

  it("each chaining call returns a NEW builder — branches don't share state", () => {
    const base = flow("branchy").step("seed", () => 1);
    const branchA = base.step("a", () => "a").build();
    const branchB = base
      .step("b", () => "b")
      .step("b2", () => "b2")
      .build();

    expect(branchA.nodes.map((n) => (n.kind === "step" ? n.name : n.kind))).toEqual(["seed", "a"]);
    expect(branchB.nodes.map((n) => (n.kind === "step" ? n.name : n.kind))).toEqual([
      "seed",
      "b",
      "b2",
    ]);
  });

  it(".version rejects non-positive-integer and regression", () => {
    expect(() => flow("v").version(0)).toThrow(/positive integer/);
    expect(() => flow("v").version(-2)).toThrow(/positive integer/);
    expect(() => flow("v").version(1.5)).toThrow(/positive integer/);
    expect(() => flow("v").version(2).version(1)).toThrow(/regress/);
  });

  it("build() captures the node graph", () => {
    const def = flow("g")
      .step("a", () => 1)
      .sleep("1h")
      .signal("k")
      .output(() => "done")
      .build();
    expect(def.name).toBe("g");
    expect(def.version).toBe(1);
    expect(def.nodes.map((n) => n.kind)).toEqual(["step", "sleep", "signal"]);
  });

  it("threads the value channel through steps and runs each step once", async () => {
    const calls: string[] = [];
    const def = flow("thread")
      .step("a", () => {
        calls.push("a");
        return 1;
      })
      .step("b", ({ input }) => {
        calls.push("b");
        return input + 1;
      })
      .output(({ input }) => input)
      .build();
    h.register(def as FlowDefinition<unknown, unknown>);

    const { runId } = await h.storage.createRun({
      name: "thread",
      version: 1,
      input: {},
    });
    const r = await h.run(runId);

    expect(r.status).toBe("completed");
    expect((await h.storage.loadRun(runId))?.output).toBe(2);
    expect(calls).toEqual(["a", "b"]);
  });

  it("sleep is transparent — the channel survives across it", async () => {
    const def = flow("sleepy")
      .step("a", () => "carried")
      .sleep(new Date(0))
      .output(({ input }) => input)
      .build();
    h.register(def as FlowDefinition<unknown, unknown>);

    const { runId } = await h.storage.createRun({
      name: "sleepy",
      version: 1,
      input: {},
    });
    await h.run(runId);
    expect((await h.storage.loadRun(runId))?.output).toBe("carried");
  });

  it("hook merge folds an earlier value past the hook", async () => {
    const def = flow("merge")
      .step("acct", () => ({ id: "x" }))
      .signal(
        "survey",
        { schema: z.object({ score: z.number() }) },
        (input: { id: string }, payload: { score: number }) => ({
          ...input,
          score: payload.score,
        }),
      )
      .output(({ input }) => input)
      .build();
    h.register(def as FlowDefinition<unknown, unknown>);

    const { runId } = await h.storage.createRun({
      name: "merge",
      version: 1,
      input: {},
    });
    await h.storage.preDeliverSignal(runId, "signal:survey", { score: 9 });
    await h.run(runId);

    expect((await h.storage.loadRun(runId))?.output).toEqual({
      id: "x",
      score: 9,
    });
  });

  describe("version routing", () => {
    it("routes a run to the exact version it started on", async () => {
      h.register(
        flow("v")
          .version(1)
          .output(() => "v1")
          .build() as FlowDefinition<unknown, unknown>,
      );
      h.register(
        flow("v")
          .version(2)
          .output(() => "v2")
          .build() as FlowDefinition<unknown, unknown>,
      );

      const r1 = await h.storage.createRun({ name: "v", version: 1, input: {} });
      const r2 = await h.storage.createRun({ name: "v", version: 2, input: {} });
      await h.run(r1.runId);
      await h.run(r2.runId);

      expect((await h.storage.loadRun(r1.runId))?.output).toBe("v1");
      expect((await h.storage.loadRun(r2.runId))?.output).toBe("v2");
    });

    it("fails a run whose version is not registered", async () => {
      h.register(
        flow("solo")
          .version(1)
          .output(() => "ok")
          .build() as FlowDefinition<unknown, unknown>,
      );
      const { runId } = await h.storage.createRun({
        name: "solo",
        version: 7,
        input: {},
      });
      const r = await h.run(runId);
      expect(r.status).toBe("failed");
      expect((await h.storage.loadRun(runId))?.error?.code).toBe("FLOW_UNKNOWN");
    });
  });

  describe("loop", () => {
    it("iterates until() returns true; each iteration's step is memoized under a unique key", async () => {
      const calls: number[] = [];
      const def = flow("loopy")
        .input(z.object({ stop: z.number() }))
        .step("seed", ({ input }) => ({ stop: input.stop, count: 0 }))
        .loop({ until: (s: { count: number; stop: number }) => s.count >= s.stop }, (sub) =>
          sub.step("tick", ({ input }) => {
            calls.push(input.count);
            return { ...input, count: input.count + 1 };
          }),
        )
        .output(({ input }) => input)
        .build();
      h.register(def as FlowDefinition<unknown, unknown>);

      const { runId } = await h.storage.createRun({
        name: "loopy",
        version: 1,
        input: { stop: 3 },
      });
      const r = await h.run(runId);
      expect(r.status).toBe("completed");
      expect(calls).toEqual([0, 1, 2]);
      expect((await h.storage.loadRun(runId))?.output).toEqual({
        stop: 3,
        count: 3,
      });
    });

    it("compat guard still flags renames inside loop bodies", async () => {
      // record a step that wouldn't match the graph — even with loops in the
      // graph, rename/kind drift is still detectable from the bases collected
      // out of the loop body.
      const { runId } = await h.storage.createRun({
        name: "loop-rename",
        version: 1,
        input: {},
      });
      const ctx = await h.ctxFor(runId);
      await ctx.step("ghost", () => "x");

      h.register(
        flow("loop-rename")
          .version(1)
          .step("init", () => ({ count: 0 }))
          .loop({ until: (s: { count: number }) => s.count >= 1 }, (sub) =>
            sub.step("tick", ({ input }) => ({ count: input.count + 1 })),
          )
          .build() as FlowDefinition<unknown, unknown>,
      );

      const r = await h.run(runId);
      expect(r.status).toBe("failed");
      expect((await h.storage.loadRun(runId))?.error?.code).toBe("REPLAY_INCOMPATIBLE_VERSION");
    });

    it("compat guard accepts dynamic loop-body keys when the base still matches", async () => {
      const { runId } = await h.storage.createRun({
        name: "loop-dynamic",
        version: 1,
        input: {},
      });
      const ctx = await h.ctxFor(runId);
      await ctx.step("tick", () => 1);
      await ctx.step("tick", () => 2); // recorded as "tick:1"

      h.register(
        flow("loop-dynamic")
          .version(1)
          .step("seed", () => 0 as number)
          .loop({ until: () => true }, (sub) => sub.step("tick", () => 0 as number))
          .build() as FlowDefinition<unknown, unknown>,
      );

      const r = await h.run(runId);
      // Loop "tick" base is in producible — recorded "tick" + "tick:1" both
      // match. compat returns null; run can proceed (and will exit the loop
      // immediately since until() is true).
      expect((await h.storage.loadRun(runId))?.error?.code).not.toBe("REPLAY_INCOMPATIBLE_VERSION");
      expect(r.status).not.toBe("failed");
    });
  });

  describe("compat guard", () => {
    it("REPLAY_INCOMPATIBLE_VERSION when a recorded step is gone from the graph", async () => {
      const { runId } = await h.storage.createRun({
        name: "drift",
        version: 1,
        input: {},
      });
      const ctx = await h.ctxFor(runId);
      await ctx.step("a", () => "done");

      h.register(
        flow("drift")
          .version(1)
          .step("b", () => "only-b")
          .build() as FlowDefinition<unknown, unknown>,
      );

      const r = await h.run(runId);
      expect(r.status).toBe("failed");
      expect((await h.storage.loadRun(runId))?.error?.code).toBe("REPLAY_INCOMPATIBLE_VERSION");
    });

    it("REPLAY_NON_DETERMINISTIC when the occurrence count of a name shrinks", async () => {
      const { runId } = await h.storage.createRun({
        name: "loopy",
        version: 1,
        input: {},
      });
      const ctx = await h.ctxFor(runId);
      await ctx.step("foo", () => "a");
      await ctx.step("foo", () => "b");
      await ctx.step("foo", () => "c");

      h.register(
        flow("loopy")
          .version(1)
          .step("foo", () => "a")
          .step("foo", () => "b")
          .build() as FlowDefinition<unknown, unknown>,
      );

      const r = await h.run(runId);
      expect(r.status).toBe("failed");
      expect((await h.storage.loadRun(runId))?.error?.code).toBe("REPLAY_NON_DETERMINISTIC");
    });
  });
});
