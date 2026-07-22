import { PGlite } from "@electric-sql/pglite";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { drizzle } from "drizzle-orm/pglite";
import { eq } from "drizzle-orm";
import type { WorkflowDb } from "../storage/db";
import { createDrizzleStorage, type TxEnqueue } from "../storage/drizzle";
import { events, RETRY_TIMER_CURSOR, runs } from "../storage/schema";
import { applyFlowSchema } from "../storage/setup";
import { RuntimeFlowContext } from "./context";
import { FlowRegistry } from "./registry";
import { playRunAttempt } from "./run-lifecycle";
import { isSuspend, FlowSuspend } from "./suspend";
import { baseContextDeps, baseRunnerDeps, silentLogger } from "./test-helpers";
import { FlowRuntimeError } from "../util/errors";
import type { Storage, FlowContext } from "./types";

interface RecordedEnqueue {
  runId: string;
  runAt?: Date;
}

const createRecorder = (): {
  enqueue: TxEnqueue;
  enqueues: RecordedEnqueue[];
} => {
  const enqueues: RecordedEnqueue[] = [];
  return {
    enqueues,
    enqueue: async (_tx, job, opts) => {
      enqueues.push({ runId: job.runId, runAt: opts?.runAt });
    },
  };
};

interface TestHarness {
  db: WorkflowDb;
  storage: Storage;
  enqueues: RecordedEnqueue[];
  registry: FlowRegistry;
  runOnce: (runId: string) => Promise<{ status: string }>;
  close: () => Promise<void>;
}

const setup = async (): Promise<TestHarness> => {
  const client = new PGlite();
  await client.waitReady;
  const db = drizzle({ client }) as unknown as WorkflowDb;
  await applyFlowSchema(db);
  const { enqueue, enqueues } = createRecorder();
  const storage = createDrizzleStorage({ db, logger: silentLogger, enqueue });
  const registry = new FlowRegistry();
  return {
    db,
    storage,
    enqueues,
    registry,
    runOnce: async (runId) => {
      const r = await playRunAttempt({ ...baseRunnerDeps(), registry, storage }, runId);
      return { status: r.status };
    },
    close: async () => {
      await client.close();
    },
  };
};

const register = (
  registry: FlowRegistry,
  name: string,
  run: (ctx: FlowContext, input: unknown) => unknown,
  opts?: { inputSchema?: z.ZodType<unknown> },
) => {
  registry.register({
    name,
    version: 1,
    inputSchema: opts?.inputSchema,
    run: async (ctx, input) => run(ctx, input),
  });
};

const createRun = async (
  storage: Storage,
  name: string,
  input: unknown,
  idempotencyKey?: string,
): Promise<string> => {
  const { runId } = await storage.createRun({
    name,
    version: 1,
    input,
    idempotencyKey,
  });
  return runId;
};

describe("workflow engine (pglite)", () => {
  let h: TestHarness;

  beforeEach(async () => {
    h = await setup();
  });
  afterEach(async () => {
    await h.close();
  });

  describe("step memoization", () => {
    it("runs step once, memoizes result across replay", async () => {
      const calls = { compute: 0 };
      register(h.registry, "memo", async (ctx) => {
        return ctx.step("compute", () => {
          calls.compute += 1;
          return 42;
        });
      });

      const runId = await createRun(h.storage, "memo", {});
      const first = await h.runOnce(runId);
      expect(first.status).toBe("completed");
      expect(calls.compute).toBe(1);

      const row = await h.storage.loadRun(runId);
      expect(row?.status).toBe("done");
      expect(row?.output).toBe(42);
    });

    it("positional cursor numbers repeated step names: foo, foo:1, foo:2", async () => {
      const runId = await createRun(h.storage, "x", {});
      const snapshot = await h.storage.loadSnapshot(runId);
      const ctx = new RuntimeFlowContext({
        ...baseContextDeps(),
        runId,
        attempt: 1,
        storage: h.storage,
        snapshot,
      });
      await ctx.step("foo", () => "a");
      await ctx.step("foo", () => "b");
      await ctx.step("foo", () => "c");

      const stored = [
        await h.storage.loadStep(runId, "foo"),
        await h.storage.loadStep(runId, "foo:1"),
        await h.storage.loadStep(runId, "foo:2"),
      ];
      expect(stored.every((s) => s?.status === "ok")).toBe(true);
      expect(stored.map((s) => s?.result)).toEqual(["a", "b", "c"]);
    });
  });

  describe("invoke caps", () => {
    it("throws INVOKE_DEPTH_EXCEEDED when ctx.invoke would exceed maxInvokeDepth", async () => {
      const childHandle = {
        name: "child",
        version: 1,
        start: async () => ({ runId: "x", status: "pending" as const }),
        startMany: async () => [],
        output: async () => undefined,
        result: async () => {
          throw new Error("not invoked in test");
        },
        wait: async () => {
          throw new Error("not invoked in test");
        },
      };
      const runId = await createRun(h.storage, "deep", {});
      // Synthesize a 5-deep parent chain ending at runId.
      let parent = runId;
      for (let i = 0; i < 4; i++) {
        const { runId: child } = await h.storage.createRun({
          name: `c${i}`,
          version: 1,
          input: {},
          parentRunId: parent,
          parentCursorKey: `invoke:c${i}@1`,
        });
        parent = child;
      }
      const ctx = new RuntimeFlowContext({
        ...baseContextDeps(),
        runId: parent,
        attempt: 1,
        storage: h.storage,
        snapshot: await h.storage.loadSnapshot(parent),
        maxInvokeDepth: 5,
        startChild: async () => "unreachable",
      });
      await expect(ctx.invoke(childHandle, {})).rejects.toMatchObject({
        code: "INVOKE_DEPTH_EXCEEDED",
        nonRetryable: true,
      });
    });

    it("throws INVOKE_FANOUT_EXCEEDED when a run has already spawned maxChildrenPerRun children", async () => {
      const childHandle = {
        name: "child",
        version: 1,
        start: async () => ({ runId: "x", status: "pending" as const }),
        startMany: async () => [],
        output: async () => undefined,
        result: async () => {
          throw new Error("not invoked in test");
        },
        wait: async () => {
          throw new Error("not invoked in test");
        },
      };
      const runId = await createRun(h.storage, "fanout", {});
      for (let i = 0; i < 3; i++) {
        await h.storage.createRun({
          name: `c${i}`,
          version: 1,
          input: {},
          parentRunId: runId,
          parentCursorKey: `invoke:c${i}@1`,
        });
      }
      const ctx = new RuntimeFlowContext({
        ...baseContextDeps(),
        runId,
        attempt: 1,
        storage: h.storage,
        snapshot: await h.storage.loadSnapshot(runId),
        maxChildrenPerRun: 3,
        startChild: async () => "unreachable",
      });
      await expect(ctx.invoke(childHandle, {})).rejects.toMatchObject({
        code: "INVOKE_FANOUT_EXCEEDED",
        nonRetryable: true,
      });
    });
  });

  describe("sleep suspension", () => {
    it("suspends when fireAt is future, enqueues for resume", async () => {
      register(h.registry, "future-sleep", async (ctx) => {
        await ctx.sleep("1h");
        return "after";
      });

      const runId = await createRun(h.storage, "future-sleep", {});
      const r = await h.runOnce(runId);
      expect(r.status).toBe("suspended");
      expect(h.enqueues.length).toBe(1);
      expect(h.enqueues[0]?.runAt).toBeInstanceOf(Date);
      expect((await h.storage.loadRun(runId))?.status).toBe("sleeping");
    });

    it("fires immediately and continues when fireAt is in the past", async () => {
      register(h.registry, "past-sleep", async (ctx) => {
        await ctx.sleep(new Date(0));
        return "done";
      });

      const runId = await createRun(h.storage, "past-sleep", {});
      const r = await h.runOnce(runId);
      expect(r.status).toBe("completed");
      expect((await h.storage.loadRun(runId))?.output).toBe("done");
    });

    it("resumes after sleep timer is recorded as fired", async () => {
      register(h.registry, "two-phase", async (ctx) => {
        await ctx.sleep("1h");
        return ctx.step("after", () => "phase2");
      });

      const runId = await createRun(h.storage, "two-phase", {});
      let r = await h.runOnce(runId);
      expect(r.status).toBe("suspended");

      await h.storage.fireTimer(runId, "sleep");

      r = await h.runOnce(runId);
      expect(r.status).toBe("completed");
      expect((await h.storage.loadRun(runId))?.output).toBe("phase2");
    });
  });

  describe("step retries", () => {
    it("retries on transient error then succeeds", async () => {
      let attempts = 0;
      register(h.registry, "flaky", async (ctx) => {
        return ctx.step(
          "may-fail",
          () => {
            attempts += 1;
            if (attempts < 3) throw new Error("nope");
            return "ok";
          },
          { retries: 5, baseBackoffMs: 1, capBackoffMs: 5 },
        );
      });

      const runId = await createRun(h.storage, "flaky", {});
      expect((await h.runOnce(runId)).status).toBe("suspended");
      expect((await h.runOnce(runId)).status).toBe("suspended");
      expect((await h.runOnce(runId)).status).toBe("completed");
      expect(attempts).toBe(3);
      expect((await h.storage.loadRun(runId))?.output).toBe("ok");
    });

    it("marks step terminal after retries are exhausted", async () => {
      register(h.registry, "doomed", async (ctx) => {
        return ctx.step(
          "broken",
          () => {
            throw new Error("always broken");
          },
          { retries: 1, baseBackoffMs: 1 },
        );
      });

      const runId = await createRun(h.storage, "doomed", {});
      await h.runOnce(runId);
      await h.runOnce(runId);
      const run = await h.storage.loadRun(runId);
      expect(run?.status).toBe("failed");
      expect(run?.error?.message).toContain("always broken");
    });

    it("classify=permanent skips retries", async () => {
      register(h.registry, "perm-fail", async (ctx) => {
        return ctx.step(
          "bad-input",
          () => {
            throw new Error("BAD_INPUT");
          },
          { retries: 5, classify: () => "permanent" as const },
        );
      });

      const runId = await createRun(h.storage, "perm-fail", {});
      const r = await h.runOnce(runId);
      expect(r.status).toBe("failed");
    });

    it("timeoutMs converts a hung step into a retryable error", async () => {
      let attempt = 0;
      register(h.registry, "hung", async (ctx) => {
        return ctx.step(
          "slow",
          () => {
            attempt += 1;
            if (attempt === 1) {
              return new Promise<string>(() => {});
            }
            return "ok";
          },
          { timeoutMs: 50, retries: 1, baseBackoffMs: 0 },
        );
      });

      const runId = await createRun(h.storage, "hung", {});
      const first = await h.runOnce(runId);
      expect(first.status).toBe("suspended");
      const second = await h.runOnce(runId);
      expect(second.status).toBe("completed");
      expect((await h.storage.loadRun(runId))?.output).toBe("ok");
      expect(attempt).toBe(2);
    });
  });

  describe("retry", () => {
    it("replays a failed run: ok steps memoized, failed step re-executed", async () => {
      let goodCalls = 0;
      let badAttempts = 0;
      register(h.registry, "two-step", async (ctx) => {
        const a = await ctx.step("good", () => {
          goodCalls += 1;
          return "A";
        });
        const b = await ctx.step(
          "bad",
          () => {
            badAttempts += 1;
            if (badAttempts <= 2) throw new Error("flaky");
            return "B";
          },
          { retries: 1, baseBackoffMs: 1 },
        );
        return `${a}-${b}`;
      });

      const runId = await createRun(h.storage, "two-step", {});
      await h.runOnce(runId);
      await h.runOnce(runId);
      expect((await h.storage.loadRun(runId))?.status).toBe("failed");
      expect(goodCalls).toBe(1);
      expect(badAttempts).toBe(2);

      const result = await h.storage.retryRun(runId);
      expect(result).toEqual({ kind: "queued" });
      const after = await h.storage.loadRun(runId);
      expect(after?.status).toBe("pending");
      expect(after?.attempts).toBe(0);
      expect(after?.error).toBeNull();

      await h.runOnce(runId);
      const done = await h.storage.loadRun(runId);
      expect(done?.status).toBe("done");
      expect(done?.output).toBe("A-B");
      expect(goodCalls).toBe(1);
      expect(badAttempts).toBe(3);
    });

    it("returns not_failed for a run that hasn't failed", async () => {
      register(h.registry, "noop", () => "ok");
      const runId = await createRun(h.storage, "noop", {});
      await h.runOnce(runId);
      expect((await h.storage.loadRun(runId))?.status).toBe("done");
      const result = await h.storage.retryRun(runId);
      expect(result).toEqual({ kind: "not_failed", status: "done" });
    });

    it("returns missing for an unknown runId", async () => {
      const result = await h.storage.retryRun("00000000-0000-0000-0000-000000000000");
      expect(result).toEqual({ kind: "missing" });
    });
  });

  describe("suspend in step is forbidden", () => {
    it("returns STEP_INVALID_AWAIT when sleep is called inside step", async () => {
      register(h.registry, "bad", async (ctx) => {
        return ctx.step("nested", async () => {
          await ctx.sleep("1h");
          return 1;
        });
      });

      const runId = await createRun(h.storage, "bad", {});
      const r = await h.runOnce(runId);
      expect(r.status).toBe("failed");
      expect((await h.storage.loadRun(runId))?.error?.code).toBe("STEP_INVALID_AWAIT");
    });
  });

  describe("hooks", () => {
    it("suspends on ctx.hook and resumes after delivery", async () => {
      register(h.registry, "approve", async (ctx) => {
        const result = await ctx.signal<{ ok: boolean }>("approval");
        return result.ok ? "approved" : "rejected";
      });

      const runId = await createRun(h.storage, "approve", {});
      expect((await h.runOnce(runId)).status).toBe("suspended");
      expect((await h.storage.loadRun(runId))?.status).toBe("awaiting_signal");

      const sig = await h.storage.deliverSignal(runId, "approval", { ok: true });
      expect(sig.kind).toBe("delivered");

      const r = await h.runOnce(runId);
      expect(r.status).toBe("completed");
      expect((await h.storage.loadRun(runId))?.output).toBe("approved");
    });

    it("buffers pre-arm signal: hook returns immediately on first call", async () => {
      const runId = await createRun(h.storage, "approve2", {});
      const buffered = await h.storage.preDeliverSignal(runId, "signal:approval", { ok: true });
      expect(buffered).toBe(true);

      register(h.registry, "approve2", async (ctx) => {
        const r = await ctx.signal<{ ok: boolean }>("approval");
        return r.ok ? "approved" : "rejected";
      });

      const r = await h.runOnce(runId);
      expect(r.status).toBe("completed");
      expect((await h.storage.loadRun(runId))?.output).toBe("approved");
    });

    it("times out when expiresAt is reached without delivery", async () => {
      register(h.registry, "timeout", async (ctx) => {
        return ctx.signal("approval", { timeout: new Date(0) });
      });

      const runId = await createRun(h.storage, "timeout", {});
      await h.runOnce(runId);
      const r = await h.runOnce(runId);
      expect(r.status).toBe("failed");
      expect((await h.storage.loadRun(runId))?.error?.code).toBe("SIGNAL_TIMEOUT");
    });

    it("second signal for the same hook is idempotent (duplicate)", async () => {
      const runId = await createRun(h.storage, "x", {});
      const first = await h.storage.deliverSignal(runId, "foo", "v1");
      const second = await h.storage.deliverSignal(runId, "foo", "v2");
      expect(first.kind).toBe("buffered");
      expect(second.kind).toBe("duplicate");
      const row = await h.storage.loadSignal(runId, "signal:foo");
      expect(row?.payload).toBe("v1");
    });
  });

  describe("idempotency", () => {
    it("returns the same run when idempotency key matches", async () => {
      const first = await h.storage.createRun({
        name: "idem",
        version: 1,
        input: { v: 1 },
        idempotencyKey: "abc",
      });
      const second = await h.storage.createRun({
        name: "idem",
        version: 1,
        input: { v: 2 },
        idempotencyKey: "abc",
      });

      expect(first.created).toBe(true);
      expect(second.created).toBe(false);
      expect(first.runId).toBe(second.runId);
    });

    it("plain inserts without idempotency keys do not collide", async () => {
      const a = await h.storage.createRun({
        name: "plain",
        version: 1,
        input: { v: 1 },
      });
      const b = await h.storage.createRun({
        name: "plain",
        version: 1,
        input: { v: 1 },
      });
      expect(a.runId).not.toBe(b.runId);
      expect(a.created).toBe(true);
      expect(b.created).toBe(true);
    });

    it("partial unique index allows multiple NULL idempotency_key rows", async () => {
      await h.storage.createRun({ name: "p", version: 1, input: {} });
      await h.storage.createRun({ name: "p", version: 1, input: {} });
      const count = await h.db.$count(runs, eq(runs.name, "p"));
      expect(count).toBe(2);
    });
  });

  describe("zod input schema", () => {
    it("rejects invalid input and marks run failed", async () => {
      register(h.registry, "typed", async (_ctx, input) => input, {
        inputSchema: z.object({ count: z.number().int().positive() }),
      });

      const runId = await createRun(h.storage, "typed", { count: -1 });
      const r = await h.runOnce(runId);
      expect(r.status).toBe("failed");
      expect((await h.storage.loadRun(runId))?.error?.code).toBe("INPUT_INVALID");
    });
  });

  describe("event audit trail", () => {
    it("records the full lifecycle of a successful run", async () => {
      register(h.registry, "audit", async (ctx) => {
        return ctx.step("only", () => "value");
      });
      const runId = await createRun(h.storage, "audit", {});
      await h.runOnce(runId);

      const rows = await h.db
        .select({ type: events.type })
        .from(events)
        .where(eq(events.runId, runId))
        .orderBy(events.at);
      const types = rows.map((r) => r.type);
      expect(types).toContain("started");
      expect(types).toContain("step_started");
      expect(types).toContain("step_ok");
      expect(types).toContain("completed");
    });

    it("event payloads do not duplicate large business data", async () => {
      const bigOutput = "x".repeat(1024);
      register(h.registry, "trimmed", async (ctx) => {
        return ctx.step("only", () => bigOutput);
      });
      const runId = await createRun(h.storage, "trimmed", {});
      await h.runOnce(runId);

      const completed = await h.db
        .select({ payload: events.payload })
        .from(events)
        .where(eq(events.runId, runId));
      const completedRow = completed.find((_r, i) => completed[i] && completed[i].payload === null);
      expect(completedRow?.payload).toBeNull();
      const stored = await h.storage.loadRun(runId);
      expect(stored?.output).toBe(bigOutput);
    });

    it("hook_resolved event payload references the hook key, not the signal payload", async () => {
      const { runId } = await h.storage.createRun({
        name: "audit-hook",
        version: 1,
        input: {},
      });
      await h.storage.deliverSignal(runId, "x", { secret: "redacted-large-blob" });

      const evts = await h.db
        .select({ type: events.type, payload: events.payload })
        .from(events)
        .where(eq(events.runId, runId));
      const resolved = evts.find((e) => e.type === "signal_delivered");
      expect(resolved?.payload).toEqual({
        cursorKey: "signal:x",
        buffered: true,
      });
      const stored = await h.storage.loadSignal(runId, "signal:x");
      expect(stored?.payload).toEqual({ secret: "redacted-large-blob" });
    });
  });

  describe("conformance: ported from vercel/workflow world-testing suite", () => {
    it("addition: ctx.step composes (mirrors workflows/addition.ts)", async () => {
      register(h.registry, "add-ten", async (ctx, input) => {
        const n = input as number;
        const a = await ctx.step("add-2", () => n + 2);
        const b = await ctx.step("add-3", () => a + 3);
        const c = await ctx.step("add-5", () => b + 5);
        return c;
      });

      const runId = await createRun(h.storage, "add-ten", 0);
      const r = await h.runOnce(runId);
      expect(r.status).toBe("completed");
      expect((await h.storage.loadRun(runId))?.output).toBe(10);
    });

    it("brokenWf: parallel step batches via Promise.all (mirrors workflows/noop.ts)", async () => {
      let counter = 0;
      register(h.registry, "broken", async (ctx) => {
        const batch1 = await Promise.all(
          Array.from({ length: 5 }, (_, i) => ctx.step(`first-${i}`, () => ++counter)),
        );
        const batch2 = await Promise.all(
          Array.from({ length: 15 }, (_, i) => ctx.step(`second-${i}`, () => ++counter)),
        );
        return { numbers: [...batch1, ...batch2] };
      });

      const runId = await createRun(h.storage, "broken", {});
      const r = await h.runOnce(runId);
      expect(r.status).toBe("completed");
      const out = (await h.storage.loadRun(runId))?.output as {
        numbers: number[];
      };
      expect(out.numbers).toHaveLength(20);
      expect(new Set(out.numbers).size).toBe(20);
    });

    it("retryable error: attempt-1 throws, attempt-2 succeeds (mirrors retriable-and-fatal.ts)", async () => {
      let attempts = 0;
      register(h.registry, "retriable", async (ctx) => {
        return ctx.step(
          "may-fail",
          () => {
            attempts += 1;
            if (attempts === 1) throw new Error("retryable");
            return { attempt: attempts };
          },
          { retries: 3, baseBackoffMs: 1, capBackoffMs: 5 },
        );
      });

      const runId = await createRun(h.storage, "retriable", {});
      expect((await h.runOnce(runId)).status).toBe("suspended");
      expect((await h.runOnce(runId)).status).toBe("completed");
      expect(attempts).toBe(2);
      const out = (await h.storage.loadRun(runId))?.output as {
        attempt: number;
      };
      expect(out.attempt).toBe(2);
    });

    it("fatal error is caught when classify=permanent (mirrors FatalError semantics)", async () => {
      register(h.registry, "fatal-caught", async (ctx) => {
        let gotFatal = false;
        try {
          await ctx.step(
            "fatal-step",
            () => {
              throw new Error("fatal");
            },
            { retries: 5, classify: () => "permanent" as const },
          );
        } catch (err) {
          if (err instanceof FlowRuntimeError && err.nonRetryable) {
            gotFatal = true;
          }
        }
        return { gotFatal };
      });

      const runId = await createRun(h.storage, "fatal-caught", {});
      const r = await h.runOnce(runId);
      expect(r.status).toBe("completed");
      const out = (await h.storage.loadRun(runId))?.output as {
        gotFatal: boolean;
      };
      expect(out.gotFatal).toBe(true);
    });

    it("null byte in step result surfaces as workflow failure (jsonb constraint)", async () => {
      register(h.registry, "null-byte", async (ctx) => {
        return ctx.step("emit", () => "null byte  ");
      });

      const runId = await createRun(h.storage, "null-byte", {});
      const r = await h.runOnce(runId);
      expect(r.status).toBe("failed");
      const run = await h.storage.loadRun(runId);
      expect(run?.status).toBe("failed");
      expect(run?.error?.message ?? "").toMatch(/unicode|0000|null/i);
    });

    it("safe string in step result round-trips cleanly", async () => {
      register(h.registry, "round-trip", async (ctx) => {
        return ctx.step("emit", () => ({ s: "héllo 🌍", n: 42, b: true }));
      });

      const runId = await createRun(h.storage, "round-trip", {});
      const r = await h.runOnce(runId);
      expect(r.status).toBe("completed");
      const run = await h.storage.loadRun(runId);
      expect(run?.output).toEqual({ s: "héllo 🌍", n: 42, b: true });
    });

    it("hook multi-signal loop (mirrors collectWithHook in hooks.ts)", async () => {
      register(h.registry, "collect", async (ctx) => {
        const collected: Array<{ data: string; done?: boolean }> = [];
        let cursor = 0;
        while (true) {
          const event = await ctx.signal<{ data: string; done?: boolean }>(`evt-${cursor}`);
          collected.push(event);
          if (event.done) break;
          cursor += 1;
        }
        return { collected };
      });

      const runId = await createRun(h.storage, "collect", {});
      expect((await h.runOnce(runId)).status).toBe("suspended");

      await h.storage.deliverSignal(runId, "evt-0", { data: "first" });
      expect((await h.runOnce(runId)).status).toBe("suspended");

      await h.storage.deliverSignal(runId, "evt-1", { data: "second" });
      expect((await h.runOnce(runId)).status).toBe("suspended");

      await h.storage.deliverSignal(runId, "evt-2", {
        data: "third",
        done: true,
      });
      expect((await h.runOnce(runId)).status).toBe("completed");

      const out = (await h.storage.loadRun(runId))?.output as {
        collected: Array<{ data: string; done?: boolean }>;
      };
      expect(out.collected).toEqual([
        { data: "first" },
        { data: "second" },
        { data: "third", done: true },
      ]);
    });

    it("step memoization survives a concurrent re-execution (replay determinism)", async () => {
      const calls: number[] = [];
      register(h.registry, "deterministic", async (ctx) => {
        const a = await ctx.step("a", () => {
          calls.push(1);
          return "A";
        });
        const b = await ctx.step("b", () => {
          calls.push(2);
          return "B";
        });
        return { a, b };
      });

      const runId = await createRun(h.storage, "deterministic", {});
      await h.runOnce(runId);
      expect(calls).toEqual([1, 2]);

      // Run completed; further runOnce calls skip (run is done).
      await h.runOnce(runId);
      await h.runOnce(runId);
      expect(calls).toEqual([1, 2]);
      expect((await h.storage.loadRun(runId))?.output).toEqual({
        a: "A",
        b: "B",
      });
    });

    it("failed_terminal step blocks workflow re-execution (mirrors terminal-state validation)", async () => {
      register(h.registry, "doomed-once", async (ctx) => {
        await ctx.step(
          "bad",
          () => {
            throw new Error("nope");
          },
          { retries: 0, baseBackoffMs: 1 },
        );
        return "unreachable";
      });

      const runId = await createRun(h.storage, "doomed-once", {});
      const r = await h.runOnce(runId);
      expect(r.status).toBe("failed");
      const step = await h.storage.loadStep(runId, "bad");
      expect(step?.status).toBe("failed_terminal");
    });

    it("idempotency dedupes concurrent run starts", async () => {
      const racers = await Promise.allSettled(
        Array.from({ length: 5 }, () =>
          h.storage.createRun({
            name: "idem-race",
            version: 1,
            input: {},
            idempotencyKey: "race-key",
          }),
        ),
      );
      const ok = racers
        .filter((r) => r.status === "fulfilled")
        .map(
          (r) => (r as { status: "fulfilled"; value: { runId: string; created: boolean } }).value,
        );
      const created = ok.filter((v) => v.created);
      expect(created).toHaveLength(1);
      const runIds = new Set(ok.map((v) => v.runId));
      expect(runIds.size).toBe(1);
    });

    it("replays cleanly across simulated worker crashes mid-workflow", async () => {
      const executions = { step1: 0, step2: 0, step3: 0 };
      register(h.registry, "crash-replay", async (ctx) => {
        const a = await ctx.step("step1", () => {
          executions.step1 += 1;
          return "a";
        });
        await ctx.sleep(new Date(0));
        const b = await ctx.step("step2", () => {
          executions.step2 += 1;
          return `${a}b`;
        });
        const c = await ctx.step("step3", () => {
          executions.step3 += 1;
          return `${b}c`;
        });
        return c;
      });

      const runId = await createRun(h.storage, "crash-replay", {});
      // Run to completion in one go (past-sleep fires inline)
      await h.runOnce(runId);
      expect(executions).toEqual({ step1: 1, step2: 1, step3: 1 });
      expect((await h.storage.loadRun(runId))?.output).toBe("abc");
    });
  });
});

describe("defineWorkflow (low-level API)", () => {
  it("registers a raw run fn and routes via engine just like a built flow", async () => {
    const h = await setup();
    try {
      const calls = { sum: 0 };
      h.registry.register({
        name: "raw-add",
        version: 1,
        inputSchema: z.object({ a: z.number(), b: z.number() }) as z.ZodType<unknown>,
        run: async (ctx, input) => {
          const { a, b } = input as { a: number; b: number };
          const result = await ctx.step("add", () => {
            calls.sum += 1;
            return a + b;
          });
          return result;
        },
      });
      const runId = await createRun(h.storage, "raw-add", { a: 2, b: 3 });
      const r = await h.runOnce(runId);
      expect(r.status).toBe("completed");
      expect((await h.storage.loadRun(runId))?.output).toBe(5);
      expect(calls.sum).toBe(1);
    } finally {
      await h.close();
    }
  });

  it("supports an unbounded ctx.hook loop (infinite-chat shape, bounded by 'end' payload)", async () => {
    const h = await setup();
    try {
      h.registry.register({
        name: "chat",
        version: 1,
        run: async (ctx) => {
          const history: Array<{ from: string; text: string }> = [];
          let i = 0;
          while (true) {
            const msg = await ctx.signal<{ text: string; end?: boolean }>("user-msg");
            if (msg.end) return history;
            history.push({ from: "user", text: msg.text });
            const reply = await ctx.step(`reply-${i}`, () => `echo: ${msg.text}`);
            history.push({ from: "agent", text: reply });
            i += 1;
          }
        },
      });

      const runId = await createRun(h.storage, "chat", {});

      // pre-deliver the FIRST hook (no hook armed yet); subsequent turns
      // use signalHook which targets the currently-armed iteration.
      await h.storage.preDeliverSignal(runId, "signal:user-msg", { text: "hello" });
      await h.runOnce(runId);
      await h.storage.deliverSignal(runId, "user-msg", { text: "again" });
      await h.runOnce(runId);
      await h.storage.deliverSignal(runId, "user-msg", { end: true });
      const r = await h.runOnce(runId);

      expect(r.status).toBe("completed");
      const out = (await h.storage.loadRun(runId))?.output as Array<{ from: string; text: string }>;
      expect(out.map((t) => t.text)).toEqual(["hello", "echo: hello", "again", "echo: again"]);
    } finally {
      await h.close();
    }
  });
});

describe("retry backoff timer", () => {
  it("arms a __retry timer on step backoff and fires it when the run resumes", async () => {
    const h = await setup();
    try {
      let attempts = 0;
      register(h.registry, "flaky", async (ctx) => {
        await ctx.step(
          "call",
          () => {
            attempts += 1;
            if (attempts === 1) throw new Error("transient");
            return "ok";
          },
          { retries: 1 },
        );
        return "done";
      });
      const runId = await createRun(h.storage, "flaky", {});

      // Pass 1: the step throws → the run suspends into retrying with a durable
      // backoff deadline (a timer), not just a queue job.
      const first = await h.runOnce(runId);
      expect(first.status).toBe("suspended");
      expect((await h.storage.loadRun(runId))?.status).toBe("retrying");
      const armed = await h.storage.loadTimer(runId, RETRY_TIMER_CURSOR);
      expect(armed).toBeDefined();
      expect(armed?.firedAt).toBeNull();
      expect(armed?.fireAt.getTime()).toBeGreaterThan(Date.now());

      // Pass 2: claiming the run fires the backoff timer; the retry succeeds.
      const second = await h.runOnce(runId);
      expect(second.status).toBe("completed");
      expect((await h.storage.loadRun(runId))?.status).toBe("done");
      expect((await h.storage.loadTimer(runId, RETRY_TIMER_CURSOR))?.firedAt).not.toBeNull();
    } finally {
      await h.close();
    }
  });
});

describe("observability config (end-to-end)", () => {
  it("a multi-step run completes with events:'off' + notify:false and steps stay durable", async () => {
    const h = await setup();
    try {
      const storage = createDrizzleStorage({
        db: h.db,
        logger: silentLogger,
        enqueue: async () => {},
        obs: { events: "off", notify: false },
      });
      register(h.registry, "quiet", async (ctx) => {
        const a = await ctx.step("a", () => 1);
        return ctx.step("b", () => a + 1);
      });
      const runId = await createRun(storage, "quiet", {});

      const r = await playRunAttempt({ ...baseRunnerDeps(), registry: h.registry, storage }, runId);
      expect(r.status).toBe("completed");
      expect((await storage.loadRun(runId))?.output).toBe(2);

      // Zero audit events written…
      const evs = await h.db.select().from(events).where(eq(events.runId, runId));
      expect(evs).toHaveLength(0);
      // …but the step rows (the resume source of truth) are all persisted.
      expect((await storage.loadStep(runId, "a"))?.status).toBe("ok");
      expect((await storage.loadStep(runId, "b"))?.status).toBe("ok");
    } finally {
      await h.close();
    }
  });
});

describe("suspend signal", () => {
  it("isSuspend narrows correctly", () => {
    const suspended = new FlowSuspend({ reason: "sleep" });
    expect(isSuspend(suspended)).toBe(true);
    expect(isSuspend(new Error("other"))).toBe(false);
    expect(isSuspend(new FlowRuntimeError({ code: "X", message: "y" }))).toBe(false);
  });
});
