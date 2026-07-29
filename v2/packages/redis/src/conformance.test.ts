import {
  claimFilterConformance,
  cronConformance,
  engineConformance,
  outboxConformance,
  queueConformance,
  reconcileConformance,
  signalConformance,
  storeConformance,
  timerConformance,
  wakeupConformance,
} from "@iterativeflow/conformance";
import { type Backend, defineFlow, registry, submit, tickOnce } from "@iterativeflow/core";
import { Redis } from "ioredis";
import { GenericContainer, type StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createRedisBackend } from "#index";

const skip = process.env.SKIP_TESTCONTAINERS === "1";

describe.skipIf(skip)("redis backend", () => {
  let container: StartedTestContainer;
  let client: Redis;

  beforeAll(async () => {
    container = await new GenericContainer("valkey/valkey:8-alpine").withExposedPorts(6379).start();
    client = new Redis(container.getMappedPort(6379), container.getHost(), {
      maxRetriesPerRequest: null,
    });
  }, 180_000);

  afterAll(async () => {
    client?.disconnect();
    await container?.stop().catch(() => undefined);
  });

  const makeBackend = async (): Promise<Backend> => {
    await client.flushdb();
    return createRedisBackend(client);
  };

  storeConformance("redis", async () => (await makeBackend()).store);
  queueConformance("redis", async () => (await makeBackend()).queue);
  claimFilterConformance("redis", () => makeBackend());
  timerConformance("redis", async () => (await makeBackend()).timer);
  wakeupConformance("redis", async () => (await makeBackend()).wakeup);
  outboxConformance("redis", () => makeBackend());
  signalConformance("redis", () => makeBackend());
  reconcileConformance("redis", () => makeBackend());
  cronConformance("redis", async () => (await makeBackend()).store);
  engineConformance("redis", () => makeBackend());

  // Real concurrency against a real Redis — proves the Lua atomicity a single-threaded memory backend can't.
  describe("atomicity + concurrency", () => {
    it("concurrent first-writer-wins: N parallel checkpoints on one key spawn exactly one child", async () => {
      const backend = await makeBackend();
      const { runId } = await backend.store.startRun({ name: "p", version: 1, input: {} });
      const childIds = Array.from({ length: 8 }, (_v, i) => `child-${i}`);

      const results = await Promise.all(
        childIds.map((cid) =>
          backend.store
            .checkpointStep(
              { runId, cursorKey: "spawn", status: "ok", result: cid, attempts: 1 },
              { spawn: [{ runId: cid, spec: { name: "c", version: 1, input: {} } }] },
            )
            .then((o) => o.result as string),
        ),
      );

      expect(new Set(results).size).toBe(1);
      const created = (await Promise.all(childIds.map((cid) => backend.store.loadRun(cid)))).filter(
        Boolean,
      );
      expect(created).toHaveLength(1);
      expect(created[0]?.run.id).toBe(results[0]);
    });

    it("concurrent claims lease each run to exactly one worker", async () => {
      const backend = await makeBackend();
      const ids: string[] = [];
      for (let i = 0; i < 20; i++) {
        const { runId } = await backend.store.startRun({ name: "f", version: 1, input: { i } });
        ids.push(runId);
      }
      for (const runId of ids) await backend.queue.enqueue(runId);
      const now = new Date("2030-01-01T00:00:00Z");
      const batches = await Promise.all(
        Array.from({ length: 4 }, () => backend.queue.claim({ limit: 10, leaseMs: 1000, now })),
      );
      const claimed = batches.flat().map((l) => l.runId);
      expect(claimed).toHaveLength(20);
      expect(new Set(claimed).size).toBe(20);
    });
  });

  describe("engine end-to-end", () => {
    const TERMINAL = new Set(["done", "failed", "canceled"]);
    const drive = async (
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

    it("invokes a child flow across the outbox and resumes with its output", async () => {
      const backend = await makeBackend();
      const child = defineFlow<{ n: number }, number>({
        name: "r-child",
        version: 1,
        run: async (_ctx, input) => input.n * 10,
      });
      const parent = defineFlow<{ n: number }, number>({
        name: "r-parent",
        version: 1,
        run: async (ctx, input) => (await ctx.invoke(child, { n: input.n })) + 1,
      });
      const flows = registry([parent, child]);
      const runId = await submit(backend, parent, { n: 5 });
      expect(await drive(backend, flows, runId)).toMatchObject({ status: "done", output: 51 });
    });
  });
});
