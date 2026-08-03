import { DatabaseSync } from "node:sqlite";
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
import { afterEach, describe, expect, it } from "vitest";
import { type SqlStorage, applySchema, createDurableObjectBackend } from "#index";

/**
 * A `SqlStorage` mock over Node's synchronous `node:sqlite` — same synchronous `exec().toArray()`
 * shape as a Durable Object's `ctx.storage.sql`. This drives the REAL DO adapter through the full
 * conformance suites without the Workers runtime. (A DO serves one request at a time, so the
 * single-writer sequential model the suites exercise matches production.)
 */
const nodeSqliteStorage = (db: DatabaseSync): SqlStorage => ({
  exec: (query, ...bindings) => ({
    toArray: () => db.prepare(query).all(...(bindings as never[])) as Record<string, unknown>[],
  }),
});

describe("durable-objects backend (SQLite over node:sqlite)", () => {
  const dbs: DatabaseSync[] = [];
  afterEach(() => {
    for (const db of dbs.splice(0)) db.close();
  });

  const makeBackend = async (): Promise<Backend> => {
    const db = new DatabaseSync(":memory:");
    dbs.push(db);
    const storage = nodeSqliteStorage(db);
    await applySchema(storage);
    return createDurableObjectBackend(storage);
  };

  storeConformance("durable-objects", async () => (await makeBackend()).store);
  queueConformance("durable-objects", async () => (await makeBackend()).queue);
  claimFilterConformance("durable-objects", () => makeBackend());
  timerConformance("durable-objects", async () => (await makeBackend()).timer);
  wakeupConformance("durable-objects", async () => (await makeBackend()).wakeup);
  outboxConformance("durable-objects", () => makeBackend());
  signalConformance("durable-objects", () => makeBackend());
  reconcileConformance("durable-objects", () => makeBackend());
  cronConformance("durable-objects", async () => (await makeBackend()).store);
  engineConformance("durable-objects", () => makeBackend());

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
        name: "do-child",
        version: 1,
        run: async (_ctx, input) => input.n * 10,
      });
      const parent = defineFlow<{ n: number }, number>({
        name: "do-parent",
        version: 1,
        run: async (ctx, input) => (await ctx.invoke(child, { n: input.n })) + 1,
      });
      const flows = registry([parent, child]);
      const runId = await submit(backend, parent, { n: 5 });
      expect(await drive(backend, flows, runId)).toMatchObject({ status: "done", output: 51 });
    });
  });
});
