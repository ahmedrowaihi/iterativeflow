import { backendConformance } from "@iterativeflow/conformance";
import { type Backend, defineFlow, submit } from "@iterativeflow/core";
import { type Collection, MongoClient } from "mongodb";
import type { StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createMongoBackend } from "#backend";
import { type Names, names } from "#collections";
import { inTx } from "#tx";
import { startMongo, stopMongo } from "#test-container";

const skip = process.env.SKIP_TESTCONTAINERS === "1";
const DB = "iterativeflow";
const n: Names = names("");

describe.skipIf(skip)("mongodb backend", () => {
  let container: StartedTestContainer;
  let client: MongoClient;

  beforeAll(async () => {
    ({ container, client } = await startMongo(DB));
  }, 180_000);

  afterAll(() => stopMongo({ container, client }));

  const makeBackend = async (): Promise<Backend> => {
    const db = client.db(DB);
    for (const c of Object.values(n)) await db.collection(c).deleteMany({});
    return createMongoBackend(client, { db: DB });
  };

  backendConformance("mongodb", () => makeBackend(), { concurrentWriters: true });

  it("transactional enqueue: caller's write + submit commit atomically (and roll back together)", async () => {
    const backend = await makeBackend();
    const orders: Collection<{ _id: string }> = client.db(DB).collection("app_orders");
    await orders.deleteMany({});
    const flow = defineFlow<Record<string, never>, number>({
      name: "fulfil",
      version: 1,
      run: async () => 1,
    });
    const orderCount = (): Promise<number> => orders.countDocuments();
    const runCount = async (): Promise<number> =>
      (await backend.store.listRuns({}, { limit: 10 })).runs.length;

    // Rollback: a throw after submit persists neither the order nor the run/job.
    await expect(
      inTx(client, async (b, session) => {
        await orders.insertOne({ _id: "o1" }, { session });
        await submit(b, flow, {});
        throw new Error("boom after submit");
      }),
    ).rejects.toThrow();
    expect(await orderCount()).toBe(0);
    expect(await runCount()).toBe(0);

    // Commit: the order AND the enqueued run land together.
    const id = await inTx(client, async (b, session) => {
      await orders.insertOne({ _id: "o2" }, { session });
      return submit(b, flow, {});
    });
    expect(await orderCount()).toBe(1);
    expect(await backend.store.loadRunRow(id)).toBeDefined();
  });

  describe("atomicity + concurrency", () => {
    it("a failing outbox rolls back the step — the write is atomic, not torn", async () => {
      const clean = await makeBackend();
      const { runId } = await clean.store.startRun({ name: "f", version: 1, input: {} });

      // A validator no document can pass makes every jobs write fail inside the transaction.
      const db = client.db(DB);
      await db.command({ collMod: n.jobs, validator: { _id: { $exists: false } } });
      try {
        await expect(
          clean.store.checkpointStep(
            { runId, cursorKey: "x", status: "ok", result: 1, attempts: 1 },
            { spawn: [{ runId: "child", spec: { name: "c", version: 1, input: {} } }] },
          ),
        ).rejects.toThrow(/validation/i);
      } finally {
        await db.command({ collMod: n.jobs, validator: {} });
      }
      const snap = await clean.store.loadRun(runId);
      expect(snap?.steps.has("x")).toBe(false);
    });
  });
});
