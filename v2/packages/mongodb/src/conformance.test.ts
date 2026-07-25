import {
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
import { randomUUID } from "node:crypto";
import { type Backend, defineFlow, registry, submit, tickOnce } from "@iterativeflow/core";
import { type Collection, type Db, MongoClient } from "mongodb";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createMongoBackend } from "#backend";
import { type Names, ensureIndexes, names } from "#collections";

const skip = process.env.SKIP_TESTCONTAINERS === "1";
const DB = "iterativeflow";
const n: Names = names("");

describe.skipIf(skip)("mongodb backend", () => {
  let container: StartedTestContainer;
  let client: MongoClient;

  beforeAll(async () => {
    container = await new GenericContainer("mongo:7")
      .withCommand(["--replSet", "rs0", "--bind_ip_all"])
      .withExposedPorts(27017)
      .withWaitStrategy(Wait.forLogMessage(/Waiting for connections/))
      .start();
    const uri = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`;
    client = new MongoClient(uri);
    await client.connect();
    await client
      .db("admin")
      .command({ replSetInitiate: {} })
      .catch(() => undefined);
    for (let i = 0; i < 40; i++) {
      const hello = await client.db("admin").command({ hello: 1 });
      if (hello.isWritablePrimary) break;
      await new Promise((r) => setTimeout(r, 500));
    }
    await ensureIndexes(client.db(DB));
  }, 180_000);

  afterAll(async () => {
    await client?.close().catch(() => undefined);
    await container?.stop().catch(() => undefined);
  });

  const makeBackend = async (): Promise<Backend> => {
    const db = client.db(DB);
    for (const c of Object.values(n)) await db.collection(c).deleteMany({});
    return createMongoBackend(client, { db: DB });
  };

  storeConformance("mongodb", async () => (await makeBackend()).store);
  queueConformance("mongodb", async () => (await makeBackend()).queue);
  timerConformance("mongodb", async () => (await makeBackend()).timer);
  wakeupConformance("mongodb", async () => (await makeBackend()).wakeup);
  outboxConformance("mongodb", () => makeBackend());
  signalConformance("mongodb", () => makeBackend());
  reconcileConformance("mongodb", () => makeBackend());
  cronConformance("mongodb", async () => (await makeBackend()).store);
  engineConformance("mongodb", () => makeBackend());

  describe("atomicity + concurrency", () => {
    it("concurrent first-writer-wins: N parallel checkpoints on one key spawn exactly one child", async () => {
      const backend = await makeBackend();
      const { runId } = await backend.store.startRun({ name: "p", version: 1, input: {} });
      const childIds = Array.from({ length: 8 }, () => randomUUID());

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

    it("a failing outbox rolls back the step — the write is atomic, not torn", async () => {
      const clean = await makeBackend();
      const { runId } = await clean.store.startRun({ name: "f", version: 1, input: {} });

      const reject = () => Promise.reject(new Error("injected fault"));
      const delegate = (target: object, prop: string | symbol) => {
        const v = Reflect.get(target, prop, target);
        return typeof v === "function" ? v.bind(target) : v;
      };
      const wrapJobs = (coll: Collection): Collection =>
        new Proxy(coll, {
          get: (target, prop) =>
            prop === "insertOne" ||
            prop === "insertMany" ||
            prop === "bulkWrite" ||
            prop === "updateOne"
              ? reject
              : delegate(target, prop),
        });
      const wrapDb = (db: Db): Db =>
        new Proxy(db, {
          get: (target, prop) =>
            prop === "collection"
              ? (name: string) => {
                  const coll = target.collection(name);
                  return name === n.jobs ? wrapJobs(coll) : coll;
                }
              : delegate(target, prop),
        });
      const faultyClient = new Proxy(client, {
        get: (target, prop) =>
          prop === "db" ? (name?: string) => wrapDb(target.db(name)) : delegate(target, prop),
      });

      const faulty = createMongoBackend(faultyClient, { db: DB });
      await expect(
        faulty.store.checkpointStep(
          { runId, cursorKey: "x", status: "ok", result: 1, attempts: 1 },
          { enqueue: [{ runId }] },
        ),
      ).rejects.toThrow();
      const snap = await clean.store.loadRun(runId);
      expect(snap?.steps.has("x")).toBe(false);
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
      const claimed: string[] = [];
      // No SKIP LOCKED: concurrent limited claims oversample the queue head, so a single round
      // leases only its head — drain across rounds; the invariant is that no run is leased twice.
      for (let round = 0; round < 10 && claimed.length < ids.length; round++) {
        const batches = await Promise.all(
          Array.from({ length: 4 }, () =>
            backend.queue.claim({ limit: 10, leaseMs: 600_000, now }),
          ),
        );
        claimed.push(...batches.flat().map((l) => l.runId));
      }
      expect(new Set(claimed).size).toBe(claimed.length);
      expect(new Set(claimed)).toEqual(new Set(ids));
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
        name: "mo-child",
        version: 1,
        run: async (_ctx, input) => input.n * 10,
      });
      const parent = defineFlow<{ n: number }, number>({
        name: "mo-parent",
        version: 1,
        run: async (ctx, input) => (await ctx.invoke(child, { n: input.n })) + 1,
      });
      const flows = registry([parent, child]);
      const runId = await submit(backend, parent, { n: 5 });
      expect(await drive(backend, flows, runId)).toMatchObject({ status: "done", output: 51 });
    });
  });
});
