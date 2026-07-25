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
import { type Backend, defineFlow, registry, submit, tickOnce } from "@iterativeflow/core";
import { MongoClient } from "mongodb";
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
