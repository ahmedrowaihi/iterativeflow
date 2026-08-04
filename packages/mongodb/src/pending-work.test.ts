import { type Db, MongoClient } from "mongodb";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createMongoBackend } from "#backend";
import { type Names, ensureIndexes, names } from "#collections";
import { pendingWorkPipeline } from "#pending-work";

const skip = process.env.SKIP_TESTCONTAINERS === "1";
const DB = "iterativeflow";
const n: Names = names("");

describe.skipIf(skip)("mongodb pendingWorkPipeline — autoscaling backlog", () => {
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

  beforeEach(async () => {
    const db = client.db(DB);
    for (const c of Object.values(n)) await db.collection(c).deleteMany({});
  });

  const backlog = async (db: Db, asOf: Date): Promise<number> => {
    const [row] = await db.collection(n.jobs).aggregate(pendingWorkPipeline(asOf)).toArray();
    return (row?.pendingWork as number) ?? 0;
  };

  it("agrees with the summed port methods (whole backlog) at a fixed instant", async () => {
    const be = createMongoBackend(client, { db: DB });
    const db = client.db(DB);
    const asOf = new Date("2030-06-01T00:00:00Z");
    const past = new Date(asOf.getTime() - 60_000);
    const future = new Date(asOf.getTime() + 3_600_000);
    const start = (name: string) =>
      be.store.startRun({ name, version: 1, input: {} }).then((r) => r.runId);

    const leased = await start("a");
    await be.queue.enqueue(leased);
    await be.queue.claim({ limit: 1, leaseMs: 600_000, now: asOf });
    await be.queue.enqueue(await start("a"));
    await be.queue.enqueue(await start("b"));
    await be.timer.schedule(await start("a"), past);
    await be.timer.schedule("orphan-run", past);
    await be.store.upsertCron({
      name: "cron-a",
      schedule: "* * * * *",
      flowName: "a",
      flowVersion: 1,
      input: {},
      nextRunAt: past,
    });
    await be.queue.enqueue(await start("a"), { runAt: future });

    const ports =
      (await be.queue.depth(asOf, undefined)).claimable +
      (await be.timer.dueCount(asOf, undefined)) +
      (await be.store.dueCronCount(asOf, undefined));
    expect(await backlog(db, asOf)).toBe(ports);
  });
});
