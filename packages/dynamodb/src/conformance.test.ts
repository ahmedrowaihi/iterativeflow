import { randomUUID } from "node:crypto";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { BatchWriteCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import {
  claimFilterConformance,
  shardedClaimConformance,
  cronConformance,
  engineConformance,
  outboxConformance,
  pendingWorkConformance,
  queueConformance,
  reconcileConformance,
  signalConformance,
  storeConformance,
  timerConformance,
  wakeupConformance,
} from "@iterativeflow/conformance";
import {
  type Backend,
  defineFlow,
  registry,
  serverlessTick,
  submit,
  tickOnce,
} from "@iterativeflow/core";
import {
  type Doc,
  DEFAULT_TABLE,
  REQUIRED_IAM_ACTIONS,
  createDynamoBackend,
  docClient,
  ensureTable,
  tableSpec,
} from "@iterativeflow/dynamodb";
import { GenericContainer, type StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const skip = process.env.SKIP_TESTCONTAINERS === "1";
const TABLE = DEFAULT_TABLE;

describe.skipIf(skip)("dynamodb backend", () => {
  let container: StartedTestContainer;
  let low: DynamoDBClient;
  let doc: ReturnType<typeof docClient>;

  beforeAll(async () => {
    container = await new GenericContainer("amazon/dynamodb-local:latest")
      .withExposedPorts(8000)
      .withCommand(["-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"])
      .start();
    const endpoint = `http://${container.getHost()}:${container.getMappedPort(8000)}`;
    low = new DynamoDBClient({
      endpoint,
      region: "us-east-1",
      credentials: { accessKeyId: "local", secretAccessKey: "local" },
    });
    doc = docClient(low);
    await ensureTable(low, TABLE);
  }, 180_000);

  afterAll(async () => {
    low?.destroy();
    await container?.stop().catch(() => undefined);
  });

  /** Delete every item so each test starts from an empty table. */
  const clearTable = async (): Promise<void> => {
    let ExclusiveStartKey: Record<string, unknown> | undefined;
    do {
      const res = await doc.send(
        new ScanCommand({ TableName: TABLE, ProjectionExpression: "pk, sk", ExclusiveStartKey }),
      );
      const items = res.Items ?? [];
      for (let i = 0; i < items.length; i += 25) {
        const batch = items
          .slice(i, i + 25)
          .map((row) => ({ DeleteRequest: { Key: { pk: row.pk, sk: row.sk } } }));
        if (batch.length)
          await doc.send(new BatchWriteCommand({ RequestItems: { [TABLE]: batch } }));
      }
      ExclusiveStartKey = res.LastEvaluatedKey;
    } while (ExclusiveStartKey);
  };

  /** A clean backend per test — real DynamoDB Local, table emptied between cases. */
  const makeBackend = async (): Promise<Backend> => {
    await clearTable();
    return createDynamoBackend(doc, { table: TABLE });
  };

  // The exact same suites the memory + postgres backends pass — one spec, every implementation.
  storeConformance("dynamodb", async () => (await makeBackend()).store);
  queueConformance("dynamodb", async () => (await makeBackend()).queue);
  claimFilterConformance("dynamodb", () => makeBackend());
  shardedClaimConformance("dynamodb", () => makeBackend());
  pendingWorkConformance("dynamodb", () => makeBackend());
  timerConformance("dynamodb", async () => (await makeBackend()).timer);
  wakeupConformance("dynamodb", async () => (await makeBackend()).wakeup);
  outboxConformance("dynamodb", () => makeBackend());
  signalConformance("dynamodb", () => makeBackend());
  reconcileConformance("dynamodb", () => makeBackend());
  cronConformance("dynamodb", async () => (await makeBackend()).store);
  engineConformance("dynamodb", () => makeBackend());

  // These need REAL concurrency + a real TransactWriteItems — memory (single-threaded) can't prove them.
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

      expect(new Set(results).size).toBe(1); // every racer agrees on ONE winner
      const winner = results[0];
      const created = (await Promise.all(childIds.map((cid) => backend.store.loadRun(cid)))).filter(
        Boolean,
      );
      expect(created).toHaveLength(1); // losers' childIds were never created
      expect(created[0]?.run.id).toBe(winner);
    });

    it("a failing outbox rolls back the step — the write is atomic, not torn", async () => {
      const clean = await makeBackend();
      const { runId } = await clean.store.startRun({ name: "f", version: 1, input: {} });

      // Reject the checkpoint's TransactWriteItems (it carries a JOB# enqueue). A non-atomic
      // backend would have already written the step; an atomic one writes nothing.
      const faulty: Doc = {
        send: (cmd: unknown) => {
          const anyCmd = cmd as { input?: unknown };
          if (JSON.stringify(anyCmd.input ?? {}).includes('"JOB#')) {
            return Promise.reject(new Error("injected fault"));
          }
          return (doc.send as (c: unknown) => Promise<unknown>)(cmd);
        },
      };
      const faultyBackend = createDynamoBackend(faulty, { table: TABLE });

      await expect(
        faultyBackend.store.checkpointStep(
          { runId, cursorKey: "x", status: "ok", result: 1, attempts: 1 },
          { enqueue: [{ runId }] },
        ),
      ).rejects.toThrow();
      const snap = await clean.store.loadRun(runId);
      expect(snap?.steps.has("x")).toBe(false); // step did NOT leak past the failed outbox
    });

    it("a within-budget spawn batch commits atomically; beyond the cap throws (core chunks fan-out)", async () => {
      const backend = await makeBackend();
      const { runId } = await backend.store.startRun({ name: "p", version: 1, input: {} });
      const childIds = Array.from({ length: 30 }, () => randomUUID());
      await backend.store.checkpointStep(
        { runId, cursorKey: "fan", status: "ok", result: childIds, attempts: 1 },
        {
          spawn: childIds.map((cid) => ({
            runId: cid,
            spec: { name: "c", version: 1, input: {} },
          })),
        },
      );
      const created = (
        await Promise.all(childIds.map((cid) => backend.store.loadRunRow(cid)))
      ).filter(Boolean);
      expect(created).toHaveLength(30);
      const leases = await backend.queue.claim({
        limit: 200,
        leaseMs: 1000,
        now: new Date("2030-01-01T00:00:00Z"),
      });
      expect(childIds.every((cid) => new Set(leases.map((l) => l.runId)).has(cid))).toBe(true);

      // A batch beyond one transaction's item budget throws loudly — core bounds fan-out so this
      // never trips in practice, but a direct over-budget checkpoint must not silently truncate.
      const { runId: r2 } = await backend.store.startRun({ name: "p", version: 1, input: {} });
      const tooMany = Array.from({ length: 60 }, () => randomUUID());
      await expect(
        backend.store.checkpointStep(
          { runId: r2, cursorKey: "fan", status: "ok", result: tooMany, attempts: 1 },
          {
            spawn: tooMany.map((cid) => ({
              runId: cid,
              spec: { name: "c", version: 1, input: {} },
            })),
          },
        ),
      ).rejects.toThrow(/transaction budget/);
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
      expect(claimed).toHaveLength(20); // all leased, none dropped
      expect(new Set(claimed).size).toBe(20); // and none leased twice
    });
  });

  // The whole durable executor, driven on real DynamoDB — steps, sleep, retry, invoke.
  describe("engine end-to-end", () => {
    const TERMINAL = new Set(["done", "failed", "canceled"]);

    const driveToSettle = async (
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
        name: "ddb-child",
        version: 1,
        run: async (_ctx, input) => input.n * 10,
      });
      const parent = defineFlow<{ n: number }, number>({
        name: "ddb-parent",
        version: 1,
        run: async (ctx, input) => (await ctx.invoke(child, { n: input.n })) + 1,
      });
      const flows = registry([parent, child]);
      const runId = await submit(backend, parent, { n: 5 });
      const settled = await driveToSettle(backend, flows, runId);
      expect(settled).toMatchObject({ status: "done", output: 51 }); // (5*10)+1
    });

    it("serverlessTick advances a durable sleep across invocations (the cron-Lambda model)", async () => {
      const backend = await makeBackend();
      const flow = defineFlow<Record<string, never>, string>({
        name: "ddb-nap",
        version: 1,
        run: async (ctx) => {
          await ctx.sleep(60_000);
          return "woke";
        },
      });
      const flows = registry([flow]);
      let clock = new Date("2030-01-01T00:00:00Z");
      const now = (): Date => clock;
      const opts = { batchMax: 16, leaseMs: 600_000, now };

      const runId = await submit(backend, flow, {});
      const s1 = await serverlessTick(backend, flows, opts);
      expect(s1.results.map((r) => r.status)).toContain("sleeping");
      // A later invocation, past the deadline, drains the timer and resumes — no loop between them.
      clock = new Date(clock.getTime() + 61_000);
      await serverlessTick(backend, flows, opts);
      expect((await backend.store.loadRun(runId))?.run.output).toBe("woke");
    });
  });

  it("tableSpec matches what ensureTable provisions, and names the required IAM actions", () => {
    const spec = tableSpec("mytable");
    expect(spec.TableName).toBe("mytable");
    expect(spec.KeySchema).toEqual([
      { AttributeName: "pk", KeyType: "HASH" },
      { AttributeName: "sk", KeyType: "RANGE" },
    ]);
    expect(spec.GlobalSecondaryIndexes?.[0].IndexName).toBe("gsi1");
    expect(REQUIRED_IAM_ACTIONS).toContain("dynamodb:TransactWriteItems");
    expect(REQUIRED_IAM_ACTIONS).toContain("dynamodb:ConditionCheckItem");
  });
});
