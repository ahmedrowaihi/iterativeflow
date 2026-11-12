import { randomUUID } from "node:crypto";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { BatchWriteCommand, ScanCommand, type ScanCommandInput } from "@aws-sdk/lib-dynamodb";
import { backendConformance } from "@iterativeflow/conformance";
import type { Backend } from "@iterativeflow/core";
import {
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
  let endpoint: string;
  let low: DynamoDBClient;
  let doc: ReturnType<typeof docClient>;

  const connect = (): DynamoDBClient =>
    new DynamoDBClient({
      endpoint,
      region: "us-east-1",
      credentials: { accessKeyId: "local", secretAccessKey: "local" },
    });

  beforeAll(async () => {
    container = await new GenericContainer("amazon/dynamodb-local:latest")
      .withExposedPorts(8000)
      .withCommand(["-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"])
      .start();
    endpoint = `http://${container.getHost()}:${container.getMappedPort(8000)}`;
    low = connect();
    doc = docClient(low);
    await ensureTable(low, TABLE);
  }, 180_000);

  afterAll(async () => {
    low?.destroy();
    await container?.stop().catch(() => undefined);
  });

  /** Delete every item so each test starts from an empty table. */
  const clearTable = async (): Promise<void> => {
    let ExclusiveStartKey: ScanCommandInput["ExclusiveStartKey"];
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
  backendConformance("dynamodb", () => makeBackend(), { concurrentWriters: true });

  // These need REAL concurrency + a real TransactWriteItems — memory (single-threaded) can't prove them.
  describe("atomicity + concurrency", () => {
    it("a failing outbox rolls back the step — the write is atomic, not torn", async () => {
      const clean = await makeBackend();
      const { runId } = await clean.store.startRun({ name: "f", version: 1, input: {} });

      // Reject the checkpoint's TransactWriteItems (its spawn carries a JOB# enqueue). A non-atomic
      // backend would have already written the step; an atomic one writes nothing.
      const faultyLow = connect();
      const faulty = docClient(faultyLow);
      faulty.middlewareStack.add(
        (next) => async (args) => {
          if (JSON.stringify(args.input).includes('"JOB#')) throw new Error("injected fault");
          return next(args);
        },
        { step: "initialize", name: "injectFault" },
      );
      const faultyBackend = createDynamoBackend(faulty, { table: TABLE });

      await expect(
        faultyBackend.store.checkpointStep(
          { runId, cursorKey: "x", status: "ok", result: 1, attempts: 1 },
          { spawn: [{ runId: randomUUID(), spec: { name: "c", version: 1, input: {} } }] },
        ),
      ).rejects.toThrow();
      faultyLow.destroy();
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
  });

  it("tableSpec matches what ensureTable provisions, and names the required IAM actions", async () => {
    const spec = tableSpec("mytable");
    expect(spec.TableName).toBe("mytable");
    expect(spec.KeySchema).toEqual([
      { AttributeName: "pk", KeyType: "HASH" },
      { AttributeName: "sk", KeyType: "RANGE" },
    ]);
    expect(spec.GlobalSecondaryIndexes?.[0].IndexName).toBe("gsi1");

    const sent = new Set<string>();
    const recording = docClient(low);
    recording.middlewareStack.add(
      (next, context) => async (args) => {
        // The document client hands its commands to the low-level client, so this sees the wire
        // operation name (`GetItemCommand`), which is the IAM action plus a suffix.
        sent.add(`dynamodb:${String(context.commandName).replace(/Command$/, "")}`);
        const input: object = args.input;
        const items =
          "TransactItems" in input && Array.isArray(input.TransactItems) ? input.TransactItems : [];
        if (items.some((i) => "ConditionCheck" in i)) {
          sent.add("dynamodb:ConditionCheckItem");
        }
        return next(args);
      },
      { step: "initialize", name: "iam-recorder" },
    );
    await clearTable();
    const b = createDynamoBackend(recording, { table: TABLE });
    const { runId } = await b.store.startRun({ name: "f", version: 1, input: {} });
    await b.store.startRun({ name: "f", version: 1, input: {}, idempotencyKey: "k" });
    await b.store.loadRunRow(runId);
    await b.store.loadRunRows([runId]);
    await b.queue.enqueue(runId);
    const [lease] = await b.queue.claim({ limit: 1, leaseMs: 60_000 });
    await b.store.checkpointStep(
      { runId, cursorKey: "s", status: "ok", result: 1, attempts: 1 },
      { requireVersion: lease.version },
    );
    await b.queue.ack(lease);
    await b.timer.schedule(runId, new Date());
    await b.timer.cancel(runId);
    await b.store.markTerminal(runId, { status: "done", output: 1 });
    await b.store.listRuns({}, { limit: 10 });
    await b.store.orphanedRuns(10);
    await b.store.deleteRuns({ before: new Date(Date.now() + 60_000) }, 10);
    expect([...sent].sort()).toEqual([...REQUIRED_IAM_ACTIONS].sort());
  });
});
