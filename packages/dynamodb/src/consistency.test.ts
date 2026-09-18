import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { createDynamoBackend, docClient } from "@iterativeflow/dynamodb";
import { describe, expect, it } from "vitest";

// DynamoDB Local is ALWAYS strongly consistent, so no container test can catch a read that forgot
// `ConsistentRead`. These spy-based assertions pin the invariant: reads on the durable decision
// path (loadRun's replay Query, base-table Gets) are strongly consistent; GSI reads (which cannot
// be) are not. Drop a ConsistentRead and one of these fails even though every e2e still passes.

interface Captured {
  name: string | undefined;
  input: object;
}

// Records each command, then fails it before it leaves the process: only the first read matters.
const spy = () => {
  const commands: Captured[] = [];
  const doc = docClient(
    new DynamoDBClient({
      region: "us-east-1",
      credentials: { accessKeyId: "spy", secretAccessKey: "spy" },
    }),
  );
  doc.middlewareStack.add(
    (_next, context) => async (args) => {
      commands.push({ name: context.commandName, input: args.input });
      throw new Error("spy: not sent");
    },
    { step: "initialize", name: "spy" },
  );
  return { doc, commands };
};

describe("dynamodb read consistency", () => {
  it("loadRun replays with a strongly-consistent Query", async () => {
    const { doc, commands } = spy();
    const backend = createDynamoBackend(doc, { table: "t" });
    await expect(backend.store.loadRun("run-1")).rejects.toThrow("spy");
    const query = commands.find((c) => c.name === "QueryCommand");
    expect(query?.input).not.toHaveProperty("IndexName"); // base table, not a GSI
    expect(query?.input).toHaveProperty("ConsistentRead", true);
  });

  it("claim reads the JOB partition off the GSI, which cannot be strongly consistent", async () => {
    const { doc, commands } = spy();
    const backend = createDynamoBackend(doc, { table: "t" });
    await expect(
      backend.queue.claim({ limit: 1, leaseMs: 1000, now: new Date("2030-01-01T00:00:00Z") }),
    ).rejects.toThrow("spy");
    const query = commands.find((c) => c.name === "QueryCommand");
    expect(query?.input).toHaveProperty("IndexName", "gsi1");
    expect(query?.input).not.toHaveProperty("ConsistentRead"); // GSI: eventually consistent, CAS-guarded
  });
});
