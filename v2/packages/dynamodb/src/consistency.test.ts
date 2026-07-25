import { createDynamoBackend } from "@iterativeflow/dynamodb";
import { describe, expect, it } from "vitest";
import type { Doc } from "#client";

// DynamoDB Local is ALWAYS strongly consistent, so no container test can catch a read that forgot
// `ConsistentRead`. These spy-based assertions pin the invariant: reads on the durable decision
// path (loadRun's replay Query, base-table Gets) are strongly consistent; GSI reads (which cannot
// be) are not. Drop a ConsistentRead and one of these fails even though every e2e still passes.

interface Captured {
  name: string;
  input: Record<string, unknown>;
}

const spy = (): { doc: Doc; commands: Captured[] } => {
  const commands: Captured[] = [];
  const doc: Doc = {
    send: (cmd: unknown) => {
      const c = cmd as { constructor: { name: string }; input: Record<string, unknown> };
      commands.push({ name: c.constructor.name, input: c.input });
      return Promise.resolve({ Items: [], Item: undefined });
    },
  };
  return { doc, commands };
};

describe("dynamodb read consistency", () => {
  it("loadRun replays with a strongly-consistent Query", async () => {
    const { doc, commands } = spy();
    const backend = createDynamoBackend(doc, { table: "t" });
    await backend.store.loadRun("run-1");
    const query = commands.find((c) => c.name === "QueryCommand");
    expect(query?.input.IndexName).toBeUndefined(); // base table, not a GSI
    expect(query?.input.ConsistentRead).toBe(true);
  });

  it("claim reads the JOB partition off the GSI, which cannot be strongly consistent", async () => {
    const { doc, commands } = spy();
    const backend = createDynamoBackend(doc, { table: "t" });
    await backend.queue.claim({ limit: 1, leaseMs: 1000, now: new Date("2030-01-01T00:00:00Z") });
    const query = commands.find((c) => c.name === "QueryCommand");
    expect(query?.input.IndexName).toBe("gsi1");
    expect(query?.input.ConsistentRead).toBeUndefined(); // GSI: eventually consistent, CAS-guarded
  });
});
