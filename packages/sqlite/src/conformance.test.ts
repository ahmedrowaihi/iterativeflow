import { backendConformance } from "@iterativeflow/conformance";
import { type Backend, defineFlow, submit } from "@iterativeflow/core";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "@libsql/client";
import { afterEach, describe, expect, it } from "vitest";
import { createSqliteBackend } from "#backend";
import { applySchema } from "#schema";
import { libsqlDb } from "#sql";
import { inTx } from "#tx";

describe("sqlite backend", () => {
  const cleanups: (() => void)[] = [];

  afterEach(() => {
    for (const c of cleanups.splice(0)) c();
  });

  // A fresh temp-file database per test — a file (not `:memory:`) so the write transaction's
  // connection shares it. No container needed.
  const makeBackend = async (): Promise<Backend> => {
    const dir = mkdtempSync(join(tmpdir(), "iflow-sqlite-"));
    const client = createClient({ url: `file:${join(dir, "test.db")}` });
    cleanups.push(() => {
      client.close();
      rmSync(dir, { recursive: true, force: true });
    });
    const sql = libsqlDb(client);
    await applySchema(sql);
    return createSqliteBackend(sql);
  };

  backendConformance("sqlite", () => makeBackend(), { concurrentWriters: false });

  it("transactional enqueue: caller's write + submit commit atomically (and roll back together)", async () => {
    const dir = mkdtempSync(join(tmpdir(), "iflow-sqlite-tx-"));
    const client = createClient({ url: `file:${join(dir, "tx.db")}` });
    cleanups.push(() => {
      client.close();
      rmSync(dir, { recursive: true, force: true });
    });
    const sql = libsqlDb(client);
    await applySchema(sql);
    await sql.query("CREATE TABLE app_orders (id TEXT PRIMARY KEY)");
    const backend = createSqliteBackend(sql);
    const flow = defineFlow<Record<string, never>, number>({
      name: "fulfil",
      version: 1,
      run: async () => 1,
    });
    const orderCount = async (): Promise<number> => {
      const rows = await sql.query("SELECT count(*) AS n FROM app_orders");
      return Number(rows[0].n);
    };
    const runCount = async (): Promise<number> =>
      (await backend.store.listRuns({}, { limit: 10 })).runs.length;

    // Rollback: a throw after submit persists neither the order nor the run/job.
    await expect(
      inTx(client, async (b, tx) => {
        await tx.query("INSERT INTO app_orders (id) VALUES ('o1')");
        await submit(b, flow, {});
        throw new Error("boom after submit");
      }),
    ).rejects.toThrow();
    expect(await orderCount()).toBe(0);
    expect(await runCount()).toBe(0);

    // Commit: the order AND the enqueued run land together.
    const id = await inTx(client, async (b, tx) => {
      await tx.query("INSERT INTO app_orders (id) VALUES ('o2')");
      return submit(b, flow, {});
    });
    expect(await orderCount()).toBe(1);
    expect(await backend.store.loadRunRow(id)).toBeDefined();
  });

  it("upgrades a database from an older release in place, keeping its runs", async () => {
    const dir = mkdtempSync(join(tmpdir(), "iflow-sqlite-"));
    const client = createClient({ url: `file:${join(dir, "test.db")}` });
    cleanups.push(() => {
      client.close();
      rmSync(dir, { recursive: true, force: true });
    });
    const sql = libsqlDb(client);
    await applySchema(sql);
    const { runId } = await createSqliteBackend(sql).store.startRun({
      name: "old",
      version: 1,
      input: {},
    });
    await sql.query("ALTER TABLE run DROP COLUMN priority");

    await applySchema(sql);
    await applySchema(sql);

    const backend = createSqliteBackend(sql);
    await backend.queue.enqueue(runId);
    const [lease] = await backend.queue.claim({ limit: 1, leaseMs: 60_000 });
    expect(lease.runId).toBe(runId);
  });
});
