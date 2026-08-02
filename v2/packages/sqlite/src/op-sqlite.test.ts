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
import type { Backend } from "@iterativeflow/core/backend";
import { afterEach, describe } from "vitest";
import { applySchema } from "#schema";
import { type OpSqliteDB, createOpSqliteBackend, opSqliteDb } from "#op-sqlite";

/**
 * An {@link OpSqliteDB} over Node's synchronous `node:sqlite` — the same `execute({ rows })` shape
 * op-sqlite exposes. The real op-sqlite driver is a React Native / web-wasm module that can't build
 * in a plain Node CI, but the adapter (`opSqliteDb`) IS our code, so driving the full sqlite backend
 * + conformance suites through this shim proves the adapter's BEGIN/COMMIT/ROLLBACK, row mapping, and
 * nested-tx reuse under the real contract; op-sqlite only has to honor the `execute` shape emulated
 * here.
 */
const nodeSqliteOpDb = (db: DatabaseSync): OpSqliteDB => ({
  execute(sql, params = []) {
    const stmt = db.prepare(sql);
    const returnsRows = /^\s*(select|with|pragma)\b/i.test(sql) || /\breturning\b/i.test(sql);
    if (returnsRows) return { rows: stmt.all(...(params as never[])) as Record<string, unknown>[] };
    stmt.run(...(params as never[]));
    return { rows: [] };
  },
});

describe("op-sqlite adapter (SQLite over node:sqlite)", () => {
  const dbs: DatabaseSync[] = [];
  afterEach(() => {
    for (const db of dbs.splice(0)) db.close();
  });

  const makeBackend = async (): Promise<Backend> => {
    const db = new DatabaseSync(":memory:");
    dbs.push(db);
    const shim = nodeSqliteOpDb(db);
    await applySchema(opSqliteDb(shim));
    return createOpSqliteBackend(shim);
  };

  storeConformance("op-sqlite", async () => (await makeBackend()).store);
  queueConformance("op-sqlite", async () => (await makeBackend()).queue);
  claimFilterConformance("op-sqlite", () => makeBackend());
  timerConformance("op-sqlite", async () => (await makeBackend()).timer);
  wakeupConformance("op-sqlite", async () => (await makeBackend()).wakeup);
  outboxConformance("op-sqlite", () => makeBackend());
  signalConformance("op-sqlite", () => makeBackend());
  reconcileConformance("op-sqlite", () => makeBackend());
  cronConformance("op-sqlite", async () => (await makeBackend()).store);
  engineConformance("op-sqlite", () => makeBackend());
});
