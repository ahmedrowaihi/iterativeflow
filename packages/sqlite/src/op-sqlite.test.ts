import { DatabaseSync } from "node:sqlite";
import { backendConformance } from "@iterativeflow/conformance";
import type { Backend } from "@iterativeflow/core/backend";
import { afterEach, describe, expect, it } from "vitest";
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
    if (returnsRows) return { rows: stmt.all(...params) };
    stmt.run(...params);
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

  backendConformance("op-sqlite", () => makeBackend(), { concurrentWriters: false });

  it("serializes overlapping transactions and queries on the one connection", async () => {
    const db = new DatabaseSync(":memory:");
    dbs.push(db);
    const sql = opSqliteDb(nodeSqliteOpDb(db));
    await sql.query("CREATE TABLE t (v TEXT)");
    const failed = sql.tx(async (tx) => {
      await tx.query("INSERT INTO t VALUES ('a')");
      await new Promise((r) => setTimeout(r, 10));
      throw new Error("roll back a");
    });
    const seen = sql.query("SELECT count(*) AS n FROM t");
    const committed = sql.tx((tx) => tx.query("INSERT INTO t VALUES ('b')"));
    await expect(failed).rejects.toThrow("roll back a");
    expect(await seen).toEqual([{ n: 0 }]);
    await committed;
    expect(await sql.query("SELECT v FROM t")).toEqual([{ v: "b" }]);
  });
});
