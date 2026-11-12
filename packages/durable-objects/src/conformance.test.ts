import { DatabaseSync } from "node:sqlite";
import { backendConformance } from "@iterativeflow/conformance";
import { type Backend } from "@iterativeflow/core";
import { afterEach, describe } from "vitest";
import { type SqlStorage, applySchema, createDurableObjectBackend } from "#index";

/**
 * A `SqlStorage` mock over Node's synchronous `node:sqlite` — same synchronous `exec().toArray()`
 * shape as a Durable Object's `ctx.storage.sql`. This drives the REAL DO adapter through the full
 * conformance suites without the Workers runtime. (A DO serves one request at a time, so the
 * single-writer sequential model the suites exercise matches production.)
 */
const nodeSqliteStorage = (db: DatabaseSync): SqlStorage => ({
  exec: (query, ...bindings) => ({
    toArray: () => db.prepare(query).all(...bindings),
  }),
});

describe("durable-objects backend (SQLite over node:sqlite)", () => {
  const dbs: DatabaseSync[] = [];
  afterEach(() => {
    for (const db of dbs.splice(0)) db.close();
  });

  const makeBackend = async (): Promise<Backend> => {
    const db = new DatabaseSync(":memory:");
    dbs.push(db);
    const storage = nodeSqliteStorage(db);
    await applySchema(storage);
    return createDurableObjectBackend(storage);
  };

  backendConformance("durable-objects", () => makeBackend(), { concurrentWriters: false });
});
