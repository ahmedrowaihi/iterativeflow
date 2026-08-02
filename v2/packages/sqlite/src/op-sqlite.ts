import type { Backend } from "@iterativeflow/core/backend";
import { type SqliteBackendOpts, createSqliteBackend } from "#backend";
import { type Sql, mapParams } from "#sql";

interface OpSqliteResult {
  rows: Record<string, unknown>[];
}

/**
 * The minimal op-sqlite database surface this adapter uses — declared structurally so the package
 * needs no dependency on `@op-engineering/op-sqlite` (or its types). `execute` is sync on native
 * (JSI) and async on web, so it may return the result or a promise of it; the adapter awaits either.
 */
export interface OpSqliteDB {
  execute(sql: string, params?: unknown[]): OpSqliteResult | Promise<OpSqliteResult>;
}

const BUSY_RETRIES = 5;
const BUSY_BASE_MS = 10;
const BUSY_CAP_MS = 200;

const isBusy = (e: unknown): boolean =>
  /\bSQLITE_BUSY\b|database (?:is|table is) locked/i.test(
    e instanceof Error ? e.message : String(e),
  );

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Adapt an [op-sqlite](https://op-engineering.github.io/op-sqlite) database to the sqlite backend's
 * {@link Sql}. One code path runs on React Native (native SQLite over JSI) AND the browser (op-sqlite
 * web, wasm + OPFS), since it only uses op-sqlite's async-safe `execute`.
 *
 * `tx` drives `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK` itself rather than op-sqlite's `transaction()`
 * wrapper, so commit-on-resolve and rollback-on-throw are deterministic, and it retries the BEGIN on
 * `SQLITE_BUSY` before surfacing it. A nested `tx` reuses the open transaction (SQLite has no nested
 * `BEGIN`), matching {@link libsqlDb}.
 */
export const opSqliteDb = (db: OpSqliteDB): Sql => {
  const query = async <R = Record<string, unknown>>(
    text: string,
    params?: readonly unknown[],
  ): Promise<R[]> => (await db.execute(text, mapParams(params))).rows as R[];
  const inFlight: Sql = { query, tx: (fn) => fn(inFlight) };
  return {
    query,
    async tx(fn) {
      for (let attempt = 0; ; attempt++) {
        try {
          await db.execute("BEGIN IMMEDIATE");
          break;
        } catch (e) {
          if (!isBusy(e) || attempt >= BUSY_RETRIES) throw e;
          await sleep(Math.min(BUSY_BASE_MS * 2 ** attempt, BUSY_CAP_MS));
        }
      }
      try {
        const out = await fn(inFlight);
        await db.execute("COMMIT");
        return out;
      } catch (e) {
        try {
          await db.execute("ROLLBACK");
        } catch {
          // the transaction already failed; the rollback is best-effort
        }
        throw e;
      }
    },
  };
};

/** Build the SQLite {@link Backend} directly on an op-sqlite database. Run {@link applySchema} once
 *  (against `opSqliteDb(db)`) before first use. */
export const createOpSqliteBackend = (db: OpSqliteDB, opts?: SqliteBackendOpts): Backend =>
  createSqliteBackend(opSqliteDb(db), opts);
