import { type Sql, type SqlBinding, type SqlRow, mapParams } from "@iterativeflow/sqlite";

/** The subset of Cloudflare's `SqlStorage` (`ctx.storage.sql`) the adapter uses — structural, so no
 *  `@cloudflare/workers-types` dependency is required. */
export interface SqlStorage {
  exec(query: string, ...bindings: SqlBinding[]): { toArray(): SqlRow[] };
}

/**
 * Adapt a Durable Object's SQLite storage (`ctx.storage.sql`) to the SQLite backend's {@link Sql}
 * seam, so the whole `@iterativeflow/sqlite` backend runs inside a DO.
 *
 * `sql.exec` is synchronous and a DO serves one request at a time, so a Store method's `tx` runs to
 * completion without another request interleaving; the DO commits an invocation's writes on return
 * and rolls them back only on an UNCAUGHT throw. DO SQLite forbids a manual `BEGIN`/`COMMIT`, so `tx`
 * can't open an explicit transaction. This is single-writer-safe and correct on the happy path; the
 * outbox's all-or-nothing guarantee under a *caught* mid-write error is weaker than the other
 * backends' explicit transactions — pending a synchronous-Store / `transactionSync` path.
 */
export const doStorageSql = (storage: SqlStorage): Sql => {
  const self: Sql = {
    query: async (text, params = []) => storage.exec(text, ...mapParams(params)).toArray(),
    tx: (fn) => fn(self),
  };
  return self;
};
