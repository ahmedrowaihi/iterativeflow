import type { Pool, PoolClient } from "pg";

/**
 * The minimal SQL surface the backend needs: parameterized `query` and a `tx` that runs a
 * unit of work on one connection. Abstracting it keeps the backend driver-agnostic (a Neon
 * HTTP driver or pglite can implement the same two methods) and — more importantly — lets
 * every outbox side-effect run on the SAME connection inside one transaction.
 *
 * Queries are parameterized (`$1`) and unnamed, so nothing pins a prepared statement — safe
 * behind RDS Proxy / PgBouncer transaction pooling.
 */
export interface Sql {
  query<R = Record<string, unknown>>(text: string, params?: readonly unknown[]): Promise<R[]>;
  tx<T>(fn: (t: Sql) => Promise<T>): Promise<T>;
}

const onClient = (c: PoolClient): Sql => ({
  query: (text, params) => c.query(text, params as unknown[]).then((r) => r.rows),
  // Already inside a transaction on this connection — reuse it, don't nest a BEGIN.
  tx: (fn) => fn(onClient(c)),
});

/** Adapt a node-postgres {@link Pool} to {@link Sql}. `tx` checks out one client for the unit. */
export const pgPool = (pool: Pool): Sql => ({
  query: (text, params) => pool.query(text, params as unknown[]).then((r) => r.rows),
  async tx(fn) {
    const c = await pool.connect();
    try {
      await c.query("BEGIN");
      const out = await fn(onClient(c));
      await c.query("COMMIT");
      return out;
    } catch (e) {
      await c.query("ROLLBACK").catch(() => undefined);
      throw e;
    } finally {
      c.release();
    }
  },
});
