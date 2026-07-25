import type { Pool, PoolConnection, ResultSetHeader, RowDataPacket } from "mysql2/promise";

/** What a write reports — MySQL has no `RETURNING`, so first-writer-wins reads `affectedRows`. */
export interface WriteResult {
  affectedRows: number;
  insertId: number;
}

/**
 * The minimal SQL surface the backend needs: positional-`?` `query` for reads, `exec` for writes
 * (returning `affectedRows`, since MySQL has no `RETURNING`), and a `tx` that runs a unit of work on
 * one connection in a transaction. Abstracting it keeps the backend driver-agnostic and runs every
 * outbox side-effect on the SAME connection. Tables are InnoDB, so `SELECT … FOR UPDATE SKIP LOCKED`
 * gives contention-free batch claims.
 */
export interface Sql {
  query<R = Record<string, unknown>>(text: string, params?: readonly unknown[]): Promise<R[]>;
  exec(text: string, params?: readonly unknown[]): Promise<WriteResult>;
  tx<T>(fn: (t: Sql) => Promise<T>): Promise<T>;
}

const write = (r: [unknown, unknown]): WriteResult => {
  const h = r[0] as ResultSetHeader;
  return { affectedRows: h.affectedRows, insertId: h.insertId };
};

const bind = (q: Pool | PoolConnection): Pick<Sql, "query" | "exec"> => ({
  query: <R>(text: string, params?: readonly unknown[]) =>
    q.query(text, params as unknown[]).then((r) => r[0] as RowDataPacket[] as R[]),
  exec: (text, params) => q.query(text, params as unknown[]).then(write),
});

const onConn = (c: PoolConnection): Sql => ({ ...bind(c), tx: (fn) => fn(onConn(c)) });

/** Adapt a `mysql2/promise` {@link Pool} to {@link Sql}. `tx` checks out one connection for the unit. */
export const mysqlPool = (pool: Pool): Sql => ({
  ...bind(pool),
  async tx(fn) {
    const c = await pool.getConnection();
    try {
      await c.beginTransaction();
      const out = await fn(onConn(c));
      await c.commit();
      return out;
    } catch (e) {
      await c.rollback().catch(() => undefined);
      throw e;
    } finally {
      c.release();
    }
  },
});
