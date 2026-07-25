import type { Client, InArgs, Transaction } from "@libsql/client";

/**
 * The minimal SQL surface the backend needs: positional-`?` `query` and a `tx` that runs a unit of
 * work atomically. Abstracting it keeps the backend driver-agnostic — a local file, Turso, or a
 * Cloudflare Durable Object's SQLite storage can each implement these two methods — and, more
 * importantly, runs every outbox side-effect inside one transaction.
 */
export interface Sql {
  query<R = Record<string, unknown>>(text: string, params?: readonly unknown[]): Promise<R[]>;
  tx<T>(fn: (t: Sql) => Promise<T>): Promise<T>;
}

const args = (params?: readonly unknown[]): InArgs =>
  (params ?? []).map((p) => (p === undefined ? null : p)) as InArgs;

const onTx = (t: Transaction): Sql => ({
  query: (text, params) =>
    t.execute({ sql: text, args: args(params) }).then((r) => r.rows as unknown[] as never),
  tx: (fn) => fn(onTx(t)),
});

/** Adapt a `@libsql/client` {@link Client} to {@link Sql}. `tx` opens one write transaction. */
export const libsqlDb = (client: Client): Sql => ({
  query: (text, params) =>
    client.execute({ sql: text, args: args(params) }).then((r) => r.rows as unknown[] as never),
  async tx(fn) {
    const t = await client.transaction("write");
    try {
      const out = await fn(onTx(t));
      await t.commit();
      return out;
    } catch (e) {
      await t.rollback().catch(() => undefined);
      throw e;
    }
  },
});
