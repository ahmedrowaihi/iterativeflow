import type { Client, Transaction } from "@libsql/client";

/** A value bound to a positional `?`. `undefined` binds as NULL. */
export type SqlParam = string | number | null | undefined;

/** A value as it reaches the driver, after `undefined` became NULL. */
export type SqlBinding = Exclude<SqlParam, undefined>;

/** A column value as any supported SQLite driver (libsql, op-sqlite, Durable Objects) returns it. */
export type SqlValue = string | number | bigint | boolean | null | ArrayBuffer | ArrayBufferView;

/** One result row, keyed by column name. */
export type SqlRow = Readonly<Record<string, SqlValue>>;

/**
 * The minimal SQL surface the backend needs: positional-`?` `query` and a `tx` that runs a unit of
 * work atomically. Abstracting it keeps the backend driver-agnostic — a local file, Turso, or a
 * Cloudflare Durable Object's SQLite storage can each implement these two methods — and, more
 * importantly, runs every outbox side-effect inside one transaction.
 */
export interface Sql {
  query(text: string, params?: readonly SqlParam[]): Promise<SqlRow[]>;
  tx<T>(fn: (t: Sql) => Promise<T>): Promise<T>;
}

/** SQLite bindings reject `undefined`; map it to NULL. Shared by every {@link Sql} driver adapter. */
export const mapParams = (params: readonly SqlParam[] = []): SqlBinding[] =>
  params.map((p) => p ?? null);

const onTx = (t: Transaction): Sql => ({
  query: async (text, params) => (await t.execute({ sql: text, args: mapParams(params) })).rows,
  tx: (fn) => fn(onTx(t)),
});

/** Adapt a `@libsql/client` {@link Client} to {@link Sql}. `tx` opens one write transaction. */
export const libsqlDb = (client: Client): Sql => ({
  query: async (text, params) =>
    (await client.execute({ sql: text, args: mapParams(params) })).rows,
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
