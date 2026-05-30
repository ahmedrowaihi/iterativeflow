import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { sql } from "drizzle-orm";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ts } from "./sql-params";

describe("ts(date)", () => {
  let client: PGlite;
  let db: ReturnType<typeof drizzle>;

  beforeEach(async () => {
    client = new PGlite();
    await client.waitReady;
    db = drizzle({ client });
  });
  afterEach(async () => {
    await client.close();
  });

  it("emits an ISO string + ::timestamptz that Postgres parses to the same instant", async () => {
    const d = new Date("2026-01-15T12:34:56.789Z");
    const result = (await db.execute(sql`SELECT ${ts(d)} AS t`)) as unknown as {
      rows: { t: Date }[];
    };
    expect(new Date(result.rows[0].t).toISOString()).toBe(d.toISOString());
  });

  it("works as a comparison RHS against a timestamptz column", async () => {
    await db.execute(sql`CREATE TABLE x (id int, at timestamptz NOT NULL)`);
    const past = new Date(Date.now() - 60_000);
    const future = new Date(Date.now() + 60_000);
    await db.execute(sql`INSERT INTO x VALUES (1, ${ts(past)}), (2, ${ts(future)})`);
    const result = (await db.execute(
      sql`SELECT id FROM x WHERE at < ${ts(new Date())} ORDER BY id`,
    )) as unknown as { rows: { id: number }[] };
    expect(result.rows.map((r) => r.id)).toEqual([1]);
  });
});
