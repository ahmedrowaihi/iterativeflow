import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { sql } from "drizzle-orm";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { silentLogger } from "../engine/test-helpers";
import type { WorkflowDb } from "./db";

const REPO_ROOT = join(fileURLToPath(import.meta.url), "..", "..", "..");
const MIGRATION = readFileSync(join(REPO_ROOT, "migrations", "0000_init.sql"), "utf8");

const apply = async (db: WorkflowDb): Promise<void> => {
  const statements = MIGRATION.split("--> statement-breakpoint")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  for (const stmt of statements) {
    await db.execute(sql.raw(stmt));
  }
};

describe("bundled migrations/0000_init.sql", () => {
  let client: PGlite;
  let db: WorkflowDb;

  beforeEach(async () => {
    client = new PGlite();
    await client.waitReady;
    db = drizzle({ client }) as unknown as WorkflowDb;
  });
  afterEach(async () => {
    await client.close();
  });

  it("creates the workflow schema with all expected tables", async () => {
    await apply(db);
    const tables = (await db.execute(
      sql`SELECT table_name FROM information_schema.tables WHERE table_schema = 'workflow' ORDER BY table_name`,
    )) as unknown as { rows: { table_name: string }[] };
    expect(tables.rows.map((r) => r.table_name)).toEqual([
      "events",
      "runs",
      "signals",
      "steps",
      "timers",
    ]);
  });

  it("is idempotent — applying twice is a no-op", async () => {
    await apply(db);
    await expect(apply(db)).resolves.not.toThrow();
    const indexes = (await db.execute(
      sql`SELECT indexname FROM pg_indexes WHERE schemaname = 'workflow' ORDER BY indexname`,
    )) as unknown as { rows: { indexname: string }[] };
    expect(indexes.rows.length).toBeGreaterThanOrEqual(7);
  });

  it("matches the runtime schema fingerprint at v2", async () => {
    await apply(db);
    const { createDrizzleStorage } = await import("./drizzle");
    const storage = createDrizzleStorage({
      db,
      enqueue: async () => undefined,
      logger: silentLogger,
    });
    expect(await storage.getSchemaVersion()).toBe(2);
  });
});
