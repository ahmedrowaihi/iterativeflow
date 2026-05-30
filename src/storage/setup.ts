import { readFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { sql } from "drizzle-orm";
import { locateShippedAsset } from "../util/asset-locate";
import type { WorkflowDb } from "./db";

const here = dirname(fileURLToPath(import.meta.url));

const splitStatements = (source: string): string[] =>
  source
    .split("--> statement-breakpoint")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

/**
 * Apply the `workflow.*` schema by executing the bundled migration.
 * Idempotent: the SQL uses `IF NOT EXISTS` everywhere.
 */
export const applyFlowSchema = async (db: WorkflowDb): Promise<void> => {
  const source = readFileSync(
    locateShippedAsset(here, "../migrations/0000_init.sql", "../../migrations/0000_init.sql"),
    "utf8",
  );
  for (const stmt of splitStatements(source)) {
    await db.execute(sql.raw(stmt));
  }
};

/** Drop the entire `workflow` schema. Destructive — meant for test teardown. */
export const dropFlowSchema = async (db: WorkflowDb): Promise<void> => {
  await db.execute(sql`DROP SCHEMA IF EXISTS "workflow" CASCADE`);
};
