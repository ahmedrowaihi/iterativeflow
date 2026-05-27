import { sql } from "drizzle-orm";
import { generateDrizzleJson, generateMigration } from "drizzle-kit/api";
import type { WorkflowDb } from "./db";
import * as schema from "./schema";

const buildCreateSql = async (): Promise<string[]> => {
  const empty = generateDrizzleJson({});
  const next = generateDrizzleJson(schema);
  return generateMigration(empty, next);
};

export const applyWorkflowSchema = async (db: WorkflowDb): Promise<void> => {
  const statements = await buildCreateSql();
  for (const stmt of statements) {
    await db.execute(sql.raw(stmt));
  }
};

export const dropWorkflowSchema = async (db: WorkflowDb): Promise<void> => {
  await db.execute(sql`DROP SCHEMA IF EXISTS "workflow" CASCADE`);
};
