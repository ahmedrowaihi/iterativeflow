import { sql } from "drizzle-orm";
import type { WorkflowDb } from "../db";

/**
 * Detect the engine-expected schema version by probing for marker columns
 * via `information_schema`. Returns `0` when the workflow schema is absent,
 * `1` for the pre-v2 layout, `2` when the v2 markers are present.
 *
 * @internal
 */
export const getSchemaVersion = (db: WorkflowDb) => async (): Promise<number> => {
  const result = (await db.execute(sql`
    SELECT
      (SELECT EXISTS(
        SELECT 1 FROM information_schema.schemata WHERE schema_name = 'workflow'
      )) AS has_schema,
      (SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'workflow' AND table_name = 'runs' AND column_name = 'parent_run_id'
      )) AS has_parent,
      (SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'workflow' AND table_name = 'runs' AND column_name = 'tags'
      )) AS has_tags,
      (SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'workflow' AND table_name = 'signals'
      )) AS has_signals
  `)) as unknown as {
    rows: {
      has_schema: boolean;
      has_parent: boolean;
      has_tags: boolean;
      has_signals: boolean;
    }[];
  };
  const row = result.rows[0];
  if (!row?.has_schema) return 0;
  if (row.has_parent && row.has_tags && row.has_signals) return 2;
  return 1;
};
