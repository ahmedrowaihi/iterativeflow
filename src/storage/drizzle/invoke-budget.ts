import { sql } from "drizzle-orm";
import { rowsOf, type StorageSliceDeps } from "./types";

/**
 * Compute `{depth, childCount}` for a run in one round-trip via a recursive
 * CTE. Used by `ctx.invoke` to enforce `maxInvokeDepth` /
 * `maxChildrenPerRun`.
 *
 * @internal
 */
export const invokeBudget =
  ({ db, tables }: StorageSliceDeps) =>
  async (runId: string): Promise<{ depth: number; childCount: number }> => {
    const { runs } = tables;
    const rows = rowsOf<{ depth: number; child_count: number }>(
      await db.execute(sql`
        WITH RECURSIVE chain AS (
          SELECT id, parent_run_id, 1 AS depth FROM ${runs} WHERE id = ${runId}::uuid
          UNION ALL
          SELECT r.id, r.parent_run_id, c.depth + 1
          FROM ${runs} r JOIN chain c ON r.id = c.parent_run_id
        )
        SELECT
          COALESCE(MAX(depth), 1)::int AS depth,
          (SELECT COUNT(*)::int FROM ${runs} WHERE parent_run_id = ${runId}::uuid) AS child_count
        FROM chain
      `),
    );
    const row = rows[0];
    return { depth: row?.depth ?? 1, childCount: row?.child_count ?? 0 };
  };
