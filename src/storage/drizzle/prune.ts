import { sql } from "drizzle-orm";
import type { WorkflowDb } from "../db";
import type { RunStatus } from "../schema";

/**
 * Delete event rows older than `olderThan` in batches. Returns the count
 * deleted in this call. Safe to drive from cron.
 *
 * @internal
 */
export const pruneEvents =
  (db: WorkflowDb) =>
  async ({
    olderThan,
    batchSize = 1000,
  }: {
    olderThan: Date;
    batchSize?: number;
  }): Promise<number> => {
    const result = (await db.execute(sql`
      WITH del AS (
        SELECT id FROM workflow.events
        WHERE at < ${olderThan}
        ORDER BY id
        LIMIT ${batchSize}
      )
      DELETE FROM workflow.events
      WHERE id IN (SELECT id FROM del)
      RETURNING id
    `)) as unknown as { rows: unknown[] };
    return result.rows.length;
  };

/**
 * Delete terminal-status run rows updated before `olderThan` in batches.
 * Returns the count deleted in this call.
 *
 * @internal
 */
export const pruneRuns =
  (db: WorkflowDb) =>
  async ({
    olderThan,
    status = ["done", "failed", "canceled"],
    batchSize = 1000,
  }: {
    olderThan: Date;
    status?: ReadonlyArray<RunStatus>;
    batchSize?: number;
  }): Promise<number> => {
    const statusList = sql.join(
      status.map((s) => sql`${s}`),
      sql`, `,
    );
    const result = (await db.execute(sql`
      WITH del AS (
        SELECT id FROM workflow.runs
        WHERE updated_at < ${olderThan}
          AND status IN (${statusList})
        ORDER BY updated_at
        LIMIT ${batchSize}
      )
      DELETE FROM workflow.runs
      WHERE id IN (SELECT id FROM del)
      RETURNING id
    `)) as unknown as { rows: unknown[] };
    return result.rows.length;
  };
