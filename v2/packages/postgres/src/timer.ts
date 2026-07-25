import type { Timer, TimerDueOpts } from "@iterativeflow/core/backend";
import { type Tables, tables } from "#schema";
import { scheduleStmt } from "#statements";
import type { Sql } from "#sql";

/** @internal */
export const createPgTimer = (sql: Sql, schema: string): Timer => {
  const t: Tables = tables(schema);

  return {
    async schedule(runId, fireAt) {
      await scheduleStmt(sql, t, runId, fireAt);
    },

    async dueBatch({ now, limit }: TimerDueOpts) {
      // Select-order-delete-return: fire-once (the DELETE consumes them) AND earliest-first
      // (the final SELECT re-imposes fire_at order, which DELETE ... RETURNING would not).
      const rows = await sql.query<{ run_id: string }>(
        `WITH due AS (
           SELECT run_id, fire_at FROM ${t.timer}
           WHERE fire_at <= $1::timestamptz
           ORDER BY fire_at
           LIMIT $2
           FOR UPDATE SKIP LOCKED
         ), consumed AS (
           DELETE FROM ${t.timer} WHERE run_id IN (SELECT run_id FROM due)
         )
         SELECT run_id FROM due ORDER BY fire_at`,
        [now ?? new Date(), limit],
      );
      return rows.map((r) => r.run_id);
    },

    async cancel(runId) {
      await sql.query(`DELETE FROM ${t.timer} WHERE run_id = $1`, [runId]);
    },
  };
};
