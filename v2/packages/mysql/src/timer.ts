import type { Timer, TimerDueOpts } from "@iterativeflow/core/backend";
import type { Tables } from "#schema";
import type { Sql } from "#sql";

/** @internal */
export const createMysqlTimer = (sql: Sql, t: Tables): Timer => {
  return {
    async schedule(runId, fireAt) {
      await sql.exec(
        `INSERT INTO ${t.timer} (run_id, fire_at) VALUES (?, ?)
         ON DUPLICATE KEY UPDATE fire_at = VALUES(fire_at)`,
        [runId, fireAt.getTime()],
      );
    },

    async dueBatch({ now, limit }: TimerDueOpts) {
      const at = (now ?? new Date()).getTime();
      return sql.tx(async (tx) => {
        const due = await tx.query<{ run_id: string }>(
          `SELECT run_id FROM ${t.timer} WHERE fire_at <= ? ORDER BY fire_at LIMIT ? FOR UPDATE SKIP LOCKED`,
          [at, limit],
        );
        if (!due.length) return [];
        const ids = due.map((r) => r.run_id);
        const holes = ids.map(() => "?").join(", ");
        await tx.exec(`DELETE FROM ${t.timer} WHERE run_id IN (${holes})`, ids);
        return ids;
      });
    },

    async cancel(runId) {
      await sql.exec(`DELETE FROM ${t.timer} WHERE run_id = ?`, [runId]);
    },
  };
};
