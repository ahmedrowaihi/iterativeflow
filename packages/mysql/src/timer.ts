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

    async dueCount(now, names) {
      if (names && names.length === 0) return 0;
      const namePredicate = names ? ` AND r.name IN (${names.map(() => "?").join(",")})` : "";
      const params = names ? [now.getTime(), ...names] : [now.getTime()];
      const rows = await sql.query<{ n: number | string }>(
        `SELECT count(*) AS n FROM ${t.timer} tm LEFT JOIN ${t.run} r ON r.id = tm.run_id
         WHERE tm.fire_at <= ?${namePredicate}`,
        params,
      );
      return Number(rows[0]?.n ?? 0);
    },

    async cancel(runId) {
      await sql.exec(`DELETE FROM ${t.timer} WHERE run_id = ?`, [runId]);
    },

    async nextDueAt(now) {
      const rows = await sql.query<{ fire_at: number | null }>(
        `SELECT min(fire_at) AS fire_at FROM ${t.timer} WHERE fire_at > ?`,
        [now.getTime()],
      );
      const at = rows[0]?.fire_at;
      return at == null ? null : new Date(Number(at));
    },
  };
};
