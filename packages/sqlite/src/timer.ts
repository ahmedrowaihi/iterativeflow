import type { Timer, TimerDueOpts } from "@iterativeflow/core/backend";
import { int, optInt, text } from "#codec";
import type { Tables } from "#schema";
import type { Sql } from "#sql";
import { inList, scheduleStmt } from "#statements";

/** @internal */
export const createSqliteTimer = (sql: Sql, t: Tables): Timer => {
  return {
    async schedule(runId, fireAt) {
      await scheduleStmt(sql, t, runId, fireAt);
    },

    async dueBatch({ now, limit }: TimerDueOpts) {
      const at = (now ?? new Date()).getTime();
      return sql.tx(async (tx) => {
        const due = await tx.query(
          `SELECT run_id FROM ${t.timer} WHERE fire_at <= ? ORDER BY fire_at LIMIT ?`,
          [at, limit],
        );
        if (!due.length) return [];
        const ids = due.map((r) => text(r, "run_id"));
        await tx.query(`DELETE FROM ${t.timer} WHERE run_id IN ${inList(ids.length)}`, ids);
        return ids;
      });
    },

    async dueCount(now, names) {
      if (names && names.length === 0) return 0;
      const filter = names ? ` AND (r.name IS NULL OR r.name IN ${inList(names.length)})` : "";
      const rows = await sql.query(
        `SELECT count(*) AS n FROM ${t.timer} tm LEFT JOIN ${t.run} r ON r.id = tm.run_id
         WHERE tm.fire_at <= ?${filter}`,
        [now.getTime(), ...(names ?? [])],
      );
      return int(rows[0], "n");
    },

    async cancel(runId) {
      await sql.query(`DELETE FROM ${t.timer} WHERE run_id = ?`, [runId]);
    },

    async nextDueAt(now) {
      const rows = await sql.query(
        `SELECT min(fire_at) AS fire_at FROM ${t.timer} WHERE fire_at > ?`,
        [now.getTime()],
      );
      const at = optInt(rows[0], "fire_at");
      return at === undefined ? null : new Date(at);
    },
  };
};
