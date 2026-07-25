import type { EnqueueOpts, Outbox } from "@iterativeflow/core/backend";
import { j } from "#codec";
import type { Tables } from "#schema";
import type { Sql } from "#sql";

const inList = (n: number): string => `(${Array.from({ length: n }, () => "?").join(",")})`;

/** @internal */
export const enqueueStmt = (
  sql: Sql,
  t: Tables,
  runId: string,
  opts?: EnqueueOpts,
): Promise<unknown> =>
  sql.exec(
    `INSERT INTO ${t.job} (run_id, run_at, priority, version) VALUES (?, ?, ?, 1)
     ON DUPLICATE KEY UPDATE run_at = VALUES(run_at), priority = VALUES(priority), version = version + 1`,
    [runId, opts?.runAt ? opts.runAt.getTime() : 0, opts?.priority ?? 0],
  );

/** @internal */
export const scheduleStmt = (sql: Sql, t: Tables, runId: string, fireAt: Date): Promise<unknown> =>
  sql.exec(
    `INSERT INTO ${t.timer} (run_id, fire_at) VALUES (?, ?)
     ON DUPLICATE KEY UPDATE fire_at = VALUES(fire_at)`,
    [runId, fireAt.getTime()],
  );

/** @internal */
export const applyOutbox = async (sql: Sql, t: Tables, fx: Outbox): Promise<void> => {
  for (const s of fx.spawn ?? []) {
    await sql.exec(
      `INSERT IGNORE INTO ${t.run}
         (id, name, version, status, input, idempotency_key, tags, parent_run_id, parent_cursor_key, depth, created_at)
       VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)`,
      [
        s.runId,
        s.spec.name,
        s.spec.version,
        j(s.spec.input),
        s.spec.idempotencyKey ?? null,
        j(s.spec.tags),
        s.spec.parentRunId ?? null,
        s.spec.parentCursorKey ?? null,
        s.spec.depth ?? 0,
        (s.spec.createdAt ?? new Date()).getTime(),
      ],
    );
    await enqueueStmt(sql, t, s.runId, s.enqueue);
  }
  for (const e of fx.enqueue ?? []) await enqueueStmt(sql, t, e.runId, e.opts);
  for (const tm of fx.timers ?? []) await scheduleStmt(sql, t, tm.runId, tm.fireAt);
  if (fx.cancelTimers?.length) {
    await sql.exec(
      `DELETE FROM ${t.timer} WHERE run_id IN ${inList(fx.cancelTimers.length)}`,
      fx.cancelTimers,
    );
  }
  if (fx.consumeSignals?.length) {
    await sql.exec(
      `DELETE FROM ${t.signal} WHERE id IN ${inList(fx.consumeSignals.length)}`,
      fx.consumeSignals,
    );
  }
  if (fx.joinTarget) {
    await sql.exec(`UPDATE ${t.run} SET join_remaining = ? WHERE id = ?`, [
      fx.joinTarget.count,
      fx.joinTarget.runId,
    ]);
  }
};
