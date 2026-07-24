import type { EnqueueOpts, Outbox } from "@iterativeflow/core/backend";
import { j } from "#codec";
import type { Tables } from "#schema";
import type { Sql } from "#sql";

/** @internal */
export const enqueueStmt = (
  sql: Sql,
  t: Tables,
  runId: string,
  opts?: EnqueueOpts,
): Promise<unknown> =>
  sql.query(
    `INSERT INTO ${t.job} AS j (run_id, run_at, priority)
     VALUES ($1, COALESCE($2::timestamptz, 'epoch'::timestamptz), $3)
     ON CONFLICT (run_id) DO UPDATE
       SET run_at = EXCLUDED.run_at, priority = EXCLUDED.priority, version = j.version + 1`,
    [runId, opts?.runAt ?? null, opts?.priority ?? 0],
  );

/** @internal */
export const scheduleStmt = (sql: Sql, t: Tables, runId: string, fireAt: Date): Promise<unknown> =>
  sql.query(
    `INSERT INTO ${t.timer} (run_id, fire_at) VALUES ($1, $2)
     ON CONFLICT (run_id) DO UPDATE SET fire_at = EXCLUDED.fire_at`,
    [runId, fireAt],
  );

/** @internal */
export const applyOutbox = async (
  sql: Sql,
  t: Tables,
  fx: Outbox,
  opRunId?: string,
): Promise<void> => {
  for (const s of fx.spawn ?? []) {
    // Insert-by-id is first-writer-wins: a replayed spawn is a no-op, so the child is created once.
    await sql.query(
      `INSERT INTO ${t.run}
         (id, name, version, status, input, idempotency_key, tags, parent_run_id, parent_cursor_key)
       VALUES ($1, $2, $3, 'pending', $4::jsonb, $5, $6, $7, $8)
       ON CONFLICT (id) DO NOTHING`,
      [
        s.runId,
        s.spec.name,
        s.spec.version,
        j(s.spec.input),
        s.spec.idempotencyKey ?? null,
        s.spec.tags ?? null,
        s.spec.parentRunId ?? null,
        s.spec.parentCursorKey ?? null,
      ],
    );
    await enqueueStmt(sql, t, s.runId, s.enqueue);
  }
  for (const e of fx.enqueue ?? []) await enqueueStmt(sql, t, e.runId, e.opts);
  for (const tm of fx.timers ?? []) await scheduleStmt(sql, t, tm.runId, tm.fireAt);
  if (fx.cancelTimers?.length) {
    await sql.query(`DELETE FROM ${t.timer} WHERE run_id = ANY($1::text[])`, [fx.cancelTimers]);
  }
  if (fx.consumeSignals?.length) {
    await sql.query(`DELETE FROM ${t.signal} WHERE id = ANY($1::text[])`, [fx.consumeSignals]);
  }
  if (fx.joinTarget !== undefined && opRunId) {
    await sql.query(`UPDATE ${t.run} SET join_remaining = $2 WHERE id = $1`, [
      opRunId,
      fx.joinTarget,
    ]);
  }
  if (fx.joinArrive) {
    const { parentRunId, wakeAlways } = fx.joinArrive;
    const rows = await sql.query<{ join_remaining: number }>(
      `UPDATE ${t.run} SET join_remaining = join_remaining - 1 WHERE id = $1 RETURNING join_remaining`,
      [parentRunId],
    );
    if (wakeAlways || (rows[0] && rows[0].join_remaining <= 0))
      await enqueueStmt(sql, t, parentRunId);
  }
};
