import {
  type EnqueueOpts,
  type EnqueueRequest,
  type Outbox,
  type RunSpec,
  distinctEnqueues,
} from "@iterativeflow/core/backend";
import { j } from "#codec";
import type { Tables } from "#schema";
import type { Sql, SqlRow } from "#sql";

/** @internal */
export const insertRunStmt = (
  sql: Sql,
  t: Tables,
  runId: string,
  spec: RunSpec,
  onConflict: string,
): Promise<SqlRow[]> =>
  sql.query(
    `INSERT INTO ${t.run}
       (id, name, version, status, input, idempotency_key, tags, parent_run_id, parent_cursor_key, depth, created_at, priority)
     VALUES ($1, $2, $3, 'pending', $4::jsonb, $5, $6, $7, $8, $9, $10, $11)
     ${onConflict}`,
    [
      runId,
      spec.name,
      spec.version,
      j(spec.input),
      spec.idempotencyKey || null,
      spec.tags ?? null,
      spec.parentRunId ?? null,
      spec.parentCursorKey ?? null,
      spec.depth ?? 0,
      spec.createdAt ?? new Date(),
      spec.priority ?? 0,
    ],
  );

/** @internal */
export const enqueueManyStmt = async (
  sql: Sql,
  t: Tables,
  requests: readonly EnqueueRequest[],
): Promise<void> => {
  const rows = distinctEnqueues(requests);
  if (rows.length === 0) return;
  await sql.query(
    `INSERT INTO ${t.job} AS j (run_id, run_at, priority, version)
     SELECT e.run_id, COALESCE(e.run_at, 'epoch'::timestamptz), COALESCE(e.priority, r.priority, 0), 1
       FROM unnest($1::text[], $2::timestamptz[], $3::int[]) AS e(run_id, run_at, priority)
       LEFT JOIN ${t.run} r ON r.id = e.run_id
     ON CONFLICT (run_id) DO UPDATE
       SET run_at = EXCLUDED.run_at, priority = EXCLUDED.priority, version = j.version + 1`,
    [
      rows.map(([runId]) => runId),
      rows.map(([, opts]) => opts?.runAt ?? null),
      rows.map(([, opts]) => opts?.priority ?? null),
    ],
  );
};

/** @internal */
export const enqueueStmt = (
  sql: Sql,
  t: Tables,
  runId: string,
  opts?: EnqueueOpts,
): Promise<void> => enqueueManyStmt(sql, t, [{ runId, opts }]);

/** @internal */
export const scheduleStmt = async (
  sql: Sql,
  t: Tables,
  runId: string,
  fireAt: Date,
): Promise<void> => {
  await sql.query(
    `INSERT INTO ${t.timer} (run_id, fire_at) VALUES ($1, $2)
     ON CONFLICT (run_id) DO UPDATE SET fire_at = EXCLUDED.fire_at`,
    [runId, fireAt],
  );
};

/** @internal */
export const applyOutbox = async (sql: Sql, t: Tables, fx: Outbox): Promise<void> => {
  for (const s of fx.spawn ?? []) {
    // Insert-by-id is first-writer-wins: a replayed spawn is a no-op, so the child is created once.
    await insertRunStmt(sql, t, s.runId, s.spec, "ON CONFLICT (id) DO NOTHING");
  }
  if (fx.spawn?.length)
    await enqueueManyStmt(
      sql,
      t,
      fx.spawn.map((s) => ({ runId: s.runId })),
    );
  for (const tm of fx.timers ?? []) await scheduleStmt(sql, t, tm.runId, tm.fireAt);
  if (fx.cancelTimers?.length) {
    await sql.query(`DELETE FROM ${t.timer} WHERE run_id = ANY($1::text[])`, [fx.cancelTimers]);
  }
  if (fx.consumeSignals?.length) {
    await sql.query(`UPDATE ${t.signal} SET consumed = true WHERE id = ANY($1::text[])`, [
      fx.consumeSignals,
    ]);
  }
  if (fx.joinTarget) {
    await sql.query(`UPDATE ${t.run} SET join_remaining = $2 WHERE id = $1`, [
      fx.joinTarget.runId,
      fx.joinTarget.count,
    ]);
  }
};
