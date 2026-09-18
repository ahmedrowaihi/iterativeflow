import {
  type EnqueueOpts,
  type EnqueueRequest,
  type Outbox,
  distinctEnqueues,
} from "@iterativeflow/core/backend";
import { j } from "#codec";
import type { Tables } from "#schema";
import type { Sql } from "#sql";

/** @internal */
export const enqueueManyStmt = (
  sql: Sql,
  t: Tables,
  requests: readonly EnqueueRequest[],
): Promise<unknown> => {
  const rows = distinctEnqueues(requests);
  if (rows.length === 0) return Promise.resolve();
  return sql.query(
    `INSERT INTO ${t.job} AS j (run_id, run_at, priority, version)
     SELECT e.run_id, COALESCE(e.run_at, 'epoch'::timestamptz), e.priority, 1
       FROM unnest($1::text[], $2::timestamptz[], $3::int[]) AS e(run_id, run_at, priority)
     ON CONFLICT (run_id) DO UPDATE
       SET run_at = EXCLUDED.run_at, priority = EXCLUDED.priority, version = j.version + 1`,
    [
      rows.map(([runId]) => runId),
      rows.map(([, opts]) => opts?.runAt ?? null),
      rows.map(([, opts]) => opts?.priority ?? 0),
    ],
  );
};

/** @internal */
export const enqueueStmt = (
  sql: Sql,
  t: Tables,
  runId: string,
  opts?: EnqueueOpts,
): Promise<unknown> => enqueueManyStmt(sql, t, [{ runId, opts }]);

/** @internal */
export const scheduleStmt = (sql: Sql, t: Tables, runId: string, fireAt: Date): Promise<unknown> =>
  sql.query(
    `INSERT INTO ${t.timer} (run_id, fire_at) VALUES ($1, $2)
     ON CONFLICT (run_id) DO UPDATE SET fire_at = EXCLUDED.fire_at`,
    [runId, fireAt],
  );

/** @internal */
export const applyOutbox = async (sql: Sql, t: Tables, fx: Outbox): Promise<void> => {
  for (const s of fx.spawn ?? []) {
    // Insert-by-id is first-writer-wins: a replayed spawn is a no-op, so the child is created once.
    await sql.query(
      `INSERT INTO ${t.run}
         (id, name, version, status, input, idempotency_key, tags, parent_run_id, parent_cursor_key, depth, created_at)
       VALUES ($1, $2, $3, 'pending', $4::jsonb, $5, $6, $7, $8, $9, $10)
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
        s.spec.depth ?? 0,
        s.spec.createdAt ?? new Date(),
      ],
    );
  }
  const enqueues = [
    ...(fx.spawn ?? []).map((s) => ({ runId: s.runId, opts: s.enqueue })),
    ...(fx.enqueue ?? []),
  ];
  if (enqueues.length) await enqueueManyStmt(sql, t, enqueues);
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
