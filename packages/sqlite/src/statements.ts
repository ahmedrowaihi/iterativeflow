import {
  type EnqueueOpts,
  type EnqueueRequest,
  type Outbox,
  distinctEnqueues,
} from "@iterativeflow/core/backend";
import { j } from "#codec";
import type { Tables } from "#schema";
import type { Sql } from "#sql";

const inList = (n: number): string => `(${Array.from({ length: n }, () => "?").join(",")})`;

// 3 binds per row, under the oldest SQLITE_MAX_VARIABLE_NUMBER (999).
const ENQUEUE_ROWS_PER_STATEMENT = 300;

/** @internal */
export const enqueueManyStmt = async (
  sql: Sql,
  t: Tables,
  requests: readonly EnqueueRequest[],
): Promise<void> => {
  const rows = distinctEnqueues(requests);
  for (let i = 0; i < rows.length; i += ENQUEUE_ROWS_PER_STATEMENT) {
    const chunk = rows.slice(i, i + ENQUEUE_ROWS_PER_STATEMENT);
    await sql.query(
      `INSERT INTO ${t.job} (run_id, run_at, priority, version)
       VALUES ${chunk.map(() => "(?, ?, ?, 1)").join(", ")}
       ON CONFLICT(run_id) DO UPDATE
         SET run_at = excluded.run_at, priority = excluded.priority, version = ${t.job}.version + 1`,
      chunk.flatMap(([runId, opts]) => [
        runId,
        opts?.runAt ? opts.runAt.getTime() : 0,
        opts?.priority ?? 0,
      ]),
    );
  }
};

/** @internal */
export const enqueueStmt = (
  sql: Sql,
  t: Tables,
  runId: string,
  opts?: EnqueueOpts,
): Promise<void> => enqueueManyStmt(sql, t, [{ runId, opts }]);

/** @internal */
export const scheduleStmt = (sql: Sql, t: Tables, runId: string, fireAt: Date): Promise<unknown> =>
  sql.query(
    `INSERT INTO ${t.timer} (run_id, fire_at) VALUES (?, ?)
     ON CONFLICT(run_id) DO UPDATE SET fire_at = excluded.fire_at`,
    [runId, fireAt.getTime()],
  );

/** @internal */
export const applyOutbox = async (sql: Sql, t: Tables, fx: Outbox): Promise<void> => {
  for (const s of fx.spawn ?? []) {
    await sql.query(
      `INSERT INTO ${t.run}
         (id, name, version, status, input, idempotency_key, tags, parent_run_id, parent_cursor_key, depth, created_at)
       VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(id) DO NOTHING`,
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
  }
  const enqueues = [
    ...(fx.spawn ?? []).map((s) => ({ runId: s.runId, opts: s.enqueue })),
    ...(fx.enqueue ?? []),
  ];
  if (enqueues.length) await enqueueManyStmt(sql, t, enqueues);
  for (const tm of fx.timers ?? []) await scheduleStmt(sql, t, tm.runId, tm.fireAt);
  if (fx.cancelTimers?.length) {
    await sql.query(
      `DELETE FROM ${t.timer} WHERE run_id IN ${inList(fx.cancelTimers.length)}`,
      fx.cancelTimers,
    );
  }
  if (fx.consumeSignals?.length) {
    await sql.query(
      `UPDATE ${t.signal} SET consumed = 1 WHERE id IN ${inList(fx.consumeSignals.length)}`,
      fx.consumeSignals,
    );
  }
  if (fx.joinTarget) {
    await sql.query(`UPDATE ${t.run} SET join_remaining = ? WHERE id = ?`, [
      fx.joinTarget.count,
      fx.joinTarget.runId,
    ]);
  }
};
