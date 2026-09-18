import {
  type EnqueueOpts,
  type EnqueueRequest,
  type Outbox,
  distinctEnqueues,
} from "@iterativeflow/core/backend";
import { j } from "#codec";
import type { Tables } from "#schema";
import type { Sql, WriteResult } from "#sql";

// 4 binds per row, under MySQL's 65535 placeholder ceiling.
const ENQUEUE_ROWS_PER_STATEMENT = 1000;

const inList = (n: number): string => `(${Array.from({ length: n }, () => "?").join(",")})`;

/** @internal */
export const enqueueManyStmt = async (
  sql: Sql,
  t: Tables,
  requests: readonly EnqueueRequest[],
): Promise<void> => {
  const rows = distinctEnqueues(requests);
  for (let i = 0; i < rows.length; i += ENQUEUE_ROWS_PER_STATEMENT) {
    const chunk = rows.slice(i, i + ENQUEUE_ROWS_PER_STATEMENT);
    // ON DUPLICATE KEY assigns left to right: version reads the stored column, so never reorder.
    await sql.exec(
      `INSERT INTO ${t.job} (run_id, run_at, priority, version)
       VALUES ${chunk.map(() => `(?, ?, COALESCE(?, (SELECT priority FROM ${t.run} WHERE id = ?), 0), 1)`).join(", ")}
       ON DUPLICATE KEY UPDATE run_at = VALUES(run_at), priority = VALUES(priority), version = version + 1`,
      chunk.flatMap(([runId, opts]) => [
        runId,
        opts?.runAt ? opts.runAt.getTime() : 0,
        opts?.priority ?? null,
        runId,
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
export const scheduleStmt = (
  sql: Sql,
  t: Tables,
  runId: string,
  fireAt: Date,
): Promise<WriteResult> =>
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
         (id, name, version, status, input, idempotency_key, tags, parent_run_id, parent_cursor_key, depth, priority, created_at)
       VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)`,
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
        s.spec.priority ?? 0,
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
    await sql.exec(
      `DELETE FROM ${t.timer} WHERE run_id IN ${inList(fx.cancelTimers.length)}`,
      fx.cancelTimers,
    );
  }
  if (fx.consumeSignals?.length) {
    await sql.exec(
      `UPDATE ${t.signal} SET consumed = 1 WHERE id IN ${inList(fx.consumeSignals.length)}`,
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
