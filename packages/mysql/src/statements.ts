import {
  type EnqueueOpts,
  type EnqueueRequest,
  type Outbox,
  type RunSpec,
  distinctEnqueues,
} from "@iterativeflow/core/backend";
import { j } from "#codec";
import type { Tables } from "#schema";
import type { Sql, WriteResult } from "#sql";

// 4 binds per row, under MySQL's 65535 placeholder ceiling.
const ENQUEUE_ROWS_PER_STATEMENT = 1000;

/** @internal */
export const inList = (n: number): string => `(${Array.from({ length: n }, () => "?").join(",")})`;

const ER_DUP_ENTRY = 1062;

// Narrower than `INSERT IGNORE`, which would also downgrade truncation and NOT NULL errors to
// warnings. InnoDB rolls back only the failed statement, so an enclosing transaction carries on.
/** @internal */
export const wroteUnlessDuplicate = async (write: Promise<WriteResult>): Promise<boolean> => {
  try {
    await write;
    return true;
  } catch (e) {
    if (e instanceof Error && "errno" in e && e.errno === ER_DUP_ENTRY) return false;
    throw e;
  }
};

/** @internal */
export const insertRunStmt = (
  sql: Sql,
  t: Tables,
  runId: string,
  spec: RunSpec,
): Promise<WriteResult> =>
  sql.exec(
    `INSERT INTO ${t.run}
       (id, name, version, status, input, idempotency_key, tags, parent_run_id, parent_cursor_key, depth, priority, created_at)
     VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      runId,
      spec.name,
      spec.version,
      j(spec.input),
      spec.idempotencyKey || null,
      j(spec.tags),
      spec.parentRunId ?? null,
      spec.parentCursorKey ?? null,
      spec.depth ?? 0,
      spec.priority ?? 0,
      (spec.createdAt ?? new Date()).getTime(),
    ],
  );

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
    await wroteUnlessDuplicate(insertRunStmt(sql, t, s.runId, s.spec));
  }
  if (fx.spawn?.length)
    await enqueueManyStmt(
      sql,
      t,
      fx.spawn.map((s) => ({ runId: s.runId })),
    );
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
