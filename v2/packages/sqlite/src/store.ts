import {
  type IdGen,
  type RunSpec,
  type RunStatus,
  type StartResult,
  type Store,
  type SuspendStatus,
  NON_SUCCESS_TERMINAL_STATUSES,
  RECONCILABLE_STATUSES,
  TERMINAL_STATUSES,
  statusList,
  zeroRunStats,
} from "@iterativeflow/core/backend";
import {
  type CronRecord,
  type RunRecord,
  type SignalRecord,
  type StepRecord,
  j,
  mapCron,
  mapRun,
  mapSignal,
  mapStep,
} from "#codec";
import type { Tables } from "#schema";
import { applyOutbox, enqueueStmt } from "#statements";
import type { Sql } from "#sql";

const sqlTuple = (statuses: readonly string[]): string =>
  `(${statuses.map((s) => `'${s}'`).join(",")})`;
const TERMINAL = sqlTuple(TERMINAL_STATUSES);
const RECONCILABLE = sqlTuple(RECONCILABLE_STATUSES);
const NON_SUCCESS_TERMINAL = sqlTuple(NON_SUCCESS_TERMINAL_STATUSES);

const inList = (n: number): string => `(${Array.from({ length: n }, () => "?").join(",")})`;

/** @internal */
export const createSqliteStore = (sql: Sql, t: Tables, id: IdGen): Store => {
  const loadStep = async (exec: Sql, runId: string, cursorKey: string) => {
    const rows = await exec.query<StepRecord>(
      `SELECT status, result, error, attempts, shape FROM ${t.step} WHERE run_id = ? AND cursor_key = ?`,
      [runId, cursorKey],
    );
    const row = rows[0];
    if (!row) throw new Error(`checkpointStep: step ${runId}/${cursorKey} vanished after write`);
    return mapStep(row);
  };

  const startOne = async (exec: Sql, spec: RunSpec): Promise<StartResult> => {
    const runId = id();
    if (spec.idempotencyKey) {
      const ins = await exec.query<{ id: string }>(
        `INSERT INTO ${t.run}
           (id, name, version, status, input, idempotency_key, tags, parent_run_id, parent_cursor_key, depth, created_at)
         VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (name, version, idempotency_key) WHERE idempotency_key IS NOT NULL
         DO NOTHING
         RETURNING id`,
        [
          runId,
          spec.name,
          spec.version,
          j(spec.input),
          spec.idempotencyKey,
          j(spec.tags),
          spec.parentRunId ?? null,
          spec.parentCursorKey ?? null,
          spec.depth ?? 0,
          (spec.createdAt ?? new Date()).getTime(),
        ],
      );
      if (ins[0]) return { runId: ins[0].id, created: true, status: "pending" };
      const hit = await exec.query<{ id: string; status: RunStatus }>(
        `SELECT id, status FROM ${t.run} WHERE name = ? AND version = ? AND idempotency_key = ?`,
        [spec.name, spec.version, spec.idempotencyKey],
      );
      return { runId: hit[0].id, created: false, status: hit[0].status };
    }
    await exec.query(
      `INSERT INTO ${t.run}
         (id, name, version, status, input, tags, parent_run_id, parent_cursor_key, depth, created_at)
       VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)`,
      [
        runId,
        spec.name,
        spec.version,
        j(spec.input),
        j(spec.tags),
        spec.parentRunId ?? null,
        spec.parentCursorKey ?? null,
        spec.depth ?? 0,
        (spec.createdAt ?? new Date()).getTime(),
      ],
    );
    return { runId, created: true, status: "pending" };
  };

  return {
    startRun(spec) {
      return startOne(sql, spec);
    },

    startManyRuns(specs) {
      return sql.tx(async (tx) => {
        const out: StartResult[] = [];
        for (const s of specs) out.push(await startOne(tx, s));
        return out;
      });
    },

    async loadRun(runId) {
      const [runRows, stepRows, sigRows] = await Promise.all([
        sql.query<RunRecord>(`SELECT * FROM ${t.run} WHERE id = ?`, [runId]),
        sql.query<StepRecord & { cursor_key: string }>(
          `SELECT cursor_key, status, result, error, attempts, shape FROM ${t.step} WHERE run_id = ?`,
          [runId],
        ),
        sql.query<SignalRecord>(
          `SELECT id, name, payload FROM ${t.signal} WHERE run_id = ? ORDER BY rowid`,
          [runId],
        ),
      ]);
      const runRow = runRows[0];
      if (!runRow) return undefined;
      return {
        run: mapRun(runRow),
        steps: new Map(stepRows.map((r) => [r.cursor_key, mapStep(r)])),
        signals: sigRows.map(mapSignal),
      };
    },

    async loadRunRow(runId) {
      const rows = await sql.query<RunRecord>(`SELECT * FROM ${t.run} WHERE id = ?`, [runId]);
      return rows[0] ? mapRun(rows[0]) : undefined;
    },

    async loadRunRows(runIds) {
      if (runIds.length === 0) return [];
      const rows = await sql.query<RunRecord>(
        `SELECT * FROM ${t.run} WHERE id IN ${inList(runIds.length)}`,
        runIds,
      );
      const byId = new Map(rows.map((r) => [r.id, mapRun(r)]));
      return runIds.map((runId) => byId.get(runId));
    },

    async arriveAtJoin(parentRunId) {
      const rows = await sql.query<{ join_remaining: number }>(
        `UPDATE ${t.run} SET join_remaining = join_remaining - 1 WHERE id = ? RETURNING join_remaining`,
        [parentRunId],
      );
      return rows[0] ? rows[0].join_remaining : undefined;
    },

    async postSignal(runId, name, payload, opts) {
      return sql.tx(async (tx) => {
        const ins = await tx.query<{ id: string }>(
          `INSERT INTO ${t.signal} (id, run_id, name, payload, idem_key)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT (run_id, idem_key) WHERE idem_key IS NOT NULL DO NOTHING
           RETURNING id`,
          [id(), runId, name, j(payload), opts?.idempotencyKey ?? null],
        );
        if (!ins[0]) return { delivered: false };
        await enqueueStmt(tx, t, runId);
        return { delivered: true };
      });
    },

    async markRunning(runId) {
      const rows = await sql.query<{ attempts: number }>(
        `UPDATE ${t.run} SET attempts = attempts + 1, status = 'running'
         WHERE id = ? AND status NOT IN ${TERMINAL}
         RETURNING attempts`,
        [runId],
      );
      if (rows[0]) return rows[0].attempts;
      const cur = await sql.query<{ attempts: number }>(
        `SELECT attempts FROM ${t.run} WHERE id = ?`,
        [runId],
      );
      if (!cur[0]) throw new Error(`markRunning: run ${runId} not found`);
      return cur[0].attempts;
    },

    checkpointStep(c, fx) {
      return sql.tx(async (tx) => {
        // libsql may run with foreign_keys off, so guard the unknown-run reject explicitly.
        const known = await tx.query(`SELECT 1 FROM ${t.run} WHERE id = ?`, [c.runId]);
        if (!known[0]) throw new Error(`checkpointStep: run ${c.runId} not found`);
        const ins = await tx.query(
          `INSERT INTO ${t.step} (run_id, cursor_key, status, result, error, attempts, shape)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (run_id, cursor_key) DO NOTHING
           RETURNING 1`,
          [c.runId, c.cursorKey, c.status, j(c.result), j(c.error), c.attempts, c.shape ?? null],
        );
        if (ins.length > 0 && fx) await applyOutbox(tx, t, fx); // outbox rides ONLY the first write
        return loadStep(tx, c.runId, c.cursorKey);
      });
    },

    suspendRun(runId, status: SuspendStatus, fx) {
      return sql.tx(async (tx) => {
        const reset = status !== "retrying" ? ", attempts = 0" : "";
        const rows = await tx.query(
          `UPDATE ${t.run} SET status = ?${reset} WHERE id = ? AND status NOT IN ${TERMINAL} RETURNING 1`,
          [status, runId],
        );
        if (rows[0] && fx) await applyOutbox(tx, t, fx);
      });
    },

    markTerminal(runId, outcome, fx) {
      return sql.tx(async (tx) => {
        const output = outcome.status === "done" ? outcome.output : undefined;
        const error = outcome.status === "done" ? undefined : outcome.error;
        const rows = await tx.query(
          `UPDATE ${t.run} SET status = ?, output = ?, error = ?
           WHERE id = ? AND status <> 'canceled'
           RETURNING 1`,
          [outcome.status, j(output), j(error), runId],
        );
        if (rows[0] && fx) await applyOutbox(tx, t, fx);
      });
    },

    async listRuns(filter, page) {
      const statuses = statusList(filter.status);
      const where: string[] = [];
      const params: unknown[] = [];
      if (statuses) {
        where.push(`status IN ${inList(statuses.length)}`);
        params.push(...statuses);
      }
      if (filter.name) {
        where.push(`name = ?`);
        params.push(filter.name);
      }
      if (filter.tag) {
        where.push(`EXISTS (SELECT 1 FROM json_each(tags) WHERE value = ?)`);
        params.push(filter.tag);
      }
      if (page.cursor) {
        where.push(`rowid < ?`);
        params.push(Number(page.cursor));
      }
      params.push(page.limit);
      const clause = where.length ? `WHERE ${where.join(" AND ")}` : "";
      const rows = await sql.query<RunRecord & { rowid: number }>(
        `SELECT rowid, * FROM ${t.run} ${clause} ORDER BY rowid DESC LIMIT ?`,
        params,
      );
      const last = rows[rows.length - 1];
      const cursor = rows.length === page.limit && last ? String(last.rowid) : undefined;
      return { runs: rows.map((r) => mapRun(r)), cursor };
    },

    async childrenOf(runId) {
      const rows = await sql.query<RunRecord>(`SELECT * FROM ${t.run} WHERE parent_run_id = ?`, [
        runId,
      ]);
      return rows.map((r) => mapRun(r));
    },

    async runStats() {
      const rows = await sql.query<{ status: RunStatus; n: number }>(
        `SELECT status, count(*) AS n FROM ${t.run} GROUP BY status`,
      );
      const stats = zeroRunStats();
      for (const r of rows) stats[r.status] = r.n;
      return stats;
    },

    async orphanedRuns(limit) {
      const rows = await sql.query<{ id: string }>(
        `SELECT id FROM (
           -- crash-stranded: non-terminal, off the queue, no pending timer to wake it
           SELECT r.id AS id, r.rowid AS rid FROM ${t.run} r
           WHERE r.status IN ${RECONCILABLE}
             AND NOT EXISTS (SELECT 1 FROM ${t.job} j WHERE j.run_id = r.id)
             AND NOT EXISTS (SELECT 1 FROM ${t.timer} tm WHERE tm.run_id = r.id)
           UNION
           -- lost parent-wake: awaiting_child parent whose join has RESOLVED (any child failed/canceled
           -- ⇒ fast-fail, or every child terminal) but whose wake was lost
           SELECT r.id AS id, r.rowid AS rid FROM ${t.run} r
           WHERE r.status = 'awaiting_child'
             AND NOT EXISTS (SELECT 1 FROM ${t.job} j WHERE j.run_id = r.id)
             AND EXISTS (SELECT 1 FROM ${t.run} c WHERE c.parent_run_id = r.id)
             AND (
               EXISTS (SELECT 1 FROM ${t.run} c
                       WHERE c.parent_run_id = r.id AND c.status IN ${NON_SUCCESS_TERMINAL})
               OR NOT EXISTS (SELECT 1 FROM ${t.run} c
                              WHERE c.parent_run_id = r.id AND c.status NOT IN ${TERMINAL})
             )
           UNION
           -- orphaned child: non-terminal, but its parent terminally failed/canceled
           SELECT r.id AS id, r.rowid AS rid FROM ${t.run} r
           JOIN ${t.run} p ON p.id = r.parent_run_id
           WHERE r.status NOT IN ${TERMINAL} AND p.status IN ${NON_SUCCESS_TERMINAL}
         ) q
         ORDER BY q.rid
         LIMIT ?`,
        [limit],
      );
      return rows.map((r) => r.id);
    },

    deleteRunsOlderThan(before, limit) {
      return sql.tx(async (tx) => {
        const ids = (
          await tx.query<{ id: string }>(
            `SELECT id FROM ${t.run}
             WHERE status IN ${TERMINAL} AND created_at < ?
             ORDER BY created_at LIMIT ?`,
            [before.getTime(), limit],
          )
        ).map((r) => r.id);
        if (ids.length === 0) return 0;
        const inIds = inList(ids.length);
        // step/signal reference the run — delete them before the run itself.
        await tx.query(`DELETE FROM ${t.step} WHERE run_id IN ${inIds}`, ids);
        await tx.query(`DELETE FROM ${t.signal} WHERE run_id IN ${inIds}`, ids);
        await tx.query(`DELETE FROM ${t.job} WHERE run_id IN ${inIds}`, ids);
        await tx.query(`DELETE FROM ${t.timer} WHERE run_id IN ${inIds}`, ids);
        await tx.query(`DELETE FROM ${t.run} WHERE id IN ${inIds}`, ids);
        return ids.length;
      });
    },

    retryRun(runId) {
      return sql.tx(async (tx) => {
        const rows = await tx.query(
          `UPDATE ${t.run} SET status = 'pending', error = NULL WHERE id = ? AND status = 'failed' RETURNING 1`,
          [runId],
        );
        if (!rows[0]) return { retried: false };
        await enqueueStmt(tx, t, runId);
        return { retried: true };
      });
    },

    async upsertCron(spec) {
      await sql.query(
        `INSERT INTO ${t.cron}
           (name, schedule, flow_name, flow_version, input, overlap, next_run_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(name) DO UPDATE SET
           schedule = excluded.schedule,
           flow_name = excluded.flow_name,
           flow_version = excluded.flow_version,
           input = excluded.input,
           overlap = excluded.overlap`,
        [
          spec.name,
          spec.schedule,
          spec.flowName,
          spec.flowVersion,
          j(spec.input),
          spec.overlap ?? "allow",
          spec.nextRunAt.getTime(),
        ],
      );
    },

    async dueCrons(now, limit) {
      const rows = await sql.query<CronRecord>(
        `SELECT * FROM ${t.cron} WHERE next_run_at <= ? ORDER BY next_run_at LIMIT ?`,
        [now.getTime(), limit],
      );
      return rows.map(mapCron);
    },

    async advanceCron(name, expectedNextRunAt, nextRunAt, lastRunAt) {
      const rows = await sql.query(
        `UPDATE ${t.cron} SET next_run_at = ?, last_run_at = ?
         WHERE name = ? AND next_run_at = ? RETURNING 1`,
        [nextRunAt.getTime(), lastRunAt.getTime(), name, expectedNextRunAt.getTime()],
      );
      return rows.length > 0;
    },
  };
};
