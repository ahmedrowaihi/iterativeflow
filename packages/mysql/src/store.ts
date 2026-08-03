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
  orphanedRunsSql,
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
export const createMysqlStore = (sql: Sql, t: Tables, id: IdGen): Store => {
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
      const ins = await exec.exec(
        `INSERT IGNORE INTO ${t.run}
           (id, name, version, status, input, idempotency_key, tags, parent_run_id, parent_cursor_key, depth, created_at)
         VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)`,
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
      if (ins.affectedRows === 1) return { runId, created: true, status: "pending" };
      const hit = await exec.query<{ id: string; status: RunStatus }>(
        `SELECT id, status FROM ${t.run} WHERE name = ? AND version = ? AND idempotency_key = ?`,
        [spec.name, spec.version, spec.idempotencyKey],
      );
      return { runId: hit[0].id, created: false, status: hit[0].status };
    }
    await exec.exec(
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
          `SELECT id, name, payload FROM ${t.signal} WHERE run_id = ? ORDER BY seq`,
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

    arriveAtJoin(parentRunId) {
      return sql.tx(async (tx) => {
        const res = await tx.exec(
          `UPDATE ${t.run} SET join_remaining = join_remaining - 1 WHERE id = ?`,
          [parentRunId],
        );
        if (res.affectedRows === 0) return undefined;
        const rows = await tx.query<{ join_remaining: number | string }>(
          `SELECT join_remaining FROM ${t.run} WHERE id = ?`,
          [parentRunId],
        );
        return rows[0] ? Number(rows[0].join_remaining) : undefined;
      });
    },

    async postSignal(runId, name, payload, opts) {
      return sql.tx(async (tx) => {
        const res = await tx.exec(
          `INSERT IGNORE INTO ${t.signal} (id, run_id, name, payload, idem_key) VALUES (?, ?, ?, ?, ?)`,
          [id(), runId, name, j(payload), opts?.idempotencyKey ?? null],
        );
        if (res.affectedRows === 0) return { delivered: false };
        await enqueueStmt(tx, t, runId);
        return { delivered: true };
      });
    },

    markRunning(runId) {
      return sql.tx(async (tx) => {
        const cur = await tx.query<{ status: RunStatus; attempts: number | string }>(
          `SELECT status, attempts FROM ${t.run} WHERE id = ? FOR UPDATE`,
          [runId],
        );
        if (!cur[0]) throw new Error(`markRunning: run ${runId} not found`);
        const attempts = Number(cur[0].attempts);
        if (TERMINAL_STATUSES.includes(cur[0].status)) return attempts;
        await tx.exec(
          `UPDATE ${t.run} SET status = 'running', attempts = attempts + 1 WHERE id = ?`,
          [runId],
        );
        return attempts + 1;
      });
    },

    checkpointStep(c, fx) {
      return sql.tx(async (tx) => {
        // No FK on step.run_id, so guard the unknown-run reject explicitly before the memo write.
        const known = await tx.query(`SELECT 1 AS ok FROM ${t.run} WHERE id = ?`, [c.runId]);
        if (!known[0]) throw new Error(`checkpointStep: run ${c.runId} not found`);
        if (fx?.requireVersion !== undefined) {
          const existing = await tx.query(
            `SELECT 1 AS ok FROM ${t.step} WHERE run_id = ? AND cursor_key = ?`,
            [c.runId, c.cursorKey],
          );
          if (existing.length === 0) {
            const job = await tx.query<{ version: number | string }>(
              `SELECT version FROM ${t.job} WHERE run_id = ? FOR UPDATE`,
              [c.runId],
            );
            if (!job[0] || Number(job[0].version) !== fx.requireVersion) {
              return { status: c.status, attempts: c.attempts, committed: false };
            }
          }
        }
        const ins = await tx.exec(
          `INSERT IGNORE INTO ${t.step} (run_id, cursor_key, status, result, error, attempts, shape)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
          [c.runId, c.cursorKey, c.status, j(c.result), j(c.error), c.attempts, c.shape ?? null],
        );
        if (ins.affectedRows === 1 && fx) await applyOutbox(tx, t, fx); // outbox rides ONLY the first write
        return loadStep(tx, c.runId, c.cursorKey);
      });
    },

    suspendRun(runId, status: SuspendStatus, fx) {
      return sql.tx(async (tx) => {
        const reset = status !== "retrying" ? ", attempts = 0" : "";
        const res = await tx.exec(
          `UPDATE ${t.run} SET status = ?${reset} WHERE id = ? AND status NOT IN ${TERMINAL}`,
          [status, runId],
        );
        if (res.affectedRows > 0 && fx) await applyOutbox(tx, t, fx);
      });
    },

    markTerminal(runId, outcome, fx) {
      return sql.tx(async (tx) => {
        const output = outcome.status === "done" ? outcome.output : undefined;
        const error = outcome.status === "done" ? undefined : outcome.error;
        const res = await tx.exec(
          `UPDATE ${t.run} SET status = ?, output = ?, error = ?
           WHERE id = ? AND status <> 'canceled'`,
          [outcome.status, j(output), j(error), runId],
        );
        if (res.affectedRows > 0 && fx) await applyOutbox(tx, t, fx);
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
        where.push(`JSON_CONTAINS(tags, JSON_QUOTE(?))`);
        params.push(filter.tag);
      }
      if (page.cursor) {
        where.push(`seq < ?`);
        params.push(Number(page.cursor));
      }
      params.push(page.limit);
      const clause = where.length ? `WHERE ${where.join(" AND ")}` : "";
      const rows = await sql.query<RunRecord & { seq: number | string }>(
        `SELECT * FROM ${t.run} ${clause} ORDER BY seq DESC LIMIT ?`,
        params,
      );
      const last = rows[rows.length - 1];
      const cursor = rows.length === page.limit && last ? String(last.seq) : undefined;
      return { runs: rows.map((r) => mapRun(r)), cursor };
    },

    async childrenOf(runId) {
      const rows = await sql.query<RunRecord>(`SELECT * FROM ${t.run} WHERE parent_run_id = ?`, [
        runId,
      ]);
      return rows.map((r) => mapRun(r));
    },

    async runStats() {
      const rows = await sql.query<{ status: RunStatus; n: number | string }>(
        `SELECT status, count(*) AS n FROM ${t.run} GROUP BY status`,
      );
      const stats = zeroRunStats();
      for (const r of rows) stats[r.status] = Number(r.n);
      return stats;
    },

    async orphanedRuns(limit) {
      const rows = await sql.query<{ id: string }>(
        orphanedRunsSql({
          run: t.run,
          job: t.job,
          timer: t.timer,
          reconcilable: RECONCILABLE,
          terminal: TERMINAL,
          nonSuccessTerminal: NON_SUCCESS_TERMINAL,
          order: "r.seq",
          limit: "?",
        }),
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
             ORDER BY seq LIMIT ?`,
            [before.getTime(), limit],
          )
        ).map((r) => r.id);
        if (ids.length === 0) return 0;
        const inIds = inList(ids.length);
        // step/signal reference the run — delete them before the run itself.
        await tx.exec(`DELETE FROM ${t.step} WHERE run_id IN ${inIds}`, ids);
        await tx.exec(`DELETE FROM ${t.signal} WHERE run_id IN ${inIds}`, ids);
        await tx.exec(`DELETE FROM ${t.job} WHERE run_id IN ${inIds}`, ids);
        await tx.exec(`DELETE FROM ${t.timer} WHERE run_id IN ${inIds}`, ids);
        await tx.exec(`DELETE FROM ${t.run} WHERE id IN ${inIds}`, ids);
        return ids.length;
      });
    },

    retryRun(runId) {
      return sql.tx(async (tx) => {
        const res = await tx.exec(
          `UPDATE ${t.run} SET status = 'pending', error = NULL WHERE id = ? AND status = 'failed'`,
          [runId],
        );
        if (res.affectedRows !== 1) return { retried: false };
        await enqueueStmt(tx, t, runId);
        return { retried: true };
      });
    },

    async upsertCron(spec) {
      await sql.exec(
        `INSERT INTO ${t.cron}
           (name, schedule, flow_name, flow_version, input, overlap, next_run_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           schedule = VALUES(schedule),
           flow_name = VALUES(flow_name),
           flow_version = VALUES(flow_version),
           input = VALUES(input),
           overlap = VALUES(overlap)`,
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

    async dueCronCount(now, names) {
      if (names && names.length === 0) return 0;
      const namePredicate = names ? ` AND flow_name IN ${inList(names.length)}` : "";
      const params = names ? [now.getTime(), ...names] : [now.getTime()];
      const rows = await sql.query<{ n: number | string }>(
        `SELECT count(*) AS n FROM ${t.cron} WHERE next_run_at <= ?${namePredicate}`,
        params,
      );
      return Number(rows[0]?.n ?? 0);
    },

    async advanceCron(name, expectedNextRunAt, nextRunAt, lastRunAt) {
      const res = await sql.exec(
        `UPDATE ${t.cron} SET next_run_at = ?, last_run_at = ?
         WHERE name = ? AND next_run_at = ?`,
        [nextRunAt.getTime(), lastRunAt.getTime(), name, expectedNextRunAt.getTime()],
      );
      return res.affectedRows === 1;
    },
  };
};
