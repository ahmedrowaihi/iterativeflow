import {
  type IdGen,
  type PurgeFilter,
  type RunSpec,
  type StartResult,
  type Store,
  type RunSelection,
  type SelectionSqlOpts,
  type SuspendStatus,
  TERMINAL_STATUSES,
  isTerminal,
  listSelection,
  orphanedRunsSql,
  cancelSelection,
  purgeSelection,
  resetsAttempts,
  retrySelection,
  runSelectionSql,
  sqlStatusTuple,
  zeroRunStats,
} from "@iterativeflow/core/backend";
import { STEP_COLUMNS, int, j, mapCron, mapRun, mapSignal, mapStep, runStatus, text } from "#codec";
import type { Tables } from "#schema";
import {
  applyOutbox,
  enqueueManyStmt,
  enqueueStmt,
  inList,
  insertRunStmt,
  wroteUnlessDuplicate,
} from "#statements";
import type { Sql, SqlParam } from "#sql";

const TERMINAL = sqlStatusTuple(TERMINAL_STATUSES);

/** @internal */
export const createMysqlStore = (sql: Sql, t: Tables, id: IdGen): Store => {
  const selectionSql: SelectionSqlOpts<number> = {
    placeholder: () => "?",
    time: (at) => at.getTime(),
    tag: (p) => `JSON_CONTAINS(tags, JSON_QUOTE(${p}))`,
    run: t.run,
  };

  const loadStep = async (exec: Sql, runId: string, cursorKey: string) => {
    const rows = await exec.query(
      `SELECT ${STEP_COLUMNS} FROM ${t.step} WHERE run_id = ? AND cursor_key = ?`,
      [runId, cursorKey],
    );
    const row = rows[0];
    if (!row) throw new Error(`checkpointStep: step ${runId}/${cursorKey} vanished after write`);
    return mapStep(row);
  };

  const startOne = async (exec: Sql, spec: RunSpec): Promise<StartResult> => {
    const runId = id();
    if (spec.idempotencyKey) {
      if (await wroteUnlessDuplicate(insertRunStmt(exec, t, runId, spec))) {
        return { runId, created: true, status: "pending" };
      }
      const hit = await exec.query(
        `SELECT id, status FROM ${t.run} WHERE name = ? AND version = ? AND idempotency_key = ?`,
        [spec.name, spec.version, spec.idempotencyKey],
      );
      return { runId: text(hit[0], "id"), created: false, status: runStatus(hit[0]) };
    }
    await insertRunStmt(exec, t, runId, spec);
    return { runId, created: true, status: "pending" };
  };

  const lockSelected = async (tx: Sql, sel: RunSelection, limit: number): Promise<string[]> => {
    const q = runSelectionSql(sel, selectionSql);
    if (!q) return [];
    const rows = await tx.query(`SELECT id FROM ${t.run} WHERE ${q.where} LIMIT ? FOR UPDATE`, [
      ...q.params,
      limit,
    ]);
    return rows.map((r) => text(r, "id"));
  };

  const deleteRuns = async (filter: PurgeFilter, limit: number): Promise<number> => {
    const q = runSelectionSql(purgeSelection(filter), selectionSql);
    if (!q) return 0;
    // Oldest-first only matters for an age sweep; without a cutoff the sort would force a full read
    // before LIMIT could bound it. `seq` is insertion order, so it tracks created_at.
    const order = filter.before === undefined ? "" : " ORDER BY seq";
    const params = [...q.params, limit];
    return sql.tx(async (tx) => {
      const ids = (
        await tx.query(
          `SELECT id FROM ${t.run}
             WHERE ${q.where}
             ${order} LIMIT ? FOR UPDATE`,
          params,
        )
      ).map((r) => text(r, "id"));
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
        sql.query(`SELECT * FROM ${t.run} WHERE id = ?`, [runId]),
        sql.query(`SELECT cursor_key, ${STEP_COLUMNS} FROM ${t.step} WHERE run_id = ?`, [runId]),
        sql.query(
          `SELECT id, name, payload FROM ${t.signal} WHERE run_id = ? AND consumed = 0 ORDER BY seq`,
          [runId],
        ),
      ]);
      const runRow = runRows[0];
      if (!runRow) return undefined;
      return {
        run: mapRun(runRow),
        steps: new Map(stepRows.map((r) => [text(r, "cursor_key"), mapStep(r)])),
        signals: sigRows.map(mapSignal),
      };
    },

    async loadRunRow(runId) {
      const rows = await sql.query(`SELECT * FROM ${t.run} WHERE id = ?`, [runId]);
      return rows[0] ? mapRun(rows[0]) : undefined;
    },

    async loadRunRows(runIds) {
      if (runIds.length === 0) return [];
      const rows = await sql.query(
        `SELECT * FROM ${t.run} WHERE id IN ${inList(runIds.length)}`,
        runIds,
      );
      const byId = new Map(rows.map(mapRun).map((run) => [run.id, run]));
      return runIds.map((runId) => byId.get(runId));
    },

    arriveAtJoin(parentRunId) {
      return sql.tx(async (tx) => {
        const res = await tx.exec(
          `UPDATE ${t.run} SET join_remaining = join_remaining - 1 WHERE id = ?`,
          [parentRunId],
        );
        if (res.affectedRows === 0) return undefined;
        const rows = await tx.query(`SELECT join_remaining FROM ${t.run} WHERE id = ?`, [
          parentRunId,
        ]);
        return rows[0] ? int(rows[0], "join_remaining") : undefined;
      });
    },

    async postSignal(runId, name, payload, opts) {
      return sql.tx(async (tx) => {
        const delivered = await wroteUnlessDuplicate(
          tx.exec(
            `INSERT INTO ${t.signal} (id, run_id, name, payload, idem_key) VALUES (?, ?, ?, ?, ?)`,
            [id(), runId, name, j(payload), opts?.idempotencyKey ?? null],
          ),
        );
        if (!delivered) return { delivered: false };
        await enqueueStmt(tx, t, runId);
        return { delivered: true };
      });
    },

    markRunning(runId) {
      return sql.tx(async (tx) => {
        const [cur] = await tx.query(
          `SELECT status, attempts FROM ${t.run} WHERE id = ? FOR UPDATE`,
          [runId],
        );
        if (!cur) throw new Error(`markRunning: run ${runId} not found`);
        const attempts = int(cur, "attempts");
        if (isTerminal(runStatus(cur))) return attempts;
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
            const job = await tx.query(`SELECT version FROM ${t.job} WHERE run_id = ? FOR UPDATE`, [
              c.runId,
            ]);
            if (!job[0] || int(job[0], "version") !== fx.requireVersion) {
              return { status: c.status, attempts: c.attempts, committed: false };
            }
          }
        }
        const first = await wroteUnlessDuplicate(
          tx.exec(
            `INSERT INTO ${t.step} (run_id, cursor_key, status, result, error, attempts, shape)
             VALUES (?, ?, ?, ?, ?, ?, ?)`,
            [c.runId, c.cursorKey, c.status, j(c.result), j(c.error), c.attempts, c.call ?? null],
          ),
        );
        if (first && fx) await applyOutbox(tx, t, fx); // outbox rides ONLY the first write
        return loadStep(tx, c.runId, c.cursorKey);
      });
    },

    suspendRun(runId, status: SuspendStatus, fx) {
      return sql.tx(async (tx) => {
        const reset = resetsAttempts(status) ? ", attempts = 0" : "";
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
           WHERE id = ? AND status NOT IN ${TERMINAL}`,
          [outcome.status, j(output), j(error), runId],
        );
        if (res.affectedRows === 0) return false;
        if (fx) await applyOutbox(tx, t, fx);
        return true;
      });
    },

    async listRuns(filter, page) {
      const q = runSelectionSql(listSelection(filter), selectionSql);
      if (!q) return { runs: [], cursor: undefined };
      const params: SqlParam[] = [...q.params];
      const where = [q.where];
      if (page.cursor) {
        where.push(`seq < ?`);
        params.push(Number(page.cursor));
      }
      params.push(page.limit);
      const rows = await sql.query(
        `SELECT * FROM ${t.run} WHERE ${where.join(" AND ")} ORDER BY seq DESC LIMIT ?`,
        params,
      );
      const last = rows[rows.length - 1];
      const cursor = rows.length === page.limit && last ? text(last, "seq") : undefined;
      return { runs: rows.map(mapRun), cursor };
    },

    async cancelRuns(filter, limit) {
      const sel = cancelSelection(filter);
      return sql.tx(async (tx) => {
        const ids = await lockSelected(tx, sel, limit);
        if (ids.length === 0) return 0;
        const inIds = inList(ids.length);
        await tx.exec(`DELETE FROM ${t.timer} WHERE run_id IN ${inIds}`, ids);
        await tx.exec(`UPDATE ${t.run} SET status = 'canceled' WHERE id IN ${inIds}`, ids);
        return ids.length;
      });
    },

    async retryRuns(filter, limit) {
      const sel = retrySelection(filter);
      return sql.tx(async (tx) => {
        const ids = await lockSelected(tx, sel, limit);
        if (ids.length === 0) return 0;
        const inIds = inList(ids.length);
        await tx.exec(
          `UPDATE ${t.run} SET status = 'pending', error = NULL, attempts = 0 WHERE id IN ${inIds}`,
          ids,
        );
        await enqueueManyStmt(
          tx,
          t,
          ids.map((runId) => ({ runId })),
        );
        return ids.length;
      });
    },

    async childrenOf(runId) {
      const rows = await sql.query(`SELECT * FROM ${t.run} WHERE parent_run_id = ?`, [runId]);
      return rows.map(mapRun);
    },

    async runStats() {
      const rows = await sql.query(`SELECT status, count(*) AS n FROM ${t.run} GROUP BY status`);
      const stats = zeroRunStats();
      for (const r of rows) stats[runStatus(r)] = int(r, "n");
      return stats;
    },

    async orphanedRuns(limit) {
      const rows = await sql.query(
        orphanedRunsSql({
          run: t.run,
          job: t.job,
          timer: t.timer,
          order: "r.seq",
          limit: "?",
        }),
        [limit],
      );
      return rows.map((r) => text(r, "id"));
    },

    deleteRuns,

    retryRun(runId) {
      return sql.tx(async (tx) => {
        const res = await tx.exec(
          `UPDATE ${t.run} SET status = 'pending', error = NULL, attempts = 0
           WHERE id = ? AND status = 'failed'`,
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
           -- MySQL applies these left to right, so compare the schedule BEFORE overwriting it
           next_run_at = IF(schedule <> VALUES(schedule), VALUES(next_run_at), next_run_at),
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
          spec.overlap,
          spec.nextRunAt.getTime(),
        ],
      );
    },

    async nextCronAt() {
      const rows = await sql.query(
        `SELECT next_run_at FROM ${t.cron} ORDER BY next_run_at LIMIT 1`,
      );
      return rows[0] ? new Date(int(rows[0], "next_run_at")) : null;
    },

    async dueCrons(now, limit) {
      const rows = await sql.query(
        `SELECT * FROM ${t.cron} WHERE next_run_at <= ? ORDER BY next_run_at LIMIT ?`,
        [now.getTime(), limit],
      );
      return rows.map(mapCron);
    },

    async listCrons() {
      const rows = await sql.query(`SELECT * FROM ${t.cron} ORDER BY name`);
      return rows.map(mapCron);
    },

    async removeCron(name) {
      const res = await sql.exec(`DELETE FROM ${t.cron} WHERE name = ?`, [name]);
      return res.affectedRows > 0;
    },

    async dueCronCount(now, names) {
      if (names && names.length === 0) return 0;
      const namePredicate = names ? ` AND flow_name IN ${inList(names.length)}` : "";
      const params = names ? [now.getTime(), ...names] : [now.getTime()];
      const [row] = await sql.query(
        `SELECT count(*) AS n FROM ${t.cron} WHERE next_run_at <= ?${namePredicate}`,
        params,
      );
      return int(row, "n");
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
