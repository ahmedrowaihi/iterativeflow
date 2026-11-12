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
import { applyOutbox, enqueueManyStmt, enqueueStmt, inList, insertRunStmt } from "#statements";
import type { Sql, SqlParam } from "#sql";

const TERMINAL = sqlStatusTuple(TERMINAL_STATUSES);

/** @internal */
export const createSqliteStore = (sql: Sql, t: Tables, id: IdGen): Store => {
  const selectionSql: SelectionSqlOpts<number> = {
    placeholder: () => "?",
    time: (at) => at.getTime(),
    tag: (p) => `EXISTS (SELECT 1 FROM json_each(tags) WHERE value = ${p})`,
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
      const ins = await insertRunStmt(
        exec,
        t,
        runId,
        spec,
        `ON CONFLICT (name, version, idempotency_key) WHERE idempotency_key IS NOT NULL
         DO NOTHING RETURNING id`,
      );
      if (ins[0]) return { runId: text(ins[0], "id"), created: true, status: "pending" };
      const hit = await exec.query(
        `SELECT id, status FROM ${t.run} WHERE name = ? AND version = ? AND idempotency_key = ?`,
        [spec.name, spec.version, spec.idempotencyKey],
      );
      return { runId: text(hit[0], "id"), created: false, status: runStatus(hit[0]) };
    }
    await insertRunStmt(exec, t, runId, spec, "");
    return { runId, created: true, status: "pending" };
  };

  const selectIds = async (tx: Sql, sel: RunSelection, limit: number): Promise<string[]> => {
    const q = runSelectionSql(sel, selectionSql);
    if (!q) return [];
    const rows = await tx.query(`SELECT id FROM ${t.run} WHERE ${q.where} LIMIT ?`, [
      ...q.params,
      limit,
    ]);
    return rows.map((r) => text(r, "id"));
  };

  const deleteRuns = async (filter: PurgeFilter, limit: number): Promise<number> => {
    const q = runSelectionSql(purgeSelection(filter), selectionSql);
    if (!q) return 0;
    // Oldest-first only matters for an age sweep; without a cutoff the sort would force a full read
    // before LIMIT could bound it.
    const order = filter.before === undefined ? "" : " ORDER BY created_at";
    const params = [...q.params, limit];
    return sql.tx(async (tx) => {
      const ids = (
        await tx.query(
          `SELECT id FROM ${t.run}
             WHERE ${q.where}
             ${order} LIMIT ?`,
          params,
        )
      ).map((r) => text(r, "id"));
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
          `SELECT id, name, payload FROM ${t.signal} WHERE run_id = ? AND consumed = 0 ORDER BY rowid`,
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
      const byId = new Map(rows.map((r) => [text(r, "id"), mapRun(r)]));
      return runIds.map((runId) => byId.get(runId));
    },

    async arriveAtJoin(parentRunId) {
      const rows = await sql.query(
        `UPDATE ${t.run} SET join_remaining = join_remaining - 1 WHERE id = ? RETURNING join_remaining`,
        [parentRunId],
      );
      return rows[0] ? int(rows[0], "join_remaining") : undefined;
    },

    async postSignal(runId, name, payload, opts) {
      return sql.tx(async (tx) => {
        const ins = await tx.query(
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
      const rows = await sql.query(
        `UPDATE ${t.run} SET attempts = attempts + 1, status = 'running'
         WHERE id = ? AND status NOT IN ${TERMINAL}
         RETURNING attempts`,
        [runId],
      );
      if (rows[0]) return int(rows[0], "attempts");
      const cur = await sql.query(`SELECT attempts FROM ${t.run} WHERE id = ?`, [runId]);
      if (!cur[0]) throw new Error(`markRunning: run ${runId} not found`);
      return int(cur[0], "attempts");
    },

    checkpointStep(c, fx) {
      return sql.tx(async (tx) => {
        // libsql may run with foreign_keys off, so guard the unknown-run reject explicitly.
        const known = await tx.query(`SELECT 1 FROM ${t.run} WHERE id = ?`, [c.runId]);
        if (!known[0]) throw new Error(`checkpointStep: run ${c.runId} not found`);
        if (fx?.requireVersion !== undefined) {
          const existing = await tx.query(
            `SELECT 1 FROM ${t.step} WHERE run_id = ? AND cursor_key = ?`,
            [c.runId, c.cursorKey],
          );
          if (existing.length === 0) {
            const job = await tx.query(`SELECT version FROM ${t.job} WHERE run_id = ?`, [c.runId]);
            if (!job[0] || int(job[0], "version") !== fx.requireVersion) {
              return { status: c.status, attempts: c.attempts, committed: false };
            }
          }
        }
        const ins = await tx.query(
          `INSERT INTO ${t.step} (run_id, cursor_key, status, result, error, attempts, shape)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (run_id, cursor_key) DO NOTHING
           RETURNING 1`,
          [c.runId, c.cursorKey, c.status, j(c.result), j(c.error), c.attempts, c.call ?? null],
        );
        if (ins.length > 0 && fx) await applyOutbox(tx, t, fx); // outbox rides ONLY the first write
        return loadStep(tx, c.runId, c.cursorKey);
      });
    },

    suspendRun(runId, status: SuspendStatus, fx) {
      return sql.tx(async (tx) => {
        const reset = resetsAttempts(status) ? ", attempts = 0" : "";
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
           WHERE id = ? AND status NOT IN ${TERMINAL}
           RETURNING 1`,
          [outcome.status, j(output), j(error), runId],
        );
        if (!rows[0]) return false;
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
        where.push(`rowid < ?`);
        params.push(Number(page.cursor));
      }
      params.push(page.limit);
      const rows = await sql.query(
        `SELECT rowid, * FROM ${t.run} WHERE ${where.join(" AND ")} ORDER BY rowid DESC LIMIT ?`,
        params,
      );
      const last = rows[rows.length - 1];
      const cursor = rows.length === page.limit && last ? String(int(last, "rowid")) : undefined;
      return { runs: rows.map((r) => mapRun(r)), cursor };
    },

    async cancelRuns(filter, limit) {
      const sel = cancelSelection(filter);
      return sql.tx(async (tx) => {
        const ids = await selectIds(tx, sel, limit);
        if (ids.length === 0) return 0;
        const inIds = inList(ids.length);
        await tx.query(`DELETE FROM ${t.timer} WHERE run_id IN ${inIds}`, ids);
        await tx.query(`UPDATE ${t.run} SET status = 'canceled' WHERE id IN ${inIds}`, ids);
        return ids.length;
      });
    },

    async retryRuns(filter, limit) {
      const sel = retrySelection(filter);
      return sql.tx(async (tx) => {
        const ids = await selectIds(tx, sel, limit);
        if (ids.length === 0) return 0;
        const inIds = inList(ids.length);
        await tx.query(
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
      return rows.map((r) => mapRun(r));
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
          order: "r.rowid",
          limit: "?",
        }),
        [limit],
      );
      return rows.map((r) => text(r, "id"));
    },

    deleteRuns,

    retryRun(runId) {
      return sql.tx(async (tx) => {
        const rows = await tx.query(
          `UPDATE ${t.run} SET status = 'pending', error = NULL, attempts = 0
             WHERE id = ? AND status = 'failed' RETURNING 1`,
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
           overlap = excluded.overlap,
           next_run_at = CASE WHEN ${t.cron}.schedule <> excluded.schedule
                              THEN excluded.next_run_at ELSE ${t.cron}.next_run_at END`,
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
      const rows = await sql.query(`DELETE FROM ${t.cron} WHERE name = ? RETURNING 1`, [name]);
      return rows.length > 0;
    },

    async dueCronCount(now, names) {
      if (names && names.length === 0) return 0;
      const filter = names ? ` AND flow_name IN ${inList(names.length)}` : "";
      const rows = await sql.query(
        `SELECT count(*) AS n FROM ${t.cron} WHERE next_run_at <= ?${filter}`,
        [now.getTime(), ...(names ?? [])],
      );
      return int(rows[0], "n");
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
