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
import { type RunRecord, type StepRecord, j, mapRun, mapStep } from "#codec";
import { type Tables, tables } from "#schema";
import { applyOutbox, enqueueStmt } from "#statements";
import type { Sql } from "#sql";

const sqlTuple = (statuses: readonly string[]): string =>
  `(${statuses.map((s) => `'${s}'`).join(",")})`;
const TERMINAL = sqlTuple(TERMINAL_STATUSES);
const RECONCILABLE = sqlTuple(RECONCILABLE_STATUSES);
const NON_SUCCESS_TERMINAL = sqlTuple(NON_SUCCESS_TERMINAL_STATUSES);

/** @internal */
export const createPgStore = (sql: Sql, schema: string, id: IdGen): Store => {
  const t: Tables = tables(schema);

  const loadStep = async (exec: Sql, runId: string, cursorKey: string) => {
    const rows = await exec.query<StepRecord>(
      `SELECT status, result, error, attempts, shape FROM ${t.step} WHERE run_id = $1 AND cursor_key = $2`,
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
           (id, name, version, status, input, idempotency_key, tags, parent_run_id, parent_cursor_key)
         VALUES ($1, $2, $3, 'pending', $4::jsonb, $5, $6, $7, $8)
         ON CONFLICT (name, version, idempotency_key) WHERE idempotency_key IS NOT NULL
         DO NOTHING
         RETURNING id`,
        [
          runId,
          spec.name,
          spec.version,
          j(spec.input),
          spec.idempotencyKey,
          spec.tags ?? null,
          spec.parentRunId ?? null,
          spec.parentCursorKey ?? null,
        ],
      );
      if (ins[0]) return { runId: ins[0].id, created: true, status: "pending" };
      const hit = await exec.query<{ id: string; status: RunStatus }>(
        `SELECT id, status FROM ${t.run} WHERE name = $1 AND version = $2 AND idempotency_key = $3`,
        [spec.name, spec.version, spec.idempotencyKey],
      );
      return { runId: hit[0].id, created: false, status: hit[0].status };
    }
    await exec.query(
      `INSERT INTO ${t.run}
         (id, name, version, status, input, tags, parent_run_id, parent_cursor_key)
       VALUES ($1, $2, $3, 'pending', $4::jsonb, $5, $6, $7)`,
      [
        runId,
        spec.name,
        spec.version,
        j(spec.input),
        spec.tags ?? null,
        spec.parentRunId ?? null,
        spec.parentCursorKey ?? null,
      ],
    );
    return { runId, created: true, status: "pending" };
  };

  return {
    startRun(spec) {
      return startOne(sql, spec);
    },

    startManyRuns(specs) {
      // Sequential on the single connection — concurrent queries on one pg client interleave.
      return sql.tx(async (tx) => {
        const out: StartResult[] = [];
        for (const s of specs) out.push(await startOne(tx, s));
        return out;
      });
    },

    async loadRun(runId) {
      const [runRows, stepRows, sigRows] = await Promise.all([
        sql.query<RunRecord>(`SELECT * FROM ${t.run} WHERE id = $1`, [runId]),
        sql.query<StepRecord & { cursor_key: string }>(
          `SELECT cursor_key, status, result, error, attempts, shape FROM ${t.step} WHERE run_id = $1`,
          [runId],
        ),
        sql.query<{ id: string; name: string; payload: unknown }>(
          `SELECT id, name, payload FROM ${t.signal} WHERE run_id = $1 ORDER BY seq`,
          [runId],
        ),
      ]);
      const runRow = runRows[0];
      if (!runRow) return undefined;
      return {
        run: mapRun(runRow),
        steps: new Map(stepRows.map((r) => [r.cursor_key, mapStep(r)])),
        signals: sigRows.map((r) => ({ id: r.id, name: r.name, payload: r.payload })),
      };
    },

    async loadRunRow(runId) {
      const rows = await sql.query<RunRecord>(`SELECT * FROM ${t.run} WHERE id = $1`, [runId]);
      return rows[0] ? mapRun(rows[0]) : undefined;
    },

    async loadRunRows(runIds) {
      if (runIds.length === 0) return [];
      const rows = await sql.query<RunRecord>(`SELECT * FROM ${t.run} WHERE id = ANY($1)`, [
        runIds,
      ]);
      const byId = new Map(rows.map((r) => [r.id, mapRun(r)]));
      return runIds.map((runId) => byId.get(runId));
    },

    async arriveAtJoin(parentRunId) {
      const rows = await sql.query<{ join_remaining: number }>(
        `UPDATE ${t.run} SET join_remaining = join_remaining - 1 WHERE id = $1 RETURNING join_remaining`,
        [parentRunId],
      );
      return rows[0] ? rows[0].join_remaining : undefined;
    },

    async postSignal(runId, name, payload, opts) {
      return sql.tx(async (tx) => {
        const ins = await tx.query<{ id: string }>(
          `INSERT INTO ${t.signal} (id, run_id, name, payload, idem_key)
           VALUES ($1, $2, $3, $4::jsonb, $5)
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
         WHERE id = $1 AND status NOT IN ${TERMINAL}
         RETURNING attempts`,
        [runId],
      );
      if (rows[0]) return rows[0].attempts;
      // Terminal (must not resurrect) or missing (must throw): read the current state to tell them apart.
      const cur = await sql.query<{ attempts: number }>(
        `SELECT attempts FROM ${t.run} WHERE id = $1`,
        [runId],
      );
      if (!cur[0]) throw new Error(`markRunning: run ${runId} not found`);
      return cur[0].attempts;
    },

    checkpointStep(c, fx) {
      return sql.tx(async (tx) => {
        // First-writer-wins in one statement; the FK makes an unknown run reject here.
        const ins = await tx.query(
          `INSERT INTO ${t.step} (run_id, cursor_key, status, result, error, attempts, shape)
           VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7)
           ON CONFLICT (run_id, cursor_key) DO NOTHING
           RETURNING 1`,
          [c.runId, c.cursorKey, c.status, j(c.result), j(c.error), c.attempts, c.shape ?? null],
        );
        if (ins.length > 0 && fx) await applyOutbox(tx, t, fx); // outbox rides ONLY the first write
        return loadStep(tx, c.runId, c.cursorKey);
      });
    },

    suspendRun(runId, status: SuspendStatus, fx, resetAttempts) {
      return sql.tx(async (tx) => {
        const rows = await tx.query(
          `UPDATE ${t.run} SET status = $2${resetAttempts ? ", attempts = 0" : ""}
           WHERE id = $1 AND status NOT IN ${TERMINAL} RETURNING 1`,
          [runId, status],
        );
        if (rows[0] && fx) await applyOutbox(tx, t, fx);
      });
    },

    markTerminal(runId, outcome, fx) {
      return sql.tx(async (tx) => {
        const output = outcome.status === "done" ? outcome.output : undefined;
        const error = outcome.status === "done" ? undefined : outcome.error;
        const rows = await tx.query(
          `UPDATE ${t.run} SET status = $2, output = $3::jsonb, error = $4::jsonb
           WHERE id = $1 AND status <> 'canceled'
           RETURNING 1`,
          [runId, outcome.status, j(output), j(error)],
        );
        if (rows[0] && fx) await applyOutbox(tx, t, fx);
      });
    },

    async listRuns(filter, page) {
      const statuses = statusList(filter.status);
      const where: string[] = [];
      const params: unknown[] = [];
      if (statuses) {
        params.push(statuses);
        where.push(`status = ANY($${params.length}::text[])`);
      }
      if (filter.name) {
        params.push(filter.name);
        where.push(`name = $${params.length}`);
      }
      if (filter.tag) {
        params.push(filter.tag);
        where.push(`$${params.length} = ANY(tags)`);
      }
      if (page.cursor) {
        params.push(page.cursor);
        where.push(`seq < $${params.length}::bigint`);
      }
      params.push(page.limit);
      const clause = where.length ? `WHERE ${where.join(" AND ")}` : "";
      const rows = await sql.query<RunRecord & { seq: string }>(
        `SELECT * FROM ${t.run} ${clause} ORDER BY seq DESC LIMIT $${params.length}`,
        params,
      );
      const last = rows[rows.length - 1];
      const cursor = rows.length === page.limit && last ? String(last.seq) : undefined;
      return { runs: rows.map((r) => mapRun(r)), cursor };
    },

    async childrenOf(runId) {
      const rows = await sql.query<RunRecord>(`SELECT * FROM ${t.run} WHERE parent_run_id = $1`, [
        runId,
      ]);
      return rows.map((r) => mapRun(r));
    },

    async runStats() {
      const rows = await sql.query<{ status: RunStatus; n: number }>(
        `SELECT status, count(*)::int AS n FROM ${t.run} GROUP BY status`,
      );
      const stats = zeroRunStats();
      for (const r of rows) stats[r.status] = r.n;
      return stats;
    },

    async orphanedRuns(max) {
      const rows = await sql.query<{ id: string }>(
        `SELECT id FROM (
           -- crash-stranded: non-terminal, off the queue, no pending timer to wake it
           SELECT r.id, r.seq FROM ${t.run} r
           WHERE r.status IN ${RECONCILABLE}
             AND NOT EXISTS (SELECT 1 FROM ${t.job} j WHERE j.run_id = r.id)
             AND NOT EXISTS (SELECT 1 FROM ${t.timer} tm WHERE tm.run_id = r.id)
           UNION
           -- lost parent-wake: awaiting_child parent whose child already finished
           SELECT r.id, r.seq FROM ${t.run} r
           WHERE r.status = 'awaiting_child'
             AND NOT EXISTS (SELECT 1 FROM ${t.job} j WHERE j.run_id = r.id)
             AND EXISTS (
               SELECT 1 FROM ${t.run} c
               WHERE c.parent_run_id = r.id AND c.status IN ${TERMINAL}
             )
           UNION
           -- orphaned child: non-terminal, but its parent terminally failed/canceled
           SELECT r.id, r.seq FROM ${t.run} r
           JOIN ${t.run} p ON p.id = r.parent_run_id
           WHERE r.status NOT IN ${TERMINAL} AND p.status IN ${NON_SUCCESS_TERMINAL}
         ) q
         ORDER BY q.seq
         LIMIT $1`,
        [max],
      );
      return rows.map((r) => r.id);
    },

    retryRun(runId) {
      return sql.tx(async (tx) => {
        const rows = await tx.query(
          `UPDATE ${t.run} SET status = 'pending', error = NULL WHERE id = $1 AND status = 'failed' RETURNING 1`,
          [runId],
        );
        if (!rows[0]) return { retried: false };
        await enqueueStmt(tx, t, runId); // ok step memos untouched → replay skips them
        return { retried: true };
      });
    },

    async upsertCron(spec) {
      // Keep the existing next_run_at on re-register so a redeploy doesn't reset schedule timing.
      await sql.query(
        `INSERT INTO ${t.cron}
           (name, schedule, flow_name, flow_version, input, overlap, next_run_at)
         VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
         ON CONFLICT (name) DO UPDATE SET
           schedule = EXCLUDED.schedule,
           flow_name = EXCLUDED.flow_name,
           flow_version = EXCLUDED.flow_version,
           input = EXCLUDED.input,
           overlap = EXCLUDED.overlap`,
        [
          spec.name,
          spec.schedule,
          spec.flowName,
          spec.flowVersion,
          j(spec.input),
          spec.overlap ?? "allow",
          spec.nextRunAt,
        ],
      );
    },

    async dueCrons(now, max) {
      const rows = await sql.query<{
        name: string;
        schedule: string;
        flow_name: string;
        flow_version: number;
        input: unknown;
        overlap: "allow" | "skip";
        next_run_at: Date;
        last_run_at: Date | null;
      }>(
        `SELECT * FROM ${t.cron} WHERE next_run_at <= $1::timestamptz ORDER BY next_run_at LIMIT $2`,
        [now, max],
      );
      return rows.map((r) => ({
        name: r.name,
        schedule: r.schedule,
        flowName: r.flow_name,
        flowVersion: r.flow_version,
        input: r.input,
        overlap: r.overlap,
        nextRunAt: r.next_run_at,
        lastRunAt: r.last_run_at ?? undefined,
      }));
    },

    async advanceCron(name, expectedNextRunAt, nextRunAt, lastRunAt) {
      const rows = await sql.query(
        `UPDATE ${t.cron} SET next_run_at = $3, last_run_at = $4
         WHERE name = $1 AND next_run_at = $2 RETURNING 1`,
        [name, expectedNextRunAt, nextRunAt, lastRunAt],
      );
      return rows.length > 0;
    },
  };
};
