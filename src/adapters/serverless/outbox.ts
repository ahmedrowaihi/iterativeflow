import { sql } from "drizzle-orm";
import type { WorkflowDb } from "../../storage/db";
import type { TxEnqueue } from "../../storage/drizzle";
import { rowsOf } from "../../storage/drizzle/types";
import { ts } from "../../util/sql-params";

const DEFAULT_TABLE = "iterativeflow_wake_outbox";

/** Configuration shared by the outbox enqueue, drain, and table helpers. */
export interface WakeOutboxOpts {
  /** Fully-qualified table name (schema-qualified if needed). Default `iterativeflow_wake_outbox`. */
  table?: string;
}

/**
 * Create the wake-outbox table if it does not exist. Run once at deploy time
 * (or from a migration). The table is the serverless queue: rows are inserted
 * transactionally with a run's suspend/start and drained by an external trigger.
 */
export const createWakeOutboxTable = async (
  db: WorkflowDb,
  opts?: WakeOutboxOpts,
): Promise<void> => {
  const name = opts?.table ?? DEFAULT_TABLE;
  const table = sql.identifier(name);
  await db.execute(sql`
    CREATE TABLE IF NOT EXISTS ${table} (
      run_id text PRIMARY KEY,
      run_at timestamptz NOT NULL DEFAULT now(),
      priority integer,
      enqueued_at timestamptz NOT NULL DEFAULT now()
    )
  `);
  await db.execute(sql`
    CREATE INDEX IF NOT EXISTS ${sql.identifier(`${name}_due_idx`)}
    ON ${table} (run_at)
  `);
};

/**
 * A {@link TxEnqueue} that records a wake in the outbox instead of a graphile
 * job. Runs inside the run's suspend/start transaction, so the wake commits
 * atomically with the run state — no dual write. One row per run (`run_id` is
 * the key); re-enqueueing a run replaces its `run_at`, mirroring graphile's
 * `job_key` replace semantics.
 */
export const createOutboxEnqueue = (opts?: WakeOutboxOpts): TxEnqueue => {
  const table = sql.identifier(opts?.table ?? DEFAULT_TABLE);
  return async (tx, job, enqueueOpts) => {
    const runAt = enqueueOpts?.runAt ? ts(enqueueOpts.runAt) : sql`now()`;
    await tx.execute(sql`
      INSERT INTO ${table} (run_id, run_at, priority)
      VALUES (${job.runId}, ${runAt}, ${enqueueOpts?.priority ?? null})
      ON CONFLICT (run_id) DO UPDATE
        SET run_at = EXCLUDED.run_at, priority = EXCLUDED.priority, enqueued_at = now()
    `);
  };
};

/** Inputs to {@link drainDueWakes}. */
export interface DrainOpts extends WakeOutboxOpts {
  /** Wake instant to compare against `run_at`. Pass a fixed clock in tests. */
  now: Date;
  /** Max runs to claim per drain. Default 100. */
  limit?: number;
}

/**
 * Claim every run whose wake is due (`run_at <= now`) and remove it from the
 * outbox, returning the run ids for the host to pass to `engine.handleRun`.
 *
 * Claim-by-delete: a crash between drain and `handleRun` leaves the run in a
 * non-terminal `workflow.*` state, which the reconciler cron re-enqueues — that
 * is the at-least-once recovery path, so the outbox itself stays simple.
 * `handleRun`'s claim makes a double-drain harmless (the second attempt is lost).
 */
export const drainDueWakes = async (db: WorkflowDb, opts: DrainOpts): Promise<string[]> => {
  const table = sql.identifier(opts.table ?? DEFAULT_TABLE);
  const result = await db.execute(sql`
    DELETE FROM ${table}
    WHERE run_id IN (
      SELECT run_id FROM ${table}
      WHERE run_at <= ${ts(opts.now)}
      ORDER BY priority NULLS LAST, run_at
      LIMIT ${opts.limit ?? 100}
      FOR UPDATE SKIP LOCKED
    )
    RETURNING run_id
  `);
  return rowsOf<{ run_id: string }>(result).map((r) => r.run_id);
};
