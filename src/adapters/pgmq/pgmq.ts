import { sql } from "drizzle-orm";
import type { RunHandler } from "../../engine/scheduler";
import type { WorkflowDb } from "../../storage/db";
import type { TxEnqueue } from "../../storage/drizzle";
import { rowsOf } from "../../storage/drizzle/types";

const DEFAULT_QUEUE = "iterativeflow_wakes";
const DEFAULT_VT_SECONDS = 60;
const DEFAULT_QTY = 100;

/** Configuration shared by the pgmq enqueue and drain helpers. */
export interface PgmqOpts {
  /** Queue name. Default `iterativeflow_wakes`. */
  queue?: string;
}

/**
 * Create the pgmq queue (run once at deploy). Requires the `pgmq` extension:
 * `CREATE EXTENSION pgmq;`. Safe to call repeatedly — `pgmq.create` is a no-op
 * when the queue already exists.
 */
export const createPgmqQueue = async (db: WorkflowDb, opts?: PgmqOpts): Promise<void> => {
  await db.execute(sql`SELECT pgmq.create((${opts?.queue ?? DEFAULT_QUEUE})::text)`);
};

const delaySeconds = (runAt?: Date): number =>
  runAt ? Math.max(0, Math.ceil((runAt.getTime() - Date.now()) / 1000)) : 0;

/**
 * A {@link TxEnqueue} that records a wake as a pgmq message instead of a graphile
 * job. Runs inside the run's suspend/start transaction, so the wake commits
 * atomically with the run state. A `run_at` in the future becomes a pgmq
 * `delay`, so the message stays invisible until the wake is due.
 */
export const createPgmqEnqueue = (opts?: PgmqOpts): TxEnqueue => {
  const queue = opts?.queue ?? DEFAULT_QUEUE;
  return async (tx, job, enqueueOpts) => {
    await tx.execute(sql`
      SELECT pgmq.send(
        (${queue})::text,
        ${sql`json_build_object('runId', ${job.runId}::text)::jsonb`},
        (${delaySeconds(enqueueOpts?.runAt)})::integer
      )
    `);
  };
};

/** Options for {@link drainAndRunPgmq}. */
export interface DrainPgmqOpts extends PgmqOpts {
  /**
   * Visibility timeout (seconds): how long a read message stays invisible while
   * `handleRun` processes it. If the process crashes, the message reappears
   * after this window — the at-least-once recovery path. Default 60.
   */
  vt?: number;
  /** Max messages to read per drain. Default 100. */
  qty?: number;
}

interface WakeMessage {
  msg_id: number;
  message: { runId: string };
}

/**
 * One serverless tick over pgmq: read every visible wake, advance each run with
 * `engine.handleRun`, and delete the message on success. A message whose
 * `handleRun` throws is left to reappear after the visibility timeout —
 * `handleRun`'s claim makes the redelivery idempotent.
 */
export const drainAndRunPgmq = async (
  engine: RunHandler,
  db: WorkflowDb,
  opts?: DrainPgmqOpts,
): Promise<{ ran: string[] }> => {
  const queue = opts?.queue ?? DEFAULT_QUEUE;
  const read = await db.execute(sql`
    SELECT msg_id, message FROM pgmq.read(
      (${queue})::text,
      (${opts?.vt ?? DEFAULT_VT_SECONDS})::integer,
      (${opts?.qty ?? DEFAULT_QTY})::integer
    )
  `);
  const messages = rowsOf<WakeMessage>(read);

  const ran: string[] = [];
  for (const m of messages) {
    try {
      await engine.handleRun(m.message.runId);
    } catch {
      // Leave the message undeleted: pgmq makes it visible again after vt, and
      // handleRun's claim makes the redelivery idempotent.
      continue;
    }
    await db.execute(sql`SELECT pgmq.delete((${queue})::text, (${m.msg_id})::bigint)`);
    ran.push(m.message.runId);
  }
  return { ran };
};
