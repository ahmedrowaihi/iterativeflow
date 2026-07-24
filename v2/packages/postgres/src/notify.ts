import type { Wakeup } from "@iterativeflow/core/backend";
import type { Pool, PoolClient } from "pg";
import { tables } from "#schema";
import type { Sql } from "#sql";

/**
 * Opt-in `LISTEN/NOTIFY` push for a resident/multi-pod Postgres deployment: two DB triggers
 * fire `pg_notify` when a run becomes claimable (dispatch) or terminates (completion), and a
 * single {@link createPgListener} connection turns those into instant local wakeups. It layers
 * over the poll-first default — a missed notify only costs a poll tick, never correctness — so
 * it stays off the serverless / RDS-Proxy path entirely (don't install the triggers there).
 *
 * The dispatch trigger fires on the `job` row itself, so it catches EVERY enqueue — including the
 * transactional-outbox ones (`ctx.invoke` spawn, `engine.signal`'s re-enqueue) that never pass
 * through `queue.enqueue`.
 */

const wakeChannel = (schema: string): string => `${schema}_wake`;
const doneChannel = (schema: string): string => `${schema}_done`;

/**
 * DDL for the NOTIFY triggers. `wake` is a per-STATEMENT trigger on the `job` insert (an enqueue is
 * always `INSERT … ON CONFLICT`, so it fires once per enqueue regardless of row count; a claim /
 * heartbeat is an `UPDATE` and never fires it) — graphile-worker moved to per-statement notify for
 * exactly this reason, and pg coalesces the identical empty payloads within a fan-out's transaction
 * into one delivery. `done` fires per-row when a run first reaches a terminal status (it needs the
 * run id in the payload). Idempotent.
 */
export const notifyDdl = (schema = "workflow"): string => {
  const t = tables(schema);
  const q = `"${schema}"`;
  return `
CREATE OR REPLACE FUNCTION ${q}.if_notify_wake() RETURNS trigger AS $$
BEGIN PERFORM pg_notify('${wakeChannel(schema)}', ''); RETURN NULL; END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ${q}.if_notify_done() RETURNS trigger AS $$
BEGIN PERFORM pg_notify('${doneChannel(schema)}', NEW.id); RETURN NULL; END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS if_wake ON ${t.job};
CREATE TRIGGER if_wake AFTER INSERT ON ${t.job}
  FOR EACH STATEMENT EXECUTE FUNCTION ${q}.if_notify_wake();

DROP TRIGGER IF EXISTS if_done ON ${t.run};
CREATE TRIGGER if_done AFTER UPDATE ON ${t.run}
  FOR EACH ROW
  WHEN (OLD.status IS DISTINCT FROM NEW.status AND NEW.status IN ('done', 'failed', 'canceled'))
  EXECUTE FUNCTION ${q}.if_notify_done();
`;
};

/** Install the NOTIFY triggers. Run once (idempotent), after {@link applySchema}. */
export const applyNotifyTriggers = async (sql: Sql, schema = "workflow"): Promise<void> => {
  await sql.query(notifyDdl(schema));
};

/** @internal */
export type ListenerState = "idle" | "listening" | "reconnecting" | "stopped";

/** The shared LISTEN connection: a completion {@link Wakeup} plus a dispatch `waitForWork`. */
export interface PgListener {
  /** Completion push for `result()` — pass as `createPgBackend(sql, { wakeup })`. */
  readonly wakeup: Wakeup;
  /** Dispatch push for the worker loop — pass as `engine.run({ waitForWork })`. Resolves on an
   *  enqueue notify or after `timeoutMs`. */
  waitForWork(timeoutMs: number): Promise<void>;
  /** Open the LISTEN connection and start dispatching notifications. */
  start(): void;
  /** Stop listening and release the connection. */
  close(): Promise<void>;
  state(): ListenerState;
}

export interface PgListenerOpts {
  /** Schema whose channels to listen on. Must match the backend's schema. Default `workflow`. */
  schema?: string;
}

const backoffMs = (attempt: number): number =>
  Math.min(30_000, 1000 * 2 ** Math.min(attempt, 5)) + Math.floor(Math.random() * 500);

/**
 * One `LISTEN` connection multiplexing both channels for a schema. `wakeup.wait(runId)` resolves
 * when the run's completion notify arrives (or on timeout); `waitForWork` resolves on any enqueue
 * notify (or timeout). Reconnects with backoff; on every (re)connect it releases all work waiters
 * so a wake missed while disconnected costs at most one poll, never a stall.
 */
export const createPgListener = (pool: Pool, opts: PgListenerOpts = {}): PgListener => {
  const schema = opts.schema ?? "workflow";
  const wake = wakeChannel(schema);
  const done = doneChannel(schema);

  const doneWaiters = new Map<string, Set<() => void>>();
  const workWaiters = new Set<() => void>();
  // Latch (graphile's `nudge`): a wake that arrives while the loop is mid-tick — with no waiter
  // registered — is remembered, so the next `waitForWork` returns immediately instead of stalling a
  // full tick. Closes the edge-trigger gap without a thundering herd (SKIP LOCKED does the rest).
  let pendingWake = false;

  const fireDone = (runId: string): void => {
    const set = doneWaiters.get(runId);
    if (!set) return;
    doneWaiters.delete(runId);
    for (const w of [...set]) w();
  };
  const fireWork = (): void => {
    if (workWaiters.size === 0) {
      pendingWake = true;
      return;
    }
    const all = [...workWaiters];
    workWaiters.clear();
    for (const w of all) w();
  };

  let state: ListenerState = "idle";
  let loop: Promise<void> | null = null;
  let abort: AbortController | null = null;

  const sleep = (ms: number, signal: AbortSignal): Promise<void> =>
    new Promise((resolve) => {
      if (signal.aborted) return resolve();
      const t = setTimeout(resolve, ms);
      signal.addEventListener("abort", () => (clearTimeout(t), resolve()), { once: true });
    });

  const start = (): void => {
    if (loop) return;
    abort = new AbortController();
    const signal = abort.signal;
    loop = (async () => {
      let attempt = 0;
      while (!signal.aborted) {
        state = attempt === 0 ? "listening" : "reconnecting";
        let client: PoolClient | null = null;
        try {
          client = await pool.connect();
          if (signal.aborted) break;
          const conn = client;
          const closed = new Promise<void>((resolve) => {
            conn.on("notification", (msg) => {
              if (msg.channel === wake) fireWork();
              else if (msg.channel === done && msg.payload) fireDone(msg.payload);
            });
            conn.once("error", () => resolve());
            conn.once("end", () => resolve());
          });
          await conn.query(`LISTEN "${wake}"`);
          await conn.query(`LISTEN "${done}"`);
          state = "listening";
          attempt = 0;
          fireWork(); // a wake may have arrived while we were disconnected — poll once now
          const release = (): void => {
            try {
              conn.release(true);
            } catch {
              // already gone
            }
          };
          signal.addEventListener("abort", release, { once: true });
          await closed;
          signal.removeEventListener("abort", release);
        } catch {
          // connect/LISTEN failed — fall through to backoff
        } finally {
          if (client) {
            try {
              client.removeAllListeners();
              client.release(true);
            } catch {
              // already released
            }
          }
        }
        if (signal.aborted) break;
        await sleep(backoffMs(attempt++), signal);
      }
      state = "stopped";
    })();
  };

  return {
    wakeup: {
      wait(runId, timeoutMs) {
        return new Promise<void>((resolve) => {
          const set = doneWaiters.get(runId) ?? new Set();
          doneWaiters.set(runId, set);
          let t: ReturnType<typeof setTimeout>;
          const settle = (): void => {
            clearTimeout(t);
            set.delete(settle);
            if (set.size === 0) doneWaiters.delete(runId);
            resolve();
          };
          set.add(settle);
          t = setTimeout(settle, timeoutMs);
        });
      },
      // Local fast path only; a completing run in another process wakes us via the `done` trigger.
      async signal(runId) {
        fireDone(runId);
      },
    },
    waitForWork(timeoutMs) {
      if (pendingWake) {
        pendingWake = false;
        return Promise.resolve();
      }
      return new Promise<void>((resolve) => {
        let t: ReturnType<typeof setTimeout>;
        const settle = (): void => {
          clearTimeout(t);
          workWaiters.delete(settle);
          resolve();
        };
        workWaiters.add(settle);
        t = setTimeout(settle, timeoutMs);
      });
    },
    start,
    async close() {
      abort?.abort();
      if (loop) await loop.catch(() => undefined);
      loop = null;
      abort = null;
      state = "idle";
      fireWork();
      for (const runId of [...doneWaiters.keys()]) fireDone(runId);
    },
    state: () => state,
  };
};
