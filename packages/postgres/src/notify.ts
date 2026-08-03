import { setTimeout as delay } from "node:timers/promises";
import { type Wakeup, createLocalWakeup } from "@iterativeflow/core/backend";
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
const progressChannel = (schema: string): string => `${schema}_progress`;

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

/**
 * DDL for the OPT-IN live-progress trigger — a per-row NOTIFY on the `event` table carrying
 * `{ runId, type }`. It rides the already-opt-in event log: a deployment with events off has no rows
 * to fire on, so it pays nothing. Install it ONLY on a dashboard host (never a worker pod) — see the
 * progress-push spec. The payload is tiny (id + type); an observer reads the full row by id.
 */
export const progressDdl = (schema = "workflow"): string => {
  const t = tables(schema);
  const q = `"${schema}"`;
  return `
CREATE OR REPLACE FUNCTION ${q}.if_notify_progress() RETURNS trigger AS $$
BEGIN PERFORM pg_notify('${progressChannel(schema)}',
  json_build_object('runId', NEW.run_id, 'type', NEW.type)::text); RETURN NULL; END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS if_progress ON ${t.event};
CREATE TRIGGER if_progress AFTER INSERT ON ${t.event}
  FOR EACH ROW EXECUTE FUNCTION ${q}.if_notify_progress();
`;
};

/** Install the opt-in progress trigger. Run once (idempotent) on a dashboard host, after {@link applySchema}. */
export const applyProgressTrigger = async (sql: Sql, schema = "workflow"): Promise<void> => {
  await sql.query(progressDdl(schema));
};

/** One live-progress event pushed by the `progress` channel — the observer reads the full row by id. */
export interface ProgressEvent {
  runId: string;
  type: string;
}

/** @internal */
export type ListenerState = "idle" | "listening" | "reconnecting" | "stopped";

/** The shared LISTEN connection: a completion {@link Wakeup} plus a dispatch `waitForWork`. */
export interface PgListener {
  /** Completion push for `result()` — pass as `createPgBackend(sql, { wakeup })`. */
  readonly wakeup: Wakeup;
  /** Dispatch push for the worker loop — pass as `engine.run({ waitForWork })`. Resolves on an
   *  enqueue notify or after `timeoutMs`. */
  waitForWork(timeoutMs: number): Promise<void>;
  /**
   * Live progress for one run as an async iterator — yields `{ runId, type }` per event as it lands,
   * across processes. Requires {@link applyProgressTrigger} installed. Break the loop (or call
   * `.return()`) to unsubscribe. Opt-in: costs nothing unless something is watching.
   */
  watch(runId: string): AsyncIterableIterator<ProgressEvent>;
  /** Progress for ALL runs (fleet view) — filter client-side. Returns an unsubscribe function. */
  onProgress(cb: (ev: ProgressEvent) => void): () => void;
  /** Open the LISTEN connection and start dispatching notifications. */
  start(): void;
  /** Stop listening and release the connection. */
  close(): Promise<void>;
  /** Current connection state — `listening` once LISTEN is live, `reconnecting` on backoff. */
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
  const progress = progressChannel(schema);

  // Completion side is exactly a local wakeup keyed by runId; the `done` notify feeds its `signal`.
  const completion = createLocalWakeup();
  const workWaiters = new Set<() => void>();
  const progressByRun = new Map<string, Set<(ev: ProgressEvent) => void>>();
  const progressAll = new Set<(ev: ProgressEvent) => void>();
  const emitProgress = (ev: ProgressEvent): void => {
    const subs = progressByRun.get(ev.runId);
    if (subs) for (const s of [...subs]) s(ev);
    for (const s of [...progressAll]) s(ev);
  };
  // Latch (graphile's `nudge`): a wake that arrives while the loop is mid-tick — with no waiter
  // registered — is remembered, so the next `waitForWork` returns immediately instead of stalling a
  // full tick. Closes the edge-trigger gap without a thundering herd (SKIP LOCKED does the rest).
  let pendingWake = false;

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

  const start = (): void => {
    if (loop) return;
    abort = new AbortController();
    const signal = abort.signal;
    loop = (async () => {
      let attempt = 0;
      while (!signal.aborted) {
        if (attempt > 0) state = "reconnecting"; // "listening" is set below once LISTEN succeeds
        let client: PoolClient | null = null;
        try {
          client = await pool.connect();
          if (signal.aborted) break;
          const conn = client;
          // A PERSISTENT error handler that outlives teardown: Aurora Serverless v2 drops idle
          // connections (57P01 on scale/failover), and if `release(true)` destroys the socket with
          // no 'error' listener left, Node turns that into an unhandled 'error' that crashes the
          // process. So we never `removeAllListeners()` (which would strip it) — this handler just
          // resolves `closed` and swallows any later teardown error. (v1's listen-loop bug.)
          const closed = new Promise<void>((resolve) => {
            conn.on("notification", (msg) => {
              if (msg.channel === wake) fireWork();
              else if (msg.channel === done && msg.payload) void completion.signal(msg.payload);
              else if (msg.channel === progress && msg.payload) {
                try {
                  emitProgress(JSON.parse(msg.payload) as ProgressEvent);
                } catch {
                  // a malformed payload is a dropped progress event, never a correctness issue
                }
              }
            });
            conn.on("error", () => resolve());
            conn.once("end", () => resolve());
          });
          await conn.query(`LISTEN "${wake}"`);
          await conn.query(`LISTEN "${done}"`);
          await conn.query(`LISTEN "${progress}"`);
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
          try {
            client?.release(true); // keeps the persistent 'error' handler → no unhandled teardown crash
          } catch {
            // already released
          }
        }
        if (signal.aborted) break;
        await delay(backoffMs(attempt++), undefined, { signal }).catch(() => undefined);
      }
      state = "stopped";
    })();
  };

  return {
    // Completion push: `signal` is the local fast path (executor's terminal write); a run completing
    // in ANOTHER process reaches us via the `done` trigger feeding `completion.signal` above.
    wakeup: completion,
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
    onProgress(cb) {
      progressAll.add(cb);
      return () => progressAll.delete(cb);
    },
    watch(runId) {
      const buffer: ProgressEvent[] = [];
      let waiting: ((r: IteratorResult<ProgressEvent>) => void) | null = null;
      let closed = false;
      const subs = progressByRun.get(runId) ?? new Set<(ev: ProgressEvent) => void>();
      progressByRun.set(runId, subs);
      const push = (ev: ProgressEvent): void => {
        if (waiting) {
          const w = waiting;
          waiting = null;
          w({ value: ev, done: false });
        } else buffer.push(ev);
      };
      subs.add(push);
      const stop = (): void => {
        closed = true;
        subs.delete(push);
        if (subs.size === 0) progressByRun.delete(runId);
      };
      return {
        [Symbol.asyncIterator]() {
          return this;
        },
        next() {
          if (buffer.length > 0) return Promise.resolve({ value: buffer.shift()!, done: false });
          if (closed) return Promise.resolve({ value: undefined, done: true });
          return new Promise<IteratorResult<ProgressEvent>>((resolve) => {
            waiting = resolve;
          });
        },
        return() {
          stop();
          if (waiting) {
            const w = waiting;
            waiting = null;
            w({ value: undefined, done: true });
          }
          return Promise.resolve({ value: undefined, done: true });
        },
      };
    },
    start,
    async close() {
      abort?.abort();
      if (loop) await loop.catch(() => undefined);
      loop = null;
      abort = null;
      state = "idle";
      fireWork(); // release the worker loop's waiter; result() waiters self-time-out then re-read
    },
    state: () => state,
  };
};
