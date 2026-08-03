import type { IdGen } from "#id";
import type { Backend } from "#ports/outbox";
import type { QueueDepth } from "#ports/queue";
import type { Page, RunFilter, RunPage, RunSnapshot, RunStatus } from "#types";
import { type Clock, systemClock } from "#engine/context";
import { type DriftPolicy, type RetryPolicy, type TickResult } from "#engine/executor";
import {
  type AnyFlow,
  type Contract,
  type Flow,
  type NoSignals,
  type SignalMap,
  type SignalName,
  type SignalPayload,
  registry,
} from "#engine/flow";
import type { ObserveOpts } from "#engine/observe";
import { type CronDef, registerCron, runDueCrons } from "#engine/schedule";
import {
  type RunHandle,
  type RunResult,
  type SubmitSpec,
  type SubmitOpts,
  type SweepResult,
  cancelRun,
  prune,
  reconcile,
  result,
  retryRun,
  serverlessTick,
  signalRun,
  submit,
  submitMany,
  tickOnce,
} from "#engine/worker";

/** A liveness snapshot: dispatch-queue health plus per-status run counts. */
export interface Liveness {
  queue: QueueDepth;
  runs: Record<RunStatus, number>;
}

/** Signal-aware sleep on the Web-standard `setTimeout` — resolves after `ms`, rejects if `signal`
 *  aborts. Removes its abort listener on resolve so a sustained drain doesn't leak one per tick. */
const delay = (ms: number, opts?: { signal?: AbortSignal }): Promise<void> =>
  new Promise((resolve, reject) => {
    const signal = opts?.signal;
    if (signal?.aborted) return reject(signal.reason);
    const onAbort = (): void => {
      clearTimeout(timer);
      reject(signal!.reason);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });

/** Defaults the engine applies to every worker cycle, so callers don't repeat them. */
export interface EngineOpts {
  /** Max runs claimed per worker cycle. Default 20. */
  batchMax?: number;
  /**
   * How long a claimed run's lease is held before another worker may re-claim it. There is no
   * heartbeat, so it must exceed the longest step's wall-clock duration or a slow run gets
   * concurrently re-executed; and with `serverlessTick` it must be ≤ the invocation timeout or a
   * batch tail is stranded until the oversized lease expires. Default 30000.
   *
   * Lease expiry is judged against each worker's own clock (not the database's), so a multi-worker
   * pool must keep its clocks NTP-synced and size `leaseMs` to absorb the residual skew (≥ longest
   * step + max skew): a fast-clocked worker that reclaims a peer's still-running run early only
   * re-executes it (bounded by the exactly-once memo — never data loss), but wastes the work.
   * Single-worker and `serverlessTick` deployments have no peer, so no skew applies.
   */
  leaseMs?: number;
  /** Retry policy for a throwing (non-terminal) step. Defaults to {@link defaultRetry}. */
  retry?: RetryPolicy;
  /** Metrics callbacks + durable event-sink wiring. */
  observe?: ObserveOpts;
  /** Id generator for runs and lease tokens. Default {@link newId}. */
  id?: IdGen;
  /** Injectable clock for deterministic tests. Defaults to the wall clock. */
  now?: Clock;
  /** Reject a submit whose JSON input exceeds this many bytes (a runaway-payload guard). */
  maxPayloadBytes?: number;
  /** How a replay that detects flow-body drift resolves — `park` (default) or `fail`. */
  driftPolicy?: DriftPolicy;
  /**
   * Wall-clock bound (ms) on each cycle's DB poll (drain + claim), so a black-holed connection
   * can't silently freeze the resident loop on a dead socket — it rejects, gets logged, and
   * re-polls on a fresh pooled connection. Bounds the poll only, never step execution. Default
   * 30000; set `0` to disable (e.g. an in-memory backend that never hangs).
   */
  pollTimeoutMs?: number;
}

const byteSize = (value: unknown): number =>
  value === undefined ? 0 : new TextEncoder().encode(JSON.stringify(value)).length;

/** Options for the resident worker loop. */
export interface RunLoopOpts {
  /** Claim-cycle cadence (ms) when there's work — the busy/floor interval. Default 200. */
  tickMs?: number;
  /**
   * Idle backoff ceiling (ms). With no work, the claim interval grows geometrically from `tickMs`
   * toward this, so an idle worker stops hammering the DB; it snaps back to `tickMs` the moment a
   * claim returns work (and a full batch re-claims immediately). Default `tickMs × 8`. A push notify
   * interrupts the wait regardless, so raising this is free when a listener is wired.
   */
  maxIdleTickMs?: number;
  /** Reconcile + cron cadence (ms) — the slower maintenance sweep. Default 5000. */
  maintenanceMs?: number;
  /**
   * Override the dispatch-push waiter. By default the loop uses the backend's
   * {@link Queue.waitForWork} if it has one (e.g. a Postgres listener wired into the backend), so
   * you rarely set this. Provide it only to supply a custom push source. `tickMs` is the backstop.
   */
  waitForWork?: (timeoutMs: number) => Promise<void>;
}

/**
 * The cohesive engine: one object bundling submission, run control, queries, cron, and the
 * worker loop over a single {@link Backend} + flow registry. This is the public surface most
 * apps use; the free functions it wraps stay available for fine-grained control.
 */
export interface Engine {
  readonly backend: Backend;

  submit<I, O, S extends SignalMap = NoSignals>(
    flow: Flow<I, O, S> | Contract<I, O, S>,
    input: I,
    opts?: SubmitOpts,
  ): Promise<RunHandle<O, S>>;
  submitMany<I>(items: readonly SubmitSpec<I>[]): Promise<string[]>;
  signal<O = unknown, S extends SignalMap = NoSignals, K extends SignalName<S> = SignalName<S>>(
    handle: RunHandle<O, S> | string,
    name: K,
    payload: SignalPayload<S, K>,
    opts?: { idempotencyKey?: string },
  ): Promise<boolean>;
  cancel(runId: string): Promise<void>;
  retry(runId: string): Promise<boolean>;
  result<O = unknown>(
    runId: RunHandle<O> | string,
    opts?: { timeoutMs?: number; pollMs?: number },
  ): Promise<RunResult<O>>;

  /** The run + its step memo + signal inbox. `undefined` if the run is gone. */
  status(runId: string): Promise<RunSnapshot | undefined>;
  listRuns(filter: RunFilter, page: Page): Promise<RunPage>;
  /** Count of runs per status — the overview/health snapshot. */
  health(): Promise<Record<RunStatus, number>>;
  /**
   * Liveness probe for a k8s/readiness check: the dispatch backlog + oldest-claimable age (rising ⇒
   * workers can't keep up or are down) alongside the per-status run counts. Read-only.
   */
  liveness(): Promise<Liveness>;

  /**
   * Probe the backend; throw a clear error if the schema is missing or unreachable (unapplied
   * `applySchema`, or the wrong database). Call it at startup to fail a readiness probe cleanly
   * instead of looping on query errors. Checks that the schema responds, not that it matches a version.
   */
  check(): Promise<void>;

  /**
   * The earliest pending timer due (sleep / retry / cron), or `null` when none — the serverless
   * wake horizon. A self-scheduling driver arms a one-shot for this instant instead of polling on a
   * fixed cadence. Signals/child-joins wake by a push on submit/signal, so they are NOT covered.
   */
  nextWakeAt(): Promise<Date | null>;

  /**
   * The autoscaling backlog as of now — claimable jobs + due timers + due crons — as one count.
   * The number a KEDA `metrics-api` scaler (or a self-terminating serverless loop) reads: counting
   * due timers/crons, not just queued jobs, is what wakes a scaled-to-zero worker for a durable
   * `ctx.sleep` or a cron occurrence. `names` scopes it to a sharded worker's flows.
   */
  pendingWork(names?: readonly string[]): Promise<number>;

  registerCron<I>(def: CronDef<I>): Promise<void>;

  /** One worker cycle: drain due timers, then claim + execute a batch. */
  tick(): Promise<TickResult[]>;
  /** Re-enqueue crash-stranded runs. Run on a slow cadence (or via {@link Engine.run}). */
  reconcile(): Promise<number>;
  /**
   * Delete terminal runs older than `olderThanMs` (with their steps/signals/events), up to `limit`
   * (default 1000). Returns how many were deleted. Schedule this yourself — retention window is a
   * deployment policy, so it is not part of the worker loop. Repeat until it returns `< limit`.
   */
  prune(olderThanMs: number, limit?: number): Promise<number>;
  /** Fire every due cron once. */
  runCrons(): Promise<number>;
  /**
   * One full cycle for a scheduled invocation (crons + reconcile + drain + claim) — call this
   * from an EventBridge / cron Lambda. No resident process; every waiting run advances on the
   * next firing, so a durable `ctx.sleep` outlives any invocation timeout.
   */
  serverlessTick(): Promise<SweepResult>;

  /** Start a resident worker loop (ticks + maintenance). Returns a stop function. */
  run(opts?: RunLoopOpts): () => Promise<void>;
}

export const createEngine = (
  backend: Backend,
  flows: readonly AnyFlow[],
  opts: EngineOpts = {},
): Engine => {
  const reg = registry(flows);
  const now = opts.now;
  const tickOpts = {
    batchMax: opts.batchMax ?? 20,
    leaseMs: opts.leaseMs ?? 30_000,
    retry: opts.retry,
    observe: opts.observe,
    id: opts.id,
    now,
    driftPolicy: opts.driftPolicy,
    pollTimeoutMs: opts.pollTimeoutMs ?? 30_000,
  };
  const clock: Clock = now ?? systemClock;
  const cap = opts.maxPayloadBytes;
  const guard = (input: unknown): void => {
    if (cap !== undefined && byteSize(input) > cap) {
      throw new Error(`submit: input exceeds maxPayloadBytes (${cap})`);
    }
  };

  return {
    backend,

    submit: async (flow, input, o) => {
      guard(input);
      return submit(backend, flow, input, o, clock);
    },
    submitMany: async (items) => {
      for (const it of items) guard(it.input);
      return submitMany(backend, items, clock);
    },
    signal: (runId: string, name: string, payload: unknown, o?: { idempotencyKey?: string }) =>
      signalRun(backend, runId, name, payload, o),
    cancel: (runId) => cancelRun(backend, runId),
    retry: (runId) => retryRun(backend, runId),
    result: (runId, o) => result(backend, runId, o),

    status: (runId) => backend.store.loadRun(runId),
    listRuns: (filter, page) => backend.store.listRuns(filter, page),
    health: () => backend.store.runStats(),

    check: async () => {
      try {
        await backend.store.runStats();
      } catch (cause) {
        throw new Error(
          "iterativeflow: backend schema is missing or unreachable — run applySchema() and check the connection",
          { cause },
        );
      }
    },

    nextWakeAt: () => backend.timer.nextDueAt(clock()),
    pendingWork: async (names) => {
      const at = clock();
      const [depth, dueTimers, dueCrons] = await Promise.all([
        backend.queue.depth(at, names),
        backend.timer.dueCount(at, names),
        backend.store.dueCronCount(at, names),
      ]);
      return depth.claimable + dueTimers + dueCrons;
    },
    liveness: async () => {
      const [queue, runs] = await Promise.all([
        backend.queue.depth(clock()),
        backend.store.runStats(),
      ]);
      return { queue, runs };
    },

    registerCron: (def) => registerCron(backend, def, clock),

    tick: () => tickOnce(backend, reg, tickOpts),
    reconcile: () => reconcile(backend, { limit: tickOpts.batchMax }),
    prune: (olderThanMs, limit = 1000) =>
      prune(backend, { before: new Date(clock().getTime() - olderThanMs), limit }),
    runCrons: () => runDueCrons(backend, clock),
    serverlessTick: () => serverlessTick(backend, reg, tickOpts),

    run(loop) {
      const tickMs = loop?.tickMs ?? 200;
      const maxIdleMs = Math.max(tickMs, loop?.maxIdleTickMs ?? tickMs * 8);
      const maintenanceMs = loop?.maintenanceMs ?? 5_000;
      const batchMax = tickOpts.batchMax;
      // Dispatch push comes off the backend's queue by default (e.g. a pg listener); an explicit
      // override is still honored. Absent both, the loop polls.
      const waitForWork = loop?.waitForWork ?? backend.queue.waitForWork?.bind(backend.queue);
      const stop = new AbortController();
      const { signal } = stop;
      const onTickError = (err: unknown): void => opts.observe?.metrics?.tickError?.(err);
      // Self-tuning claim loop across the whole duty cycle: a FULL batch means more work is almost
      // certainly waiting, so re-claim immediately (saturated → max throughput); a PARTIAL batch
      // waits the floor `tickMs`; an EMPTY batch backs off geometrically toward `maxIdleMs` (idle →
      // stop hammering the DB). A push notify or the current timeout interrupts the wait.
      const tickLoop = (async () => {
        let idleMs = tickMs;
        while (!signal.aborted) {
          const results = await tickOnce(backend, reg, tickOpts).catch((e): TickResult[] => {
            onTickError(e);
            return [];
          });
          if (signal.aborted) break;
          idleMs = results.length > 0 ? tickMs : Math.min(idleMs * 2, maxIdleMs);
          if (results.length >= batchMax) {
            // Saturated — re-claim without an idle wait, but yield one macrotask so a synchronous
            // backend (memory) can't starve the maintenance sweep during a sustained drain.
            await delay(0, { signal }).catch(() => undefined);
            continue;
          }
          await (waitForWork
            ? waitForWork(idleMs).catch(onTickError)
            : delay(idleMs, { signal }).catch(() => undefined));
        }
      })();
      const maintenance = setInterval(() => {
        if (signal.aborted) return;
        void reconcile(backend, { limit: tickOpts.batchMax }).catch(onTickError);
        void runDueCrons(backend, clock).catch(onTickError);
      }, maintenanceMs);
      return async () => {
        stop.abort();
        clearInterval(maintenance);
        await tickLoop.catch(() => undefined);
      };
    },
  };
};
