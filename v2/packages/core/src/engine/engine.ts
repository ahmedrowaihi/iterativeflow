import type { IdGen } from "#id";
import type { Backend } from "#ports/outbox";
import type { EnqueueOpts } from "#ports/queue";
import type { Page, RunFilter, RunPage, RunSnapshot, RunStatus } from "#types";
import { type Clock, systemClock } from "#engine/context";
import { type DriftPolicy, type RetryPolicy, type TickResult } from "#engine/executor";
import {
  type AnyFlow,
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
  type SubmitItem,
  type SubmitOpts as WorkerSubmitOpts,
  type SweepResult,
  cancelRun,
  reconcile,
  result,
  retryRun,
  serverlessTick,
  signalRun,
  submit,
  submitMany,
  tickOnce,
} from "#engine/worker";

/** Defaults the engine applies to every worker cycle, so callers don't repeat them. */
export interface EngineOpts {
  batchMax?: number;
  leaseMs?: number;
  retry?: RetryPolicy;
  observe?: ObserveOpts;
  id?: IdGen;
  now?: Clock;
  /** Reject a submit whose JSON input exceeds this many bytes (a runaway-payload guard). */
  maxPayloadBytes?: number;
  driftPolicy?: DriftPolicy;
}

const byteSize = (value: unknown): number =>
  value === undefined ? 0 : new TextEncoder().encode(JSON.stringify(value)).length;

/** Options for the resident worker loop. */
export interface RunLoopOpts {
  /** Claim-cycle cadence (ms). Default 200. */
  tickMs?: number;
  /** Reconcile + cron cadence (ms) — the slower maintenance sweep. Default 5000. */
  maintenanceMs?: number;
  /**
   * Optional push seam: block up to `tickMs`, returning early when work is enqueued. Provide a
   * NOTIFY-backed waiter (e.g. `createPgListener(...).waitForWork`) to dispatch on enqueue instead
   * of waiting out the poll tick. Omit for pure polling. `tickMs` is always the backstop.
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
    flow: Flow<I, O, S>,
    input: I,
    opts?: SubmitOpts,
  ): Promise<RunHandle<O, S>>;
  submitMany<I>(items: readonly SubmitItem<I>[]): Promise<string[]>;
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

  registerCron<I>(def: CronDef<I>): Promise<void>;

  /** One worker cycle: drain due timers, then claim + execute a batch. */
  tick(): Promise<TickResult[]>;
  /** Re-enqueue crash-stranded runs. Run on a slow cadence (or via {@link Engine.run}). */
  reconcile(): Promise<number>;
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

type SubmitOpts = WorkerSubmitOpts & EnqueueOpts;

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
      return submit(backend, flow, input, o);
    },
    submitMany: async (items) => {
      for (const it of items) guard(it.input);
      return submitMany(backend, items);
    },
    signal: (runId: string, name: string, payload: unknown, o?: { idempotencyKey?: string }) =>
      signalRun(backend, runId, name, payload, o),
    cancel: (runId) => cancelRun(backend, runId),
    retry: (runId) => retryRun(backend, runId),
    result: (runId, o) => result(backend, runId, o),

    status: (runId) => backend.store.loadRun(runId),
    listRuns: (filter, page) => backend.store.listRuns(filter, page),
    health: () => backend.store.runStats(),

    registerCron: (def) => registerCron(backend, def, clock),

    tick: () => tickOnce(backend, reg, tickOpts),
    reconcile: () => reconcile(backend, { max: tickOpts.batchMax }),
    runCrons: () => runDueCrons(backend, clock),
    serverlessTick: () => serverlessTick(backend, reg, tickOpts),

    run(loop) {
      const tickMs = loop?.tickMs ?? 200;
      const maintenanceMs = loop?.maintenanceMs ?? 5_000;
      const waitForWork = loop?.waitForWork;
      const stop = new AbortController();
      const { signal } = stop;
      const onTickError = (err: unknown): void => opts.observe?.metrics?.tickError?.(err);
      const sleep = (ms: number): Promise<void> =>
        new Promise((r) => {
          const t = setTimeout(r, ms);
          signal.addEventListener("abort", () => (clearTimeout(t), r()), { once: true });
        });
      // Push-aware claim loop: tick, then wait up to tickMs — returning early when `waitForWork`
      // reports an enqueue. With no push seam it degrades to a fixed poll every tickMs.
      const tickLoop = (async () => {
        while (!signal.aborted) {
          await tickOnce(backend, reg, tickOpts).catch(onTickError);
          if (signal.aborted) break;
          await (waitForWork ? waitForWork(tickMs).catch(onTickError) : sleep(tickMs));
        }
      })();
      const maintenance = setInterval(() => {
        if (signal.aborted) return;
        void reconcile(backend, { max: tickOpts.batchMax }).catch(onTickError);
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
