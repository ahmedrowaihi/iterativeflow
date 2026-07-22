import type { IdGen } from "#id";
import type { Backend } from "#ports/outbox";
import type { EnqueueOpts } from "#ports/queue";
import type { Page, RunFilter, RunPage, RunSnapshot, RunStatus } from "#types";
import { type Clock, systemClock } from "#engine/context";
import { type RetryPolicy, type TickResult } from "#engine/executor";
import { type AnyFlow, type Flow, registry } from "#engine/flow";
import type { ObserveOpts } from "#engine/observe";
import { type CronDef, registerCron, runDueCrons } from "#engine/schedule";
import {
  type RunResult,
  type SubmitItem,
  cancelRun,
  reconcile,
  result,
  retryRun,
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
}

const byteSize = (value: unknown): number =>
  value === undefined ? 0 : new TextEncoder().encode(JSON.stringify(value)).length;

/** Options for the resident worker loop. */
export interface RunLoopOpts {
  /** Claim-cycle cadence (ms). Default 200. */
  tickMs?: number;
  /** Reconcile + cron cadence (ms) — the slower maintenance sweep. Default 5000. */
  maintenanceMs?: number;
}

/**
 * The cohesive engine: one object bundling submission, run control, queries, cron, and the
 * worker loop over a single {@link Backend} + flow registry. This is the public surface most
 * apps use; the free functions it wraps stay available for fine-grained control.
 */
export interface Engine {
  readonly backend: Backend;

  submit<I>(flow: Flow<I, unknown>, input: I, opts?: SubmitOpts): Promise<string>;
  submitMany<I>(items: readonly SubmitItem<I>[]): Promise<string[]>;
  signal(
    runId: string,
    name: string,
    payload: unknown,
    opts?: { idempotencyKey?: string },
  ): Promise<boolean>;
  cancel(runId: string): Promise<void>;
  retry(runId: string): Promise<boolean>;
  result(runId: string, opts?: { timeoutMs?: number; pollMs?: number }): Promise<RunResult>;

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

  /** Start a resident worker loop (ticks + maintenance). Returns a stop function. */
  run(opts?: RunLoopOpts): () => Promise<void>;
}

type SubmitOpts = { idempotencyKey?: string; tags?: readonly string[] } & EnqueueOpts;

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
    signal: (runId, name, payload, o) => signalRun(backend, runId, name, payload, o),
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

    run(loop) {
      const tickMs = loop?.tickMs ?? 200;
      const maintenanceMs = loop?.maintenanceMs ?? 5_000;
      let stopped = false;
      const ticker = setInterval(() => {
        if (!stopped) void tickOnce(backend, reg, tickOpts);
      }, tickMs);
      const maintenance = setInterval(() => {
        if (stopped) return;
        void reconcile(backend, { max: tickOpts.batchMax });
        void runDueCrons(backend, clock);
      }, maintenanceMs);
      return async () => {
        stopped = true;
        clearInterval(ticker);
        clearInterval(maintenance);
      };
    },
  };
};
