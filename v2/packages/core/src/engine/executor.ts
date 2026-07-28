import { type IdGen, newId } from "#id";
import type { Backend, Outbox } from "#ports/outbox";
import type { Lease } from "#ports/queue";
import { isTerminal } from "#status";
import type { DriftPolicy, FlowError, SuspendStatus, TerminalOutcome } from "#types";
import { cancelDescendants, cancelRun } from "#engine/cancel";
import { type Clock, type SuspendHolder, makeCtx, systemClock } from "#engine/context";
import { type FlowRegistry, flowKey } from "#engine/flow";
import { type EventType, type ObserveOpts, makeObserver } from "#engine/observe";
import {
  AwaitChildSignal,
  AwaitSignalSignal,
  CodedError,
  FlowDriftError,
  SleepSignal,
  StepFailedError,
  isControlSignal,
} from "#engine/signals";

/** How a run retries after a (non-terminal) throw. All tunable per deployment. */
export interface RetryPolicy {
  /** Max invocations before the run is failed terminally. */
  maxAttempts: number;
  /** First backoff delay; doubles each attempt up to `maxDelayMs`. */
  baseDelayMs: number;
  /** Ceiling for the exponential backoff. */
  maxDelayMs: number;
}

/** The retry policy applied when a deployment injects none. */
export const defaultRetry: RetryPolicy = {
  maxAttempts: 10,
  baseDelayMs: 1_000,
  maxDelayMs: 60_000,
};

/** The outcome status of one tick on a run. */
export type TickStatus =
  | "done"
  | "failed"
  | "sleeping"
  | "awaiting_child"
  | "awaiting_signal"
  | "retrying"
  | "gone"
  | "already_terminal"
  | "unknown_flow"
  | "flow_drift"
  | "canceled";

/**
 * What one tick did with a run. On a failure, transient retry, or drift it also carries the error
 * (and, for a drift, the cursor key it drifted at), so a driver (e.g. a serverless `SweepResult`
 * consumer) can log or route WHY a run failed / is retrying / drifted without reading the store.
 */
export interface TickResult {
  runId: string;
  status: TickStatus;
  error?: FlowError;
  cursorKey?: string;
}

export type { DriftPolicy } from "#types";

export interface TickOpts {
  now?: Clock;
  retry?: RetryPolicy;
  id?: IdGen;
  observe?: ObserveOpts;
  driftPolicy?: DriftPolicy;
}

const toFlowError = (e: unknown): FlowError => {
  if (e instanceof CodedError) return { code: e.code, message: e.message, stack: e.stack };
  if (e instanceof Error) return { code: e.name || "ERROR", message: e.message, stack: e.stack };
  return { code: "ERROR", message: String(e) };
};

const backoff = (attempt: number, p: RetryPolicy, now: Date): Date =>
  new Date(now.getTime() + Math.min(p.baseDelayMs * 2 ** (attempt - 1), p.maxDelayMs));

/**
 * Execute one claimed run to its next durable boundary (completion, suspend, or retry) and
 * release the lease. Idempotent across crashes: a re-claim re-invokes the flow and memoized
 * ctx calls short-circuit, so only un-run work executes again.
 */
export const runTick = async (
  backend: Backend,
  flows: FlowRegistry,
  lease: Lease,
  opts: TickOpts = {},
): Promise<TickResult> => {
  const now = opts.now ?? systemClock;
  const retry = opts.retry ?? defaultRetry;
  const id = opts.id ?? newId;
  const obs = makeObserver(opts.observe);
  const { store, queue, wakeup } = backend;

  const snap = await store.loadRun(lease.runId);
  if (!snap) {
    await queue.ack(lease, { now: now() });
    return { runId: lease.runId, status: "gone" };
  }
  if (isTerminal(snap.run.status)) {
    await queue.ack(lease, { now: now() });
    return { runId: snap.run.id, status: "already_terminal" };
  }
  const run = snap.run;

  const res = (status: TickStatus, extra?: Omit<TickResult, "runId" | "status">): TickResult => ({
    runId: run.id,
    status,
    ...extra,
  });

  const finish = async (
    status: "done" | "failed",
    outcome: TerminalOutcome,
    event: EventType,
    meta?: Record<string, unknown>,
  ): Promise<TickResult> => {
    await store.markTerminal(run.id, outcome);
    // cancelDescendants is idempotent and no-ops when childrenOf is empty, so a childless failure
    // costs one empty query on the rare failure path — cheaper than scanning every tick's memo.
    if (status === "failed") await cancelDescendants(backend, run.id);
    await obs.event(event, run.id, now(), meta);
    obs.metrics.runSettled?.(run.id, status);
    if (run.parentRunId) {
      const remaining = await store.arriveAtJoin(run.parentRunId);
      if (status !== "done" || (remaining !== undefined && remaining <= 0)) {
        await queue.enqueue(run.parentRunId);
      }
    }
    // Wake any result(run.id) waiter now the run is terminal — the local push fast path (a
    // NOTIFY-backed wakeup also nudges other processes; poll backstops either way).
    await wakeup.signal(run.id);
    await queue.ack(lease, { now: now() });
    return res(status, outcome.status === "failed" ? { error: outcome.error } : undefined);
  };

  const suspend = async (
    status: SuspendStatus,
    tickStatus: TickStatus,
    fx?: Outbox,
    extra?: Omit<TickResult, "runId" | "status">,
  ): Promise<TickResult> => {
    await store.suspendRun(run.id, status, fx);
    await obs.event("run.suspended", run.id, now(), { status });
    obs.metrics.runSuspended?.(run.id, status);
    await queue.ack(lease, { now: now() });
    return res(tickStatus, extra);
  };

  // The deployed code can't advance this run yet — the flow isn't registered (`unknown_flow`) or its
  // shape drifted under it (`flow_drift`). Park and re-check on a flat delay; a redeploy or version
  // bump recovers it. (The dead-letter cap still bounds a permanently-stuck run.)
  const parkForRedeploy = (
    tickStatus: TickStatus,
    extra?: Omit<TickResult, "runId" | "status">,
  ): Promise<TickResult> =>
    suspend(
      "retrying",
      tickStatus,
      { timers: [{ runId: run.id, fireAt: new Date(now().getTime() + retry.baseDelayMs) }] },
      extra,
    );

  const flow = flows.get(flowKey(run.name, run.version));
  if (!flow) return parkForRedeploy("unknown_flow");

  // Structured concurrency, crash-safe: a child never outlives its parent's non-success termination.
  // The push cascade (cancelDescendants) may not have reached this child if a worker died mid-cascade;
  // this pull check finishes the job on the child's next dispatch.
  if (run.parentRunId) {
    const parent = await store.loadRunRow(run.parentRunId);
    if (parent?.status === "failed" || parent?.status === "canceled") {
      await cancelRun(backend, run.id);
      await queue.ack(lease, { now: now() });
      return res("canceled");
    }
  }

  const attempt = await store.markRunning(run.id);
  // Dead-letter cap: markRunning bumps attempts on EVERY claim, so a step that crashes the
  // worker (uncatchable — the catch below never runs) would otherwise re-claim forever. Once
  // attempts pass the cap, fail terminally without executing, bounding the poison pill.
  if (attempt > retry.maxAttempts) {
    return finish(
      "failed",
      {
        status: "failed",
        error: {
          code: "RUN_ATTEMPTS_EXHAUSTED",
          message: `run exceeded ${retry.maxAttempts} attempts`,
        },
      },
      "run.failed",
      { code: "RUN_ATTEMPTS_EXHAUSTED" },
    );
  }
  if (attempt === 1) {
    await obs.event("run.started", run.id, now());
    obs.metrics.runStarted?.(run.id);
  }

  // `snap` was loaded after the claim, so it already holds every durable step + signal; the
  // exclusive lease means nothing else writes them mid-tick. No second load needed.
  const suspendState: SuspendHolder = {};
  const ctx = makeCtx({
    backend,
    snap,
    attempt,
    now,
    id,
    obs,
    signals: flow.signals,
    maxFanOut: flow.policy?.maxFanOut,
    maxDepth: flow.policy?.maxDepth,
    suspend: suspendState,
  });

  try {
    const output = await flow.run(ctx, run.input);
    // The body returned — but if it caught and swallowed a suspend without issuing another ctx call,
    // honour the suspend instead of completing at the wrong point.
    if (suspendState.signal) throw suspendState.signal;
    return finish("done", { status: "done", output }, "run.completed");
  } catch (e) {
    if (e instanceof SleepSignal) {
      return suspend("sleeping", "sleeping", { timers: [{ runId: run.id, fireAt: e.wakeAt }] });
    }
    if (e instanceof AwaitChildSignal) return suspend("awaiting_child", "awaiting_child");
    if (e instanceof AwaitSignalSignal) return suspend("awaiting_signal", "awaiting_signal");
    if (isControlSignal(e)) throw e; // future signals must be handled explicitly

    if (e instanceof FlowDriftError) {
      if ((flow.policy?.drift ?? opts.driftPolicy ?? "park") === "park") {
        return parkForRedeploy("flow_drift", { cursorKey: e.cursorKey });
      }
      const err = toFlowError(e);
      return finish("failed", { status: "failed", error: err }, "run.failed", { error: err });
    }

    if (attempt < retry.maxAttempts && !(e instanceof StepFailedError)) {
      return suspend(
        "retrying",
        "retrying",
        { timers: [{ runId: run.id, fireAt: backoff(attempt, retry, now()) }] },
        { error: toFlowError(e) },
      );
    }

    const error = toFlowError(e);
    return finish("failed", { status: "failed", error }, "run.failed", { error });
  }
};
