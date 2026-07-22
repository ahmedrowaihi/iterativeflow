import { type IdGen, newId } from "#id";
import type { Backend, Outbox } from "#ports/outbox";
import type { Lease } from "#ports/queue";
import { isTerminal } from "#status";
import type { FlowError, RunRow, SuspendStatus, TerminalOutcome } from "#types";
import { type Clock, makeCtx, systemClock } from "#engine/context";
import { type FlowRegistry, flowKey } from "#engine/flow";
import { type EventType, type ObserveOpts, makeObserver } from "#engine/observe";
import {
  AwaitChildSignal,
  AwaitSignalSignal,
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
  maxDelayMs: number;
}

export const defaultRetry: RetryPolicy = {
  maxAttempts: 10,
  baseDelayMs: 1_000,
  maxDelayMs: 60_000,
};

/** What one tick did with the run — for worker metrics and tests. */
export type TickResult =
  | "done"
  | "failed"
  | "sleeping"
  | "awaiting_child"
  | "awaiting_signal"
  | "retrying"
  | "gone"
  | "already_terminal"
  | "unknown_flow";

export interface TickOpts {
  now?: Clock;
  retry?: RetryPolicy;
  id?: IdGen;
  observe?: ObserveOpts;
}

const toFlowError = (e: unknown): FlowError => {
  if (e instanceof StepFailedError) return { code: e.code, message: e.message, stack: e.stack };
  if (e instanceof Error) return { code: e.name || "ERROR", message: e.message, stack: e.stack };
  return { code: "ERROR", message: String(e) };
};

const parentWake = (run: RunRow): Outbox | undefined =>
  run.parentRunId ? { enqueue: [{ runId: run.parentRunId }] } : undefined;

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
    return "gone";
  }
  if (isTerminal(snap.run.status)) {
    await queue.ack(lease, { now: now() });
    return "already_terminal";
  }
  const run = snap.run;

  const finish = async (
    status: "done" | "failed",
    outcome: TerminalOutcome,
    event: EventType,
    meta?: Record<string, unknown>,
  ): Promise<TickResult> => {
    await store.markTerminal(run.id, outcome, parentWake(run));
    await obs.event(event, run.id, now(), meta);
    obs.metrics.runSettled?.(run.id, status);
    if (run.parentRunId) await wakeup.signal(run.parentRunId);
    await queue.ack(lease, { now: now() });
    return status;
  };

  const suspend = async (
    status: SuspendStatus,
    result: TickResult,
    fx?: Outbox,
  ): Promise<TickResult> => {
    await store.suspendRun(run.id, status, fx);
    await obs.event("run.suspended", run.id, now(), { status });
    obs.metrics.runSuspended?.(run.id, status);
    await queue.ack(lease, { now: now() });
    return result;
  };

  const flow = flows.get(flowKey(run.name, run.version));
  if (!flow) {
    // Unknown flow: park with a timer so `drainTimers` re-enqueues it and it re-checks the
    // registry — a deploy that (re)registers the flow recovers the run automatically.
    await suspend("retrying", "unknown_flow", {
      timers: [{ runId: run.id, fireAt: new Date(now().getTime() + retry.baseDelayMs) }],
    });
    return "unknown_flow";
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
  const ctx = makeCtx({ backend, snap, attempt, now, id, obs });

  try {
    const output = await flow.run(ctx, run.input);
    return finish("done", { status: "done", output }, "run.completed");
  } catch (e) {
    if (e instanceof SleepSignal) {
      return suspend("sleeping", "sleeping", { timers: [{ runId: run.id, fireAt: e.wakeAt }] });
    }
    if (e instanceof AwaitChildSignal) return suspend("awaiting_child", "awaiting_child");
    if (e instanceof AwaitSignalSignal) return suspend("awaiting_signal", "awaiting_signal");
    if (isControlSignal(e)) throw e; // future signals must be handled explicitly

    if (attempt < retry.maxAttempts && !(e instanceof StepFailedError)) {
      return suspend("retrying", "retrying", {
        timers: [{ runId: run.id, fireAt: backoff(attempt, retry, now()) }],
      });
    }

    const error = toFlowError(e);
    return finish("failed", { status: "failed", error }, "run.failed", { error });
  }
};
