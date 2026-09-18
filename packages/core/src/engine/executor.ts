import { type IdGen, newId } from "#id";
import type { Backend, Outbox } from "#ports/outbox";
import type { Lease } from "#ports/queue";
import { isTerminal } from "#status";
import type { DriftPolicy, FlowError, SuspendStatus, TerminalOutcome } from "#types";
import { cancelDescendants, cancelRun } from "#engine/cancel";
import { type Clock, type SuspendHolder, makeCtx, systemClock } from "#engine/context";
import { type FlowRegistry, flowKey } from "#engine/flow";
import { type ObserveOpts, makeObserver } from "#engine/observe";
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

/** Renew a run's lease once less than this fraction of it remains: frequent enough that a long
 *  multi-step run never loses the lease, rare enough that quick steps don't each cost a heartbeat. */
const LEASE_RENEW_BELOW = 0.5;

const needsRenewal = (lease: Lease, leaseMs: number, at: number): boolean =>
  lease.expiresAt.getTime() - at <= leaseMs * LEASE_RENEW_BELOW;

// A deploy takes minutes, so a parked run needn't be re-claimed every second.
const REDEPLOY_RECHECK_MS = 30_000;

// renew() no-ops until the lease is half spent, so a quarter-lease tick always lands before expiry.
// Clamped to half the lease as well, or the floor would outrun a very short lease and every renewal
// would land after it had already gone.
const LEASE_KEEPALIVE_FRACTION = 0.25;
const MIN_KEEPALIVE_MS = 250;

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
  | "canceled"
  | "lease_lost";

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
  leaseMs?: number;
}

// Flatten an Error's `.cause` chain to a string: a wrapper (e.g. DrizzleQueryError) carries the
// real error — the pg SQLSTATE and detail — on `.cause`, which is otherwise dropped. Bounded depth
// guards a self-referential chain.
const causeChain = (e: Error): string | undefined => {
  const parts: string[] = [];
  let c: unknown = e.cause;
  for (let depth = 0; c && depth < 8; depth++) {
    if (c instanceof Error) {
      parts.push(c.message ? `${c.name}: ${c.message}` : c.name);
      c = c.cause;
    } else {
      parts.push(String(c));
      break;
    }
  }
  return parts.length ? parts.join(" ← ") : undefined;
};

const toFlowError = (cause: unknown): FlowError => {
  if (cause instanceof CodedError)
    return {
      code: cause.code,
      message: cause.message,
      stack: cause.stack,
      cause: causeChain(cause),
    };
  if (cause instanceof Error)
    return {
      code: cause.name || "ERROR",
      message: cause.message,
      stack: cause.stack,
      cause: causeChain(cause),
    };
  return { code: "ERROR", message: String(cause) };
};

const backoff = (attempt: number, p: RetryPolicy, now: Date): Date =>
  new Date(now.getTime() + Math.min(p.baseDelayMs * 2 ** (attempt - 1), p.maxDelayMs));

// A batch is claimed at once but run one by one, so a run late in the batch may have spent most of
// its lease waiting. Renew it before starting; `undefined` means a peer may already hold it.
const leaseForStart = async (
  queue: Backend["queue"],
  lease: Lease,
  leaseMs: number | undefined,
  at: Date,
): Promise<Lease | undefined> => {
  const remaining = lease.expiresAt.getTime() - at.getTime();
  if (remaining <= 0) return undefined;
  if (leaseMs === undefined || !needsRenewal(lease, leaseMs, at.getTime())) return lease;
  return queue.heartbeat(lease, { leaseMs, now: at }).catch(() => undefined);
};

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

  const started = await leaseForStart(queue, lease, opts.leaseMs, now());
  if (!started) return { runId: lease.runId, status: "lease_lost" };

  const snap = await store.loadRun(lease.runId);
  if (!snap) {
    await queue.ack(started, { now: now() });
    return { runId: lease.runId, status: "gone" };
  }
  if (isTerminal(snap.run.status)) {
    await queue.ack(started, { now: now() });
    return { runId: snap.run.id, status: "already_terminal" };
  }
  const run = snap.run;
  const flowLabel = { name: run.name, version: run.version };

  const res = (status: TickStatus, extra?: Omit<TickResult, "runId" | "status">): TickResult => ({
    runId: run.id,
    status,
    ...extra,
  });

  // Best-effort lease renewal as the run commits durable progress. A lost lease is caught by the
  // first-writer-wins memo + reconcile, so a failed renewal just lets the step's at-least-once
  // contract stand. `held` is the current lease, used to ack when the tick ends.
  let held = started;
  const leaseMs = opts.leaseMs;
  const keepalive =
    leaseMs === undefined
      ? undefined
      : {
          everyMs: Math.min(
            Math.max(MIN_KEEPALIVE_MS, Math.floor(leaseMs * LEASE_KEEPALIVE_FRACTION)),
            Math.floor(leaseMs * LEASE_RENEW_BELOW),
          ),
          renew: async (): Promise<void> => {
            if (!needsRenewal(held, leaseMs, now().getTime())) return;
            held = await queue.heartbeat(held, { leaseMs, now: now() }).catch(() => held);
          },
        };

  const finish = async (
    outcome: Exclude<TerminalOutcome, { status: "canceled" }>,
  ): Promise<TickResult> => {
    const { status } = outcome;
    await store.markTerminal(run.id, outcome);
    // cancelDescendants is idempotent and no-ops when childrenOf is empty, so a childless failure
    // costs one empty query on the rare failure path — cheaper than scanning every tick's memo.
    if (status === "failed") await cancelDescendants(backend, run.id);
    if (outcome.status === "failed") {
      await obs.event("run.failed", run.id, now(), { error: outcome.error });
    } else {
      await obs.event("run.completed", run.id, now());
    }
    obs.metrics.runSettled?.(run.id, status, flowLabel, {
      durationMs: run.createdAt ? now().getTime() - run.createdAt.getTime() : undefined,
      errorCode: outcome.status === "failed" ? outcome.error.code : undefined,
    });
    if (run.parentRunId) {
      const remaining = await store.arriveAtJoin(run.parentRunId);
      if (status !== "done" || (remaining !== undefined && remaining <= 0)) {
        await queue.enqueue(run.parentRunId);
      }
    }
    // Wake any result(run.id) waiter now the run is terminal — the local push fast path (a
    // NOTIFY-backed wakeup also nudges other processes; poll backstops either way).
    await wakeup.signal(run.id);
    await queue.ack(held, { now: now() });
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
    obs.metrics.runSuspended?.(run.id, status, flowLabel);
    await queue.ack(held, { now: now() });
    return res(tickStatus, extra);
  };

  // The deployed code can't advance this run yet — the flow isn't registered (`unknown_flow`) or its
  // calls drifted under it (`flow_drift`). `parked` is not a failure, so it doesn't spend the
  // dead-letter budget: the run waits, visibly, for a redeploy or a version bump.
  const parkForRedeploy = (
    tickStatus: "unknown_flow" | "flow_drift",
    extra?: Omit<TickResult, "runId" | "status">,
  ): Promise<TickResult> => {
    obs.metrics.redeployParked?.(run.id, tickStatus);
    return suspend(
      "parked",
      tickStatus,
      { timers: [{ runId: run.id, fireAt: new Date(now().getTime() + REDEPLOY_RECHECK_MS) }] },
      extra,
    );
  };

  const flow = flows.get(flowKey(run.name, run.version));
  if (!flow) return parkForRedeploy("unknown_flow");

  // Structured concurrency, crash-safe: a child never outlives its parent's non-success termination.
  // The push cascade (cancelDescendants) may not have reached this child if a worker died mid-cascade;
  // this pull check finishes the job on the child's next dispatch.
  if (run.parentRunId) {
    const parent = await store.loadRunRow(run.parentRunId);
    if (parent?.status === "failed" || parent?.status === "canceled") {
      await cancelRun(backend, run.id);
      await queue.ack(held, { now: now() });
      return res("canceled");
    }
  }

  const attempt = await store.markRunning(run.id);
  // Dead-letter cap: markRunning bumps attempts on EVERY claim, so a step that crashes the
  // worker (uncatchable — the catch below never runs) would otherwise re-claim forever. Once
  // attempts pass the cap, fail terminally without executing, bounding the poison pill.
  if (attempt > retry.maxAttempts) {
    return finish({
      status: "failed",
      error: {
        code: "RUN_ATTEMPTS_EXHAUSTED",
        message: `run exceeded ${retry.maxAttempts} attempts`,
      },
    });
  }
  if (attempt === 1) {
    await obs.event("run.started", run.id, now());
    obs.metrics.runStarted?.(run.id, flowLabel);
  }

  // `snap` was loaded after the claim, so it already holds every durable step + signal; the
  // exclusive lease means nothing else writes them mid-tick. No second load needed.
  const suspendState: SuspendHolder = { inflight: new Set() };
  // `Promise.all` rejects on the first suspend while sibling calls are still writing.
  const settleInflight = async (): Promise<void> => {
    while (suspendState.inflight.size > 0) await Promise.allSettled(suspendState.inflight);
  };
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
    keepalive,
    claimVersion: lease.version,
  });

  try {
    const output = await flow.run(ctx, run.input).finally(settleInflight);
    // The body returned — but if it caught and swallowed a suspend without issuing another ctx call,
    // honour the suspend instead of completing at the wrong point.
    if (suspendState.signal) throw suspendState.signal;
    return finish({ status: "done", output });
  } catch (e) {
    const wake = suspendState.wakeAt && {
      timers: [{ runId: run.id, fireAt: suspendState.wakeAt }],
    };
    if (e instanceof SleepSignal) return suspend("sleeping", "sleeping", wake);
    if (e instanceof AwaitChildSignal) return suspend("awaiting_child", "awaiting_child", wake);
    if (e instanceof AwaitSignalSignal) return suspend("awaiting_signal", "awaiting_signal", wake);
    if (isControlSignal(e)) throw e; // future signals must be handled explicitly

    if (e instanceof FlowDriftError) {
      if ((flow.policy?.drift ?? opts.driftPolicy ?? "park") === "park") {
        return parkForRedeploy("flow_drift", { cursorKey: e.cursorKey });
      }
      const err = toFlowError(e);
      return finish({ status: "failed", error: err });
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
    return finish({ status: "failed", error });
  }
};
