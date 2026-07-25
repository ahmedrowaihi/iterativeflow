import type { IdGen } from "#id";
import type { Backend } from "#ports/outbox";
import type { EnqueueOpts } from "#ports/queue";
import { isTerminal } from "#status";
import type { FlowError, RunStatus } from "#types";
import { type Clock, systemClock } from "#engine/context";
import { type DriftPolicy, type RetryPolicy, type TickResult, runTick } from "#engine/executor";
import {
  type Contract,
  type Flow,
  type FlowRegistry,
  type NoSignals,
  type SignalMap,
  type SignalName,
  type SignalPayload,
  validateInput,
} from "#engine/flow";
import { runDueCrons } from "#engine/schedule";
import { DuplicateRunError } from "#engine/signals";
import type { ObserveOpts } from "#engine/observe";

/**
 * A run id, branded with the flow's output type `O` and signal map `S`. It IS a string at runtime;
 * the phantom brand lets {@link result} recover the output type and {@link signalRun} type the
 * signal name + payload. Pass it wherever a `runId` string is expected.
 */
export type RunHandle<O = unknown, S extends SignalMap = NoSignals> = string & {
  readonly __out?: O;
  readonly __sig?: S;
};

/** How a `submit` with an existing `idempotencyKey` behaves: reuse the existing run, or throw. */
export type OnDuplicate = "reuse" | "error";

/** Everything `submit`/`engine.submit` accepts — dedup/tag options plus the enqueue schedule
 *  ({@link EnqueueOpts}'s `runAt`/`priority`). */
export interface SubmitOpts extends EnqueueOpts {
  idempotencyKey?: string;
  tags?: readonly string[];
  /** On an `idempotencyKey` hit: `"reuse"` (default) returns the existing handle; `"error"` throws. */
  onDuplicate?: OnDuplicate;
}

/** Submit a run: create it (idempotent) and enqueue it if freshly created. Returns a typed handle.
 *  Accepts a {@link Flow} or a {@link Contract} — the latter for a caller that doesn't own the body. */
export const submit = async <I, O, S extends SignalMap = NoSignals>(
  backend: Backend,
  flow: Flow<I, O, S> | Contract<I, O, S>,
  input: I,
  opts?: SubmitOpts,
): Promise<RunHandle<O, S>> => {
  const validated = await validateInput(flow, input);
  const { runId, created } = await backend.store.startRun({
    name: flow.name,
    version: flow.version,
    input: validated,
    idempotencyKey: opts?.idempotencyKey,
    tags: opts?.tags,
  });
  if (created) {
    await backend.queue.enqueue(runId, { runAt: opts?.runAt, priority: opts?.priority });
  } else if (opts?.onDuplicate === "error") {
    throw new DuplicateRunError(runId, opts.idempotencyKey ?? "");
  }
  return runId as RunHandle<O, S>;
};

/** One item of a batch submit: a flow, its input, and optional per-run dispatch options. */
export interface SubmitSpec<I = unknown> {
  flow: Flow<I, any, any>;
  input: I;
  idempotencyKey?: string;
  tags?: readonly string[];
  runAt?: Date;
  priority?: number;
}

/**
 * Submit many runs in one atomic batch (the runs are created together via `startManyRuns`),
 * then enqueue the freshly-created ones. Returns their ids, aligned with `items`. Idempotent
 * items that already existed are returned but not re-enqueued.
 */
export const submitMany = async <I>(
  backend: Backend,
  items: readonly SubmitSpec<I>[],
): Promise<string[]> => {
  const specs = await Promise.all(
    items.map(async (it) => ({
      name: it.flow.name,
      version: it.flow.version,
      input: await validateInput(it.flow, it.input),
      idempotencyKey: it.idempotencyKey,
      tags: it.tags,
    })),
  );
  const results = await backend.store.startManyRuns(specs);
  await Promise.all(
    results.map((r, i) =>
      r.created
        ? backend.queue.enqueue(r.runId, { runAt: items[i].runAt, priority: items[i].priority })
        : undefined,
    ),
  );
  return results.map((r) => r.runId);
};

/**
 * Deliver an external signal to a run and wake it. The delivery + re-enqueue are atomic
 * (durable); the wakeup is a best-effort latency nudge. Idempotent on `idempotencyKey`.
 * Returns `false` if the signal was an idempotent duplicate.
 */
export const signalRun = async <
  O = unknown,
  S extends SignalMap = NoSignals,
  K extends SignalName<S> = SignalName<S>,
>(
  backend: Backend,
  handle: RunHandle<O, S> | string,
  name: K,
  payload: SignalPayload<S, K>,
  opts?: { idempotencyKey?: string },
): Promise<boolean> => {
  const { delivered } = await backend.store.postSignal(handle, name, payload, opts);
  if (delivered) await backend.wakeup.signal(handle);
  return delivered;
};

/** The settled outcome of a run, as returned by {@link result}. `O` is the flow's output type. */
export interface RunResult<O = unknown> {
  status: Extract<RunStatus, "done" | "failed" | "canceled">;
  output?: O;
  error?: FlowError;
}

/**
 * Re-drive a `failed` run, keeping its completed step memos so only the work after the
 * failure re-runs. A no-op on a run that isn't `failed`. Returns whether it retried.
 */
export const retryRun = async (backend: Backend, runId: string): Promise<boolean> => {
  const { retried } = await backend.store.retryRun(runId);
  if (retried) await backend.wakeup.signal(runId);
  return retried;
};

/**
 * Poll-first await of a run's terminal outcome: re-read the store, and between reads sleep on
 * `wakeup.wait` (which returns early on a signal, or after the poll tick). Connection-safe by
 * default — no `LISTEN` pinned. Throws on timeout.
 */
export const result = async <O = unknown>(
  backend: Backend,
  runId: RunHandle<O> | string,
  opts?: { timeoutMs?: number; pollMs?: number; now?: Clock },
): Promise<RunResult<O>> => {
  const nowMs = (): number => (opts?.now ? opts.now().getTime() : Date.now());
  const pollMs = opts?.pollMs ?? 500;
  const deadline =
    opts?.timeoutMs === undefined ? Number.POSITIVE_INFINITY : nowMs() + opts.timeoutMs;
  for (;;) {
    const run = await backend.store.loadRunRow(runId);
    if (!run) throw new Error(`result: run ${runId} not found`);
    if (isTerminal(run.status)) {
      return { status: run.status, output: run.output as O, error: run.error };
    }
    const remaining = deadline - nowMs();
    if (remaining <= 0) throw new Error(`result: run ${runId} did not settle before timeout`);
    await backend.wakeup.wait(runId, Math.min(pollMs, remaining));
  }
};

export { cancelRun } from "#engine/cancel";

/** Move every due timer back onto the queue. Returns how many were re-enqueued. */
export const drainTimers = async (
  backend: Backend,
  opts: { limit: number; now?: Date },
): Promise<number> => {
  const due = await backend.timer.dueBatch({ now: opts.now, limit: opts.limit });
  await Promise.all(due.map((runId) => backend.queue.enqueue(runId)));
  return due.length;
};

/**
 * Re-enqueue runs stranded off the queue (crash between a state write and its enqueue, or a
 * lost parent-wake). Idempotent — re-enqueueing a run that is actually fine just makes it
 * re-check and re-suspend. Run this on a slow interval (or as an internal cron). Returns how
 * many were re-enqueued.
 */
export const reconcile = async (backend: Backend, opts: { limit: number }): Promise<number> => {
  const orphans = await backend.store.orphanedRuns(opts.limit);
  await Promise.all(orphans.map((runId) => backend.queue.enqueue(runId)));
  return orphans.length;
};

/**
 * Retention sweep: delete up to `limit` terminal runs (with their steps/signals/events) created
 * before `before`. Live runs are untouched. Returns how many were deleted. Schedule it on a slow
 * cadence (a cron, or your own timer) — the window is a deployment policy, so it is not wired into
 * the worker loop. Call repeatedly until it returns `< limit` to drain a large backlog.
 */
export const prune = async (
  backend: Backend,
  opts: { before: Date; limit: number },
): Promise<number> => backend.store.deleteRunsOlderThan(opts.before, opts.limit);

export interface TickOnceOpts {
  batchMax: number;
  leaseMs: number;
  retry?: RetryPolicy;
  now?: Clock;
  id?: IdGen;
  observe?: ObserveOpts;
  driftPolicy?: DriftPolicy;
}

/**
 * One worker cycle: drain due timers back onto the queue, then claim and execute a batch.
 * A resident worker calls this on an interval; a serverless worker calls it per invocation.
 * Returns the per-run tick results for metrics/tests.
 */
export const tickOnce = async (
  backend: Backend,
  flows: FlowRegistry,
  opts: TickOnceOpts,
): Promise<TickResult[]> => {
  const now = opts.now ?? systemClock;
  await drainTimers(backend, { limit: opts.batchMax, now: now() });
  const leases = await backend.queue.claim({
    limit: opts.batchMax,
    leaseMs: opts.leaseMs,
    now: now(),
  });
  const results: TickResult[] = [];
  for (const lease of leases) {
    results.push(
      await runTick(backend, flows, lease, {
        now,
        retry: opts.retry,
        id: opts.id,
        observe: opts.observe,
        driftPolicy: opts.driftPolicy,
      }),
    );
  }
  return results;
};

/** What one {@link serverlessTick} advanced — for the invoking cron Lambda's logs/metrics. */
export interface SweepResult {
  /** Cron occurrences fired. */
  fired: number;
  /** Crash-stranded / lost-wake runs re-enqueued. */
  reconciled: number;
  /** Runs claimed and advanced this cycle, by outcome. */
  results: TickResult[];
}

/**
 * One full engine cycle for a scheduled (serverless) invocation: fire due crons, re-drive
 * orphans, then drain due timers and claim + execute a batch. Designed to BE an EventBridge /
 * cron Lambda — no resident loop, no daemon. Every waiting run advances on the next scheduled
 * firing, so a durable `ctx.sleep` outlives any single invocation's timeout. Prefer
 * {@link tickOnce} alone for a resident worker that already runs maintenance on its own cadence.
 *
 * Set `opts.leaseMs` no larger than the invocation's timeout: a claimed run whose invocation is
 * killed mid-batch only becomes re-claimable once its lease expires, so an oversized lease strands
 * the un-executed tail of the batch for that long. Size `opts.batchMax` to what one invocation can
 * realistically drain within its budget. Crons that fire more occurrences than `batchMax` (or timers
 * exceeding it) are durable and simply advance over the following invocations. Cron catch-up
 * coalesces: an occurrence missed while nothing was invoking fires once on the next sweep, not once
 * per missed slot.
 */
export const serverlessTick = async (
  backend: Backend,
  flows: FlowRegistry,
  opts: TickOnceOpts,
): Promise<SweepResult> => {
  const now = opts.now ?? systemClock;
  // Both must land before tickOnce claims, so this cycle's crons/orphans are claimable now.
  const [fired, reconciled] = await Promise.all([
    runDueCrons(backend, now),
    reconcile(backend, { limit: opts.batchMax }),
  ]);
  const results = await tickOnce(backend, flows, opts);
  return { fired, reconciled, results };
};
