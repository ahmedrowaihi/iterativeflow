import type { IdGen } from "#id";
import type { Backend } from "#ports/outbox";
import type { EnqueueOpts } from "#ports/queue";
import { isTerminal } from "#status";
import type { FlowError, RunStatus } from "#types";
import { type Clock, systemClock } from "#engine/context";
import { type RetryPolicy, type TickResult, runTick } from "#engine/executor";
import { type Flow, type FlowRegistry, validateInput } from "#engine/flow";
import type { ObserveOpts } from "#engine/observe";

/** Submit a run: create it (idempotent) and enqueue it if freshly created. Returns its id. */
export const submit = async <I>(
  backend: Backend,
  flow: Flow<I, unknown>,
  input: I,
  opts?: { idempotencyKey?: string; tags?: readonly string[] } & EnqueueOpts,
): Promise<string> => {
  const validated = await validateInput(flow, input);
  const { runId, created } = await backend.store.startRun({
    name: flow.name,
    version: flow.version,
    input: validated,
    idempotencyKey: opts?.idempotencyKey,
    tags: opts?.tags,
  });
  if (created) await backend.queue.enqueue(runId, { runAt: opts?.runAt, priority: opts?.priority });
  return runId;
};

/** One item of a batch submit: a flow, its input, and optional per-run dispatch options. */
export interface SubmitItem<I = unknown> {
  flow: Flow<I, unknown>;
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
  items: readonly SubmitItem<I>[],
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
export const signalRun = async (
  backend: Backend,
  runId: string,
  name: string,
  payload: unknown,
  opts?: { idempotencyKey?: string },
): Promise<boolean> => {
  const { delivered } = await backend.store.postSignal(runId, name, payload, opts);
  if (delivered) await backend.wakeup.signal(runId);
  return delivered;
};

/** The settled outcome of a run, as returned by {@link result}. */
export interface RunResult {
  status: Extract<RunStatus, "done" | "failed" | "canceled">;
  output?: unknown;
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
export const result = async (
  backend: Backend,
  runId: string,
  opts?: { timeoutMs?: number; pollMs?: number; now?: () => number },
): Promise<RunResult> => {
  const clock = opts?.now ?? (() => Date.now());
  const pollMs = opts?.pollMs ?? 500;
  const deadline =
    opts?.timeoutMs === undefined ? Number.POSITIVE_INFINITY : clock() + opts.timeoutMs;
  for (;;) {
    const snap = await backend.store.loadRun(runId);
    if (!snap) throw new Error(`result: run ${runId} not found`);
    if (isTerminal(snap.run.status)) {
      return { status: snap.run.status, output: snap.run.output, error: snap.run.error };
    }
    const remaining = deadline - clock();
    if (remaining <= 0) throw new Error(`result: run ${runId} did not settle before timeout`);
    await backend.wakeup.wait(runId, Math.min(pollMs, remaining));
  }
};

/**
 * Cancel a run and cascade to its (non-terminal) descendants. Cancel is sticky and clears
 * the run's pending timer atomically. In-flight step effects on a worker mid-tick may still
 * land (cooperative cancel) — the run's markRunning guard stops the NEXT dispatch, not the
 * one already executing.
 */
export const cancelRun = async (backend: Backend, runId: string): Promise<void> => {
  await backend.store.markTerminal(runId, { status: "canceled" }, { cancelTimers: [runId] });
  const children = await backend.store.childrenOf(runId);
  for (const c of children) {
    if (!isTerminal(c.status)) await cancelRun(backend, c.id);
  }
};

/** Move every due timer back onto the queue. Returns how many were re-enqueued. */
export const drainTimers = async (
  backend: Backend,
  opts: { max: number; now?: Date },
): Promise<number> => {
  const due = await backend.timer.dueBatch({ now: opts.now, max: opts.max });
  for (const runId of due) await backend.queue.enqueue(runId);
  return due.length;
};

/**
 * Re-enqueue runs stranded off the queue (crash between a state write and its enqueue, or a
 * lost parent-wake). Idempotent — re-enqueueing a run that is actually fine just makes it
 * re-check and re-suspend. Run this on a slow interval (or as an internal cron). Returns how
 * many were re-enqueued.
 */
export const reconcile = async (backend: Backend, opts: { max: number }): Promise<number> => {
  const orphans = await backend.store.orphanedRuns(opts.max);
  for (const runId of orphans) await backend.queue.enqueue(runId);
  return orphans.length;
};

export interface TickOnceOpts {
  batchMax: number;
  leaseMs: number;
  retry?: RetryPolicy;
  now?: Clock;
  id?: IdGen;
  observe?: ObserveOpts;
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
  await drainTimers(backend, { max: opts.batchMax, now: now() });
  const leases = await backend.queue.claim({
    max: opts.batchMax,
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
      }),
    );
  }
  return results;
};
