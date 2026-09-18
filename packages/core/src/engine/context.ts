import type { IdGen } from "#id";
import type { Backend } from "#ports/outbox";
import type { DeliveredSignal, RunRow, RunSnapshot, RunSpec } from "#types";
import {
  type AnyFlow,
  type ChildResult,
  type ChildResults,
  type Flow,
  type FlowOutputs,
  type InvokeSpec,
  type InvokeOpts,
  type InvokeSpecFor,
  type SignalMap,
  type SignalName,
  type SignalPayload,
  type SignalSchema,
  flowKey,
  validateSignal,
} from "#engine/flow";
import { type Observer, spanIdOf, traceIdOf } from "#engine/observe";
import {
  AwaitChildSignal,
  AwaitSignalSignal,
  type ControlSignal,
  FlowDriftError,
  SleepSignal,
  isControlSignal,
  StepFailedError,
  StepTimeoutError,
} from "#engine/signals";

/** What a step's `fn` receives — the abort signal (fires on timeout) and its attempt number. */
export interface StepArg {
  /** Aborts when the step times out or its final in-invocation attempt fails. Wire into fetch/etc. */
  signal: AbortSignal;
  /** 1-indexed in-invocation attempt number. */
  attempt: number;
}

/** Per-step execution policy. Retries and timeout are within-invocation (fast transient recovery). */
export interface StepPolicy {
  /** Extra in-invocation attempts on throw before the step's error propagates. Default 0. */
  retries?: number;
  /** Delay between those in-invocation retries. Blocks the worker, so keep it small; for long
   *  durable backoff, let the step throw and use run-level retry. Default 0. */
  retryDelayMs?: number;
  /** Reject the step's `fn` if it runs longer than this (and abort its signal). No timeout by default.
   *  Declaring it also holds the run's lease open while the step runs, so a step longer than
   *  `leaseMs` is not re-claimed mid-flight; without it, one that is runs again on another worker. */
  timeoutMs?: number;
  /**
   * Decide whether an error is worth retrying. A `permanent` verdict fails the step (and the
   * run) immediately — no in-invocation retries, no run-level retry. `transient` (the default)
   * retries as configured. `attempt` is the 1-indexed in-invocation try. Use it to fail fast on
   * 4xx/validation errors and retry 5xx/timeouts.
   */
  classify?: (cause: unknown, attempt: number) => "transient" | "permanent";
}

/** The result of a `ctx.signal(name, { timeoutMs })` await: the delivered payload, or a timeout. */
export type SignalOutcome<T> = { received: true; payload: T } | { received: false };

/**
 * The durable context handed to a flow body. Every method is a memoized checkpoint: on the
 * first invocation it runs and persists; on every replay it returns the persisted result
 * without re-running. Cursor keys are POSITIONAL (`s0`, `s1`, …) — the deterministic-replay
 * contract is that a flow issues the same ctx calls in the same order each invocation.
 */
export interface Ctx<S extends SignalMap = SignalMap> {
  /** This run's id. */
  readonly runId: string;
  /** 1-indexed attempt number of the current invocation (survives crashes). */
  readonly attempt: number;

  /**
   * Run `fn` once and memoize its result. On replay the stored result is returned and `fn`
   * is NOT re-run. `fn` is at-least-once across a crash BEFORE the checkpoint commits, so
   * keep its side-effects idempotent; the memo is exactly-once. `policy` adds in-invocation
   * retries, a timeout, and error classification. Durable long backoff is still the run-level retry.
   *
   * A step is a leaf: its body must not call `ctx`. Durable waits and child runs belong in the flow
   * body, and nested durable work belongs in a child flow (`ctx.invoke`). A body that closes over
   * `ctx` shifts the cursor on its first run only, so the run parks as drifted on replay.
   *
   * The memo round-trips through the backend's JSON, so `T` describes what `fn` returns, not
   * necessarily what a replay hands back: a `Date` returns as an ISO string. Return JSON-native
   * values, and parse at the boundary.
   */
  step<T>(name: string, fn: (arg: StepArg) => Promise<T> | T, policy?: StepPolicy): Promise<T>;

  /** Durably park the run for `ms`, releasing the worker. Resumes after the deadline. */
  sleep(ms: number): Promise<void>;

  /** Durably park until `date`. */
  sleepUntil(date: Date): Promise<void>;

  /**
   * Spawn `flow(input)` as a child run and return its output. The child is created exactly
   * once (recorded in the step memo); the parent parks until the child completes, then
   * resumes with the child's output. A child failure surfaces as a thrown error; pass
   * `{ onChildFailure: "settle" }` to get the child's {@link ChildResult} instead.
   */
  invoke<CI, CO>(
    flow: Flow<CI, CO, any>,
    input: CI,
    opts?: { onChildFailure: "fail" },
  ): Promise<CO>;
  invoke<CI, CO>(
    flow: Flow<CI, CO, any>,
    input: CI,
    opts: { onChildFailure: "settle" },
  ): Promise<ChildResult<CO>>;

  /**
   * Fan out: spawn every child in parallel and join, resolving with the outputs in order. Fast-fail
   * — if any child fails (or is canceled), the parent fails and its still-running siblings are
   * cancelled (structured concurrency). With `{ onChildFailure: "settle" }` it instead waits for
   * every child and resolves with each one's {@link ChildResult}. Children spawn in chunks, each an
   * atomic memoized checkpoint.
   */
  invoke<const F extends readonly AnyFlow[]>(
    specs: { readonly [K in keyof F]: InvokeSpecFor<F[K]> },
    opts?: { onChildFailure: "fail" },
  ): Promise<FlowOutputs<F>>;
  invoke<const F extends readonly AnyFlow[]>(
    specs: { readonly [K in keyof F]: InvokeSpecFor<F[K]> },
    opts: { onChildFailure: "settle" },
  ): Promise<ChildResults<F>>;

  /**
   * Durably wait for an external signal named `name` and return its payload. If a matching
   * signal is already in the inbox it is consumed immediately; otherwise the run parks until
   * one is delivered (`engine.signal`). Consumption is memoized, so a replay returns the same
   * payload without re-waiting.
   *
   * When the flow declares a `signals` map, only those names compile and each returns its
   * validated payload. A flow with no `signals` map takes any name, and the payload is `unknown`.
   */
  signal<K extends SignalName<S>>(name: K): Promise<SignalPayload<S, K>>;
  /** Await a signal with a deadline. Resolves `{ received: true, payload }` if it arrives within
   *  `timeoutMs`, else `{ received: false }`. A signal delivered before the timeout commits always
   *  wins — the deadline decision is consistent with the durable inbox. */
  signal<K extends SignalName<S>>(
    name: K,
    opts: { timeoutMs: number },
  ): Promise<SignalOutcome<SignalPayload<S, K>>>;

  /**
   * Emit a durable log line to the event sink (visible on the dashboard timeline), tagged to this
   * run. Fire-and-forget and NOT memoized: it is suppressed while the flow replays its already-durable
   * prefix, so a line logs once even though the body re-runs on every crash/wake resume. A no-op when
   * no sink is wired or the observe `level` is `lifecycle`/`off`.
   */
  log<D>(message: string, data?: D): void;
}

/** The clock the executor threads in — injectable for deterministic tests. */
export type Clock = () => Date;

/** Wall-clock default. Passed where a deployment doesn't inject its own {@link Clock}. */
export const systemClock: Clock = () => new Date();

// Guards against an unbounded runaway fan-out (children per invoke) and recursion (invoke nesting).
const MAX_FAN_OUT = 10_000;
const MAX_DEPTH = 32;

// Children spawned per atomic checkpoint. A fixed core constant (NOT a per-backend value) so the
// chunk count and memo fingerprints are identical on every backend — a backend's transaction budget
// must not leak into the durable replay fingerprint. Kept small enough for the tightest backend's
// atomic-write budget; each backend guards its own limit at checkpoint time.
const FAN_OUT_CHUNK = 40;

/** Anything carrying a value this run already wrote to its durable log, or a child run's output. */
interface Logged {
  readonly result?: unknown;
}

// SAFETY: every logged value came from flow code registered under the name and version the call
// names — a step's return, a schema-validated signal payload, or the output of a child run of that
// exact flow version — and the drift guard has checked that the call at this cursor wrote it. That
// is the replay contract the whole engine rests on.
const fromLog = <T>(entry: Logged): T => entry.result as T;

const pause = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const errCode = (cause: unknown): string =>
  cause instanceof Error ? cause.name || "STEP_FAILED" : "STEP_FAILED";
const errMsg = (cause: unknown): string => (cause instanceof Error ? cause.message : String(cause));

const childIdsOf = (entry: Logged): string[] =>
  Array.isArray(entry.result) ? entry.result.map(String) : [];

const withTimeout = async <T>(
  run: (arg: StepArg) => Promise<T> | T,
  arg: StepArg,
  controller: AbortController,
  ms?: number,
): Promise<T> => {
  if (!ms) return await run(arg);
  let handle: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    handle = setTimeout(() => {
      controller.abort();
      reject(new StepTimeoutError(ms));
    }, ms);
  });
  try {
    return await Promise.race([Promise.resolve(run(arg)), timeout]);
  } finally {
    clearTimeout(handle);
  }
};

const withLeaseHeld = async <T>(
  work: () => Promise<T>,
  keepalive?: LeaseKeepalive,
  timeoutMs?: number,
): Promise<T> => {
  // No declared timeout means no honest ceiling: renewing anyway would turn a hung step into a
  // permanent stall, so leave it reclaimable.
  if (!keepalive || !timeoutMs) return await work();
  const timer = setInterval(() => void keepalive.renew(), keepalive.everyMs);
  try {
    return await work();
  } finally {
    clearInterval(timer);
  }
};

const runWithPolicy = async <T>(
  fn: (arg: StepArg) => Promise<T> | T,
  policy?: StepPolicy,
): Promise<T> => {
  const retries = policy?.retries ?? 0;
  for (let attempt = 1; ; attempt++) {
    const controller = new AbortController();
    try {
      return await withTimeout(
        fn,
        { signal: controller.signal, attempt },
        controller,
        policy?.timeoutMs,
      );
    } catch (e) {
      controller.abort();
      // A suspend is not a failure: it must not reach `classify` (which would fail the run on a
      // normal sleep) and must not burn a retry (which would re-run the body on every park).
      if (isControlSignal(e)) throw e;
      // A permanent error fails the step (and the run) immediately — a StepFailedError is
      // non-retryable, so neither the in-invocation loop nor the run-level retry re-runs it.
      if (policy?.classify?.(e, attempt) === "permanent")
        throw new StepFailedError(errCode(e), errMsg(e));
      if (attempt > retries) throw e; // transient budget spent → let run-level retry take over
      if (policy?.retryDelayMs) await pause(policy.retryDelayMs);
    }
  }
};

/** @internal */
export interface SuspendHolder {
  signal?: ControlSignal;
  wakeAt?: Date;
  inflight: Set<Promise<unknown>>;
}

/** @internal */
export interface LeaseKeepalive {
  renew: () => Promise<void>;
  everyMs: number;
}

/** @internal */
export interface CtxDeps {
  backend: Backend;
  snap: RunSnapshot;
  attempt: number;
  now: Clock;
  id: IdGen;
  obs: Observer;
  signals?: Readonly<Record<string, SignalSchema<unknown>>>;
  maxFanOut?: number;
  maxDepth?: number;
  suspend: SuspendHolder;
  keepalive?: LeaseKeepalive;
  claimVersion: number;
}

/** @internal */
export const makeCtx = ({
  backend,
  snap,
  attempt,
  now,
  id,
  obs,
  signals,
  maxFanOut,
  maxDepth,
  suspend,
  keepalive,
  claimVersion,
}: CtxDeps): Ctx => {
  const runId = snap.run.id;
  const depth = snap.run.depth ?? 0;
  const traceId = obs.tracer ? traceIdOf(runId) : "";
  const consumed = new Set<string>();

  // Siblings issued in the same synchronous turn (`Promise.all`) still start; a call made after the
  // turn means the suspend was swallowed, so memoAt re-throws it.
  let armedThisTurn = false;
  const arm = (sig: ControlSignal): ControlSignal => {
    if (!suspend.signal) {
      suspend.signal = sig;
      armedThisTurn = true;
      queueMicrotask(() => {
        armedThisTurn = false;
      });
    }
    const at =
      sig instanceof SleepSignal
        ? sig.wakeAt
        : sig instanceof AwaitSignalSignal
          ? sig.deadline
          : undefined;
    if (at && (!suspend.wakeAt || at < suspend.wakeAt)) suspend.wakeAt = at;
    return sig;
  };

  let n = 0;

  const track = <T>(op: Promise<T>): Promise<T> => {
    suspend.inflight.add(op);
    const done = (): void => void suspend.inflight.delete(op);
    op.then(done, done);
    return op;
  };

  const spawnSpec = <I>(flow: AnyFlow, input: I, key: string): RunSpec => ({
    name: flow.name,
    version: flow.version,
    input,
    parentRunId: runId,
    parentCursorKey: key,
    depth: depth + 1,
    createdAt: now(),
  });

  const guardDepth = (): void => {
    const cap = maxDepth ?? MAX_DEPTH;
    if (depth + 1 > cap) {
      throw new Error(`ctx.invoke: child depth ${depth + 1} exceeds the ${cap} nesting cap`);
    }
  };

  // undefined while the child still runs; without `settle` a failed or canceled child throws.
  const childResult = (
    row: RunRow | undefined,
    settle: boolean,
  ): ChildResult<unknown> | undefined => {
    if (row?.status === "done") return { status: "done", output: row.output };
    if (row?.status !== "failed" && row?.status !== "canceled") return undefined;
    if (!settle) {
      throw new StepFailedError(
        row.error?.code ?? "CHILD_FAILED",
        row.error?.message ?? "child did not complete",
      );
    }
    if (row.status === "canceled") return { status: "canceled" };
    return {
      status: "failed",
      error: row.error ?? { code: "CHILD_FAILED", message: "child did not complete" },
    };
  };

  const outputOf = (result: ChildResult<unknown>): Logged => ({
    result: result.status === "done" ? result.output : undefined,
  });

  const pendingFor = (name: string): DeliveredSignal | undefined =>
    snap.signals.find((s) => s.name === name && !consumed.has(s.id));

  // Validate a consumed payload against its declared schema; `consumed` stops a later wait in this
  // invocation from draining the same signal.
  const validated = async (pending: DeliveredSignal): Promise<DeliveredSignal> => {
    consumed.add(pending.id);
    const schema = signals?.[pending.name];
    if (!schema) return pending;
    try {
      return { ...pending, payload: await validateSignal(schema, pending.name, pending.payload) };
    } catch (e) {
      // Permanent — the inbox payload won't change on retry, so fail the run.
      throw new StepFailedError("SIGNAL_INVALID", errMsg(e));
    }
  };

  // Advance the cursor and fetch the memo at it. `call` is the `kind:label` of the call
  // being made now; if a memo recorded a different call here, the flow body drifted under this run.
  const memoAt = (call: string) => {
    // A prior suspend was caught and swallowed by the flow body. Re-throw it before advancing, so
    // the suspend still reaches the engine and a `try/catch` around `ctx.*` can't strand a run.
    if (suspend.signal && !armedThisTurn) throw suspend.signal;
    const key = `s${n++}`;
    const memo = snap.steps.get(key);
    if (memo?.call !== undefined && memo.call !== call) {
      throw new FlowDriftError(key, memo.call, call);
    }
    return { key, memo };
  };

  const step = async <T>(
    name: string,
    fn: (arg: StepArg) => Promise<T> | T,
    policy?: StepPolicy,
  ): Promise<T> => {
    const call = `step:${name}`;
    const { key, memo } = memoAt(call);
    if (memo) return fromLog<T>(memo);
    const startedAt = now();
    const spanId = obs.tracer ? spanIdOf(runId, key) : "";
    let result: T;
    try {
      result = await withLeaseHeld(() => runWithPolicy(fn, policy), keepalive, policy?.timeoutMs);
    } catch (e) {
      obs.tracer?.span({
        runId,
        traceId,
        spanId,
        name,
        startedAt,
        endedAt: now(),
        error: { code: errCode(e), message: errMsg(e) },
      });
      throw e;
    }
    const stored = await backend.store.checkpointStep({
      runId,
      cursorKey: key,
      status: "ok",
      result,
      attempts: attempt,
      call,
    });
    await keepalive?.renew();
    obs.tracer?.span({ runId, traceId, spanId, name, startedAt, endedAt: now() });
    await obs.event("step.finished", runId, now(), { cursorKey: key });
    obs.metrics.stepFinished?.(runId, key, { durationMs: now().getTime() - startedAt.getTime() });
    return fromLog<T>(stored);
  };

  // Pin a deadline into a memo once (as an ISO string) so replay/re-park reuse the same instant
  // instead of recomputing it and sliding it forward. Shared by ctx.sleep and a timed ctx.signal.
  const pinDeadline = async (call: string, wakeAt: Date): Promise<Date> => {
    const { key, memo } = memoAt(call);
    if (memo) return new Date(String(memo.result));
    await backend.store.checkpointStep({
      runId,
      cursorKey: key,
      status: "ok",
      result: wakeAt.toISOString(),
      attempts: attempt,
      call,
    });
    return wakeAt;
  };

  const parkUntil = async (wakeAt: Date): Promise<void> => {
    const at = await pinDeadline("sleep", wakeAt);
    if (now().getTime() >= at.getTime()) return;
    throw arm(new SleepSignal(at));
  };

  const invokeOne = async <I>(flow: AnyFlow, input: I, settle: boolean): Promise<Logged> => {
    guardDepth();
    const call = `invoke:${flowKey(flow.name, flow.version)}`;
    const { key, memo } = memoAt(call);
    let childId: string;
    if (memo) {
      childId = String(memo.result);
    } else {
      // First-writer-wins: a concurrent invocation may already have spawned this step, in which
      // case the checkpoint is a no-op returning THAT winner's childId — trust the returned value.
      const candidate = id();
      const stored = await backend.store.checkpointStep(
        { runId, cursorKey: key, status: "ok", result: candidate, attempts: attempt, call },
        {
          spawn: [{ runId: candidate, spec: spawnSpec(flow, input, key) }],
          joinTarget: { runId, count: 1 },
        },
      );
      childId = String(stored.result);
    }
    const result = childResult(await backend.store.loadRunRow(childId), settle);
    if (!result) throw arm(new AwaitChildSignal(childId));
    return settle ? { result } : outputOf(result);
  };

  const invokeMany = async (specs: readonly InvokeSpec[], settle: boolean): Promise<Logged> => {
    guardDepth();
    const cap = maxFanOut ?? MAX_FAN_OUT;
    if (specs.length > cap) {
      throw new Error(`ctx.invoke: fan-out of ${specs.length} exceeds the ${cap} cap`);
    }
    const childIds: string[] = [];
    for (let i = 0; i < specs.length; i += FAN_OUT_CHUNK) {
      const chunk = specs.slice(i, i + FAN_OUT_CHUNK);
      const call = `invokeAll:${i / FAN_OUT_CHUNK}:${chunk.length}`;
      const { key, memo } = memoAt(call);
      if (memo) {
        childIds.push(...childIdsOf(memo));
        continue;
      }
      const ids = chunk.map(() => id());
      const stored = await backend.store.checkpointStep(
        { runId, cursorKey: key, status: "ok", result: ids, attempts: attempt, call },
        {
          spawn: chunk.map((s, j) => ({ runId: ids[j], spec: spawnSpec(s.flow, s.input, key) })),
          joinTarget: i === 0 ? { runId, count: specs.length } : undefined,
        },
      );
      childIds.push(...childIdsOf(stored));
      await keepalive?.renew();
    }
    // Per-mode call: the memo holds outputs or ChildResults, so a mode switch must read as drift.
    const joinCall = `${settle ? "invokeAllSettled" : "invokeAllJoin"}:${specs.length}`;
    const { key: joinKey, memo: joinMemo } = memoAt(joinCall);
    if (joinMemo) return joinMemo;
    const rows = await backend.store.loadRunRows(childIds);
    const results: ChildResult<unknown>[] = [];
    for (const row of rows) {
      const result = childResult(row, settle);
      if (!result) throw arm(new AwaitChildSignal(childIds[0] ?? runId));
      results.push(result);
    }
    return backend.store.checkpointStep({
      runId,
      cursorKey: joinKey,
      status: "ok",
      result: settle ? results : results.map((r) => outputOf(r).result),
      attempts: attempt,
      call: joinCall,
    });
  };

  const settles = <V>(opts: V): boolean =>
    opts instanceof Object && "onChildFailure" in opts && opts.onChildFailure === "settle";

  function invoke<CI, CO>(
    flow: Flow<CI, CO, any>,
    input: CI,
    opts?: { onChildFailure: "fail" },
  ): Promise<CO>;
  function invoke<CI, CO>(
    flow: Flow<CI, CO, any>,
    input: CI,
    opts: { onChildFailure: "settle" },
  ): Promise<ChildResult<CO>>;
  function invoke<const F extends readonly AnyFlow[]>(
    specs: { readonly [K in keyof F]: InvokeSpecFor<F[K]> },
    opts?: { onChildFailure: "fail" },
  ): Promise<FlowOutputs<F>>;
  function invoke<const F extends readonly AnyFlow[]>(
    specs: { readonly [K in keyof F]: InvokeSpecFor<F[K]> },
    opts: { onChildFailure: "settle" },
  ): Promise<ChildResults<F>>;
  function invoke<CI, CO>(
    target: Flow<CI, CO, any> | readonly InvokeSpec[],
    second?: CI | InvokeOpts,
    third?: InvokeOpts,
  ):
    | Promise<CO>
    | Promise<ChildResult<CO>>
    | Promise<FlowOutputs<readonly AnyFlow[]>>
    | Promise<ChildResults<readonly AnyFlow[]>> {
    if ("run" in target) {
      if (settles(third)) {
        return track<ChildResult<CO>>(
          invokeOne(target, second, true).then(fromLog<ChildResult<CO>>),
        );
      }
      return track<CO>(invokeOne(target, second, false).then(fromLog<CO>));
    }
    if (settles(second)) {
      return track<ChildResults<readonly AnyFlow[]>>(
        invokeMany(target, true).then(fromLog<ChildResults<readonly AnyFlow[]>>),
      );
    }
    return track<FlowOutputs<readonly AnyFlow[]>>(
      invokeMany(target, false).then(fromLog<FlowOutputs<readonly AnyFlow[]>>),
    );
  }

  // Unbounded wait: park until the signal arrives, then return its payload.
  const awaitSignal = async <P>(name: string): Promise<P> => {
    const call = `signal:${name}`;
    const { key, memo } = memoAt(call);
    if (memo) return fromLog<P>(memo);
    const pending = pendingFor(name);
    if (!pending) throw arm(new AwaitSignalSignal(name));
    const delivered = await validated(pending);
    const stored = await backend.store.checkpointStep(
      {
        runId,
        cursorKey: key,
        status: "ok",
        result: delivered.payload,
        attempts: attempt,
        call,
      },
      { consumeSignals: [pending.id] },
    );
    return fromLog<P>(stored);
  };

  // Bounded wait: pin the deadline once, then resolve to the payload or a timeout.
  const awaitSignalUntil = async <P>(
    name: string,
    timeoutMs: number,
  ): Promise<SignalOutcome<P>> => {
    const call = `signal:${name}`;
    const deadline = await pinDeadline(`signalWait:${name}`, new Date(now().getTime() + timeoutMs));
    const { key, memo } = memoAt(call);
    if (memo) return fromLog<SignalOutcome<P>>(memo);
    const pending = pendingFor(name);
    if (pending) {
      const delivered = await validated(pending);
      const stored = await backend.store.checkpointStep(
        {
          runId,
          cursorKey: key,
          status: "ok",
          result: { received: true, payload: delivered.payload },
          attempts: attempt,
          call,
        },
        { consumeSignals: [pending.id], cancelTimers: [runId] },
      );
      return fromLog<SignalOutcome<P>>(stored);
    }
    if (now().getTime() < deadline.getTime()) throw arm(new AwaitSignalSignal(name, deadline));
    // Deadline passed with an empty snapshot inbox — commit the timeout, but only if no signal
    // raced in since the snapshot; if one did, re-park so the next tick consumes it, not drops it.
    const stored = await backend.store.checkpointStep(
      {
        runId,
        cursorKey: key,
        status: "ok",
        result: { received: false },
        attempts: attempt,
        call,
      },
      { requireVersion: claimVersion },
    );
    if (stored.committed === false) throw arm(new AwaitSignalSignal(name, deadline));
    return fromLog<SignalOutcome<P>>(stored);
  };

  function signal<K extends SignalName<SignalMap>>(name: K): Promise<SignalPayload<SignalMap, K>>;
  function signal<K extends SignalName<SignalMap>>(
    name: K,
    opts: { timeoutMs: number },
  ): Promise<SignalOutcome<SignalPayload<SignalMap, K>>>;
  function signal<P>(
    name: string,
    opts?: { timeoutMs?: number },
  ): Promise<P> | Promise<SignalOutcome<P>> {
    return opts?.timeoutMs === undefined
      ? track(awaitSignal<P>(name))
      : track(awaitSignalUntil<P>(name, opts.timeoutMs));
  }

  return {
    runId,
    attempt,
    step: (name, fn, policy) => track(step(name, fn, policy)),
    sleep: (ms) => track(parkUntil(new Date(now().getTime() + ms))),
    sleepUntil: (date) => track(parkUntil(date)),
    invoke,
    signal,
    log(message, data) {
      // Skip entirely when nothing records it, and while replaying the already-logged durable prefix.
      if (!obs.records("run.log") || n < snap.steps.size) return;
      void Promise.resolve(obs.event("run.log", runId, now(), { message, data })).catch(() => {});
    },
  };
};
