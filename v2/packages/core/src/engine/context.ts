import type { IdGen } from "#id";
import type { Backend } from "#ports/outbox";
import type { RunSnapshot, StepOutcome } from "#types";
import {
  type Flow,
  type InvokeOutputs,
  type InvokeSpec,
  type SignalMap,
  type SignalName,
  type SignalPayload,
  type SignalSchemas,
  flowKey,
  validateSignal,
} from "#engine/flow";
import type { Observer } from "#engine/observe";
import {
  AwaitChildSignal,
  AwaitSignalSignal,
  FlowDriftError,
  SleepSignal,
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
  /** Reject the step's `fn` if it runs longer than this (and abort its signal). No timeout by default. */
  timeoutMs?: number;
  /**
   * Decide whether an error is worth retrying. A `permanent` verdict fails the step (and the
   * run) immediately — no in-invocation retries, no run-level retry. `transient` (the default)
   * retries as configured. Use it to fail fast on 4xx/validation errors and retry 5xx/timeouts.
   */
  classify?: (error: unknown) => "transient" | "permanent";
}

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
   * retries, a timeout, and error classification; `fn` receives an {@link StepArg} (abort
   * signal + attempt). Durable long backoff is still the run-level retry.
   */
  step<T>(name: string, fn: (arg: StepArg) => Promise<T> | T, policy?: StepPolicy): Promise<T>;

  /** Durably park the run for `ms`, releasing the worker. Resumes after the deadline. */
  sleep(ms: number): Promise<void>;

  /** Durably park until `date`. */
  sleepUntil(date: Date): Promise<void>;

  /**
   * Spawn `flow(input)` as a child run and return its output. The child is created exactly
   * once (recorded in the step memo); the parent parks until the child completes, then
   * resumes with the child's output. A child failure surfaces as a thrown error.
   */
  invoke<CI, CO>(flow: Flow<CI, CO, any>, input: CI): Promise<CO>;

  /**
   * Fan out: spawn every child in parallel and join, resolving with the outputs in order. Fast-fail
   * — if any child fails (or is canceled), the parent fails and its still-running siblings are
   * cancelled (structured concurrency). Children spawn in chunks, each an atomic memoized checkpoint.
   */
  invoke<const T extends readonly InvokeSpec[]>(specs: T): Promise<InvokeOutputs<T>>;

  /**
   * Durably wait for an external signal named `name` and return its payload. If a matching
   * signal is already in the inbox it is consumed immediately; otherwise the run parks until
   * one is delivered (`engine.signal`). Consumption is memoized, so a replay returns the same
   * payload without re-waiting.
   *
   * When the flow declares a `signals` map, only those names compile and each returns its declared
   * payload type. A flow with no `signals` map is unchanged — any name, payload `unknown`.
   */
  signal<K extends SignalName<S>>(name: K): Promise<SignalPayload<S, K>>;
}

/** The clock the executor threads in — injectable for deterministic tests. */
export type Clock = () => Date;

/** Wall-clock default. Passed where a deployment doesn't inject its own {@link Clock}. */
export const systemClock: Clock = () => new Date();

const pause = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const errCode = (e: unknown): string =>
  e instanceof Error ? e.name || "STEP_FAILED" : "STEP_FAILED";
const errMsg = (e: unknown): string => (e instanceof Error ? e.message : String(e));

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
      // A permanent error fails the step (and the run) immediately — a StepFailedError is
      // non-retryable, so neither the in-invocation loop nor the run-level retry re-runs it.
      if (policy?.classify?.(e) === "permanent") throw new StepFailedError(errCode(e), errMsg(e));
      if (attempt > retries) throw e; // transient budget spent → let run-level retry take over
      if (policy?.retryDelayMs) await pause(policy.retryDelayMs);
    }
  }
};

/** @internal */
export interface CtxDeps {
  backend: Backend;
  snap: RunSnapshot;
  attempt: number;
  now: Clock;
  id: IdGen;
  obs: Observer;
  /** The running flow's signal schemas — validate each consumed payload against its declared one. */
  signals?: SignalSchemas<SignalMap>;
}

/** @internal */
export const makeCtx = ({ backend, snap, attempt, now, id, obs, signals }: CtxDeps): Ctx => {
  const runId = snap.run.id;
  let cursor = 0;

  // Advance the cursor and fetch the memo at it. `shape` is the `kind:label` of the call being made
  // now; if a memo recorded a different shape here, the flow body drifted under this run.
  const memoAt = (shape: string): { key: string; memo: StepOutcome | undefined } => {
    const key = `s${cursor++}`;
    const memo = snap.steps.get(key);
    if (memo?.shape !== undefined && memo.shape !== shape) {
      throw new FlowDriftError(key, memo.shape, shape);
    }
    return { key, memo };
  };
  const consumed = new Set<string>();

  const step = async <T>(
    name: string,
    fn: (arg: StepArg) => Promise<T> | T,
    policy?: StepPolicy,
  ): Promise<T> => {
    const shape = `step:${name}`;
    const { key, memo } = memoAt(shape);
    if (memo) {
      if (memo.status === "failed_terminal") {
        throw new StepFailedError(memo.error?.code ?? "STEP_FAILED", memo.error?.message ?? "");
      }
      return memo.result as T;
    }
    const result = await runWithPolicy(fn, policy);
    const stored = await backend.store.checkpointStep({
      runId,
      cursorKey: key,
      status: "ok",
      result,
      attempts: attempt,
      shape,
    });
    await obs.event("step.finished", runId, now(), { cursorKey: key });
    obs.metrics.stepFinished?.(runId, key);
    return stored.result as T;
  };

  const parkUntil = async (wakeAt: Date): Promise<void> => {
    const { key, memo } = memoAt("sleep");
    const at = memo ? new Date(memo.result as string) : wakeAt; // stored as an ISO string
    if (!memo) {
      await backend.store.checkpointStep({
        runId,
        cursorKey: key,
        status: "ok",
        result: at.toISOString(),
        attempts: attempt,
        shape: "sleep",
      });
    }
    if (now().getTime() >= at.getTime()) return;
    throw new SleepSignal(at);
  };

  // Children spawn CHUNK-at-a-time so each spawn checkpoint stays within DynamoDB's transaction
  // budget — one atomic, memoized write per chunk (no unrecoverable two-phase overflow).
  const CHUNK = 20;
  const invokeMany = async (specs: readonly InvokeSpec[]): Promise<unknown[]> => {
    const childIds: string[] = [];
    for (let i = 0; i < specs.length; i += CHUNK) {
      const chunk = specs.slice(i, i + CHUNK);
      const shape = `invokeAll:${i / CHUNK}:${chunk.length}`;
      const { key, memo } = memoAt(shape);
      if (memo) {
        childIds.push(...(memo.result as string[]));
        continue;
      }
      const ids = chunk.map(() => id());
      const stored = await backend.store.checkpointStep(
        { runId, cursorKey: key, status: "ok", result: ids, attempts: attempt, shape },
        {
          spawn: chunk.map((s, j) => ({
            runId: ids[j],
            spec: {
              name: s.flow.name,
              version: s.flow.version,
              input: s.input,
              parentRunId: runId,
              parentCursorKey: key,
            },
          })),
        },
      );
      childIds.push(...(stored.result as string[]));
    }
    const kids = await Promise.all(childIds.map((cid) => backend.store.loadRunRow(cid)));
    if (kids.some((k) => !k)) throw new AwaitChildSignal(childIds[0]); // some spawns not visible yet
    const failed = kids.find((k) => k?.status === "failed" || k?.status === "canceled");
    if (failed) {
      throw new StepFailedError(
        failed.error?.code ?? "CHILD_FAILED",
        failed.error?.message ?? "a fan-out child did not complete",
      );
    }
    if (kids.every((k) => k?.status === "done")) return kids.map((k) => k?.output);
    throw new AwaitChildSignal(childIds[0]); // some children still running — park
  };

  return {
    runId,
    attempt,
    step,
    sleep: (ms) => parkUntil(new Date(now().getTime() + ms)),
    sleepUntil: (date) => parkUntil(date),

    invoke: (async (
      flowOrSpecs: Flow<unknown, unknown, any> | readonly InvokeSpec[],
      input?: unknown,
    ): Promise<unknown> => {
      if (Array.isArray(flowOrSpecs)) return invokeMany(flowOrSpecs);
      const flow = flowOrSpecs as Flow<unknown, unknown, any>;
      const shape = `invoke:${flowKey(flow.name, flow.version)}`;
      const { key, memo } = memoAt(shape);
      let childId: string;
      if (memo) {
        childId = memo.result as string;
      } else {
        const candidate = id();
        // First-writer-wins: if a concurrent invocation already spawned this step, the
        // checkpoint is a no-op that returns THAT winner's childId — trust the returned
        // value, not our candidate, or we'd await a child that was never created.
        const stored = await backend.store.checkpointStep(
          { runId, cursorKey: key, status: "ok", result: candidate, attempts: attempt, shape },
          {
            spawn: [
              {
                runId: candidate,
                spec: {
                  name: flow.name,
                  version: flow.version,
                  input,
                  parentRunId: runId,
                  parentCursorKey: key,
                },
              },
            ],
          },
        );
        childId = stored.result as string;
      }
      const child = await backend.store.loadRunRow(childId);
      if (!child) throw new AwaitChildSignal(childId); // spawn committed; child row not visible yet
      if (child.status === "done") return child.output;
      if (child.status === "failed" || child.status === "canceled") {
        throw new StepFailedError(
          child.error?.code ?? "CHILD_FAILED",
          child.error?.message ?? `child ${flowKey(flow.name, flow.version)} did not complete`,
        );
      }
      throw new AwaitChildSignal(childId);
    }) as Ctx["invoke"],

    async signal<T>(name: string): Promise<T> {
      const shape = `signal:${name}`;
      const { key, memo } = memoAt(shape);
      if (memo) return memo.result as T;
      const pending = snap.signals.find((s) => s.name === name && !consumed.has(s.id));
      if (!pending) throw new AwaitSignalSignal(name);
      consumed.add(pending.id); // don't let a later wait in this invocation drain the same one
      const schema = signals?.[name];
      let payload = pending.payload;
      if (schema) {
        try {
          payload = await validateSignal(schema, name, pending.payload);
        } catch (e) {
          // Permanent — the inbox payload won't change on retry, so fail the run.
          throw new StepFailedError("SIGNAL_INVALID", e instanceof Error ? e.message : String(e));
        }
      }
      const stored = await backend.store.checkpointStep(
        { runId, cursorKey: key, status: "ok", result: payload, attempts: attempt, shape },
        { consumeSignals: [pending.id] },
      );
      return stored.result as T;
    },
  };
};
