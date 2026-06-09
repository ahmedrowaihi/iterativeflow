import { computeBackoff } from "../util/backoff";
import { type Duration, toFireAt, toMs } from "../util/duration";
import { asError, FlowRuntimeError, toFlowError } from "../util/errors";
import { enforcePayloadCap } from "../util/payload-cap";
import { formatIssues, validate } from "../util/standard-schema";
import { runWithTimeout } from "../util/timeout";
import { type Cursor, createKeyCursor, signalBase, sleepBase } from "./cursor";
import { FlowSuspend, isSuspend } from "./suspend";
import type { EventType } from "../storage/schema";
import type {
  FlowContext,
  FlowHandle,
  InvokeOpts,
  Logger,
  MetricsRecorder,
  RunSnapshot,
  SignalOpts,
  StepArg,
  StepOpts,
  Storage,
} from "./types";

/**
 * State for one executing run, shared by the per-node-kind execution helpers
 * below. The snapshot is loaded once at claim time; each helper mutates it in
 * place when it persists a new row so subsequent `await` points see it.
 *
 * @internal
 */
export interface RunContextState {
  readonly runId: string;
  readonly attempt: number;
  readonly storage: Storage;
  readonly snapshot: RunSnapshot;
  readonly logger: Logger;
  readonly abortSignal: AbortSignal;
  readonly defaultStepTimeoutMs?: number;
  readonly maxStepResultBytes?: number;
  readonly maxInvokeDepth: number;
  readonly maxChildrenPerRun: number;
  readonly metrics?: MetricsRecorder;
  readonly flow?: { name: string; version: number };
  readonly cursor: Cursor;
  readonly startChild?: (
    parentCursorKey: string,
    childHandle: FlowHandle<unknown, unknown>,
    input: unknown,
    opts: InvokeOpts | undefined,
  ) => Promise<string>;
}

const emitEvent = (
  state: RunContextState,
  type: EventType,
  cursorKey: string,
  payload?: unknown,
): Promise<void> => state.storage.recordEvent({ runId: state.runId, type, cursorKey, payload });

const emitStepFinished = (
  state: RunContextState,
  cursorKey: string,
  status: "ok" | "failed_retry" | "failed_terminal",
  startedMs: number,
): void => {
  if (!state.flow) return;
  state.metrics?.stepFinished?.({
    name: state.flow.name,
    version: state.flow.version,
    step: cursorKey,
    status,
    durationMs: Date.now() - startedMs,
  });
};

const runStep = async <T>(
  state: RunContextState,
  name: string,
  fn: (arg: StepArg) => Promise<T> | T,
  opts: StepOpts = {},
): Promise<T> => {
  const cursorKey = state.cursor.next(name);
  const existing = state.snapshot.steps.get(cursorKey);

  if (existing?.status === "ok") return existing.result as T;
  if (existing?.status === "failed_terminal") {
    const e = existing.error ?? { code: "STEP_FAILED", message: "step previously failed" };
    throw new FlowRuntimeError({ code: e.code, message: e.message, nonRetryable: true });
  }

  const attempts = (existing?.attempts ?? 0) + 1;
  const maxAttempts = (opts.retries ?? 0) + 1;
  const startedMs = Date.now();

  await state.storage.startStep(state.runId, cursorKey, attempts);
  await emitEvent(state, "step_started", cursorKey, { attempts });

  let fnResult: T;
  let fnError: unknown = null;
  try {
    fnResult = await runWithTimeout(
      (signal) => fn({ input: undefined as unknown, signal, attempt: attempts }),
      opts.timeoutMs ?? state.defaultStepTimeoutMs,
      `step "${cursorKey}"`,
      state.abortSignal,
    );
  } catch (err) {
    if (isSuspend(err)) {
      throw new FlowRuntimeError({
        code: "STEP_INVALID_AWAIT",
        message: `ctx.sleep / ctx.signal / ctx.invoke cannot be called inside ctx.step("${cursorKey}"). Move them to top-level flow code.`,
        nonRetryable: true,
      });
    }
    fnError = err;
  }

  if (fnError === null) {
    const result = fnResult!;
    enforcePayloadCap(`Step "${cursorKey}" result`, result, state.maxStepResultBytes);
    const finished = await state.storage.finishStep({
      runId: state.runId,
      cursorKey,
      status: "ok",
      result,
      attempts,
    });
    await emitEvent(state, "step_ok", cursorKey, { attempts });
    state.snapshot.steps.set(cursorKey, finished);
    emitStepFinished(state, cursorKey, "ok", startedMs);
    return result;
  }

  const error = toFlowError(fnError);
  const isNonRetryable = fnError instanceof FlowRuntimeError && fnError.nonRetryable;
  const classification = opts.classify ? opts.classify(asError(fnError)) : "transient";
  const exhausted = attempts >= maxAttempts;
  const terminal = isNonRetryable || classification === "permanent" || exhausted;

  if (terminal) {
    await state.storage.finishStep({
      runId: state.runId,
      cursorKey,
      status: "failed_terminal",
      error,
      attempts,
    });
    await emitEvent(state, "step_terminal", cursorKey, { code: error.code, attempts });
    emitStepFinished(state, cursorKey, "failed_terminal", startedMs);
    throw new FlowRuntimeError({ ...error, nonRetryable: true });
  }

  await state.storage.finishStep({
    runId: state.runId,
    cursorKey,
    status: "failed_retry",
    error,
    attempts,
  });
  emitStepFinished(state, cursorKey, "failed_retry", startedMs);
  await emitEvent(state, "step_failed", cursorKey, { code: error.code, attempts });

  const wakeAt = new Date(
    Date.now() +
      computeBackoff(attempts, {
        policy: opts.backoff,
        baseMs: opts.baseBackoffMs,
        capMs: opts.capBackoffMs,
      }),
  );
  throw new FlowSuspend({ reason: "step_retry", wakeAt });
};

const runSleep = async (state: RunContextState, duration: Duration): Promise<void> => {
  const cursorKey = state.cursor.next(sleepBase());
  const existing = state.snapshot.timers.get(cursorKey);

  if (existing?.firedAt) return;

  const fireAt = existing?.fireAt ?? toFireAt(duration);
  if (!existing) {
    await state.storage.createTimer(state.runId, cursorKey, fireAt);
    await emitEvent(state, "sleep_scheduled", cursorKey, { fireAt });
  }

  if (fireAt <= new Date()) {
    const fired = await state.storage.fireTimer(state.runId, cursorKey);
    await emitEvent(state, "sleep_fired", cursorKey);
    state.snapshot.timers.set(cursorKey, fired);
    return;
  }

  throw new FlowSuspend({ reason: "sleep", wakeAt: fireAt });
};

const validateSignalPayload = async <T>(
  name: string,
  payload: unknown,
  opts: SignalOpts<T>,
): Promise<T> => {
  if (!opts.schema) return payload as T;
  const parsed = await validate(opts.schema, payload);
  if (parsed.issues) {
    throw new FlowRuntimeError({
      code: "SIGNAL_PAYLOAD_INVALID",
      message: `Signal "${name}" payload failed schema: ${formatIssues(parsed.issues)}`,
      nonRetryable: true,
    });
  }
  return parsed.value;
};

const awaitSignal = async <T = unknown>(
  state: RunContextState,
  name: string,
  opts: SignalOpts<T> = {},
): Promise<T> => {
  const cursorKey = state.cursor.next(signalBase(name));
  const existing = state.snapshot.signals.get(cursorKey);

  if (existing?.delivered) {
    return validateSignalPayload(name, existing.payload, opts);
  }

  if (existing?.expiresAt && existing.expiresAt <= new Date()) {
    await emitEvent(state, "signal_timeout", cursorKey);
    throw new FlowRuntimeError({
      code: "SIGNAL_TIMEOUT",
      message: `Signal "${name}" expired`,
      nonRetryable: true,
    });
  }

  if (!existing) {
    const expiresAt = opts.timeout ? new Date(Date.now() + toMs(opts.timeout)) : undefined;
    const result = await state.storage.armOrConsumeSignal(state.runId, cursorKey, expiresAt);

    if (result.kind === "consumed") {
      state.snapshot.signals.set(cursorKey, result.row);
      return validateSignalPayload(name, result.payload, opts);
    }

    throw new FlowSuspend({
      reason: "awaiting_signal",
      wakeOn: cursorKey,
      wakeAt: expiresAt,
    });
  }

  throw new FlowSuspend({
    reason: "awaiting_signal",
    wakeOn: cursorKey,
    wakeAt: existing.expiresAt ?? undefined,
  });
};

const runInvoke = async <I, O>(
  state: RunContextState,
  handle: FlowHandle<I, O>,
  input: I,
  opts?: InvokeOpts,
): Promise<O> => {
  if (!state.startChild) {
    throw new FlowRuntimeError({
      code: "STEP_INVALID_AWAIT",
      message: "ctx.invoke is not supported in this runtime",
      nonRetryable: true,
    });
  }
  const cursorKey = state.cursor.next(`invoke:${handle.name}@${handle.version}`);

  const existing = await state.storage.findChildRun(state.runId, cursorKey);
  if (existing) {
    if (existing.status === "done") return existing.output as O;
    if (existing.status === "failed" || existing.status === "canceled") {
      const e = existing.error ?? {
        code: existing.status === "canceled" ? "RUN_CANCELED" : "FLOW_UNKNOWN",
        message: `child flow "${handle.name}" ended ${existing.status}`,
      };
      throw new FlowRuntimeError({
        code: e.code,
        message: `child flow "${handle.name}" ${existing.status}: ${e.message}`,
        nonRetryable: true,
      });
    }
    throw new FlowSuspend({ reason: "awaiting_signal", wakeOn: cursorKey });
  }

  const { depth, childCount } = await state.storage.invokeBudget(state.runId);
  if (depth >= state.maxInvokeDepth) {
    throw new FlowRuntimeError({
      code: "INVOKE_DEPTH_EXCEEDED",
      message: `ctx.invoke chain would exceed maxInvokeDepth=${state.maxInvokeDepth} (current depth ${depth})`,
      nonRetryable: true,
    });
  }
  if (childCount >= state.maxChildrenPerRun) {
    throw new FlowRuntimeError({
      code: "INVOKE_FANOUT_EXCEEDED",
      message: `run "${state.flow?.name ?? state.runId}" already has ${childCount} children; maxChildrenPerRun=${state.maxChildrenPerRun}`,
      nonRetryable: true,
    });
  }

  await state.startChild(cursorKey, handle as unknown as FlowHandle<unknown, unknown>, input, opts);
  throw new FlowSuspend({ reason: "awaiting_signal", wakeOn: cursorKey });
};

/** Inputs to {@link RuntimeFlowContext}. The cursor is created inline so callers don't have to. */
type ContextDeps = Omit<RunContextState, "cursor">;

/**
 * The {@link FlowContext} exposed to user code inside a flow body. Deep module:
 * the small step/sleep/signal/invoke interface fronts all of the run-execution
 * logic (cursor walk, snapshot memoization, suspend throwing) co-located here.
 *
 * @internal
 */
export class RuntimeFlowContext implements FlowContext {
  readonly runId: string;
  readonly attempt: number;
  private readonly state: RunContextState;

  constructor(deps: ContextDeps) {
    this.state = { ...deps, cursor: createKeyCursor() };
    this.runId = deps.runId;
    this.attempt = deps.attempt;
  }

  step<T>(name: string, fn: (arg: StepArg) => Promise<T> | T, opts?: StepOpts): Promise<T> {
    return runStep(this.state, name, fn, opts);
  }

  sleep(duration: Duration): Promise<void> {
    return runSleep(this.state, duration);
  }

  signal<T = unknown>(name: string, opts?: SignalOpts<T>): Promise<T> {
    return awaitSignal(this.state, name, opts);
  }

  invoke<I, O>(handle: FlowHandle<I, O>, input: I, opts?: InvokeOpts): Promise<O> {
    return runInvoke(this.state, handle, input, opts);
  }

  log(message: string, payload?: Record<string, unknown>): void {
    this.state.logger.info(message, { runId: this.runId, ...payload });
  }
}
