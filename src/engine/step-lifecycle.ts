import { computeBackoff } from "../util/backoff";
import { FlowRuntimeError, toFlowError } from "../util/errors";
import { enforcePayloadCap } from "../util/payload-cap";
import { runWithTimeout } from "../util/timeout";
import type { RunContextState } from "./context-state";
import { FlowSuspend, isSuspend } from "./suspend";
import type { StepArg, StepOpts } from "./types";

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

/** @internal */
export const runStep = async <T>(
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
  const maxAttempts = (opts.retries ?? 3) + 1;
  const startedMs = Date.now();

  await state.storage.startStep(state.runId, cursorKey, attempts);
  await state.storage.recordEvent({
    runId: state.runId,
    type: "step_started",
    cursorKey,
    payload: { attempts },
  });

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
    await state.storage.finishStep({
      runId: state.runId,
      cursorKey,
      status: "ok",
      result,
      attempts,
    });
    await state.storage.recordEvent({
      runId: state.runId,
      type: "step_ok",
      cursorKey,
      payload: { attempts },
    });
    state.snapshot.steps.set(cursorKey, {
      runId: state.runId,
      cursorKey,
      status: "ok",
      result,
      error: null,
      attempts,
      startedAt: existing?.startedAt ?? new Date(),
      completedAt: new Date(),
    });
    emitStepFinished(state, cursorKey, "ok", startedMs);
    return result;
  }

  const error = toFlowError(fnError);
  const isNonRetryable = fnError instanceof FlowRuntimeError && fnError.nonRetryable;
  const classification = opts.classify
    ? opts.classify(fnError instanceof Error ? fnError : new Error(String(fnError)))
    : "transient";
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
    await state.storage.recordEvent({
      runId: state.runId,
      type: "step_terminal",
      cursorKey,
      payload: { code: error.code, attempts },
    });
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
  await state.storage.recordEvent({
    runId: state.runId,
    type: "step_failed",
    cursorKey,
    payload: { code: error.code, attempts },
  });

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
