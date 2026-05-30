import { toMs } from "../util/duration";
import { FlowRuntimeError } from "../util/errors";
import { formatIssues, validate } from "../util/standard-schema";
import type { RunContextState } from "./context-state";
import { signalBase } from "./cursor";
import { FlowSuspend } from "./suspend";
import type { SignalOpts } from "./types";

const validatePayload = async <T>(
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

/** @internal */
export const awaitSignal = async <T = unknown>(
  state: RunContextState,
  name: string,
  opts: SignalOpts<T> = {},
): Promise<T> => {
  const cursorKey = state.cursor.next(signalBase(name));
  const existing = state.snapshot.signals.get(cursorKey);

  if (existing?.delivered) {
    return validatePayload(name, existing.payload, opts);
  }

  if (existing?.expiresAt && existing.expiresAt <= new Date()) {
    await state.storage.recordEvent({
      runId: state.runId,
      type: "signal_timeout",
      cursorKey,
    });
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
      state.snapshot.signals.set(cursorKey, {
        runId: state.runId,
        cursorKey,
        delivered: true,
        payload: result.payload,
        expiresAt: null,
        createdAt: new Date(),
        deliveredAt: new Date(),
      });
      return validatePayload(name, result.payload, opts);
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
