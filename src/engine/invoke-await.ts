import { FlowRuntimeError } from "../util/errors";
import type { RunContextState } from "./context-state";
import { FlowSuspend } from "./suspend";
import type { FlowHandle, InvokeOpts } from "./types";

/** @internal */
export const runInvoke = async <I, O>(
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
