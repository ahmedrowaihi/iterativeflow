import type { Duration } from "../util/duration";
import type { RunContextState } from "./context-state";
import { createKeyCursor } from "./cursor";
import { runInvoke } from "./invoke-await";
import { awaitSignal } from "./signal-await";
import { runSleep } from "./sleep-lifecycle";
import { runStep } from "./step-lifecycle";
import type { FlowContext, FlowHandle, InvokeOpts, SignalOpts, StepArg, StepOpts } from "./types";

/** Inputs to {@link RuntimeFlowContext}. The cursor is created inline so callers don't have to. */
type ContextDeps = Omit<RunContextState, "cursor">;

/**
 * The {@link FlowContext} exposed to user code inside a flow body. A thin
 * facade — each method delegates to its lifecycle module, so the state
 * (cursor, snapshot, storage, caps) lives in one place and the per-node-kind
 * logic lives in its own module.
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
