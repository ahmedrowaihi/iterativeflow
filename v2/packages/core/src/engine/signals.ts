/**
 * Control-flow signals thrown by the context to unwind a flow invocation without it being
 * a failure. They are NOT errors — the executor catches them and turns them into a durable
 * suspend. A user flow must never catch-and-swallow them (see {@link isControlSignal}).
 */

/** Thrown by `ctx.sleep` / `ctx.sleepUntil` to park the run until a deadline. */
export class SleepSignal {
  readonly kind = "sleep" as const;
  constructor(readonly wakeAt: Date) {}
}

/** Thrown by `ctx.invoke` when a spawned child hasn't completed yet — park until it does. */
export class AwaitChildSignal {
  readonly kind = "await_child" as const;
  constructor(readonly childRunId: string) {}
}

/** Thrown by `ctx.signal` when no matching signal is in the inbox — park until one arrives. */
export class AwaitSignalSignal {
  readonly kind = "await_signal" as const;
  constructor(
    readonly name: string,
    readonly deadline?: Date,
  ) {}
}

export type ControlSignal = SleepSignal | AwaitChildSignal | AwaitSignalSignal;

export const isControlSignal = (e: unknown): e is ControlSignal =>
  e instanceof SleepSignal || e instanceof AwaitChildSignal || e instanceof AwaitSignalSignal;

/** An error carrying a stable machine-readable `code`, distinct from the class name. */
export abstract class CodedError extends Error {
  abstract readonly code: string;
}

/** A step's `fn` failed permanently, or an awaited child run failed/was canceled — fails the run. */
export class StepFailedError extends CodedError {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "StepFailedError";
    this.code = code;
  }
}

/**
 * Thrown on replay when the `ctx` call at a cursor no longer matches the memo recorded there — the
 * flow body was reordered or refactored while this run was in flight. NOT a control signal: the
 * executor applies the engine's `driftPolicy` (park-recoverable or hard-fail). Fix by restoring the
 * flow's original call shape, or bump the flow `version` so new runs use the new shape.
 */
export class FlowDriftError extends CodedError {
  readonly code = "FLOW_DRIFT" as const;
  constructor(
    readonly cursorKey: string,
    readonly expected: string,
    readonly actual: string,
  ) {
    super(`flow drift at ${cursorKey}: memo is "${expected}" but the code now issues "${actual}"`);
    this.name = "FlowDriftError";
  }
}

/** Thrown by `submit`/`engine.submit` with `onDuplicate: "error"` when the idempotency key exists. */
export class DuplicateRunError extends CodedError {
  readonly code = "RUN_DUPLICATE" as const;
  constructor(
    readonly runId: string,
    idempotencyKey: string,
  ) {
    super(`submit: a run already exists for idempotencyKey "${idempotencyKey}" (${runId})`);
    this.name = "DuplicateRunError";
  }
}

/** Thrown when a step's `fn` exceeds its `timeoutMs`. Counts against the step's retry budget. */
export class StepTimeoutError extends Error {
  constructor(ms: number) {
    super(`step exceeded its ${ms}ms timeout`);
    this.name = "StepTimeoutError";
  }
}
