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
  constructor(readonly name: string) {}
}

export type ControlSignal = SleepSignal | AwaitChildSignal | AwaitSignalSignal;

export const isControlSignal = (e: unknown): e is ControlSignal =>
  e instanceof SleepSignal || e instanceof AwaitChildSignal || e instanceof AwaitSignalSignal;

/**
 * A step that reached a terminal failure (checkpointed as `failed_terminal`). On replay it
 * is re-thrown from `ctx.step` so control flow is identical to the original failing run.
 */
export class StepFailedError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "StepFailedError";
    this.code = code;
  }
}

/** Thrown when a step's `fn` exceeds its `timeoutMs`. Counts against the step's retry budget. */
export class StepTimeoutError extends Error {
  constructor(ms: number) {
    super(`step exceeded its ${ms}ms timeout`);
    this.name = "StepTimeoutError";
  }
}
