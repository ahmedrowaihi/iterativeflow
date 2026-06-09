import type { SuspendReason } from "./types";

export interface SuspendOpts {
  wakeAt?: Date;
  wakeOn?: string;
  reason: SuspendReason;
}

/**
 * Control-flow signal thrown by `ctx.sleep` / `ctx.signal` / `ctx.invoke` to
 * park a run; the engine catches it and writes a transient status. It is not a
 * failure.
 *
 * Because it extends `Error`, a `try/catch` around a `ctx.*` call **swallows the
 * suspend** and the run never parks. Any catch wrapping `ctx.*` must re-throw it:
 *
 * @example
 * ```ts
 * try {
 *   await ctx.signal("approval", { timeout: "24h" });
 * } catch (err) {
 *   if (isSuspend(err)) throw err; // let the run park
 *   // ...handle real errors
 * }
 * ```
 */
export class FlowSuspend extends Error {
  readonly wakeAt?: Date;
  readonly wakeOn?: string;
  readonly reason: SuspendReason;

  constructor(opt: SuspendOpts) {
    super(`flow suspended (${opt.reason})`);
    this.name = "FlowSuspend";
    this.wakeAt = opt.wakeAt;
    this.wakeOn = opt.wakeOn;
    this.reason = opt.reason;
  }
}

/** Type guard for {@link FlowSuspend}. Use to re-throw suspends from a `catch` that wraps `ctx.*`. */
export const isSuspend = (err: unknown): err is FlowSuspend => err instanceof FlowSuspend;
