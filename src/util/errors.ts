import type { FlowError, FlowErrorCode } from "../storage/schema";

/** Construct a plain {@link FlowError} for storage/event payloads. */
export const flowError = (code: FlowErrorCode, message: string): FlowError => ({
  code,
  message,
});

/**
 * Thrown from inside a step or directly from a flow body. Set `nonRetryable`
 * to bypass retries when the failure is permanent (e.g. validation error).
 */
export class FlowRuntimeError extends Error {
  /** Stable error code carried over into the persisted {@link FlowError}. */
  readonly code: string;
  /** When `true`, retries are skipped and the step / run is marked terminal. */
  readonly nonRetryable: boolean;

  /** @param opt - `code` is required; `nonRetryable` defaults to `false`. */
  constructor(opt: { code: string; message: string; nonRetryable?: boolean; cause?: unknown }) {
    super(
      opt.message,
      opt.cause === undefined ? undefined : ({ cause: opt.cause } as ErrorOptions),
    );
    this.name = "FlowRuntimeError";
    this.code = opt.code;
    this.nonRetryable = opt.nonRetryable ?? false;
  }
}

/**
 * Coerce any thrown value into an `Error`, wrapping non-Error throws.
 *
 * @internal
 */
export const asError = (err: unknown): Error =>
  err instanceof Error ? err : new Error(String(err));

/** Normalize any thrown value into a {@link FlowError} suitable for persisting. */
export const toFlowError = (err: unknown): FlowError => {
  if (err instanceof FlowRuntimeError) {
    return { code: err.code, message: err.message, stack: err.stack };
  }
  if (err instanceof Error) {
    const code = (err as Error & { code?: string }).code ?? err.name ?? "UNKNOWN";
    return { code, message: err.message, stack: err.stack };
  }
  return { code: "UNKNOWN", message: String(err) };
};
