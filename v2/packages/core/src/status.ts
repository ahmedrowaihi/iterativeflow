import { RUN_STATUSES, type RunStatus } from "#types";

/** The terminal run states — the one place they're written; every subset derives from it. */
export const TERMINAL_STATUSES: readonly RunStatus[] = ["done", "failed", "canceled"];

/** Whether a run has settled and must never be resurrected. Narrows to the terminal subset. */
export const isTerminal = (status: RunStatus): status is "done" | "failed" | "canceled" =>
  (TERMINAL_STATUSES as readonly string[]).includes(status);

/** The non-terminal (still-live) states — DERIVED, so a new status can't drift out of it. */
export const ACTIVE_STATUSES: readonly RunStatus[] = RUN_STATUSES.filter((s) => !isTerminal(s));

/**
 * States the reconciler re-drives when a run is off the queue with no timer: the actively
 * progressing ones. DERIVED = active minus the states that legitimately wait on an external
 * event (a signal or a child), which have no queue/timer of their own.
 */
export const RECONCILABLE_STATUSES: readonly RunStatus[] = ACTIVE_STATUSES.filter(
  (s) => s !== "awaiting_signal" && s !== "awaiting_child",
);

/** Type guard for an untrusted string (query params, external input). */
export const isRunStatus = (s: string): s is RunStatus =>
  (RUN_STATUSES as readonly string[]).includes(s);

/** A fresh all-zero per-status counter — completeness is enforced by `Record<RunStatus, …>`. */
export const zeroRunStats = (): Record<RunStatus, number> => ({
  pending: 0,
  running: 0,
  sleeping: 0,
  awaiting_signal: 0,
  awaiting_child: 0,
  retrying: 0,
  done: 0,
  failed: 0,
  canceled: 0,
});

/** Normalize a `RunFilter.status` (one, several, or none) to an array — or `undefined`. */
export const statusList = (status?: RunStatus | readonly RunStatus[]): RunStatus[] | undefined =>
  status === undefined ? undefined : Array.isArray(status) ? [...status] : [status as RunStatus];
