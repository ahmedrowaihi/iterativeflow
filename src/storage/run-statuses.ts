/** All run lifecycle states, in lifecycle order. */
export const RUN_STATUSES = [
  "pending",
  "running",
  "sleeping",
  "awaiting_signal",
  "retrying",
  "done",
  "failed",
  "canceled",
] as const;

/** Run lifecycle states. Terminal: `done`, `failed`, `canceled`. */
export type RunStatus = (typeof RUN_STATUSES)[number];

const TERMINAL_RUN_STATUSES = new Set<RunStatus>(["done", "failed", "canceled"]);

/** Runs still progressing — every state that isn't terminal. */
export const ACTIVE_RUN_STATUSES: ReadonlySet<RunStatus> = new Set(
  RUN_STATUSES.filter((s) => !TERMINAL_RUN_STATUSES.has(s)),
);
