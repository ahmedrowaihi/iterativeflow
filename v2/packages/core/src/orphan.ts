import { NON_SUCCESS_TERMINAL_STATUSES, RECONCILABLE_STATUSES, isTerminal } from "#status";
import type { RunStatus } from "#types";

/** The minimal run shape the orphan check reads — satisfied by both `RunRow` and a backend's raw row. */
export interface OrphanRun {
  id: string;
  status: RunStatus;
  parentRunId?: string;
}

/**
 * The lookups the orphan check needs over the current run set. A backend builds this from its own
 * scan (memory/DynamoDB); Postgres expresses the same predicate in SQL instead.
 */
export interface OrphanView {
  hasJob(runId: string): boolean;
  hasTimer(runId: string): boolean;
  childrenOf(runId: string): readonly OrphanRun[];
  runById(runId: string): OrphanRun | undefined;
}

/**
 * Whether reconcile should re-drive `r`. Three cases: crash-stranded (reconcilable but off the queue
 * with no wake timer); a fan-out parent whose join has RESOLVED — any child failed/canceled
 * (fast-fail) or every child terminal — but whose wake was lost; or a live child whose parent
 * terminated without success (structured-concurrency cancel that never reached it).
 */
export const isOrphaned = (r: OrphanRun, v: OrphanView): boolean => {
  if (RECONCILABLE_STATUSES.includes(r.status) && !v.hasJob(r.id) && !v.hasTimer(r.id)) return true;

  if (r.status === "awaiting_child" && !v.hasJob(r.id)) {
    const kids = v.childrenOf(r.id);
    const resolved =
      kids.length > 0 &&
      (kids.some((c) => NON_SUCCESS_TERMINAL_STATUSES.includes(c.status)) ||
        kids.every((c) => isTerminal(c.status)));
    if (resolved) return true;
  }

  if (!isTerminal(r.status) && r.parentRunId) {
    const parent = v.runById(r.parentRunId);
    if (parent && NON_SUCCESS_TERMINAL_STATUSES.includes(parent.status)) return true;
  }

  return false;
};
