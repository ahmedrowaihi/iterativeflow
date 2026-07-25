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
 * scan (memory/DynamoDB); the SQL backends express the same predicate as {@link orphanedRunsSql}.
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

/** Dialect specifics for {@link orphanedRunsSql} — table identifiers and status tuples are already
 *  rendered (schema/prefix applied, `('done',…)`), so the builder stays dialect-neutral. */
export interface OrphanSqlOpts {
  run: string;
  job: string;
  timer: string;
  /** Status sets rendered as SQL tuples, e.g. `('done','failed','canceled')`. */
  reconcilable: string;
  terminal: string;
  nonSuccessTerminal: string;
  /** Insertion-order column to page by, e.g. `r.seq` (Postgres/MySQL) or `r.rowid` (SQLite). */
  order: string;
  /** Bind placeholder for the `LIMIT`, e.g. `$1` (Postgres) or `?` (SQLite/MySQL). */
  limit: string;
}

/**
 * The SQL form of {@link isOrphaned}: the same three-case predicate as one UNION query, for the SQL
 * backends that detect orphans in the database instead of a scan. Single source so a new orphan case
 * lands in both the TS predicate and every SQL backend at once. Returns run ids ordered oldest-first.
 */
export const orphanedRunsSql = (o: OrphanSqlOpts): string =>
  `SELECT id FROM (
     -- crash-stranded
     SELECT r.id AS id, ${o.order} AS ord FROM ${o.run} r
     WHERE r.status IN ${o.reconcilable}
       AND NOT EXISTS (SELECT 1 FROM ${o.job} j WHERE j.run_id = r.id)
       AND NOT EXISTS (SELECT 1 FROM ${o.timer} tm WHERE tm.run_id = r.id)
     UNION
     -- resolved-join parent whose wake was lost
     SELECT r.id AS id, ${o.order} AS ord FROM ${o.run} r
     WHERE r.status = 'awaiting_child'
       AND NOT EXISTS (SELECT 1 FROM ${o.job} j WHERE j.run_id = r.id)
       AND EXISTS (SELECT 1 FROM ${o.run} c WHERE c.parent_run_id = r.id)
       AND (
         EXISTS (SELECT 1 FROM ${o.run} c
                 WHERE c.parent_run_id = r.id AND c.status IN ${o.nonSuccessTerminal})
         OR NOT EXISTS (SELECT 1 FROM ${o.run} c
                        WHERE c.parent_run_id = r.id AND c.status NOT IN ${o.terminal})
       )
     UNION
     -- orphaned child of a failed/canceled parent
     SELECT r.id AS id, ${o.order} AS ord FROM ${o.run} r
     JOIN ${o.run} p ON p.id = r.parent_run_id
     WHERE r.status NOT IN ${o.terminal} AND p.status IN ${o.nonSuccessTerminal}
   ) q
   ORDER BY q.ord
   LIMIT ${o.limit}`;
