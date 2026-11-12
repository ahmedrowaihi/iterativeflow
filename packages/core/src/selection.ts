import {
  RUN_STATUSES,
  TERMINAL_STATUSES,
  type PurgeFilter,
  type RunFilter,
  type RunStatus,
} from "#types";
import { ACTIVE_STATUSES, statusList } from "#status";

/**
 * The runs one Store read or bulk write acts on, already narrowed: `statuses` is the filter's status
 * set intersected with what the operation is defined on, so it can never widen past it (a purge never
 * reaches a live run, a retry never reaches a non-failed one). Empty `statuses` selects nothing.
 */
export interface RunSelection {
  statuses: readonly RunStatus[];
  name?: string;
  version?: number;
  tag?: string;
  /** Created strictly before this instant. */
  before?: Date;
  /** Leave out runs whose parent is still live: a fan-out parent re-reads every child when it joins,
   *  so deleting a finished child under it would leave the parent waiting forever. */
  spareChildrenOfLiveParents?: true;
}

const narrow = (
  asked: RunStatus | readonly RunStatus[] | undefined,
  allowed: readonly RunStatus[],
) => {
  const list = statusList(asked);
  if (!list) return allowed;
  const set = new Set<string>(allowed);
  return list.filter((s) => set.has(s));
};

const requirePredicate = (filter: RunFilter | PurgeFilter, op: string, hint = ""): void => {
  if (Object.values(filter).every((v) => v === undefined)) {
    throw new Error(`${op}: filter needs at least one predicate${hint}`);
  }
};

/** The selection a {@link Store.listRuns} page reads. An empty filter lists every run. */
export const listSelection = (filter: RunFilter): RunSelection => ({
  statuses: narrow(filter.status, RUN_STATUSES),
  name: filter.name,
  version: filter.version,
  tag: filter.tag,
});

const runSetSelection = (
  filter: RunFilter,
  allowed: readonly RunStatus[],
  op: string,
): RunSelection => {
  requirePredicate(filter, op);
  return { ...listSelection(filter), statuses: narrow(filter.status, allowed) };
};

/**
 * The selection a {@link Store.cancelRuns} sweep cancels: live runs only.
 * @throws {Error} when the filter carries no predicate — a set operation over everything must be
 * spelled out, not defaulted into.
 */
export const cancelSelection = (filter: RunFilter): RunSelection =>
  runSetSelection(filter, ACTIVE_STATUSES, "cancelRuns");

/**
 * The selection a {@link Store.retryRuns} sweep re-drives: `failed` runs only.
 * @throws {Error} when the filter carries no predicate.
 */
export const retrySelection = (filter: RunFilter): RunSelection =>
  runSetSelection(filter, ["failed"], "retryRuns");

/**
 * The selection a {@link Store.deleteRuns} sweep deletes: terminal runs only.
 * @throws {Error} when the filter carries no predicate — "delete all history" must be explicit.
 */
export const purgeSelection = (filter: PurgeFilter): RunSelection => {
  requirePredicate(filter, "deleteRuns", "; pass { before: new Date() } to purge all history");
  return {
    statuses: narrow(filter.status, TERMINAL_STATUSES),
    name: filter.name,
    version: filter.version,
    before: filter.before,
    spareChildrenOfLiveParents: true,
  };
};

/** The run fields a selection reads — satisfied by `RunRow` and by a backend's raw row. */
export interface SelectableRun {
  name: string;
  version: number;
  status: RunStatus;
  tags?: readonly string[];
  createdAt?: Date;
  parentRunId?: string;
}

/**
 * The row predicate of a selection, for the backends that scan instead of querying. `statusOf` looks
 * up another run's status (`undefined` = gone); a purge selection, which spares children of live
 * parents, requires it.
 */
export const runMatcher = (
  sel: RunSelection,
  statusOf?: (runId: string) => RunStatus | undefined,
): ((run: SelectableRun) => boolean) => {
  if (sel.spareChildrenOfLiveParents && !statusOf) {
    throw new Error("runMatcher: a purge selection needs a statusOf lookup for parent runs");
  }
  const statuses = new Set<string>(sel.statuses);
  const terminal = new Set<string>(TERMINAL_STATUSES);
  const cutoff = sel.before?.getTime();
  const parentIsLive = (run: SelectableRun): boolean => {
    if (run.parentRunId === undefined) return false;
    const parent = statusOf?.(run.parentRunId);
    return parent !== undefined && !terminal.has(parent);
  };
  return (run) =>
    statuses.has(run.status) &&
    (sel.name === undefined || run.name === sel.name) &&
    (sel.version === undefined || run.version === sel.version) &&
    (sel.tag === undefined || (run.tags?.includes(sel.tag) ?? false)) &&
    (cutoff === undefined || (run.createdAt?.getTime() ?? 0) < cutoff) &&
    !(sel.spareChildrenOfLiveParents && parentIsLive(run));
};

/** A status set as a SQL literal tuple, e.g. `('done','failed')`. Only ever fed core status constants. */
export const sqlStatusTuple = (statuses: readonly RunStatus[]): string =>
  `(${statuses.map((s) => `'${s}'`).join(",")})`;

/** Dialect specifics for {@link runSelectionSql}. */
export interface SelectionSqlOpts<Time> {
  /** Renders the nth (1-based) bind placeholder: `` (n) => `$${n}` `` for Postgres, `() => "?"` else. */
  placeholder: (n: number) => string;
  /** How an instant binds — a `Date` for Postgres, epoch ms for the integer-time backends. */
  time: (at: Date) => Time;
  /** Renders "the run's `tags` contain the value bound at `placeholder`". Required so a backend
   *  cannot silently drop a tag filter and widen a bulk cancel or retry to every tenant. */
  tag: (placeholder: string) => string;
  /** The run table, rendered (schema/prefix applied) and NOT aliased in the outer query — the
   *  live-parent guard correlates against it by name. */
  run: string;
}

/**
 * The SQL form of {@link runMatcher}: the `WHERE` body and its binds, numbered from 1, so a trailing
 * clause takes `placeholder(params.length + 1)`. `undefined` when the selection has no status — the
 * caller touches nothing rather than emitting an empty `IN ()`. Statuses render as literals, not
 * binds: a partial index over the terminal statuses is only usable when the planner can prove the
 * query's predicate implies the index's, which it cannot do through a bind parameter.
 */
export const runSelectionSql = <Time>(
  sel: RunSelection,
  o: SelectionSqlOpts<Time>,
): { where: string; params: (string | number | Time)[] } | undefined => {
  if (sel.statuses.length === 0) return undefined;
  const params: (string | number | Time)[] = [];
  const where = [`status IN ${sqlStatusTuple(sel.statuses)}`];
  const bind = (value: string | number | Time): string => {
    params.push(value);
    return o.placeholder(params.length);
  };
  if (sel.name !== undefined) where.push(`name = ${bind(sel.name)}`);
  if (sel.version !== undefined) where.push(`version = ${bind(sel.version)}`);
  if (sel.tag !== undefined) where.push(o.tag(bind(sel.tag)));
  if (sel.before !== undefined) where.push(`created_at < ${bind(o.time(sel.before))}`);
  if (sel.spareChildrenOfLiveParents) {
    where.push(
      `NOT EXISTS (SELECT 1 FROM ${o.run} p WHERE p.id = ${o.run}.parent_run_id AND p.status NOT IN ${sqlStatusTuple(TERMINAL_STATUSES)})`,
    );
  }
  return { where: where.join(" AND "), params };
};
