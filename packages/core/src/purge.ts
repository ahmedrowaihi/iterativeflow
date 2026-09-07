import { TERMINAL_STATUSES, type PurgeFilter, type RunStatus, type TerminalStatus } from "#types";
import { isTerminal, statusList } from "#status";

/**
 * The statuses a {@link Store.deleteRuns} sweep may delete: `filter.status` intersected with the
 * terminal set, or every terminal state when unset. The intersection is what makes the terminal-only
 * guard unconditional — an untyped caller asking for `running` gets it dropped rather than honoured.
 * An empty result means the filter selects nothing, not everything.
 *
 * @throws {Error} when the filter carries no predicate — "delete all history" must be explicit.
 */
export const purgeStatuses = (filter: PurgeFilter): readonly TerminalStatus[] => {
  if (Object.values(filter).every((v) => v === undefined)) {
    throw new Error(
      "deleteRuns: filter needs at least one predicate; pass { before: new Date() } to purge all history",
    );
  }
  const asked = statusList(filter.status);
  return asked ? asked.filter(isTerminal) : TERMINAL_STATUSES;
};

/** The minimal run shape a purge reads — satisfied by `RunRow` and by a backend's raw row. */
export interface PurgeRun {
  name: string;
  version: number;
  status: RunStatus;
  createdAt?: Date;
}

/**
 * The row predicate for the scanning backends (memory/Redis/DynamoDB), built once per sweep so the
 * status set and cutoff aren't recomputed per row. The SQL backends express the same predicate as
 * {@link purgeWhereSql} — single source, so a new purge predicate lands on both sides at once.
 */
export const purgeMatcher = (filter: PurgeFilter): ((run: PurgeRun) => boolean) => {
  const statuses = new Set<string>(purgeStatuses(filter));
  const cutoff = filter.before?.getTime();
  return (run) =>
    statuses.has(run.status) &&
    (cutoff === undefined || (run.createdAt?.getTime() ?? 0) < cutoff) &&
    (filter.name === undefined || run.name === filter.name) &&
    (filter.version === undefined || run.version === filter.version);
};

/** Dialect specifics for {@link purgeWhereSql}. */
export interface PurgeSqlOpts {
  /** Renders the nth (1-based) bind placeholder: `` (n) => `$${n}` `` for Postgres, `() => "?"` else. */
  placeholder: (n: number) => string;
  /** How an instant binds — a `Date` for Postgres, epoch ms for the integer-time backends. */
  time: (at: Date) => unknown;
}

/**
 * The SQL form of {@link purgeMatcher}: the `WHERE` body and its binds for the run-selecting half of
 * a purge. `undefined` when the filter selects no terminal status at all — the caller deletes nothing
 * rather than emitting an empty `IN ()`. Binds are numbered from 1, so a trailing `LIMIT` takes
 * `placeholder(params.length + 1)`.
 */
export const purgeWhereSql = (
  filter: PurgeFilter,
  o: PurgeSqlOpts,
): { where: string; params: unknown[] } | undefined => {
  const statuses = purgeStatuses(filter);
  if (statuses.length === 0) return undefined;
  const params: unknown[] = [...statuses];
  const where = [`status IN (${statuses.map((_, i) => o.placeholder(i + 1)).join(",")})`];
  const add = (column: string, value: unknown): void => {
    params.push(value);
    where.push(`${column} ${o.placeholder(params.length)}`);
  };
  if (filter.before !== undefined) add("created_at <", o.time(filter.before));
  if (filter.name !== undefined) add("name =", filter.name);
  if (filter.version !== undefined) add("version =", filter.version);
  return { where: where.join(" AND "), params };
};
