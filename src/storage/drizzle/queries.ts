import { and, asc, desc, eq, gte, inArray, lte, sql } from "drizzle-orm";
import type { WorkflowDb } from "../db";
import { runs, signals, steps, timers } from "../schema";
import type { ListRunsOpts, ListRunsPage, RunDetail } from "../types";

/** @internal */
export const loadRunDetail =
  (db: WorkflowDb) =>
  async (runId: string): Promise<RunDetail | undefined> => {
    const detail = await Promise.all([
      db.select().from(runs).where(eq(runs.id, runId)).limit(1),
      db.select().from(steps).where(eq(steps.runId, runId)),
      db.select().from(timers).where(eq(timers.runId, runId)),
      db.select().from(signals).where(eq(signals.runId, runId)),
    ]);
    const run = detail[0][0];
    if (!run) return undefined;
    return {
      run,
      steps: detail[1],
      timers: detail[2],
      signals: detail[3],
    } satisfies RunDetail;
  };

/** @internal */
export const loadOutput =
  (db: WorkflowDb) =>
  async (runId: string): Promise<unknown> => {
    const row = await db
      .select({ output: runs.output, status: runs.status })
      .from(runs)
      .where(eq(runs.id, runId))
      .limit(1);
    if (row.length === 0 || row[0].status !== "done") return undefined;
    return row[0].output;
  };

/** @internal */
export const listRuns =
  (db: WorkflowDb) =>
  async (query: ListRunsOpts): Promise<ListRunsPage> => {
    const conds = [] as ReturnType<typeof eq>[];
    if (query.name) conds.push(eq(runs.name, query.name));
    if (query.status?.length) conds.push(inArray(runs.status, [...query.status]));
    if (query.tag)
      conds.push(
        sql`${runs.tags} && ARRAY[${query.tag}]::text[]` as unknown as ReturnType<typeof eq>,
      );
    if (query.since) conds.push(gte(runs.createdAt, query.since));
    if (query.until) conds.push(lte(runs.createdAt, query.until));
    if (query.cursor) {
      conds.push(
        sql`(${runs.createdAt}, ${runs.id}) < (${query.cursor.createdAt}, ${query.cursor.id}::uuid)` as unknown as ReturnType<
          typeof eq
        >,
      );
    }
    const limit = Math.min(query.limit ?? 50, 500);
    const rows = await db
      .select()
      .from(runs)
      .where(conds.length ? and(...conds) : undefined)
      .orderBy(desc(runs.createdAt), desc(runs.id))
      .limit(limit + 1);
    const page = rows.slice(0, limit);
    const more = rows.length > limit;
    const next = more
      ? { createdAt: page[page.length - 1].createdAt, id: page[page.length - 1].id }
      : undefined;
    return { runs: page, next } satisfies ListRunsPage;
  };

/** @internal */
export const findChildRun =
  (db: WorkflowDb) => async (parentRunId: string, parentCursorKey: string) => {
    const rows = await db
      .select()
      .from(runs)
      .where(and(eq(runs.parentRunId, parentRunId), eq(runs.parentCursorKey, parentCursorKey)))
      .limit(1);
    return rows[0];
  };

/** @internal */
export const listChildren = (db: WorkflowDb) => (parentRunId: string) =>
  db.select().from(runs).where(eq(runs.parentRunId, parentRunId));

// re-export asc for parity in other modules without re-importing drizzle
export { asc };
