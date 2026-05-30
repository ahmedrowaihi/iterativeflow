import { eq, sql } from "drizzle-orm";
import type { WorkflowDb } from "../db";
import { runs } from "../schema";
import type { TxEnqueue } from "./types";

/**
 * Fire `pg_notify('flow_terminal', runId)` so subscribed engine instances
 * wake their `handle.result(runId)` waiters. If the run has a parent, also
 * re-enqueue the parent (it may be sleeping in `ctx.invoke`).
 *
 * @internal
 */
export const notifyTerminal =
  (db: WorkflowDb, enqueue: TxEnqueue) =>
  async (runId: string): Promise<void> => {
    const row = await db
      .select({ parentRunId: runs.parentRunId })
      .from(runs)
      .where(eq(runs.id, runId))
      .limit(1);
    if (row[0]?.parentRunId) {
      await enqueue(db, row[0].parentRunId);
    }
    await db.execute(sql`SELECT pg_notify('flow_terminal', ${runId})`);
  };
