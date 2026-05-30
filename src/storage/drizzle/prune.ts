import { and, inArray, lte } from "drizzle-orm";
import type { RunStatus } from "../schema";
import type { StorageSliceDeps } from "./types";

/** @internal */
export const pruneEvents =
  ({ db, tables }: StorageSliceDeps) =>
  async ({
    olderThan,
    batchSize = 1000,
  }: {
    olderThan: Date;
    batchSize?: number;
  }): Promise<number> => {
    const { events } = tables;
    const candidates = await db
      .select({ id: events.id })
      .from(events)
      .where(lte(events.at, olderThan))
      .limit(batchSize);
    if (candidates.length === 0) return 0;
    const ids = candidates.map((r) => r.id);
    const deleted = await db
      .delete(events)
      .where(inArray(events.id, ids))
      .returning({ id: events.id });
    return deleted.length;
  };

/** @internal */
export const pruneRuns =
  ({ db, tables }: StorageSliceDeps) =>
  async ({
    olderThan,
    status = ["done", "failed", "canceled"],
    batchSize = 1000,
  }: {
    olderThan: Date;
    status?: ReadonlyArray<RunStatus>;
    batchSize?: number;
  }): Promise<number> => {
    const { runs } = tables;
    const candidates = await db
      .select({ id: runs.id })
      .from(runs)
      .where(and(lte(runs.updatedAt, olderThan), inArray(runs.status, [...status])))
      .limit(batchSize);
    if (candidates.length === 0) return 0;
    const ids = candidates.map((r) => r.id);
    const deleted = await db.delete(runs).where(inArray(runs.id, ids)).returning({ id: runs.id });
    return deleted.length;
  };
