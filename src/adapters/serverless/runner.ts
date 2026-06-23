import type { RunHandler } from "../../engine/scheduler";
import type { WorkflowDb } from "../../storage/db";
import { type DrainOpts, drainDueWakes } from "./outbox";

/** Options for {@link drainAndRun}. `now` defaults to the current instant. */
export type DrainAndRunOpts = Omit<DrainOpts, "now"> & { now?: Date };

/**
 * One serverless tick: drain every due wake and advance each run once. Wire to a
 * scheduled trigger (cron, queue consumer) that POSTs a `/drain` route. Runs
 * sequentially; a host wanting parallelism can call `engine.handleRun` per id
 * from {@link drainDueWakes} directly.
 */
export const drainAndRun = async (
  engine: RunHandler,
  db: WorkflowDb,
  opts?: DrainAndRunOpts,
): Promise<{ ran: string[] }> => {
  const ids = await drainDueWakes(db, { ...opts, now: opts?.now ?? new Date() });
  for (const id of ids) await engine.handleRun(id);
  return { ran: ids };
};
