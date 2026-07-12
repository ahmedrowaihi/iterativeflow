import { dur } from "@/lib/format";
import { ACTIVE_STATUSES, type RunFull, type RunListItem } from "@/lib/types";

export const isRunning = (run: RunListItem): boolean =>
  ACTIVE_STATUSES.has(run.status) && !!run.startedAt && !run.completedAt;

export const runDuration = (run: RunListItem | RunFull): string => {
  if (run.completedAt) return dur(run.startedAt ?? run.createdAt, run.completedAt);
  if (ACTIVE_STATUSES.has(run.status) && run.startedAt) return dur(run.startedAt, null);
  return "—";
};
