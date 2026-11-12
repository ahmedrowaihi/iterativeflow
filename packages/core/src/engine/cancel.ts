import type { Backend } from "#ports/outbox";
import { isTerminal } from "#status";
import type { TerminalStatus } from "#types";

/**
 * Cancel a run and cascade to its non-terminal descendants. Cancel is sticky and clears the run's
 * pending timer atomically, and a canceled child wakes the parent waiting on it. In-flight step
 * effects on a worker mid-tick may still land (cooperative cancel) — the run's markRunning guard
 * stops the NEXT dispatch, not the one already executing.
 */
export const cancelRun = async (backend: Backend, runId: string): Promise<void> => {
  const run = await backend.store.loadRunRow(runId);
  const won = await backend.store.markTerminal(
    runId,
    { status: "canceled" },
    { cancelTimers: [runId] },
  );
  if (won && run?.parentRunId) await arriveAtParent(backend, run.parentRunId, "canceled");
  await cancelDescendants(backend, runId);
};

/**
 * A child that just settled arrives at its parent's fan-out join. The parent wakes once every child
 * has arrived, or at once on a non-success (fast-fail). Best-effort: a missed wake is caught by the
 * reconcile sweep (a resolved-join parent with no job).
 * @internal
 */
export const arriveAtParent = async (
  backend: Backend,
  parentRunId: string,
  settled: TerminalStatus,
): Promise<void> => {
  const remaining = await backend.store.arriveAtJoin(parentRunId);
  if (remaining === undefined) return;
  if (settled !== "done" || remaining <= 0) await backend.queue.enqueue(parentRunId);
};

/**
 * Cancel every non-terminal descendant of `runId` — structured concurrency: a run's children do not
 * outlive its non-success termination (an explicit cancel, or a failure). Idempotent; terminal
 * children are skipped.
 */
export const cancelDescendants = async (backend: Backend, runId: string): Promise<void> => {
  const children = await backend.store.childrenOf(runId);
  for (const c of children) {
    if (!isTerminal(c.status)) await cancelRun(backend, c.id);
  }
};
