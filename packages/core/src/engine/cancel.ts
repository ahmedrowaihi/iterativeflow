import type { Backend } from "#ports/outbox";
import { isTerminal } from "#status";

/**
 * Cancel a run and cascade to its non-terminal descendants. Cancel is sticky and clears the run's
 * pending timer atomically, and a canceled child wakes the parent waiting on it. In-flight step
 * effects on a worker mid-tick may still land (cooperative cancel) — the run's markRunning guard
 * stops the NEXT dispatch, not the one already executing.
 */
export const cancelRun = async (backend: Backend, runId: string): Promise<void> => {
  const run = await backend.store.loadRunRow(runId);
  await backend.store.markTerminal(runId, { status: "canceled" }, { cancelTimers: [runId] });
  if (run?.parentRunId && !isTerminal(run.status)) await wakeParent(backend, run.parentRunId);
  await cancelDescendants(backend, runId);
};

const wakeParent = async (backend: Backend, parentRunId: string): Promise<void> => {
  const parent = await backend.store.loadRunRow(parentRunId);
  if (!parent || isTerminal(parent.status)) return;
  await backend.store.arriveAtJoin(parentRunId);
  await backend.queue.enqueue(parentRunId);
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
