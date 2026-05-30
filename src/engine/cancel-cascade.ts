import type { Storage } from "./types";

interface Deps {
  storage: Storage;
  runControllers: Map<string, AbortController>;
  notifyTerminal: (runId: string) => void;
}

/**
 * BFS over `parent_run_id` to cancel a run and every still-in-flight
 * descendant. Aborts the in-flight `AbortSignal` for each, marks the run
 * `canceled`, records an event, and notifies any `handle.result(runId)`
 * waiter — local and remote (via `storage.notifyTerminal`).
 *
 * @internal
 */
export const createCancelCascade =
  (deps: Deps) =>
  async (runId: string, reason?: string): Promise<void> => {
    const seen = new Set<string>();
    const stack = [runId];
    while (stack.length) {
      const cur = stack.pop()!;
      if (seen.has(cur)) continue;
      seen.add(cur);
      const children = await deps.storage.listChildren(cur);
      for (const c of children) {
        if (c.status === "done" || c.status === "failed" || c.status === "canceled") continue;
        stack.push(c.id);
      }
      // Cancel after collecting children so we don't lose the parent_run_id link.
      const controller = deps.runControllers.get(cur);
      if (controller) controller.abort(reason ?? "canceled");
      await deps.storage.transaction(async (tx) => {
        await tx.lockRun(cur);
        await tx.markCanceled(cur, reason);
        await tx.recordEvent({ runId: cur, type: "canceled", payload: { reason } });
      });
      await deps.storage.notifyTerminal(cur).catch(() => undefined);
      deps.notifyTerminal(cur);
    }
  };
