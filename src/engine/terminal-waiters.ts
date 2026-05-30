/**
 * In-process registry of `handle.result(runId)` waiters. Notified by both the
 * runner (when a run terminates locally) and the LISTEN subscription (when a
 * remote engine instance terminates a run).
 *
 * @internal
 */
export interface TerminalWaiters {
  /** Resolves when the run terminates or the optional `timeoutMs` elapses. */
  wait(runId: string, timeoutMs?: number): Promise<void>;
  /** Fire every waiter currently registered for `runId`. */
  notify(runId: string): void;
}

/** @internal */
export const createTerminalWaiters = (): TerminalWaiters => {
  const map = new Map<string, Set<(_: void) => void>>();

  const notify = (runId: string): void => {
    const waiters = map.get(runId);
    if (!waiters) return;
    map.delete(runId);
    for (const w of waiters) w();
  };

  return {
    notify,
    wait(runId, timeoutMs) {
      return new Promise((resolve, reject) => {
        const waiters = map.get(runId) ?? new Set();
        const cleanup = () => {
          if (timer) clearTimeout(timer);
          waiters.delete(resolveFn);
          if (waiters.size === 0) map.delete(runId);
        };
        const resolveFn = () => {
          cleanup();
          resolve();
        };
        const timer = timeoutMs
          ? setTimeout(() => {
              cleanup();
              reject(new Error(`handle.result timed out after ${timeoutMs}ms`));
            }, timeoutMs)
          : undefined;
        waiters.add(resolveFn);
        map.set(runId, waiters);
      });
    },
  };
};
