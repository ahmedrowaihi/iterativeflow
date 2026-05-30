/**
 * In-process registry of `handle.wait(runId, { until })` waiters. Keys are
 * `${runId}|${kind}|${cursorKey}`. Fired by the LISTEN loop on
 * `flow_progress` notifications.
 *
 * @internal
 */

export type ProgressKind = "step" | "signal";

interface Waiter {
  resolve: () => void;
}

/** @internal */
export interface ProgressWaiters {
  /** Resolves on the matching event, rejects on `timeoutMs` or `abortPromise`. */
  wait(
    runId: string,
    kind: ProgressKind,
    cursorKey: string,
    opts: { timeoutMs?: number; abortPromise?: Promise<never> },
  ): Promise<void>;
  /** Fire any waiters registered for this exact key. */
  notify(runId: string, kind: ProgressKind, cursorKey: string): void;
}

const keyOf = (runId: string, kind: ProgressKind, cursorKey: string): string =>
  `${runId}|${kind}|${cursorKey}`;

/** @internal */
export const createProgressWaiters = (): ProgressWaiters => {
  const map = new Map<string, Set<Waiter>>();

  const notify = (runId: string, kind: ProgressKind, cursorKey: string): void => {
    const k = keyOf(runId, kind, cursorKey);
    const waiters = map.get(k);
    if (!waiters) return;
    map.delete(k);
    for (const w of waiters) w.resolve();
  };

  return {
    notify,
    wait(runId, kind, cursorKey, opts) {
      return new Promise<void>((resolve, reject) => {
        const k = keyOf(runId, kind, cursorKey);
        const waiters = map.get(k) ?? new Set<Waiter>();
        const cleanup = () => {
          if (timer) clearTimeout(timer);
          waiters.delete(waiter);
          if (waiters.size === 0) map.delete(k);
        };
        const waiter: Waiter = {
          resolve: () => {
            cleanup();
            resolve();
          },
        };
        const timer = opts.timeoutMs
          ? setTimeout(() => {
              cleanup();
              reject(
                new Error(
                  `handle.wait timed out after ${opts.timeoutMs}ms waiting for ${kind} "${cursorKey}"`,
                ),
              );
            }, opts.timeoutMs)
          : undefined;
        waiters.add(waiter);
        map.set(k, waiters);

        if (opts.abortPromise) {
          opts.abortPromise.catch((err) => {
            cleanup();
            reject(err);
          });
        }
      });
    },
  };
};

/** @internal — parse a `flow_progress` payload `kind:runId:cursorKey`. */
export const parseProgressPayload = (
  payload: string,
): { kind: ProgressKind; runId: string; cursorKey: string } | null => {
  const firstColon = payload.indexOf(":");
  if (firstColon === -1) return null;
  const kindStr = payload.slice(0, firstColon);
  if (kindStr !== "step" && kindStr !== "signal") return null;
  const secondColon = payload.indexOf(":", firstColon + 1);
  if (secondColon === -1) return null;
  const runId = payload.slice(firstColon + 1, secondColon);
  const cursorKey = payload.slice(secondColon + 1);
  if (!runId || !cursorKey) return null;
  return { kind: kindStr, runId, cursorKey };
};
