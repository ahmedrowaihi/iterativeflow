import type { Wakeup } from "#ports/wakeup";

/**
 * The process-local, edge-triggered {@link Wakeup} — the connection-safe default shared by
 * every backend. `wait` is the inter-poll sleep the poll-first loop uses; `signal` wakes
 * current in-process waiters early. It pins nothing (no `LISTEN`, no stream), so it is safe
 * behind RDS Proxy / PgBouncer out of the box. Cross-process push (Postgres `NOTIFY`,
 * DynamoDB Streams) is a future opt-in; correctness never depends on it — the engine re-reads
 * the store every tick regardless.
 */
export const createLocalWakeup = (): Wakeup => {
  const waiters = new Map<string, Set<() => void>>();

  return {
    wait(runId, timeoutMs) {
      return new Promise<void>((resolve) => {
        let bucket = waiters.get(runId);
        if (!bucket) {
          bucket = new Set();
          waiters.set(runId, bucket);
        }
        const set = bucket;
        let handle: ReturnType<typeof setTimeout>;
        const done = (): void => {
          clearTimeout(handle);
          set.delete(done);
          if (set.size === 0) waiters.delete(runId);
          resolve();
        };
        set.add(done);
        handle = setTimeout(done, timeoutMs);
      });
    },

    async signal(runId) {
      const set = waiters.get(runId);
      if (!set) return;
      for (const done of [...set]) done();
    },
  };
};
