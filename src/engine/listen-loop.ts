import type { Pool, PoolClient } from "pg";
import type { Logger } from "./types";

/** @internal */
export type ListenState = "idle" | "connecting" | "listening" | "reconnecting" | "stopped";

/**
 * Resilient `LISTEN flow_terminal` loop. Reconnects with exponential backoff
 * (1s → 30s + jitter) until {@link ListenLoop.stop} is called. State is
 * observable via {@link ListenLoop.state} so `engine.health()` can report it.
 *
 * @internal
 */
export interface ListenLoop {
  start(): void;
  stop(): Promise<void>;
  state(): ListenState;
}

const computeBackoffMs = (attempt: number): number => {
  const base = Math.min(30_000, 1000 * 2 ** Math.min(attempt, 5));
  return base + Math.floor(Math.random() * 500);
};

const sleepCancellable = (ms: number, signal: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const t = setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(t);
        resolve();
      },
      { once: true },
    );
  });

interface CreateOpts {
  pool: Pool;
  channels: ReadonlyArray<string>;
  onNotify: (channel: string, payload: string) => void;
  logger: Logger;
}

/** @internal */
export const createListenLoop = (opt: CreateOpts): ListenLoop => {
  let state: ListenState = "idle";
  let loopPromise: Promise<void> | null = null;
  let abort: AbortController | null = null;

  const start = (): void => {
    if (loopPromise !== null) return;
    abort = new AbortController();
    const aborter = abort;
    loopPromise = (async () => {
      let attempt = 0;
      while (!aborter.signal.aborted) {
        state = attempt === 0 ? "connecting" : "reconnecting";
        let client: PoolClient | null = null;
        try {
          client = await opt.pool.connect();
          if (aborter.signal.aborted) {
            client.release();
            client = null;
            break;
          }
          const conn = client;
          const disconnect = new Promise<Error>((resolve) => {
            conn.on("notification", (msg) => {
              if (msg.payload) opt.onNotify(msg.channel, msg.payload);
            });
            conn.once("error", resolve);
            conn.once("end", () => resolve(new Error("connection ended")));
          });
          for (const channel of opt.channels) {
            await conn.query(`LISTEN ${channel}`);
          }
          state = "listening";
          attempt = 0;
          const onAbortRelease = () => {
            try {
              conn.release(true);
            } catch {
              // already gone
            }
          };
          aborter.signal.addEventListener("abort", onAbortRelease, { once: true });
          const err = await disconnect;
          aborter.signal.removeEventListener("abort", onAbortRelease);
          opt.logger.warn("flow.listen.disconnected", { message: err.message });
        } catch (err) {
          opt.logger.warn("flow.listen.connect_failed", {
            message: err instanceof Error ? err.message : String(err),
            attempt,
          });
        } finally {
          if (client) {
            try {
              client.removeAllListeners();
              client.release(true);
            } catch {
              // already released
            }
            client = null;
          }
        }
        if (aborter.signal.aborted) break;
        attempt++;
        await sleepCancellable(computeBackoffMs(attempt - 1), aborter.signal);
      }
      state = "stopped";
    })();
  };

  return {
    start,
    state: () => state,
    async stop() {
      if (abort) abort.abort();
      if (loopPromise) await loopPromise.catch(() => undefined);
      loopPromise = null;
      abort = null;
      state = "idle";
    },
  };
};
