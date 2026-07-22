/**
 * Completion-signalling port — one of the four v2 ports. The core's `result()` / `wait()`
 * is **poll-first**: it re-reads the Store in a loop and calls `wait` between reads.
 * `wait` returns either when a signal for `runId` arrives (low-latency fast path) or when
 * the timeout elapses (the poll tick). This keeps completion pooler-safe by default:
 *
 * - **poll deployment** (RDS Proxy / serverless): `signal` is a no-op, `wait` just sleeps
 *   the tick — the core polls. No `LISTEN` connection pinned.
 * - **resident / push**: `signal` is backed by Postgres `NOTIFY` (dedicated connection) or
 *   DynamoDB Streams, so `wait` returns immediately on completion.
 *
 * Signalling is edge-triggered: a signal wakes *current* waiters only. A missed signal is
 * harmless — the core re-reads state on the next tick regardless.
 */
export interface Wakeup {
  /** Block up to `timeoutMs`, returning early if a signal for `runId` arrives. */
  wait(runId: string, timeoutMs: number): Promise<void>;

  /** Wake any current waiters on `runId`. A no-op for poll-only deployments. */
  signal(runId: string): Promise<void>;
}
