import type { EnqueueOpts, Queue } from "#ports/queue";
import type { Store } from "#ports/store";
import type { Timer } from "#ports/timer";
import type { Wakeup } from "#ports/wakeup";
import type { RunSpec } from "#types";

/**
 * A child run to create AND enqueue as part of an atomic Store write. The `runId` is
 * caller-generated so the parent can record it in its step memo in the SAME commit — a
 * replay re-issues the identical spawn, and insert-by-id is first-writer-wins, so the
 * child is created exactly once across crashes.
 */
export interface SpawnRequest {
  runId: string;
  spec: RunSpec;
  enqueue?: EnqueueOpts;
}

/** An existing run to (re-)enqueue atomically with a Store write (e.g. wake a parent). */
export interface EnqueueRequest {
  runId: string;
  opts?: EnqueueOpts;
}

/** A durable deadline to set atomically with a Store write (sleep / retry backoff). */
export interface TimerRequest {
  runId: string;
  fireAt: Date;
}

/**
 * The transactional-outbox payload — side-effects that a durable Store write commits in
 * the SAME transaction as the state change, so there is no window where the state moved
 * but the follow-on work was lost (or fired twice) after a crash.
 *
 * This is the single seam that makes the engine crash-safe on any backend: memory commits
 * it single-threaded, Postgres in one `BEGIN…COMMIT`, DynamoDB in one `TransactWriteItems`.
 *
 * Wakeup signals are deliberately NOT here — they are poll-first and best-effort (a missed
 * signal only costs latency, never correctness), so the engine fires them post-commit.
 */
export interface Outbox {
  /** Child runs to create + enqueue atomically (`ctx.invoke`). Insert-by-id is idempotent. */
  spawn?: readonly SpawnRequest[];
  /** Existing runs to (re-)enqueue atomically (continue self, wake a suspended parent). */
  enqueue?: readonly EnqueueRequest[];
  /** Durable deadlines to set atomically (sleep, retry). */
  timers?: readonly TimerRequest[];
  /**
   * Pending timers to cancel atomically with the state write — so a run that completes (or is
   * canceled) clears any pending wake timer in the SAME write, and no stale timer later
   * re-dispatches a terminal run.
   */
  cancelTimers?: readonly string[];
  /**
   * Inbox signal ids to consume atomically with the checkpoint that records them — a
   * `ctx.signal` wait deletes the delivered signal in the SAME write that memoizes its payload,
   * so a replay can't consume it twice.
   */
  consumeSignals?: readonly string[];
}

/**
 * A cohesive backend: the four ports over one shared substrate. The outbox guarantee is a
 * property of a Backend — `store.checkpointStep(c, fx)` commits the step and `fx`'s
 * spawns/enqueues/timers against THIS backend's queue/timer atomically. The three
 * standalone `*Conformance` suites pin each port in isolation; `outboxConformance` pins
 * the atomic composition.
 */
export interface Backend {
  readonly store: Store;
  readonly queue: Queue;
  readonly timer: Timer;
  readonly wakeup: Wakeup;
}
