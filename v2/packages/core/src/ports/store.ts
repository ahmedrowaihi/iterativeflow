import type {
  CronRow,
  CronSpec,
  Page,
  RunFilter,
  RunPage,
  RunRow,
  RunSnapshot,
  RunSpec,
  RunStatus,
  StepCheckpoint,
  StepOutcome,
  SuspendStatus,
  TerminalOutcome,
} from "#types";
import type { Outbox } from "#ports/outbox";

/** The result of creating (or idempotently re-finding) a run. */
export interface StartResult {
  runId: string;
  created: boolean;
  status: RunStatus;
}

/**
 * Durable checkpoint store — one of the four v2 ports. The core speaks only to this
 * interface; backends (in-memory, Postgres, DynamoDB) implement it with the best
 * primitive available. Every implementation must pass `storeConformance`.
 *
 * Design invariant: **one durable write per step** (`checkpointStep`). There is no
 * pre-fn "step started" marker — run-level attempt bounding (`markRunning`) replaces it,
 * so a crash mid-step simply re-runs the step (at-least-once) without a wasted write.
 */
export interface Store {
  /**
   * Insert the run if new; on an idempotency-key hit return the existing run with
   * `created: false`. Idempotent — concurrent callers with the same key converge on one
   * run (PG: unique index; Dynamo: conditional put).
   */
  startRun(spec: RunSpec): Promise<StartResult>;

  /**
   * Insert many runs as one atomic unit — batch is the primitive; `startRun` is a batch of
   * one. Either every run in the batch is created or none is (a partial batch never lands).
   * Per-spec idempotency still applies, so a batch may mix created + already-existing runs;
   * results align 1:1 with `specs`.
   */
  startManyRuns(specs: readonly RunSpec[]): Promise<StartResult[]>;

  /** Load the run + its completed-step memo + pending signal inbox. `undefined` if gone. */
  loadRun(runId: string): Promise<RunSnapshot | undefined>;

  /**
   * Load just the run row — no step memo or signal inbox. For callers that inspect only
   * status/output/error (awaiting a child, polling `result`). Cheaper than {@link loadRun}.
   */
  loadRunRow(runId: string): Promise<RunRow | undefined>;

  /**
   * Batched {@link loadRunRow}: one round-trip for many ids instead of N. Returns rows aligned to
   * `runIds` (same length, `undefined` where a run is gone). The fan-out join reads every child
   * outcome through this, so a join over M children costs O(M/batch) round-trips, not O(M).
   */
  loadRunRows(runIds: readonly string[]): Promise<(RunRow | undefined)[]>;

  /**
   * A child arriving at its parent's fan-out join: atomically decrement `parentRunId`'s join
   * countdown ({@link Outbox.joinTarget}) and return the new value, or `undefined` if the parent is
   * already gone (nothing to wake). The executor wakes the parent when this reaches zero (all
   * children arrived); a missed wake is caught by the reconcile `lostParentWake` sweep, so this only
   * reduces parent wakes from O(children) to O(1) — it never gates correctness.
   */
  arriveAtJoin(parentRunId: string): Promise<number | undefined>;

  /**
   * Deliver a signal to a run's inbox AND re-enqueue the run atomically, so a parked
   * `awaiting_signal` run wakes and consumes it on its next tick. Idempotent on
   * `idempotencyKey` (scoped to the run) — a retried delivery lands once. Returns whether the
   * signal was newly delivered (`false` = idempotent duplicate).
   */
  postSignal(
    runId: string,
    name: string,
    payload: unknown,
    opts?: { idempotencyKey?: string },
  ): Promise<{ delivered: boolean }>;

  /** Transition to `running` and increment `attempts`. Returns the new attempt count. */
  markRunning(runId: string): Promise<number>;

  /**
   * Persist a step's terminal outcome — the single durable write per step. **Idempotent
   * and first-writer-wins**: a second checkpoint for the same `(runId, cursorKey)` does
   * NOT overwrite; it returns the already-stored outcome.
   *
   * This makes the *memo* exactly-once, NOT the step's side-effects: `fn` runs before the
   * checkpoint, so a crash before it commits re-runs `fn` (at-least-once effect). Keep step
   * bodies idempotent. The caller must treat the RETURNED `result` as authoritative — under
   * a race the winner's value comes back, which may differ from what this caller passed in
   * (e.g. the winning `ctx.invoke`'s childId).
   *
   * When `fx` is given, its spawns/enqueues/timers commit atomically with the checkpoint
   * — and ONLY when the checkpoint is the first writer. On an idempotent hit the outbox is
   * skipped (the original write already committed the original outbox), so a replayed
   * `ctx.invoke` never double-spawns.
   */
  checkpointStep(c: StepCheckpoint, fx?: Outbox): Promise<StepOutcome>;

  /**
   * Suspend a running run: `sleeping` (waiting on a timer), `awaiting_signal` (waiting on
   * an external signal), or `retrying` (backing off before re-dispatch). `fx` commits the
   * wake mechanism atomically with the status change — a sleep sets status + its timer in
   * one write, so a crash can't strand a run `sleeping` with no timer to wake it. A no-op
   * on a run that is already terminal.
   *
   * A forward-progress park (`sleeping`/`awaiting_signal`/`awaiting_child`) zeroes the dispatch
   * counter in the SAME write; only `retrying` keeps it — so the poison-pill cap counts *no-progress*
   * re-claims (uncatchable crash loops), not legitimate durable resumes.
   */
  suspendRun(runId: string, status: SuspendStatus, fx?: Outbox): Promise<void>;

  /**
   * Take the run terminal. Must not override an existing `canceled`. `fx` commits atomically with
   * the status write (e.g. clearing a pending wake timer). The fan-out parent-wake is NOT part of
   * this write — the executor decrements the parent's join countdown ({@link arriveAtJoin}) and
   * enqueues it afterward, best-effort, backstopped by the reconcile `lostParentWake` sweep.
   */
  markTerminal(runId: string, outcome: TerminalOutcome, fx?: Outbox): Promise<void>;

  /** List runs newest-first, filtered and paged. The ops/dashboard read surface. */
  listRuns(filter: RunFilter, page: Page): Promise<RunPage>;

  /** The direct children of a run (spawned via `ctx.invoke`) — powers the cancel cascade. */
  childrenOf(runId: string): Promise<readonly RunRow[]>;

  /** Count of runs per status — the health / overview snapshot. */
  runStats(): Promise<Record<RunStatus, number>>;

  /**
   * Runs that should be on the queue but aren't — non-terminal runs with no live job and no
   * pending timer (stranded by a crash between a state write and its enqueue, or by a lost
   * wakeup). The reconciler re-enqueues them. Returns up to `max`, oldest first.
   */
  orphanedRuns(max: number): Promise<readonly string[]>;

  /**
   * Re-drive a `failed` run: reset it to `pending`, clear the error, and re-enqueue —
   * atomically. Completed (`ok`) step memos are KEPT, so replay skips them and only the work
   * after the failure re-runs. A no-op (`retried: false`) on a run that isn't `failed`.
   */
  retryRun(runId: string): Promise<{ retried: boolean }>;

  /** Register or update a cron. Keeps the existing `nextRunAt` if the cron already exists. */
  upsertCron(spec: CronSpec): Promise<void>;

  /** Crons whose `nextRunAt` has passed — candidates to fire this cycle. */
  dueCrons(now: Date, max: number): Promise<readonly CronRow[]>;

  /**
   * Advance a cron's `nextRunAt` — but ONLY if it still equals `expectedNextRunAt` (CAS). The
   * winner of that CAS is the single worker that fires this occurrence, so a cron never
   * double-fires across a fleet. Returns whether this caller won.
   */
  advanceCron(
    name: string,
    expectedNextRunAt: Date,
    nextRunAt: Date,
    lastRunAt: Date,
  ): Promise<boolean>;
}
