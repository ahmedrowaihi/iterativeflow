import type {
  CronRow,
  CronSpec,
  Page,
  PurgeFilter,
  RunFilter,
  RunPage,
  RunRow,
  RunSnapshot,
  RunSpec,
  RunStatus,
  CheckpointResult,
  StepCheckpoint,
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
 * interface; each backend implements it with the best
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
   * Insert many runs, each created idempotently (per-spec idempotency key), results aligned 1:1
   * with `specs` — a batch may mix created + already-existing runs. All-or-none is NOT promised: a
   * backend commits the batch in as few atomic writes as its substrate allows, so a failure can land
   * part of it. Idempotency keys make re-submitting the whole batch safe.
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
   * children arrived); a missed wake is caught by the reconcile sweep (a resolved-join parent with no job), so this only
   * reduces parent wakes from O(children) to O(1) — it never gates correctness.
   */
  arriveAtJoin(parentRunId: string): Promise<number | undefined>;

  /**
   * Deliver a signal to a run's inbox AND re-enqueue the run atomically, so a parked
   * `awaiting_signal` run wakes and consumes it on its next tick. Idempotent on
   * `idempotencyKey` (scoped to the run) — a retried delivery lands once. Returns whether the
   * signal was newly delivered (`false` = idempotent duplicate).
   */
  postSignal<P>(
    runId: string,
    name: string,
    payload: P,
    opts?: { idempotencyKey?: string },
  ): Promise<{ delivered: boolean }>;

  /** Transition to `running` and increment `attempts`; returns the new attempt count. A terminal run
   *  is never resurrected — a late re-dispatch (e.g. a stale timer after a cancel) returns its
   *  attempts unchanged so the caller sees it is terminal and acks without executing. */
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
   * When `fx` is given, its spawns/timers commit atomically with the checkpoint
   * — and ONLY when the checkpoint is the first writer. On an idempotent hit the outbox is
   * skipped (the original write already committed the original outbox), so a replayed
   * `ctx.invoke` never double-spawns.
   */
  checkpointStep(c: StepCheckpoint, fx?: Outbox): Promise<CheckpointResult>;

  /**
   * Suspend a running run: `sleeping` (waiting on a timer), `awaiting_signal` (waiting on an external
   * signal), `awaiting_child` (waiting on a fan-out join), `retrying` (backing off before re-dispatch)
   * or `parked` (waiting for a redeploy). `fx` commits the
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
   * Take the run terminal. A no-op on a run that is already terminal: an outcome is final, so a late
   * cancel cannot erase a finished run's output or error. Returns whether THIS call made the run
   * terminal — `false` on the no-op, so a caller that lost the race skips its follow-ups (events, the
   * parent-arrive). `fx` commits atomically with the status write (e.g. clearing a pending wake timer),
   * and only when it returns `true`. The fan-out parent-wake is NOT part of
   * this write — the executor decrements the parent's join countdown ({@link arriveAtJoin}) and
   * enqueues it afterward, best-effort, backstopped by the reconcile sweep (a resolved-join parent with no job).
   */
  markTerminal(runId: string, outcome: TerminalOutcome, fx?: Outbox): Promise<boolean>;

  /** List runs newest-first, filtered and paged. The ops/dashboard read surface. */
  listRuns(filter: RunFilter, page: Page): Promise<RunPage>;

  /**
   * Cancel up to `limit` LIVE runs matching `filter` — the bulk form of a `cancelRun` sweep, for an
   * operator abandoning a queue rather than a run. Terminal runs are never touched, whatever the
   * filter says. Descendants are NOT walked here: a child whose parent went non-success cancels
   * itself on its next dispatch, and reconcile re-enqueues exactly those children, so the cascade
   * still completes — one maintenance interval later instead of inline. Returns how many were
   * canceled, so a caller batches until `< limit`. Throws on a filter with no predicate.
   */
  cancelRuns(filter: RunFilter, limit: number): Promise<number>;

  /**
   * Re-drive up to `limit` `failed` runs matching `filter` — the bulk form of {@link retryRun}, with
   * the same semantics per row: completed step memos are kept, `attempts` is zeroed, and each run is
   * re-enqueued. Returns how many were retried. Throws on a filter with no predicate.
   */
  retryRuns(filter: RunFilter, limit: number): Promise<number>;

  /** The direct children of a run (spawned via `ctx.invoke`) — powers the cancel cascade. */
  childrenOf(runId: string): Promise<readonly RunRow[]>;

  /** Count of runs per status — the health / overview snapshot. */
  runStats(): Promise<Record<RunStatus, number>>;

  /**
   * The retention sweep: delete up to `limit` TERMINAL runs matching `filter` (age, flow name, flow
   * version, terminal status), with their steps, signals, and events. The terminal guard holds whatever
   * the filter says, so live runs stay untouchable — and it holds at delete time, so a run retried
   * mid-sweep survives. A child whose parent is still live is never deleted either: the parent re-reads
   * every child when it joins. Returns how many runs were deleted. Throws on a filter with no predicate.
   * A terminal parent's children may still be deleted; if the parent is later retried, its join
   * reports each missing child as failed (`CHILD_GONE`).
   */
  deleteRuns(filter: PurgeFilter, limit: number): Promise<number>;

  /**
   * Runs that should be on the queue but aren't — non-terminal runs with no live job and no
   * pending timer (stranded by a crash between a state write and its enqueue, or by a lost
   * wakeup). The reconciler re-enqueues them. Returns up to `limit`, oldest first.
   */
  orphanedRuns(limit: number): Promise<readonly string[]>;

  /**
   * Re-drive a `failed` run: reset it to `pending`, clear the error, zero `attempts`, and re-enqueue
   * — atomically. Completed (`ok`) step memos are KEPT, so replay skips them and only the work after
   * the failure re-runs. A no-op (`retried: false`) on a run that isn't `failed`.
   *
   * Zeroing `attempts` is what makes a dead-lettered run retryable at all: the executor's cap is
   * checked against it before the body runs, so a retry that left it spent would re-fail the run
   * with `RUN_ATTEMPTS_EXHAUSTED` without executing anything.
   */
  retryRun(runId: string): Promise<{ retried: boolean }>;

  /** Register or update a cron. Keeps the existing `nextRunAt` when the schedule is unchanged, so a
   *  redeploy re-registering it doesn't reset the timing — a CHANGED schedule takes the new one, or
   *  the old cadence would outlive the deploy that changed it. */
  upsertCron(spec: CronSpec): Promise<void>;

  /** Every registered cron, by name. */
  listCrons(): Promise<readonly CronRow[]>;

  /** Remove a cron. Returns whether a row was there — a row outlives its deregistration in code. */
  removeCron(name: string): Promise<boolean>;

  /** The earliest `nextRunAt` across every cron (overdue ones included), or `null` with none
   *  registered — the cron half of the serverless wake horizon. One bounded read on a due-ordered
   *  index, never a scan. */
  nextCronAt(): Promise<Date | null>;

  /** Crons whose `nextRunAt` has passed — candidates to fire this cycle. */
  dueCrons(now: Date, limit: number): Promise<readonly CronRow[]>;

  /** Count crons due at/before `now` (the due-cron contribution to the autoscaling backlog: a due
   *  cron won't fire at zero replicas until a worker is woken). `names` restricts to crons whose
   *  `flowName` is in the set (per-shard); omitted ⇒ all. Read-only. */
  dueCronCount(now: Date, names?: readonly string[]): Promise<number>;

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
