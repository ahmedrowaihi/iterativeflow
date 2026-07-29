/** Options when enqueuing a run for dispatch. */
export interface EnqueueOpts {
  /** Earliest instant the run may be claimed. Omitted ⇒ immediately claimable. */
  runAt?: Date;
  /** Lower = sooner. Default 0. */
  priority?: number;
}

/** Options for a claim cycle — all tunable per deployment. */
export interface ClaimOpts {
  /** Max runs to lease this cycle (batch size). */
  limit: number;
  /** How long the lease is held before it expires and another worker may re-claim. */
  leaseMs: number;
  /** Injectable clock (tests / deterministic conformance). Defaults to now. */
  now?: Date;
  /** Restrict the claim to runs whose flow `name` is in this set — a sharded worker passes the flow
   *  names it registered so it never blind-claims a run for a `name` it can't execute (which would
   *  park `unknown_flow` and churn the queue). Matches on `name` only: a registered name at an
   *  unregistered *version* still leases, then parks for redeploy — the intended handoff, not a shard miss.
   *  Omitted ⇒ no filter (claim any run — a monolith); a set filters, and an empty set leases nothing. */
  names?: readonly string[];
}

/** A liveness snapshot of the dispatch queue — a rising backlog or age means workers can't keep up. */
export interface QueueDepth {
  /** Jobs due and unleased right now — the backlog waiting for a free worker. */
  claimable: number;
  /** Jobs currently leased to a worker (in flight). */
  leased: number;
  /** Age (ms) of the oldest claimable job, or `null` if none is claimable. */
  oldestClaimableAgeMs: number | null;
}

/**
 * Compute a {@link QueueDepth} from an in-memory job set — the shared body of the memory + DynamoDB
 * `Queue.depth`, so the claimable/leased/oldest-age definition lives once. Postgres derives the same
 * numbers in SQL and doesn't use this.
 */
export const queueDepthOf = (
  jobs: readonly { runAt: number; leaseExpires?: number }[],
  nowMs: number,
): QueueDepth => {
  const claimable = jobs.filter(
    (j) => j.runAt <= nowMs && (j.leaseExpires === undefined || j.leaseExpires <= nowMs),
  );
  const oldest = claimable.length ? Math.min(...claimable.map((j) => j.runAt)) : null;
  return {
    claimable: claimable.length,
    leased: jobs.filter((j) => j.leaseExpires !== undefined && j.leaseExpires > nowMs).length,
    oldestClaimableAgeMs: oldest === null ? null : nowMs - oldest,
  };
};

/** A held claim on a run. `token` proves ownership for heartbeat/ack. */
export interface Lease {
  runId: string;
  token: string;
  expiresAt: Date;
  /**
   * The job's enqueue-version at claim time. If an enqueue (a child-wake or `postSignal`)
   * bumps it DURING the lease, `ack` must not delete the job — it releases it instead, so the
   * wake survives the completing worker's ack. Closes the ack-clobbers-wake race.
   */
  version: number;
}

/**
 * Dispatch / claim port — one of the four v2 ports. This is the seam that frees the core
 * from any specific queue engine: the universal implementation is **lease-CAS**
 * (conditional set of owner + expiry), which works on Postgres, DynamoDB, or in memory.
 * A Postgres backend may override `claim` with `SKIP LOCKED` batch fetch for built-in
 * fan-out; the contract (and `queueConformance`) is identical either way.
 */
export interface Queue {
  /** Enqueue (or re-enqueue) a run. Re-enqueue is an upsert keyed by `runId`. */
  enqueue(runId: string, opts?: EnqueueOpts): Promise<void>;

  /**
   * Lease up to `limit` due, unleased runs to this worker for `leaseMs`. A leased run is
   * invisible to other claimers until its lease expires (crash recovery) or is `ack`ed.
   */
  claim(opts: ClaimOpts): Promise<Lease[]>;

  /**
   * Extend a held lease. Throws if the lease is no longer held — either its token was
   * re-claimed by another worker, or it has **expired** (mirrors a SQL/Dynamo CAS of
   * `WHERE owner = ? AND lease_expiry > now()`).
   */
  heartbeat(lease: Lease, opts: { leaseMs: number; now?: Date }): Promise<Lease>;

  /**
   * Complete the run. A **no-op** unless the caller still holds a valid, unexpired lease. If
   * the job's enqueue-version is unchanged since `claim`, the job is removed; if it was
   * re-enqueued mid-lease (a wake), the job is instead RELEASED for immediate re-claim rather
   * than deleted — so a wake that raced this ack is never lost. Takes `now` for deterministic
   * expiry.
   */
  ack(lease: Lease, opts?: { now?: Date }): Promise<void>;

  /**
   * Liveness snapshot: backlog, in-flight, and oldest-claimable age as of `now`. Postgres answers it
   * with one aggregate query; memory and DynamoDB read the whole job set, so call it on a coarse
   * cadence (a readiness probe every few seconds), not per request.
   */
  depth(now: Date): Promise<QueueDepth>;

  /**
   * Optional dispatch push: block up to `timeoutMs`, returning early when a run is enqueued. A
   * backend backed by a real notification bus (Postgres `LISTEN/NOTIFY`) implements this so the
   * worker loop dispatches on enqueue instead of waiting out the poll tick; backends without one
   * omit it and the loop polls every `timeoutMs`. Never load-bearing — `timeoutMs` is the backstop.
   */
  waitForWork?(timeoutMs: number): Promise<void>;
}
