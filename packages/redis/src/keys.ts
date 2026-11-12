/**
 * Key layout + hash-field contract shared by all four ports. Single source of truth so the Store's
 * outbox and the standalone Queue/Timer produce identical structures.
 *
 * Per-run keys carry a `{runId}` hash-tag so a future Redis Cluster co-locates them on one slot; the
 * shared dispatch keys (`queue`, `timers`, …) don't, so a Lua touching both is single-node only for
 * now. On a single node (Valkey/Dragonfly) the tags are inert.
 */
export interface Keys {
  run(runId: string): string;
  steps(runId: string): string;
  inbox(runId: string): string;
  sigIdem(runId: string): string;
  job(runId: string): string;
  /** SET of a run's direct child ids — powers `childrenOf` without a full scan. Written on spawn. */
  children(runId: string): string;
  /** Every per-run key above — what deleting a run removes. */
  perRun(runId: string): string[];
  /** `job(id)` split around the id, for a Lua that builds job keys from run ids it reads. */
  jobAffix: readonly [string, string];
  /** `run(id)` split around the id, for a Lua that builds run keys from run ids it reads. */
  runAffix: readonly [string, string];
  queue: string;
  timers: string;
  runIndex: string;
  idem: string;
  crons: string;
  cronsDue: string;
  seq: string;
}

export const makeKeys = (prefix: string): Keys => {
  const affix = (kind: string) => [`${prefix}:${kind}:{`, "}"] as const;
  const scoped = (kind: string) => {
    const [head, tail] = affix(kind);
    return (runId: string) => `${head}${runId}${tail}`;
  };
  const perRun = {
    run: scoped("run"),
    steps: scoped("steps"),
    inbox: scoped("inbox"),
    sigIdem: scoped("sigidem"),
    job: scoped("job"),
    children: scoped("children"),
  };
  return {
    ...perRun,
    perRun: (runId) => Object.values(perRun).map((key) => key(runId)),
    jobAffix: affix("job"),
    runAffix: affix("run"),
    queue: `${prefix}:queue`,
    timers: `${prefix}:timers`,
    runIndex: `${prefix}:runs`,
    idem: `${prefix}:idem`,
    crons: `${prefix}:crons`,
    cronsDue: `${prefix}:cronsdue`,
    seq: `${prefix}:seq`,
  };
};

/**
 * `run:{id}` HASH fields — the run row. All values are strings; ints and JSON are encoded/decoded by
 * the codec. `joinRemaining` is the fan-out countdown; `seq` is monotonic insertion order (listing +
 * orphan ordering).
 */
export const RUN = {
  id: "id",
  name: "name",
  version: "version",
  status: "status",
  input: "input",
  output: "output",
  error: "error",
  attempts: "attempts",
  idempotencyKey: "idempotencyKey",
  tags: "tags",
  parentRunId: "parentRunId",
  parentCursorKey: "parentCursorKey",
  depth: "depth",
  createdAt: "createdAt",
  joinRemaining: "joinRemaining",
  seq: "seq",
  priority: "priority",
} as const;

/**
 * `job:{id}` HASH fields — the Queue's per-run dispatch state, owned by the Queue and written
 * identically by the Store's outbox enqueue. `version` bumps on every enqueue; `ack` CASes on it so a
 * wake that re-enqueues mid-lease releases (not deletes) the job. The `queue` ZSET scores a run by
 * `runAt` (ms); a leased/future run stays in the ZSET and is filtered out at claim time.
 */
export const JOB = {
  runAt: "runAt",
  priority: "priority",
  version: "version",
  leaseToken: "leaseToken",
  leaseExpires: "leaseExpires",
} as const;
