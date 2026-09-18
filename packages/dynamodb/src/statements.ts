import { type CancellationReason, TransactionCanceledException } from "@aws-sdk/client-dynamodb";
import type { TransactWriteCommandInput } from "@aws-sdk/lib-dynamodb";
import type { EnqueueOpts, Outbox, RunSpec, SpawnRequest } from "@iterativeflow/core/backend";
import { enc, nextSeq } from "#codec";
import { JOB_GSI_PK, RUN_GSI2_PK, TIMER_GSI_PK, childGsiPk, key, pad } from "#schema";

/** @internal */
export type TxItem = NonNullable<TransactWriteCommandInput["TransactItems"]>[number];

// Dynamo's hard TransactWriteItems cap. The checkpoint gate reserves 2 entries and each spawn costs
// 2, so a chunk of FAN_OUT_CHUNK (40) children fits with margin; core chunks fan-out to that fixed
// size, so a checkpoint always fits and an over-budget checkpoint is a loud invariant violation.
/** @internal */
export const MAX_TX_ITEMS = 100;

/** @internal */
export const buildRunItem = (spec: RunSpec, runId: string) => {
  const seq = nextSeq();
  const createdAt = spec.createdAt ?? new Date();
  return {
    ...key.run(runId),
    type: "run",
    id: runId,
    name: spec.name,
    version: spec.version,
    status: "pending",
    input: enc(spec.input),
    attempts: 0,
    // ISO-8601 sorts lexicographically = chronologically, so the retention scan compares it directly.
    createdAt: createdAt.toISOString(),
    idempotencyKey: spec.idempotencyKey,
    tags: spec.tags ? [...spec.tags] : undefined,
    parentRunId: spec.parentRunId,
    parentCursorKey: spec.parentCursorKey,
    depth: spec.depth ?? 0,
    priority: spec.priority,
    seq,
    // Child runs join the parent's gsi1 partition (childrenOf → Query). removeUndefinedValues
    // drops these for root runs, keeping them out of the index.
    gsi1pk: spec.parentRunId ? childGsiPk(spec.parentRunId) : undefined,
    gsi1sk: spec.parentRunId ? pad(seq) : undefined,
    // gsi2: every run, ordered newest-first by listRuns. createdAt (a stable wall clock) is the
    // cross-instance sort; the in-process seq only disambiguates same-millisecond runs, since a
    // module-level counter rewinds on every Lambda cold start and can't order across instances.
    gsi2pk: RUN_GSI2_PK,
    gsi2sk: `${pad(createdAt.getTime())}#${pad(seq)}`,
  };
};

const runAtMs = (opts?: EnqueueOpts): number => (opts?.runAt ? opts.runAt.getTime() : 0);

/** @internal */
export const enqueueParams = (
  table: string,
  runId: string,
  opts: EnqueueOpts | undefined,
  runPriority: number | undefined,
) => {
  const priority = opts?.priority ?? runPriority ?? 0;
  return {
    TableName: table,
    Key: key.job(runId),
    UpdateExpression:
      "SET #type = :type, runId = :runId, runAt = :runAt, priority = :priority, gsi1pk = :gpk, gsi1sk = :gsk ADD version :one",
    ExpressionAttributeNames: { "#type": "type" },
    ExpressionAttributeValues: {
      ":type": "job",
      ":runId": runId,
      ":runAt": runAtMs(opts),
      ":priority": priority,
      ":gpk": JOB_GSI_PK,
      ":gsk": `${pad(priority)}#${pad(runAtMs(opts))}`,
      ":one": 1,
    },
  };
};

const enqueueTx = (
  table: string,
  runId: string,
  opts?: EnqueueOpts,
  runPriority?: number,
): TxItem => ({
  Update: enqueueParams(table, runId, opts, runPriority),
});

const scheduleTx = (table: string, runId: string, fireAt: Date): TxItem => ({
  Put: {
    TableName: table,
    Item: {
      ...key.timer(runId),
      type: "timer",
      runId,
      fireAt: fireAt.getTime(),
      gsi1pk: TIMER_GSI_PK,
      gsi1sk: pad(fireAt.getTime()),
    },
  },
});

const cancelTimerTx = (table: string, runId: string): TxItem => ({
  Delete: { TableName: table, Key: key.timer(runId) },
});

const SIG_ID_SEP = "\u0000";
/** @internal */
export const encodeSignalId = (pk: string, sk: string): string => `${pk}${SIG_ID_SEP}${sk}`;
const decodeSignalId = (id: string) => {
  const [pk, sk] = id.split(SIG_ID_SEP);
  return { pk, sk };
};

const consumeSignalTx = (table: string, encodedId: string): TxItem => ({
  Delete: { TableName: table, Key: decodeSignalId(encodedId) },
});

/** @internal */
export const spawnTx = (table: string, s: SpawnRequest): [TxItem, TxItem] => [
  {
    Put: {
      TableName: table,
      Item: buildRunItem(s.spec, s.runId),
      ConditionExpression: "attribute_not_exists(pk)",
    },
  },
  enqueueTx(table, s.runId, s.enqueue, s.spec.priority),
];

/** @internal */
export const outboxParts = (
  table: string,
  fx: Outbox | undefined,
  priorities: ReadonlyMap<string, number>,
) => {
  const nonSpawn: TxItem[] = [];
  for (const e of fx?.enqueue ?? [])
    nonSpawn.push(enqueueTx(table, e.runId, e.opts, priorities.get(e.runId)));
  for (const t of fx?.timers ?? []) nonSpawn.push(scheduleTx(table, t.runId, t.fireAt));
  for (const id of fx?.cancelTimers ?? []) nonSpawn.push(cancelTimerTx(table, id));
  for (const s of fx?.consumeSignals ?? []) nonSpawn.push(consumeSignalTx(table, s));
  return { nonSpawn, spawns: fx?.spawn ?? [] };
};

/** @internal */
export const cancellationReasons = (cause: unknown): CancellationReason[] | undefined =>
  cause instanceof TransactionCanceledException ? (cause.CancellationReasons ?? []) : undefined;

const failed = (r?: CancellationReason): boolean => r?.Code === "ConditionalCheckFailed";
/** @internal */
export const conditionFailedAt = (
  reasons: CancellationReason[] | undefined,
  index: number,
): boolean => failed(reasons?.[index]);
