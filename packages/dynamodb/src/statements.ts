import { type CancellationReason, TransactionCanceledException } from "@aws-sdk/client-dynamodb";
import type { TransactWriteCommandInput } from "@aws-sdk/lib-dynamodb";
import {
  type EnqueueOpts,
  FAN_OUT_CHUNK,
  type Outbox,
  type RunSpec,
  type SpawnRequest,
  enqueueFields,
} from "@iterativeflow/core/backend";
import { enc } from "#codec";
import { JOB_GSI_PK, RUN_GSI2_PK, TIMER_GSI_PK, childGsiPk, key, pad } from "#schema";

/** @internal */
export type TxItem = NonNullable<TransactWriteCommandInput["TransactItems"]>[number];

// Dynamo's hard TransactWriteItems cap.
/** @internal */
export const MAX_TX_ITEMS = 100;
const CHECKPOINT_GATE_ITEMS = 3;
const SPAWN_TX_ITEMS = 2;
if (SPAWN_TX_ITEMS * FAN_OUT_CHUNK + CHECKPOINT_GATE_ITEMS > MAX_TX_ITEMS) {
  throw new Error(
    `FAN_OUT_CHUNK ${FAN_OUT_CHUNK} overflows DynamoDB's ${MAX_TX_ITEMS}-item transaction cap`,
  );
}

let counter = 0;
// Process-monotonic order token (Postgres uses a bigint IDENTITY); a multi-process deployment
// would source it from a Dynamo atomic counter.
/** @internal */
export const nextSeq = (): number => ++counter;

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

/** @internal */
export const enqueueParams = (
  table: string,
  runId: string,
  opts: EnqueueOpts | undefined,
  runPriority: number | undefined,
) => {
  const { runAtMs, priority } = enqueueFields(opts, runPriority);
  return {
    TableName: table,
    Key: key.job(runId),
    UpdateExpression:
      "SET #type = :type, runId = :runId, runAt = :runAt, priority = :priority, gsi1pk = :gpk, gsi1sk = :gsk ADD version :one",
    ExpressionAttributeNames: { "#type": "type" },
    ExpressionAttributeValues: {
      ":type": "job",
      ":runId": runId,
      ":runAt": runAtMs,
      ":priority": priority,
      ":gpk": JOB_GSI_PK,
      ":gsk": `${pad(priority)}#${pad(runAtMs)}`,
      ":one": 1,
    },
  };
};

/** @internal */
export const timerItem = (runId: string, fireAt: Date) => ({
  ...key.timer(runId),
  type: "timer",
  runId,
  fireAt: fireAt.getTime(),
  gsi1pk: TIMER_GSI_PK,
  gsi1sk: pad(fireAt.getTime()),
});

const scheduleTx = (table: string, runId: string, fireAt: Date): TxItem => ({
  Put: { TableName: table, Item: timerItem(runId, fireAt) },
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
  { Update: enqueueParams(table, s.runId, undefined, s.spec.priority) },
];

/** @internal */
export const outboxParts = (table: string, fx: Outbox | undefined) => {
  const nonSpawn: TxItem[] = [];
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
