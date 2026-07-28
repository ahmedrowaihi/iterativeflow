import {
  CreateTableCommand,
  type CreateTableCommandInput,
  type DynamoDBClient,
  ResourceInUseException,
  waitUntilTableExists,
} from "@aws-sdk/client-dynamodb";

/** Default single table. All entities live here so an outbox is one `TransactWriteItems`. */
export const DEFAULT_TABLE = "iterativeflow";

const PAD = 20;
// Left-pad so lexical sk/GSI order matches numeric order.
/** @internal */
export const pad = (n: number): string => Math.trunc(n).toString().padStart(PAD, "0");

// A run and everything hanging off it (steps, signals) share one `RUN#<id>` partition, so
// `loadRun` is a single Query; jobs/timers/crons/idem markers get their own partitions.
/** @internal */
export const key = {
  run: (id: string) => ({ pk: `RUN#${id}`, sk: "#RUN" }),
  runPk: (id: string) => `RUN#${id}`,
  step: (runId: string, cursorKey: string) => ({ pk: `RUN#${runId}`, sk: `STEP#${cursorKey}` }),
  signal: (runId: string, seq: number, sigId: string) => ({
    pk: `RUN#${runId}`,
    sk: `SIG#${pad(seq)}#${sigId}`,
  }),
  sigIdem: (runId: string, idemKey: string) => ({ pk: `RUN#${runId}`, sk: `SIGIDEM#${idemKey}` }),
  job: (runId: string) => ({ pk: `JOB#${runId}`, sk: "#JOB" }),
  timer: (runId: string) => ({ pk: `TIMER#${runId}`, sk: "#TIMER" }),
  cron: (name: string) => ({ pk: `CRON#${name}`, sk: "#CRON" }),
  idem: (name: string, version: number, idemKey: string) => ({
    pk: `IDEM#${JSON.stringify([name, version, idemKey])}`,
    sk: "#IDEM",
  }),
};

/** @internal */
export const TIMER_GSI_PK = "TIMER";

/** @internal */
export const JOB_GSI_PK = "JOB";

/** @internal */
export const CRON_DUE_GSI_PK = "CRON_DUE";

// gsi2 lists/counts all runs newest-first by createdAt without a full-table Scan. One constant
// partition, written once at run creation (status changes never touch it, so the hot claim path
// pays nothing).
/** @internal */
export const RUN_GSI2_PK = "RUN";

// gsi1 is overloaded: each item type namespaces its own gsi1pk. A child run joins the parent's
// partition so `childrenOf` is a Query, not a Scan. Root runs set none (sparse).
/** @internal */
export const childGsiPk = (parentRunId: string): string => `CHILD#${parentRunId}`;

/**
 * The table's key + GSI shape, as data — provision it yourself in CDK / CloudFormation /
 * Terraform (the production path; `ensureTable` needs `CreateTable` IAM and sits outside your
 * IaC's drift/backup control). `pk`/`sk` are the single-table primary key; `gsi1` orders due
 * timers, claimable jobs, due crons and a run's children; `gsi2` lists/counts all runs by createdAt.
 * `PAY_PER_REQUEST` and any PITR/tags are your choice at provision time — the engine only requires
 * these keys and these two indexes.
 */
export const tableSpec = (table = DEFAULT_TABLE): CreateTableCommandInput => ({
  TableName: table,
  BillingMode: "PAY_PER_REQUEST",
  AttributeDefinitions: [
    { AttributeName: "pk", AttributeType: "S" },
    { AttributeName: "sk", AttributeType: "S" },
    { AttributeName: "gsi1pk", AttributeType: "S" },
    { AttributeName: "gsi1sk", AttributeType: "S" },
    { AttributeName: "gsi2pk", AttributeType: "S" },
    { AttributeName: "gsi2sk", AttributeType: "S" },
  ],
  KeySchema: [
    { AttributeName: "pk", KeyType: "HASH" },
    { AttributeName: "sk", KeyType: "RANGE" },
  ],
  GlobalSecondaryIndexes: [
    {
      IndexName: "gsi1",
      KeySchema: [
        { AttributeName: "gsi1pk", KeyType: "HASH" },
        { AttributeName: "gsi1sk", KeyType: "RANGE" },
      ],
      Projection: { ProjectionType: "ALL" },
    },
    {
      IndexName: "gsi2",
      KeySchema: [
        { AttributeName: "gsi2pk", KeyType: "HASH" },
        { AttributeName: "gsi2sk", KeyType: "RANGE" },
      ],
      Projection: { ProjectionType: "ALL" },
    },
  ],
});

/**
 * The exact IAM actions the backend needs on the workflow table + its `gsi1`. `TransactWriteItems`
 * and `ConditionCheckItem` are the important extras — a CDK `grantReadWriteData` omits them, and
 * without them every atomic outbox write fails. Grant these on `arn:…:table/<name>` and
 * `arn:…:table/<name>/index/*`.
 */
export const REQUIRED_IAM_ACTIONS: readonly string[] = [
  "dynamodb:GetItem",
  "dynamodb:PutItem",
  "dynamodb:UpdateItem",
  "dynamodb:DeleteItem",
  "dynamodb:Query",
  "dynamodb:Scan",
  "dynamodb:ConditionCheckItem",
  "dynamodb:TransactWriteItems",
];

/**
 * Create the table + its GSI if absent, then block until active. Idempotent (a
 * `ResourceInUseException` is swallowed). A dev / quickstart convenience — production should
 * provision from {@link tableSpec} in its own IaC instead.
 */
export const ensureTable = async (low: DynamoDBClient, table = DEFAULT_TABLE): Promise<void> => {
  try {
    await low.send(new CreateTableCommand(tableSpec(table)));
  } catch (e) {
    if (!(e instanceof ResourceInUseException)) throw e;
  }
  await waitUntilTableExists({ client: low, maxWaitTime: 60 }, { TableName: table });
};
