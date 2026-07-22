import {
  CreateTableCommand,
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

/**
 * Create the table + its one GSI if absent, then block until it is active. Idempotent — a
 * `ResourceInUseException` (table already exists) is swallowed. GSI1 orders timers by fire
 * instant (`gsi1pk = "TIMER"`, `gsi1sk = pad(fireAt)`) for the `dueBatch` range query.
 */
export const ensureTable = async (low: DynamoDBClient, table = DEFAULT_TABLE): Promise<void> => {
  try {
    await low.send(
      new CreateTableCommand({
        TableName: table,
        BillingMode: "PAY_PER_REQUEST",
        AttributeDefinitions: [
          { AttributeName: "pk", AttributeType: "S" },
          { AttributeName: "sk", AttributeType: "S" },
          { AttributeName: "gsi1pk", AttributeType: "S" },
          { AttributeName: "gsi1sk", AttributeType: "S" },
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
        ],
      }),
    );
  } catch (e) {
    if (!(e instanceof ResourceInUseException)) throw e;
  }
  await waitUntilTableExists({ client: low, maxWaitTime: 60 }, { TableName: table });
};
