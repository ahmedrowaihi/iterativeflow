# DynamoDB

`@iterativeflow/dynamodb` runs the engine on a single DynamoDB table. Use it for serverless on AWS
(Lambda) and when you want a managed, pay-per-use datastore with no servers to run.

## Requirements

- AWS credentials with access to one DynamoDB table and its index. `REQUIRED_IAM_ACTIONS` lists the
  exact actions to grant.
- An `@aws-sdk/client-dynamodb` client. The engine wraps it in a document client.

## Install

```bash
npm install @iterativeflow/dynamodb @iterativeflow/core @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb
```

`@aws-sdk/client-dynamodb` and `@aws-sdk/lib-dynamodb` are peer dependencies (3.0 or newer).

## Set up the table

The engine uses one table with a global secondary index. `ensureTable` creates it if it's missing
(idempotent) — fine for dev; in production, create the table with your IaC (Terraform, CDK) using
`tableSpec`, and grant `REQUIRED_IAM_ACTIONS`.

```ts
// db.ts
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { docClient, ensureTable } from "@iterativeflow/dynamodb";

export const low = new DynamoDBClient({});
export const doc = docClient(low);

await ensureTable(low); // creates the "iterativeflow" table + index if absent
```

Pass a table name to both if you don't want the default (`iterativeflow`): `ensureTable(low, "flows")`
and `createDynamoBackend(doc, { table: "flows" })`.

## Run a worker

```ts
// worker.ts
import { createDynamoBackend } from "@iterativeflow/dynamodb";
import { createEngine, defineFlow } from "@iterativeflow/core";
import { doc } from "./db";

const backend = createDynamoBackend(doc);

const greet = defineFlow({
  name: "greet",
  version: 1,
  run: async (ctx, input: { name: string }) => `hi ${input.name}`,
});

const engine = createEngine(backend, [greet]);
const stop = engine.run();

await engine.submit(greet, { name: "world" });

// on shutdown:
await stop();
```

On Lambda, don't run a resident loop — call
`serverlessTick(backend, registry([greet]), { batchMax: 20, leaseMs: 30_000 })` once per invocation and
use the returned `nextWakeAt` to schedule the next one (EventBridge Scheduler, or an SQS message with
`DelaySeconds`).

## Limits

- **400 KB per item.** A run's input, output, and step results are stored inline. Large payloads are
  rejected. Keep big blobs in S3 and store a pointer.
- **Batch sizes:** the engine chunks its own batch reads/writes to DynamoDB's limits (25 write / 100
  read per call) and retries unprocessed items. Nothing for you to tune.

## Operating in production

### Capacity mode

On-demand (the default) auto-scales and is the easiest fit. Know its one edge: it scales to at most 2×
your previous peak within a 30-minute window, so a sudden larger-than-2× spike throttles — pre-warm
before a known burst. Provisioned + auto-scaling works too and rides burst capacity while it reacts.

### Throttling

The AWS SDK retries throttling with backoff by default; keep that on. Hot partitions come from
low-cardinality keys — the engine keys items by run id (high-cardinality), so this is handled, but
watch `ThrottledRequests` under heavy load and consider provisioned capacity if you see sustained
throttling.

### Consistency

The engine reads its own state from the base table with strong consistency where correctness needs it,
and uses the index (eventually consistent) only for the coarse backlog counts. Your flow code doesn't
have to think about this — just don't build your own logic that gates correctness on an index read.

### Retention

Prune terminal runs with `await engine.prune(7 * 24 * 60 * 60 * 1000)`. DynamoDB TTL is another option
for coarse cleanup and doesn't consume write capacity, but it deletes within a few days of expiry (not
promptly), so don't rely on it as a correctness boundary.

### Health checks and autoscaling

- `engine.health()` returns run counts per status; `engine.liveness()` returns the dispatch backlog and
  oldest-claimable age.
- For autoscaling, serve `engine.pendingWork(names?)` over HTTP (the dashboard's `GET /api/metrics`) for
  a KEDA `metrics-api` scaler.

### Running a pool of workers

Workers judge lease expiry by their own clock — keep clocks NTP-synced and size `leaseMs` above your
longest step plus skew. See [deployment](../deployment.md).

## Troubleshooting

- **`Item size has exceeded the maximum allowed size`.** A payload passed 400 KB — offload it to S3.
- **`ProvisionedThroughputExceededException` under load.** Throttling. Keep SDK retries on; consider
  provisioned capacity or pre-warming for spikes.
- **`AccessDeniedException`.** The IAM role is missing an action from `REQUIRED_IAM_ACTIONS`.

## Other backends

Postgres, MySQL, SQLite, MongoDB, Redis, and Durable Objects each have their own guide in this folder.
Cross-cutting topics live in [deployment](../deployment.md).
