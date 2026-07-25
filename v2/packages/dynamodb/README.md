# @iterativeflow/dynamodb

DynamoDB [`Backend`](../core) for [iterativeflow](https://github.com/ahmedrowaihi/iterativeflow)
v2. A single table with two GSIs holds all four ports; a durable checkpoint
commits as one `TransactWriteItems`. Decision-path reads use `ConsistentRead`.
A natural fit for serverless (Lambda + EventBridge) with `serverlessTick`.

```bash
npm install @iterativeflow/dynamodb @iterativeflow/core @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb
```

```ts
import { createEngine } from "@iterativeflow/core";
import { createDynamoBackend, docClient, ensureTable } from "@iterativeflow/dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const low = new DynamoDBClient({});
await ensureTable(low, "iterativeflow"); // dev only; needs CreateTable IAM

const engine = createEngine(createDynamoBackend(docClient(low), { table: "iterativeflow" }), [
  myFlow,
]);
```

## Table & IAM

| You want                       | Use                                                |
| ------------------------------ | -------------------------------------------------- |
| Dev / quickstart table         | `ensureTable(lowLevelClient, table?)`              |
| Provision in CDK / CFN / TF    | `tableSpec(table?)` (the single-table + GSI shape) |
| The exact permissions to grant | `REQUIRED_IAM_ACTIONS`                             |

`TransactWriteItems` and `ConditionCheckItem` are **not** granted by a CDK
`grantReadWriteData` — grant them explicitly on the table and `index/*`, or every
atomic write fails once the role is locked down. See
[docs/v2/MIGRATION.md](../../../docs/v2/MIGRATION.md).
