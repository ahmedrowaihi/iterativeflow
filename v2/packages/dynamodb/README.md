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

> [!WARNING]
> **`table.grantReadWriteData(fn)` is not enough.** CDK's grant omits
> `TransactWriteItems` and `ConditionCheckItem` — the actions the durable
> checkpoint uses — so every atomic write fails with an opaque `AccessDenied`
> once the role is locked down. Grant the exported `REQUIRED_IAM_ACTIONS`
> explicitly on the table **and** its `index/*`:
>
> ```ts
> import { REQUIRED_IAM_ACTIONS } from "@iterativeflow/dynamodb";
>
> fn.addToRolePolicy(
>   new iam.PolicyStatement({
>     actions: [...REQUIRED_IAM_ACTIONS],
>     resources: [table.tableArn, `${table.tableArn}/index/*`],
>   }),
> );
> ```

See [docs/v2/MIGRATION.md](../../../docs/v2/MIGRATION.md).
