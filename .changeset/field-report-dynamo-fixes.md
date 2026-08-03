---
"@iterativeflow/dynamodb": patch
---

First-user field-report fixes (serverless/Lambda):

- **Time-ordered run index.** `listRuns` ordered by a `gsi2` sort key derived from an in-process
  counter (`nextSeq`) that resets to 0 on every Lambda cold start, so runs came back in arbitrary
  order across instances. The sort key now derives from the run's `createdAt` (a cross-instance wall
  clock), with the process counter kept only as a same-millisecond tiebreak — so `listRuns` is
  genuinely newest-first everywhere. `Page.cursor` stays an opaque token and the query is unchanged;
  new rows sort correctly immediately.
- **IAM docs.** A loud README callout that CDK's `grantReadWriteData` omits `TransactWriteItems` /
  `ConditionCheckItem` (the atomic-checkpoint actions) — grant the exported `REQUIRED_IAM_ACTIONS` or
  every atomic write fails with an opaque `AccessDenied`.
