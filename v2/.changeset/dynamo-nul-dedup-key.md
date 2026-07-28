---
"@iterativeflow/dynamodb": patch
---

Fix a stray NUL byte in the DynamoDB `startManyRuns` in-batch dedup key. The key concatenated
`name@version\0idempotencyKey` with a raw NUL byte, which made git treat `store.ts` as binary (hiding
its diffs) and tripped text tooling. Replaced with `JSON.stringify([name, version, idempotencyKey])` —
collision-safe and consistent with the persisted idempotency-key format. No runtime behavior change.
