---
"@iterativeflow/core": minor
"@iterativeflow/dynamodb": patch
"@iterativeflow/postgres": patch
"@iterativeflow/memory": patch
---

Audit sweep — correctness, type-safety, and naming consistency:

- **Typed fan-out inputs**: `ctx.invoke([{ flow, input }, …])` now type-checks each child `input`
  against ITS own flow (was `any` on the many-form), inferred from a flow tuple so the joined
  outputs stay per-child typed. Replaces the spec-tuple-keyed `InvokeOutputs` with `FlowOutputs` +
  `InvokeSpecFor` on the public surface.
- **DynamoDB `startManyRuns` batches atomic chunks**: the earlier per-run create (one write per run,
  unbounded fan-out on a large `submitMany`) is replaced by within-batch idempotency-key dedup +
  atomic `TransactWriteItems` chunks bounded by the 100-item cap, falling back to per-run create only
  for a chunk a concurrent creator races. Restores per-chunk all-or-none without regressing dedup.
- **Renames (breaking)**: the type-only signal helper `type<T>()` → `signalType<T>()`; the batch-submit
  spec `SubmitItem` → `SubmitSpec`; the row-limit SPI param `max` → `limit` (`claim`, `dueBatch`,
  `orphanedRuns`, `dueCrons`, `reconcile`, `drainTimers`).
- **Correctness**: the reconcile lost-parent-wake fires only on a _resolved_ fan-out join (fast-fail
  preserved) instead of any terminal child; cron no longer throws on a valid sparse schedule spanning
  a leap cycle.
- **Cleanup**: removed the unreachable `failed_terminal` step status and the unused `Queue.release`;
  extracted the triplicated orphan predicate to one shared `isOrphaned`.
