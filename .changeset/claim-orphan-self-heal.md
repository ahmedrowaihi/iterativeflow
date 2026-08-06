---
"@iterativeflow/memory": patch
"@iterativeflow/postgres": patch
"@iterativeflow/mysql": patch
"@iterativeflow/sqlite": patch
"@iterativeflow/dynamodb": patch
"@iterativeflow/redis": patch
"@iterativeflow/mongodb": patch
---

Fix: a flow-name-filtered `claim` no longer skips a job whose run row is gone.

Previously, such an orphan (a run deleted out of band while its job lingered) resolved to no flow name, failed the `names` filter, and was dropped every tick — never leased, never acked — so it stayed permanently claimable and inflated `queue.depth()` / `oldestClaimableAgeMs` (a `run_at=0` orphan reads as a ~56-year-old backlog item), making a healthy engine's readiness check look stalled. The unfiltered claim path already self-healed (runTick loads the gone run and acks it); the name-filtered path dropped the job before it could be leased.

The name-filtered claim now leases a run-less job too, so `runTick`'s existing gone-path acks it. (`prune` already deletes a run's job and timer, so the primary orphan source was already handled — this closes the out-of-band case and the claim-path asymmetry.)
