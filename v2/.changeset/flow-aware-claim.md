---
"@iterativeflow/core": minor
---

Flow-aware claiming — sharded workers only lease runs they can execute.

`Queue.claim` (`ClaimOpts`) takes an optional `names?: readonly string[]`: the claim is restricted to
runs whose flow `name` is in the set. `tickOnce` derives it automatically from the worker's registered
flows, so `engine.run` / `serverlessTick` shard with zero config — a pod that registers a disjoint
subset of flows never blind-claims a run it has no handler for.

**Why:** with partitioned pods (an API pod dispatches many flows; each worker pod registers only a
few), a blind claim leases a run for an unregistered flow, which parks `unknown_flow` BEFORE
`markRunning` bumps `attempts` — so it never exhausts to a dead-letter and re-parks on `baseDelayMs`
forever. Roughly one wrong-pod escape per claim cycle, no error logs; high-cadence flows never
converge. Filtering the claim by registered name removes the bounce at the source.

- `names` omitted ⇒ no filter (a monolith claims everything — unchanged behavior).
- Matches on `name` only: a registered name at an unregistered _version_ still leases and then parks
  for redeploy — the intended rolling-deploy handoff, not a shard miss.
- Every backend implements the filter (SQL `LEFT JOIN run`; memory/redis/mongodb/dynamodb look up the
  run's name), proven by a new `claimFilterConformance` case across all eight backends.
