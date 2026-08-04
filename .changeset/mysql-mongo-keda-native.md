---
"@iterativeflow/mysql": minor
"@iterativeflow/mongodb": minor
---

KEDA-native autoscaling for MySQL and MongoDB, matching Postgres.

- **MySQL:** `applySchema` now creates a `pending_work(flow_names, as_of)` SQL function (claimable jobs + due timers + due crons), so KEDA's mysql scaler can read the backlog directly. `flow_names` is a JSON array (`NULL` = whole backlog); `as_of` is epoch ms. Creating it needs the `CREATE ROUTINE` privilege.
- **MongoDB:** a new `pendingWorkPipeline(now)` export returns a `$unionWith` aggregation that counts the backlog across the jobs/timers/crons collections in one query — MongoDB has no stored functions and KEDA's mongodb scaler is single-collection, so this is the mongo-side metric.
