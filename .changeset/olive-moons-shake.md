---
"@iterativeflow/postgres": minor
---

The generated drizzle schema now carries the `pending_work` autoscaling function as an exported
`pendingWorkSql` string, and `pendingWorkDdl(schema)` is exported from the package.

`applySchema` creates that function, but the generated drizzle schema is tables only — drizzle cannot
express a `CREATE FUNCTION`. So a consumer who takes the "you own this file, run your own migrations"
path ended up with every table and no `pending_work`, and a KEDA scaler pointed at it failed on every
poll. KEDA surfaces no error for a failed trigger query: the deployment just pins at its last replica
count, which is indistinguishable from a cooldown that hasn't elapsed.

Run `await db.execute(sql.raw(pendingWorkSql))` in a migration alongside the tables. `ddl()`
interpolates the same string, so the two can never drift, and `iterativeflow-pg-drizzle` now prints
the `to_regprocedure` check to verify it landed.

If you run `applySchema`, nothing changes — its output is byte-identical.
