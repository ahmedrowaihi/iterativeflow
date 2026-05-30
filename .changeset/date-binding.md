---
"iterativeflow": patch
---

Fix JS `Date` binding in raw `sql\`\``fragments — caused runtime failures on`postgres-js`/`neon-serverless`drivers, which (unlike`node-postgres`) don't natively encode `Date` in positional params when drizzle hasn't propagated column type info.

Three sites affected:

- `reconcile.ts` — `${runs.updatedAt} < ${olderThan}` rewritten via drizzle's typed `lt(col, date)` so the column's `timestamptz` encoder runs. The `EXISTS` subqueries (no JS values, only `NOW()`) stay raw.
- `queries.ts` — cursor tuple compare `(createdAt, id) < (...)` casts the JS-Date param to `::timestamptz` in SQL. Tuple compare can't go through `lt`, so the cast is the cheapest correct fix.
- `adapters/graphile/index.ts` — `add_job(... run_at => ${opts.runAt} ...)` cast to `::timestamptz` for the same reason. Affected every delayed enqueue (sleeps, retries, `delay` start opt).

Consumers using postgres-js or neon-serverless no longer need to spin up a separate `node-postgres` handle for the engine's pool.

A single `ts(date)` helper in `src/util/sql-params.ts` centralizes the cast — every Date param in a raw `sql\`\`` fragment goes through it. Easier to grep for, easier to extend (uuid/bigint/etc.) if the next driver-portability footgun shows up.
