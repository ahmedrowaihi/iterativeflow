---
"iterativeflow": patch
---

Fix JS `Date` binding in raw `sql\`\``fragments — caused runtime failures on`postgres-js`/`neon-serverless`drivers, which (unlike`node-postgres`) don't natively encode `Date` in positional params when drizzle hasn't propagated column type info.

Two sites affected:

- `reconcile.ts` — `${runs.updatedAt} < ${olderThan}` rewritten via drizzle's typed `lt(col, date)` so the column's `timestamptz` encoder runs. The `EXISTS` subqueries (no JS values, only `NOW()`) stay raw.
- `queries.ts` — cursor tuple compare `(createdAt, id) < (...)` casts the JS-Date param to `::timestamptz` in SQL. Tuple compare can't go through `lt`, so the cast is the cheapest correct fix.

Consumers using postgres-js or neon-serverless no longer need to spin up a separate `node-postgres` handle for the engine's pool.
