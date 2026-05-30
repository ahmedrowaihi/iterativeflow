---
"iterativeflow": patch
---

Fix `db.execute()` result-shape assumption that broke on drivers other than `drizzle-orm/node-postgres`. `invoke-budget` and `schema-version` probes were reading `result.rows[0]`, but `postgres-js` (and some drizzle 1.x driver builds) return the rows array directly — those consumers were getting `undefined.rows` and patching the dist by hand.

Added a `rowsOf()` helper that handles both shapes and used it at both call sites. `pg_notify` and other fire-and-forget executes are unaffected.
