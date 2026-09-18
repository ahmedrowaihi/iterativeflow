---
"@iterativeflow/core": patch
---

Fix: `cancelMany({ tag })` and `retryMany({ tag })` ignored the tag on Postgres, MySQL, SQLite and
Durable Objects, and acted on **every** matching run instead. `engine.cancelMany({ tag: "tenant:42" })`
cancelled all live runs for all tenants, up to the limit.

The tag is now part of the query on every backend. Memory, Redis, DynamoDB and MongoDB were already
correct. If you called either method with a `tag` filter on an affected backend since 2.4.0, check for
runs that were cancelled or retried unintentionally.
