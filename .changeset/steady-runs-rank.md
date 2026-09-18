---
"@iterativeflow/core": minor
---

A run now keeps its priority for its whole life. Priority was stored only on the queue job, which is
deleted each time a run parks, so a run submitted with `priority: -10` became ordinary priority after
its first `ctx.sleep`, signal, retry or manual retry.

Priority is now stored on the run. Any enqueue that doesn't pass a `priority` uses the run's own.

**Schema:** adds a `priority` column to the `run` table. `applySchema` adds it in place on Postgres,
MySQL and SQLite; existing runs read as priority 0, which is how they already behaved. MongoDB, Redis
and DynamoDB need no migration.
