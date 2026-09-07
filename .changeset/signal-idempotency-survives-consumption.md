---
"@iterativeflow/core": patch
---

Fix: `postSignal` stayed idempotent only until the signal was consumed, on half the backends.

`postSignal` promises "idempotent on `idempotencyKey` — a retried delivery lands once", and
`@iterativeflow/webhooks` leans on it: its default key is `${event.id}:${runId}:${name}`, so a
provider redelivering the same event is supposed to be a no-op.

Postgres, MySQL, SQLite and MongoDB dedupe via a unique index **on the signal row itself**, and
consuming a signal deleted that row — destroying the only record that the key had ever been seen. A
redelivery after consumption therefore landed as a brand-new signal, and a flow that awaits the same
signal more than once (a loop, a second gate) consumed it as genuine. Memory, Redis and DynamoDB keep
a separate dedupe record and were always correct, so the guarantee was backend-dependent — and the
two backends the docs push for production SQL were the unsafe ones.

Consumption is now a soft mark (`signal.consumed`) rather than a delete, so the key survives for the
life of the run and retention still reclaims it with everything else. The inbox read filters consumed
rows, so nothing else changes.

The conformance suite could not see this: it posted the same key twice back-to-back and never after
consumption. It now has a `post → consume → re-post` case, which is what proves all eight backends
agree.

Schema: adds a `consumed` column to the `signal` table. `applySchema` adds it in place on Postgres,
MySQL and SQLite (guarded, since MySQL and SQLite have no `ADD COLUMN IF NOT EXISTS` and it runs on
every boot); MongoDB needs no migration. The generated drizzle schema mirrors the new column.
