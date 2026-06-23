---
"iterativeflow": minor
---

Pluggable scheduling + serverless adapter (ADR 0003).

The engine now drives runs through a `Dispatcher` seam, defaulting to the
resident graphile worker — behavior is unchanged for existing setups. New:

- `engine.handleRun(runId)` — one stateless claim → replay → suspend cycle, for
  serverless hosts to call per request.
- `engine.reconcile()` — re-enqueue orphaned runs (the recovery sweep, now
  drivable from a serverless `/cron`).
- `EngineOpts.dispatcher` and the `Scheduler` / `Dispatcher` interfaces.
- New `iterativeflow/serverless` subpath: `createOutboxEnqueue` (transactional
  wake outbox), `drainDueWakes`, `drainAndRun`, `createWakeOutboxTable`,
  `createServerlessDispatcher`. Run durable workflows on Vercel / Lambda /
  Cloudflare against your own Postgres — including scale-to-zero databases — with
  state that never leaves your DB. See `docs/serverless.md`.
- New `iterativeflow/pgmq` subpath: `createPgmqEnqueue`, `drainAndRunPgmq`,
  `createPgmqQueue` — drive the same serverless model over a pgmq queue, gaining
  native visibility-timeout redelivery.

All additive; no schema change.
