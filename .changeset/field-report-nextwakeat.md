---
"@iterativeflow/core": minor
---

Self-scheduling serverless (`nextWakeAt`) — first-user field-report feature.

A cron-cadence serverless driver advances a `ctx.sleep(15s)` only at the cron floor (1 minute on
AWS). Now `serverlessTick`'s `SweepResult` carries **`nextWakeAt`** — the earliest pending timer
(sleep / retry / cron) after the tick drained the due ones — and **`engine.nextWakeAt()`** exposes
the horizon standalone, both backed by a new **`Timer.nextDueAt(now)`** port method (one bounded read
on each backend's due-ordered index, never a scan). A driver arms a one-shot (EventBridge Scheduler /
SQS `DelaySeconds` / Step Functions `Wait`) for exactly `nextWakeAt` and pays nothing while idle, so
cost scales with pending work instead of wall-clock. `nextWakeAt` is timer-only; signals and
child-joins wake by a push on submit/signal. Additive — fixed-cadence drivers and `engine.run()` are
unaffected.
