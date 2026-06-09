---
"iterativeflow": patch
---

Reap orphaned `cron:*` jobs on worker startup.

When a cron is removed from code, graphile-worker stops scheduling it but already-enqueued `cron:<name>` jobs linger with no task handler — they sit forever, erroring across deploy cutovers. `startGraphileWorker` now runs a best-effort purge after `run()`, completing any `cron:*` job whose task is no longer registered. It never throws, so a reap failure can't block worker startup.

The cron policy (jitter, overlap, reaping) now lives in its own `cron` module that the graphile adapter drives.
