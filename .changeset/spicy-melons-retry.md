---
"iterativeflow": patch
---

Fix: a run could be orphaned in `retrying` forever. A step retry now records its backoff deadline as a durable `workflow.timers` row (symmetric with `ctx.sleep`), instead of leaving it only in the queue job's `run_at` where the reconciler couldn't see it.

Previously the retry backoff lived solely in the graphile/pgmq/outbox job. If that wake was lost — the timer fired and the follow-up worker was hard-killed mid-step — the run sat in `retrying` with nothing in `workflow.*` to recover against (observed 30h+). The reconciler's long-standing `retrying + timer due` branch had never matched, because retries never wrote a timer.

Now:

- `armRetryTimer` writes/updates one `__retry` timer per run at the backoff deadline; the queue job is demoted to just the wake.
- A healthy backoff has a future unfired timer → reconciler leaves it alone (no premature retry, at any backoff length). An orphaned retry has an overdue unfired timer → recovered on the same path as a stuck sleep, ~1 min after the missed deadline.
- The retry timer is fired when the run is claimed back out of `retrying`, so it can't linger and misfire a later `ctx.sleep`.
- A retrying orphan whose attempts are already exhausted is taken terminal (`failed` / `RUN_ATTEMPTS_EXHAUSTED`) instead of looping.
