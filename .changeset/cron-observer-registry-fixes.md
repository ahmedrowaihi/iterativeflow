---
"@iterativeflow/core": patch
---

Four correctness fixes: cron schedule changes, `n/step` cron syntax, observer isolation, and duplicate
flow registration.

**A changed cron schedule never took effect.** `upsertCron` preserved `nextRunAt` whenever the row
already existed, so re-registering `0 3 * * *` as `*/5 * * * *` updated the stored schedule but left
the next fire at tomorrow 03:00 — the old cadence outlived the deploy that changed it, by up to a
year for a yearly cron. Timing is now preserved only when the schedule string is unchanged, on all 8
backends, and pinned by a conformance case. (The redeploy-doesn't-reset-timing behaviour it was
protecting is unchanged and still tested.)

**`0/15 * * * *` silently meant "hourly".** A bare number with a step discarded the step, so a
common, valid Vixie/Quartz expression parsed as the single value `{0}` and fired 4× less often than
written — accepted at registration, no error anywhere. `n/step` is now a range from `n` to the field
maximum.

**A throwing event sink could derail a run.** `Observer.event` let a sink rejection escape into the
executor, where it was caught as a flow error — between `markTerminal` and the parent-wake, that
skipped `arriveAtJoin` and the ack, so a completed child left its parent waiting for the reconcile
sweep and the tick reported `retrying` for a run that was `done`. With `level: "all"` a persistently
failing sink advanced a run one step per attempt until it dead-lettered. Sink and tracer failures now
surface through `metrics.tickError` and never touch control flow, which is what "observability is
never load-bearing" was supposed to mean.

**Two flows registered under the same `name@version` silently kept the last one**, so every run of
the first executed the second body — and the drift guard cannot catch it, because the fingerprint is
`kind:label`, not the body. `registry()` now throws.
