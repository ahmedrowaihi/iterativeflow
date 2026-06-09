---
"iterativeflow": patch
---

Recover runs whose worker crashed mid-execution.

A run that died while `status = running` could never resume: the reconciler re-enqueued it but left the status `running`, and `claimRun` rejects `running` as "lost" — so the re-enqueued job was skipped forever and the run hung permanently. The reconciler now resets a stuck `running` run to `retrying` before re-enqueuing, so the next claim succeeds. Guarded by the existing `reconciler.runningStuckMs` threshold (default 10 min).
