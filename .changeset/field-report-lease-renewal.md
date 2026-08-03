---
"@iterativeflow/core": minor
---

Checkpoint-based lease renewal (first-user field report #4).

A long or many-step run advanced its whole flow body under the single claim-time lease, so `leaseMs`
had to exceed the longest run's wall-clock or a slow run got its lease stolen and double-executed. The
executor now renews the lease (`queue.heartbeat`) as the run commits steps — best-effort, and only
once the lease is half-consumed so quick steps don't each cost a heartbeat write. Long multi-step runs
are safe instead of banned by the "`leaseMs` > longest run" convention. Backend-agnostic — uses the
existing `heartbeat` port method; no backend changes.
