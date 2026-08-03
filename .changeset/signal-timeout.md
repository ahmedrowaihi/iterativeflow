---
"@iterativeflow/core": minor
---

`ctx.signal(name, { timeoutMs })` — await a signal with a deadline.

Resolves `{ received: true, payload }` if the signal arrives within `timeoutMs`, else `{ received: false }`.
A plain `ctx.signal` wait parks until the signal arrives; the timed form can now give up. The timeout
decision is **linearizable with the durable inbox**: `postSignal` bumps the run's dispatch version as it
delivers, and the timeout commits under a `requireVersion` guard that write-conflicts on that same job row on
every backend (SQL `FOR UPDATE`, redis Lua, mongo doc-conflict, dynamo `ConditionCheck`). So a signal
delivered before the timeout commits always wins, and one that raced the deadline is re-consumed on the next
tick instead of being silently dropped — no orphaned signals, on any of the 8 backends.

New public surface: `SignalOutcome<T>`, `Outbox.requireVersion` (a checkpoint precondition), and
`CheckpointResult` (checkpointStep's return type, which carries the guard result off the persisted-memo shape).
