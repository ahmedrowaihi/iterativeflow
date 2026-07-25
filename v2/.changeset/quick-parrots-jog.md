---
"@iterativeflow/core": minor
"@iterativeflow/postgres": minor
"@iterativeflow/dynamodb": minor
"@iterativeflow/memory": minor
---

Close out the deferred parity items:

- **Invoke depth cap**: a per-run `depth` (0 for a submit, parent+1 per child) and `policy.maxDepth`
  (default 32) reject runaway `ctx.invoke` recursion before spawning. Persisted on all three backends.
- **Retention**: `Store.deleteRunsOlderThan(before, limit)` + `engine.prune(olderThanMs, limit?)`
  delete terminal runs (and their steps/signals/events) past a cutoff; not wired into the loop —
  schedule it yourself. Runs now carry `createdAt` (on `RunRow`), stamped once from the engine clock
  at submit/spawn so it agrees with the prune cutoff under any injected clock.
- **`ctx.log(message, data?)`**: a durable, replay-suppressed run log line to the event sink.
- **`defineContract`**: a type-only I/O + signal contract so a caller that doesn't own a flow's body
  (another service, the Go worker) can `submit`/`result`/`signal` it with full type-safety.
- **Health liveness**: `Queue.depth(now)` (backlog / in-flight / oldest-claimable age) and
  `engine.liveness()` for a k8s readiness probe.
- **Tracing**: a `Tracer` hook on `ObserveOpts` emitting one durable span per executed step —
  `traceId` stable per run, `spanId` derived from the step cursor (idempotent across replay),
  dependency-free. Wire it to `@opentelemetry/api`.
- **Live progress push** (opt-in, Postgres): `applyProgressTrigger` + `createPgListener.watch(runId)`
  / `onProgress(cb)` — a third `LISTEN/NOTIFY` channel on the existing socket, off the worker hot path.
