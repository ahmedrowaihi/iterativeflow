# v1 → v2 parity ledger

Resolution of the feature-parity audit. Every v1 capability is PRESENT, or deliberately
dropped/deferred with a reason. This is the gate: v2 does not ship "done" while a shipped v1
feature is silently missing.

## PRESENT in v2

| Capability                    | v2 surface                                                                                                                                                                                  |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Imperative authoring          | `defineFlow`                                                                                                                                                                                |
| Fluent authoring              | `builder().step().output()` (typed accumulator, per-step policy)                                                                                                                            |
| Durable steps + memo          | `ctx.step`, first-writer-wins checkpoint                                                                                                                                                    |
| Step policy                   | retries, retryDelayMs, timeoutMs, `classify` transient/permanent, AbortSignal + attempt                                                                                                     |
| Sleep                         | `ctx.sleep` / `ctx.sleepUntil` (durable, memoized deadline)                                                                                                                                 |
| Sub-workflows                 | `ctx.invoke` (spawn child atomically via outbox, resume with output)                                                                                                                        |
| External signals              | `ctx.signal` (durable inbox) + `engine.signal` (deliver + wake, idempotent)                                                                                                                 |
| Idempotency                   | `idempotencyKey` on submit + signals + cron occurrences                                                                                                                                     |
| Batch dispatch                | `submitMany` + atomic `startManyRuns` (all-or-none)                                                                                                                                         |
| Transactional enqueue         | `inTx(pool, (backend, tx) => …)` — caller's writes + submit in one COMMIT                                                                                                                   |
| Run-level retry + backoff     | `RetryPolicy` (maxAttempts, exponential base→cap)                                                                                                                                           |
| Dead-letter / attempt cap     | `RUN_ATTEMPTS_EXHAUSTED` guard (poison-pill bound)                                                                                                                                          |
| Cancel + cascade              | `cancelRun` / `engine.cancel` (sticky, cascades to children, clears timer)                                                                                                                  |
| Retry a failed run            | `retryRun` / `engine.retry` (keeps ok memos)                                                                                                                                                |
| Await completion              | `result()` / `engine.result` (poll-first, timeout)                                                                                                                                          |
| Orphan reconciliation         | `reconcile` + `orphanedRuns` (crash-stranded + lost-parent-wake backstop)                                                                                                                   |
| Cron / recurring              | `registerCron` + `runDueCrons` (UTC parser, CAS single-fire, overlap-skip)                                                                                                                  |
| Query surface                 | `listRuns` (status/tag/name + cursor), `status`, `childrenOf`, `health`/`runStats`                                                                                                          |
| Observability events          | gated durable event log (`all`/`lifecycle`/`off`) + Postgres sink + `listEvents`                                                                                                            |
| Metrics hooks                 | `Metrics` callbacks (runStarted/Settled/Suspended/stepFinished)                                                                                                                             |
| Input validation              | Standard-Schema `input` on a flow, checked at submit                                                                                                                                        |
| Per-flow type-safety          | `submit` → `RunHandle<O, S>`, `result` recovers `O`, `signal` typed by the flow's `signals` map — restores v1 `FlowContract<I,O>` typing, adds typed signals ([CONTRACTS.md](CONTRACTS.md)) |
| Cohesive engine               | `createEngine(backend, flows, opts)` — the facade + resident `run()` loop                                                                                                                   |
| Payload guard                 | `maxPayloadBytes` on the engine                                                                                                                                                             |
| Schema setup                  | `applySchema` / `ddl` (Postgres); `ensureTable` / `tableSpec` / `REQUIRED_IAM_ACTIONS` (DynamoDB)                                                                                           |
| Consumer-owned drizzle schema | `drizzleSchema` + `iterativeflow-pg-drizzle` bin — typed reads, FKs to `workflow.run`, own migrations (stable + beta drizzle, drift-tested vs `ddl()`)                                      |
| Serverless execution          | `serverlessTick` / `engine.serverlessTick` — one cron-Lambda invocation fires crons + reconciles + drains + advances a batch (no daemon)                                                    |
| Dashboard                     | `createDashboard(engine)` — mountable Web `fetch` handler + self-contained UI                                                                                                               |
| Postgres backend              | full ports + outbox, real-concurrency + torn-write proven                                                                                                                                   |
| DynamoDB backend              | full ports + outbox (single-table + one GSI), `TransactWriteItems`, two-phase fan-out; same conformance suites                                                                              |
| In-memory backend             | reference + oracle for conformance                                                                                                                                                          |

## Deliberately dropped (with sign-off)

- **graphile-worker / pgmq / drizzle adapters** — replaced by the native lease-CAS `job` table +
  raw `Sql` abstraction. The capabilities graphile carried (cron, transactional enqueue) were
  re-homed as first-class v2 features above.
- **Custom-column table extension (`FlowTables` generic)** — dropped per decision. Correlate via
  run **tags + your own tables** (the transactional-enqueue pattern), not by extending core tables.
- **Graph builder nodes + static drift detection** — dropped per decision. The linear builder plus
  imperative `defineFlow` (which does sleep/signal/loop/branch via `ctx`) covers control flow.

## Deferred (small, noted — not silently missing)

- **Invoke depth / children-per-run caps** — needs a `depth` on the run; small follow-up. Payload
  cap is in; fan-out cap is the remaining safety knob.
- **Flow-definition fingerprint drift guard** — version bumping + the `unknown_flow` park already
  prevent mis-execution across incompatible deploys; a fingerprint check is an optional hardening.
- **Retention / prune sweeps** — runs are retained; a prune cron + `deleteRunsOlderThan` is a
  follow-up (pairs with the internal-cron mechanism now that cron exists).
- **`ctx.log` scoped logger, tracing spans, `defineContract`, health liveness beyond runStats** —
  low-impact ergonomics; deferred.
