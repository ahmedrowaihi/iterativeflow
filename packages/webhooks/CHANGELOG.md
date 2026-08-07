# @iterativeflow/webhooks

## 2.1.1

### Patch Changes

- @iterativeflow/core@2.1.1

## 2.1.0

### Patch Changes

- @iterativeflow/core@2.1.0

## 2.0.1

### Patch Changes

- @iterativeflow/core@2.0.1

## 2.0.0

### Minor Changes

- d35db90: New: **`@iterativeflow/webhooks`** — the inbound webhook edge. `webhookSignalBridge(backend, {
verify, correlate })` verifies a signed provider webhook (Web Crypto HMAC, timing-safe, no
  `node:crypto`/`Buffer` — runs on Node, Workers, and the edge) and delivers it as a durable signal a
  parked flow can `await ctx.signal(...)`, so a run can pause on an external event (a `/qa run`
  comment, a preview deploy, a payment, a human approval) and resume crash-safe. Provider-agnostic: a
  pluggable `WebhookVerifier` with a `github` preset and an `hmacVerifier` building block for any
  hex-HMAC provider; `correlate` maps an event to the run(s) it wakes (fan-out supported). Delivery is
  idempotent on the provider's delivery id (atomic delivery + re-enqueue via `signalRun`). No runtime
  dependency beyond core.

### Patch Changes

- d35db90: Pre-release audit pass — correctness, consistency, and hardening fixes:

  - **core (cron at-most-once bug):** `runDueCrons` now starts the occurrence's idempotent run BEFORE
    advancing the schedule CAS. Previously a crash between `advanceCron` and `startRun` dropped the
    occurrence silently — at-most-once delivery inside an otherwise at-least-once engine. Also adds the
    `orphanedRunsSql` and `assertSqlIdentifier` backend-SPI helpers.
  - **mysql (atomicity bug):** transactions now run at READ COMMITTED, not MySQL's REPEATABLE READ
    default, so a concurrent first-writer-wins checkpoint's re-read sees the winner's just-committed
    row — matching Postgres, the model the store targets. Surfaced by a new concurrency conformance
    test.
  - **dynamodb / mongodb (lease version):** `claim` captures the job `version` from the atomic lease
    write (`ReturnValues: ALL_NEW` / the `findOneAndUpdate` result) rather than a stale pre-lease read,
    matching the other six backends.
  - **redis (performance):** `listRuns` scans the run index in bounded windows instead of loading every
    run on an interactive page.
  - **postgres:** the job `version` seeds at 1 like every other backend; the orphan query uses the
    shared `orphanedRunsSql`. **sqlite / mysql** share the same builder (one predicate, one home).
  - **hardening:** the webhook `hmacVerifier` refuses an empty secret at construction (fail closed);
    the SQL backends validate the schema/table-prefix identifier at construction.

- Updated dependencies [d35db90]
- Updated dependencies [d483f4f]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
- Updated dependencies [d35db90]
  - @iterativeflow/core@2.0.0

## 2.0.0-alpha.11

### Patch Changes

- Updated dependencies [0101884]
  - @iterativeflow/core@2.0.0-alpha.11

## 2.0.0-alpha.10

### Patch Changes

- Updated dependencies [f84d352]
  - @iterativeflow/core@2.0.0-alpha.10

## 2.0.0-alpha.9

### Patch Changes

- Updated dependencies [b8a9bb2]
  - @iterativeflow/core@2.0.0-alpha.9

## 2.0.0-alpha.8

### Patch Changes

- Updated dependencies [33d361c]
  - @iterativeflow/core@2.0.0-alpha.8

## 2.0.0-alpha.7

### Patch Changes

- @iterativeflow/core@2.0.0-alpha.7

## 2.0.0-alpha.6

### Patch Changes

- @iterativeflow/core@2.0.0-alpha.6

## 2.0.0-alpha.5

### Patch Changes

- Updated dependencies [f5df1e8]
  - @iterativeflow/core@2.0.0-alpha.5

## 2.0.0-alpha.4

### Patch Changes

- Updated dependencies [3a1d828]
  - @iterativeflow/core@2.0.0-alpha.4

## 2.0.0-alpha.3

### Patch Changes

- Updated dependencies [5b07ed6]
- Updated dependencies [acbe2bb]
- Updated dependencies [2257a3e]
- Updated dependencies [539a1c2]
- Updated dependencies [12f3baa]
  - @iterativeflow/core@2.0.0-alpha.3

## 2.0.0-alpha.2

### Minor Changes

- 2501f53: New: **`@iterativeflow/webhooks`** — the inbound webhook edge. `webhookSignalBridge(backend, {
verify, correlate })` verifies a signed provider webhook (Web Crypto HMAC, timing-safe, no
  `node:crypto`/`Buffer` — runs on Node, Workers, and the edge) and delivers it as a durable signal a
  parked flow can `await ctx.signal(...)`, so a run can pause on an external event (a `/qa run`
  comment, a preview deploy, a payment, a human approval) and resume crash-safe. Provider-agnostic: a
  pluggable `WebhookVerifier` with a `github` preset and an `hmacVerifier` building block for any
  hex-HMAC provider; `correlate` maps an event to the run(s) it wakes (fan-out supported). Delivery is
  idempotent on the provider's delivery id (atomic delivery + re-enqueue via `signalRun`). No runtime
  dependency beyond core.

### Patch Changes

- e1ef077: Pre-release audit pass — correctness, consistency, and hardening fixes:

  - **core (cron at-most-once bug):** `runDueCrons` now starts the occurrence's idempotent run BEFORE
    advancing the schedule CAS. Previously a crash between `advanceCron` and `startRun` dropped the
    occurrence silently — at-most-once delivery inside an otherwise at-least-once engine. Also adds the
    `orphanedRunsSql` and `assertSqlIdentifier` backend-SPI helpers.
  - **mysql (atomicity bug):** transactions now run at READ COMMITTED, not MySQL's REPEATABLE READ
    default, so a concurrent first-writer-wins checkpoint's re-read sees the winner's just-committed
    row — matching Postgres, the model the store targets. Surfaced by a new concurrency conformance
    test.
  - **dynamodb / mongodb (lease version):** `claim` captures the job `version` from the atomic lease
    write (`ReturnValues: ALL_NEW` / the `findOneAndUpdate` result) rather than a stale pre-lease read,
    matching the other six backends.
  - **redis (performance):** `listRuns` scans the run index in bounded windows instead of loading every
    run on an interactive page.
  - **postgres:** the job `version` seeds at 1 like every other backend; the orphan query uses the
    shared `orphanedRunsSql`. **sqlite / mysql** share the same builder (one predicate, one home).
  - **hardening:** the webhook `hmacVerifier` refuses an empty secret at construction (fail closed);
    the SQL backends validate the schema/table-prefix identifier at construction.

- Updated dependencies [e1ef077]
- Updated dependencies [3377316]
- Updated dependencies [a624058]
- Updated dependencies [f7bf20f]
- Updated dependencies [dc2b059]
- Updated dependencies [11d3aa2]
  - @iterativeflow/core@2.0.0-alpha.2
