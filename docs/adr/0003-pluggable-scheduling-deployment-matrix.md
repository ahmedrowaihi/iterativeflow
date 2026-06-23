# ADR 0003 — Pluggable scheduling (resident / in-DB / serverless deployment matrix)

- **Status:** Accepted — Phases 1, 2, 3 implemented
- **Date:** 2026-06-23
- **Deciders:** iterativeflow maintainers
- **Relates to:** [ADR 0001 — per-flow task routing](./0001-per-flow-task-routing.md), [ADR 0002 — enqueue-only handles](./0002-enqueue-only-handles.md)

## Context

The engine is durable on Postgres, but its _driver_ — the thing that wakes a run
and the thing that runs it — is hardwired to a **resident process**:
graphile-worker polling + a `LISTEN flow_terminal` connection. That assumption
shows up in exactly four seams, which I traced through the code:

| #   | Seam                                                               | Where                                                                                                                                                     | Abstracted today?              |
| --- | ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| 1   | **Enqueue** — "schedule a wake for `runId`, optionally at `runAt`" | `TxEnqueue` (`src/storage/drizzle/types.ts:11`); swappable via `opt.worker.enqueue` (`src/engine/engine.ts:288`)                                          | ✅ **already a public option** |
| 2   | **Dispatch** — pull a job and call the flow body                   | `startGraphileWorker` (`src/engine/engine.ts:438`, only inside `listen()`)                                                                                | ❌ hardcoded                   |
| 3   | **Result wait** — how `handle.result()` learns a run terminated    | `pg_notify` (`src/storage/drizzle/notify.ts`) → `createListenLoop` (`src/engine/listen-loop.ts`) → in-process `Map` (`src/engine/terminal-waiters.ts:24`) | ❌ hardcoded                   |
| 4   | **Cron** — periodic firing (reconcile, retention, user crons)      | graphile cron (`src/engine/engine.ts:438-445`)                                                                                                            | ❌ hardcoded                   |

Three facts found in the code decide the whole design:

1. **Seam 1 already covers sleeps and the hot path.** A sleep is just an enqueue
   with a future `runAt` (`createGraphileTxEnqueue` passes `run_at => runAt`,
   `src/adapters/graphile/index.ts:27`). Signal-wake and parent-`invoke`-wake also
   ride `enqueue` (`signals.ts:67`, `notify.ts:18`). So **one** "schedule a wake"
   primitive — with an optional delay — already carries start, resume, sleep, and
   signal. This is the load-bearing seam, and it is already pluggable.
2. **Seam 3 cannot be serverless.** `terminal-waiters` is an in-process
   `Map<runId, Set<resolve>>`. The invocation that _starts_ a run has exited by the
   time another invocation _terminates_ it, so a blocking `handle.result()` is
   physically impossible without a resident process. Serverless callers must poll
   `engine.status()` or receive a terminal webhook. A constraint, not a preference.
3. **`pg_cron` / `pg_net` require an always-on Postgres background worker.** Neon
   (the canonical serverless PG) documents: _"pg_cron jobs will only run when your
   compute is active … use it only on computes where you have disabled scale to
   zero."_ And `pg_net` is **not** in the RDS/Aurora supported-extension lists —
   effectively Supabase + self-hosted only. So a database-internal scheduler is
   incompatible with scale-to-zero, and unavailable on a major managed provider.

### Why now

Downstream users on serverless Postgres (Neon, Vercel Postgres, Aurora
Serverless) and serverless compute (Vercel, Lambda, Cloudflare) cannot adopt the
library cleanly today: the resident `LISTEN` connection either pins the compute
always-on (defeating scale-to-zero economics) or loses its subscription on
suspend (`compute-lifecycle`: _"NOTIFY/LISTEN subscriptions are lost when the
compute suspends"_). The serverless adapter is therefore not only a new feature —
it removes a latent incompatibility the library has right now.

## Decision

Keep **all run/step/replay state in Postgres, unchanged**. Make seams 2–4
pluggable behind small strategy interfaces, and formalize seam 1 (already
abstracted) as the single scheduling primitive. The same engine then runs in any
cell of a **deployment matrix**; the developer picks a preset (or mixes axes).

```ts
/** Seam 1 — schedule a wake. Already exists as TxEnqueue; this is its public face. */
interface Scheduler {
  wake(runId: string, opts?: { runAt?: Date; priority?: number }): Promise<void>;
}

/** Seam 2 — drive runs. Resident: own a poll loop. Serverless: expose an entrypoint. */
interface Dispatcher {
  /** Begin dispatching. Resident impls start a loop; HTTP impls are a no-op here
   *  and instead surface `handleRun` to a route. */
  start(handleRun: (runId: string) => Promise<void>): Promise<void>;
  stop(): Promise<void>;
}

/** Seam 3 — how a caller learns a run terminated. */
interface ResultStrategy {
  /** Resident: LISTEN + in-process waiter. Serverless: poll status / webhook. */
  waitForTerminal(runId: string, timeoutMs?: number): Promise<void>;
  /** Called by the runner when a run terminates locally (in-process fast-path). */
  onLocalTerminal(runId: string): void;
}

/** Seam 4 — periodic firing for internal + user crons. */
interface CronScheduler {
  schedule(name: string, cron: string, handler: () => Promise<void>): void;
  start(): Promise<void>;
  stop(): Promise<void>;
}
```

`handleRun(runId)` — the single stateless "claim → replay → run-to-suspend →
persist" cycle — is extracted as an engine-level function (it already exists as
the body of the graphile task handler, `src/adapters/graphile/index.ts` via
`runWorkflow`). Resident dispatch calls it from a loop; serverless dispatch
exposes it to an HTTP route. **The replay engine, cursor keys, claim, snapshot,
retry, and reconcile are untouched** — they are the same regardless of who calls
`handleRun`.

### The matrix

Two questions select a preset: _is the app compute always-on?_ and _is the
Postgres scale-to-zero?_

|                     | **A · Resident** (today)  | **B · In-DB serverless**           | **C · Fully serverless**                                               |
| ------------------- | ------------------------- | ---------------------------------- | ---------------------------------------------------------------------- |
| App compute         | always-on                 | serverless                         | serverless                                                             |
| Postgres            | any                       | **always-on only**                 | **scale-to-zero ✅**                                                   |
| `Scheduler`         | graphile `add_job`        | `pgmq.send` (delay = sleep)        | external queue w/ native delay (QStash / SQS / CF Queue / EventBridge) |
| `Dispatcher`        | graphile resident worker  | `POST /run` ← pg_cron+pg_net drain | `POST /run` ← queue push                                               |
| `ResultStrategy`    | LISTEN + in-process await | poll / webhook                     | poll / webhook                                                         |
| `CronScheduler`     | graphile cron             | pg_cron                            | external scheduler                                                     |
| External services   | **none**                  | **none** (3 PG extensions)         | **one** scheduler/queue                                                |
| DB sleeps when idle | no                        | no                                 | **yes**                                                                |

**Forcing constraints** (from Context, fact 2 & 3):

- Postgres scale-to-zero ⇒ pg_cron/pg_net unusable ⇒ **B is off the table, C is
  the only path.**
- `pg_net` absent on RDS/Aurora ⇒ "B" there degrades to "C with an always-on PG"
  (external cron hitting `/drain`).
- Serverless compute (B or C) ⇒ no in-process `handle.result()`; callers poll or
  webhook.

## Design

### Phase 1 — extract the seams, ship Preset A behavior-identical (pure refactor)

No new capability; no behavior change; every existing test stays green. This is
the unlock for everything else and must land first.

1. **`src/engine/scheduler.ts`** (new) — define `Scheduler`, `Dispatcher`,
   `ResultStrategy`, `CronScheduler`. `Scheduler.wake` is a thin face over the
   existing `EnqueueRun`; the engine already resolves `name`/`version` from
   `runs` (ADR 0001 §3, `enqueueRun` in `src/storage/drizzle/index.ts`), so
   `wake(runId, opts)` delegates to that.
2. **`src/adapters/graphile/`** — wrap today's wiring as the default impls:
   `GraphileScheduler` (= `createGraphileTxEnqueue`), `GraphileDispatcher` (=
   `startGraphileWorker`, owns the poll loop), `GraphileCron` (= the existing cron
   path). No logic moves; only the entry points are renamed/wrapped.
3. **`src/engine/listen-result.ts`** (new) — `ListenResultStrategy` wrapping
   `createListenLoop` + `terminal-waiters` + `progress-waiters` verbatim. This is
   the Preset-A `ResultStrategy`.
4. **`src/engine/engine.ts`** — `createEngine` accepts optional
   `scheduler` / `dispatcher` / `resultStrategy` / `cron`, **defaulting to the
   graphile/listen impls** when `pool` + `db` are supplied (current constructor
   shape preserved). `listen()` calls `dispatcher.start(handleRun)` instead of
   `startGraphileWorker` directly; `handle.result()` calls
   `resultStrategy.waitForTerminal`. `notifyTerminal` keeps firing `pg_notify`
   (Preset A consumes it); serverless strategies simply don't subscribe.

Public-surface note: `EngineOpts` gains optional fields only — additive. The
default path (`{ db, pool }`) compiles and behaves exactly as before.

### Phase 2 — Preset C (fully serverless), the differentiated path

1. **`handleRun` as a public entrypoint** — export `engine.handleRun(runId)` so an
   HTTP route can call it. One invocation = one claim→replay→suspend cycle; on
   suspend it re-arms via `scheduler.wake(runId, { runAt })` and returns.
2. **`ExternalScheduler`** — `wake()` publishes to an external queue with native
   delay. Ship one reference impl (`@iterativeflow/qstash` or an in-repo
   `HttpScheduler` that POSTs `/run` directly for `runAt <= now` and uses the
   queue's delay for future `runAt`). The queue's retry/dedup composes with the
   existing claim guard (`claim.ts:42` returns `lost` for already-running) — no
   new dedup needed.
3. **`PollResultStrategy`** — `waitForTerminal` polls `engine.status(runId)` with
   backoff (no LISTEN); plus an optional terminal webhook fired from
   `notifyTerminal` (swap the `pg_notify` for an HTTP post when configured).
4. **`ExternalCron`** — internal reconcile/retention crons (`internal-crons.ts`)
   are driven by an external trigger hitting `/cron/:name`. The reconciler doubles
   as the safety-net sweeper for stuck runs (`engine.ts:423`).

`handle.result()` on a serverless engine throws a clear error directing callers
to `status()`/webhook — the seam-3 constraint, surfaced loudly, not silently.

### Phase 3 — Preset B (in-DB, zero external infra, always-on PG)

1. **`PgmqScheduler`** — `wake()` calls `pgmq.send(queue, {runId}, delaySeconds)`.
   pgmq is pure SQL (no background worker) so it installs anywhere; the delay is
   the sleep timer.
2. **`PgCronDispatcher`** — a `pg_cron` job invokes a drain function that reads a
   pgmq batch and `pg_net`-POSTs each `runId` to `/run`. Guarded behind a doc note:
   **requires always-on Postgres + pg_net (Supabase/self-host).**
3. Ship the enabling SQL as an optional migration, not a hard dependency.

## Consequences

- **One engine, three deployment shapes.** Resident users see no change.
  Serverless users get scale-to-zero on both compute and Postgres for the first
  time. State never leaves the user's database in any preset.
- **`EngineOpts` grows optional strategy fields** (additive → minor bump). Default
  construction unchanged. Run `npm run api:update`, commit
  `etc/iterativeflow.api.md`.
- **New public surface:** `engine.handleRun`, the four strategy interfaces, and
  the per-preset adapter packages. Each phase is independently shippable.
- **`handle.result()` semantics are preset-dependent** — blocking on Preset A,
  poll/webhook on B/C. Documented as the serverless contract; throws (not hangs)
  when called on a non-blocking strategy.
- **CONTEXT.md update:** the "Storage … Don't add a second adapter unless
  something actually needs to vary across it" note now _has_ its justification —
  serverless varies across the driver. Add "Scheduler / Dispatcher / Result
  strategy" to the glossary as the named seam between "stays in your Postgres"
  (state) and "varies by deployment" (driver).

## Migration / back-compat

- **Phase 1 is behavior-identical.** Default impls = today's graphile/listen path;
  no schema change, no queue change, no API break. Existing tests are the
  regression guard.
- **Presets B/C are opt-in** via new `EngineOpts` fields and adapter packages; a
  user who passes only `{ db, pool }` stays on Preset A forever.
- No `workflow.*` schema change in any phase — the schema-name and `flow_terminal`
  wire commitments (CONTEXT.md "Stable, not negotiable") are preserved. Serverless
  result delivery is _additive_ (webhook/poll) alongside `flow_terminal`, not a
  rename.

## Test plan

**Phase 1 (refactor — prove no behavior change):**

- All existing suites green unchanged: `engine.test.ts`, `durability.test.ts`,
  `multi-instance.test.ts`, `integration.test.ts`, `flow-routing.test.ts`,
  `handle-wait.test.ts`, `corpus.test.ts`.
- New `src/engine/scheduler.test.ts`: assert `createEngine({ db, pool })` wires the
  graphile/listen defaults (same task-list, same `flow_terminal` subscription) —
  a structural test that the default preset is unchanged.

**Phase 2 (Preset C):**

- New `src/adapters/external/handle-run.test.ts` using the PGlite/in-memory pool
  (already a dev dep: `@electric-sql/pglite`): drive a flow purely through repeated
  `engine.handleRun(runId)` calls with a fake `Scheduler` that records `wake`s and
  re-invokes synchronously. Assert: step memoization across invocations, a sleep
  re-arms with the right `runAt`, a signal resumes, the run reaches `done` with
  **no resident worker and no LISTEN**.
- `PollResultStrategy` test: `waitForTerminal` resolves after `status()` flips to
  terminal; throws on timeout.
- Assert `handle.result()` on a serverless engine throws the directing error.

**Phase 3 (Preset B):**

- Testcontainer with `pgmq` installed (SQL-only install): `PgmqScheduler.wake`
  enqueues a pgmq message with the correct delay; a drain reads it and runs the
  flow to completion. Reuse the two-pool harness from `multi-instance.test.ts`.
- Skip-guard the pg_net/pg_cron path in CI (needs the extension); cover the drain
  logic with a direct call instead.

**Cross-cutting:**

- Replay corpus (`tests/replay-corpus`) must pass identically under Preset C
  dispatch — the corpus is the proof that replay is driver-independent.

## Tooling gates (every phase, before PR)

```
npm run typecheck && npm run lint && npm run test
npm run api:update      # commit etc/iterativeflow.api.md
npm run docs:check && npm run size:check
```

## Implementation status

- **Phase 1 (done).** `Scheduler` / `Dispatcher` / `DispatcherStartOpts` in
  `src/engine/scheduler.ts`; `createGraphileDispatcher` in
  `src/adapters/graphile/dispatcher.ts`; engine `listen()`/`stop()`/`health()`
  routed through the dispatcher; `engine.handleRun` exposed; `EngineOpts.dispatcher`
  added. Behavior-identical — all prior suites green.
- **Phase 2 (done).** Provider-agnostic serverless adapter under
  `src/adapters/serverless/`, exported at the `iterativeflow/serverless` subpath:
  `createOutboxEnqueue` (transactional outbox `TxEnqueue`), `drainDueWakes`
  (claim-by-delete drain), `drainAndRun` (turn-key tick), `createWakeOutboxTable`,
  `createServerlessDispatcher` (no-op). Plus `engine.reconcile()` — the orphan
  recovery sweep, now drivable from a serverless `/cron` (the resident build runs
  it on a cron; serverless had no public access to it). The outbox carries
  `run_at` so step-retry timing survives without graphile; crash recovery falls
  back to `reconcile()`. Proven on PGlite in `serverless.test.ts`: a flow advances
  across sleep + signal via `drainAndRun` + `engine.handleRun` alone — no resident
  worker, no LISTEN, steps memoized — and an orphaned run recovers via
  `reconcile()`. Usage guide: `docs/serverless.md`.

  Seam 3 (result delivery) shipped as a simple `EngineOpts.results: "listen" |
"poll"` mode, not the pluggable `ResultStrategy` interface this ADR first
  sketched — two modes don't justify an interface (clarity over cleverness).
  `"poll"` makes `handle.result()`/`wait()` throw on a non-terminal run, turning a
  silent serverless hang into a directing error.

  As-built note: the `Scheduler` interface sketched in the Decision section was
  **not** shipped — seam 1 is the existing `TxEnqueue` (swapped per deployment via
  `opt.worker.enqueue`), so a separate `Scheduler` type would be dead surface.
  Only `Dispatcher` (+ `DispatcherStartOpts`, `RunHandler`) is exported. The
  `Scheduler`/`wake()` names in the Decision narrative below describe the concept;
  the realized form is `TxEnqueue`.

- **Phase 3 (done).** `iterativeflow/pgmq` subpath: `createPgmqEnqueue` (a
  `TxEnqueue` over `pgmq.send`, `run_at` → pgmq `delay`), `drainAndRunPgmq` (read
  visible wakes → `handleRun` → delete on success; failures redeliver after the
  visibility timeout), `createPgmqQueue`. Proven against a **real pgmq queue** in
  `pgmq.test.ts` (testcontainer `ghcr.io/pgmq/pg16-pgmq`): a flow advances across
  a pgmq-delayed sleep + a signal with steps memoized, and an orphan recovers via
  `reconcile()`. The `pg_cron` + `pg_net` recipe for driving either queue from
  inside an always-on Postgres is documented in `docs/serverless.md`.

Future: concrete external schedulers (QStash / SQS / EventBridge) extending the
Phase 2 outbox model. Not yet covered: the `serverless`/`pgmq` subpath surfaces
are not tracked by api-extractor (it analyzes `dist/index.d.ts` only); add reports
if those surfaces grow.

## Alternatives considered and rejected

- **One big-bang serverless rewrite.** Rejected: the engine's value is the replay
  core; rewriting risks it. Phasing keeps Preset A as a continuously-green
  regression oracle and ships value incrementally.
- **Database-internal scheduler as the universal serverless answer (pg_cron +
  pg_net everywhere).** Rejected on evidence: incompatible with scale-to-zero
  (Neon docs), and `pg_net` unavailable on RDS/Aurora. It is one _flavor_ (Preset
  B, always-on PG), not the default.
- **Drop `LISTEN`/resident entirely and make everything external.** Rejected:
  resident + blocking `result()` is the best DX for the always-on majority and has
  zero external dependencies. The matrix keeps it as Preset A.
- **A managed orchestrator (Temporal/Inngest-style).** Rejected: it moves workflow
  state out of the user's Postgres and adds per-step billing — the opposite of the
  library's "durable on _your_ Postgres" identity.

```

## Resolved (planning session, 2026-06-23)

1. **Method names** — `Scheduler.wake(runId, { runAt? })`, `Dispatcher`,
   `engine.handleRun(runId)`. Named for the serverless reader; `wake` matches the
   sleep/signal mental model.
2. **Package layout** — interfaces + Preset A stay in core; serverless adapters
   ship as **in-repo subpath exports** (`src/adapters/*`, e.g. `iterativeflow/pgmq`)
   with optional peer deps. No monorepo split.
3. **Preset C reference scheduler** — provider-agnostic **`HttpScheduler`** first:
   POST `/run` for due wakes; surface a "schedule at `runAt`" callback the host
   wires to any timer. Concrete QStash / SQS / EventBridge adapters extend it later.
4. **Webhook vs poll** (Preset C `result()`) — poll default (zero-config), webhook
   opt-in.

### Phase 1 build order (executing now)

`Scheduler` + `Dispatcher` interfaces (`src/engine/scheduler.ts`) →
`GraphileDispatcher` wrapping `startGraphileWorker` → engine `listen()`/`stop()`/
`health()` routed through the `Dispatcher` → `engine.handleRun` exposed →
optional `dispatcher` on `EngineOpts` defaulting to graphile. Behavior-identical;
existing suites are the regression oracle. `ResultStrategy` extraction (seam 3)
and `Scheduler.wake` root-face follow in 1b.
```
