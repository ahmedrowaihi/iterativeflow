# @iterativeflow/core

## 2.4.0

### Minor Changes

- 2f1bb99: Bulk control: `engine.cancelMany(filter, limit)` and `engine.retryMany(filter, limit)`.

  Bulk creation (`submitMany`) and bulk deletion (`purge`) existed; control did not, so abandoning a
  queue or re-driving a failure wave meant paging ids and issuing one round trip per run — and
  `cancelRun` walks `childrenOf` recursively, so it was well over N. A downstream deployment reported
  ~11k failed runs of one flow and had built a sweep with a page cap, a time budget and a `seen` set to
  work around it.

  Both take the same `RunFilter` the ops UI already passes to `listRuns` (now with `version`), so "act
  on what I filtered" needs no new vocabulary, and both mirror `deleteRuns`: narrow by filter, cap with
  `limit`, return the count acted on, repeat until `< limit`. A filter with no predicate throws — a set
  operation over everything has to be spelled out.

  Cancel intersects with the live set and retry with `failed`, unconditionally, so neither can be
  widened past the statuses it is defined on. Retry zeroes `attempts` per row, matching the single-run
  fix, or a dead-lettered run would be re-failed without executing.

  Bulk cancel deliberately does not walk descendants: a child whose parent went non-success cancels
  itself on its next dispatch, and reconcile re-enqueues exactly those children, so the cascade still
  completes — one maintenance interval later instead of inline. That is what keeps this one round trip
  rather than a recursive graph walk.

- 6ed85c5: Crons are readable and removable, and metrics carry the flow they belong to.

  **`engine.listCrons()` / `engine.removeCron(name)`.** The cron surface was write-only — `upsertCron`,
  `dueCrons`, `dueCronCount`, `advanceCron` — with no way to see what was registered or to retire one.
  Because registration is an upsert, deleting a cron from your source did nothing: the row outlived it
  and kept firing forever, and the only fix was raw SQL against the table. Both are on the `Store` port
  and implemented by all 8 backends, with a conformance case.

  **`Metrics` callbacks now carry a flow label.** They passed only a `runId`, so labelling a metric by
  flow meant a store read inside the callback and per-flow p95 or failure-rate was effectively
  unobtainable. `runStarted`, `runSettled` and `runSuspended` now receive `{ name, version }`;
  `runSettled` also gets `durationMs` (wall-clock from the run's creation) and `errorCode`, and
  `stepFinished` gets its own `durationMs`. Existing callbacks keep working — the additions are trailing
  parameters.

- 6f2f7b4: `engine.pause()` / `engine.resume()` / `engine.isPaused()` — drain a running worker without stopping it.

  The only lever before this was the stop function from `engine.run()`, which aborts the tick loop
  _and_ clears the maintenance interval, so reconcile and crons stop with it. There was no way to say
  "stop taking new work, finish what you have" — the documented workaround was to deploy a worker
  registered for zero flows.

  `pause()` stops the run loop claiming; in-flight runs finish, reconcile and crons keep running, and a
  paused loop parked in its idle backoff **wakes immediately** rather than after up to
  `maxIdleTickMs`. That is what makes it react to a start/stop command on a live pod instead of on a
  poll interval: pausing flips an abort gate the loop is waiting on, so it is a push, not a poll. It
  takes effect on the next claim, so a batch already claimed still drains — leases are never abandoned.

  It gates the `run()` loop only. A caller driving `tick()` or `serverlessTick()` themselves pauses by
  not calling them, which needs no API, so `tick()` is deliberately not gated — an explicit call means
  you meant it.

  `Queue.waitForWork` takes an optional `AbortSignal` so a push-listener wait is interruptible too;
  previously a loop with `createPgListener` wired could sit out its full backoff before noticing either
  a pause or a stop.

  This is the worker-level pause only. Pausing a single flow, a single run, or a cron is durable
  fleet-wide state and a separate piece of work.

### Patch Changes

- c5443d5: Four correctness fixes: cron schedule changes, `n/step` cron syntax, observer isolation, and duplicate
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

- 2000935: Documentation accuracy pass, plus three small hardening fixes.

  The reference docs the package READMEs link to (`CONTRACTS.md`, `RECOVERY.md`, `MIGRATION.md`,
  `ARCHITECTURE.md`, `PARITY.md`) are now in the repository. `.gitignore` excluded all of `docs/`, so
  those links 404'd for every reader — the design notes, ADR drafts and docs plan stay local, which is
  what the rule was for.

  Corrected against the source: `result()` waits **indefinitely** without `timeoutMs` (and the README's
  headline example now passes one); Postgres has shipped cross-process push, so `createLocalWakeup` no
  longer calls it "a future opt-in"; Redis is single-node, not "or a Cluster" — the outbox Lua spans
  keys and a Cluster fails with `CROSSSLOT`; MongoDB's dedup indexes are partial, not sparse; DynamoDB
  needs **two** GSIs, not one; `inTx` exists for MongoDB too; the `maxFanOut` (10 000) and `maxDepth`
  (32) caps are documented where they're declared; the conformance suite list is complete (12, not 9,
  with the single-writer exemption stated); the React Native example passes the right argument; and the
  core README no longer advertises an alpha version.

  Hardening: `createPgListener` validates its schema identifier like every other interpolation site in
  the package; the dashboard's HTML escaper covers quotes and backticks, and its two inline `onclick`
  handlers are delegated listeners, so no value derived from a run id can reach a script context.

  Also adds `CONTRIBUTING.md` (Node 22.5+, corepack, the Docker-optional `SKIP_TESTCONTAINERS=1` path
  that was documented only inside `lefthook.yml`) and an `engines` floor to all 12 manifests, so a Node
  mismatch is a clear message instead of a stack trace.

- 789e7fc: Make the memo's runtime type the same on every backend, and gate the published packages.

  `ctx.step` is typed `Promise<T>`, but the memo round-trips through the backend's storage, so what a
  replay hands back was **backend-dependent**: Postgres/MySQL/SQLite/Redis/DynamoDB store JSON and turn
  a `Date` into a string, MongoDB stores BSON and kept it a `Date`, and the in-memory backend used
  `structuredClone` and kept it too. So `ctx.step("now", () => new Date())` returned a `Date` in tests
  and a `string` in production — the divergence was invisible precisely where you'd catch it.

  Values that cross the durable boundary (run input/output/error, step results, signal payloads) are
  now JSON-normalized on MongoDB and in memory, matching what every other backend already did. A new
  store-conformance case pins it, and it is what caught the MongoDB divergence — the doc change alone
  had asserted "a string on every backend", which was false.

  `ctx.step`'s JSDoc now says this outright: `T` describes what `fn` returns, not necessarily what a
  replay hands back.

  Also adds a packaging gate (`publint` over all 12 packages in CI — every `exports`, `files` and
  `types` entry must point at something the build actually emits; all 12 pass today, so this locks in
  a property that was previously unverified luck), a Renovate config that groups non-major updates and
  keeps backend drivers and `tsdown` on their own PRs, and a note in the deployment guide that
  `applySchema` builds indexes and a plain `CREATE INDEX` takes a write lock — at scale, run it as a
  migration or pre-create with `CONCURRENTLY`.

- 9eb11e2: Fix: a `ctx.*` call inside a `ctx.step` body no longer corrupts replay, and a suspend inside a step
  is no longer treated as a step failure.

  Two bugs on the same boundary, both reachable from the idiom the README documents — the builder hands
  `ctx` to every step fn, and the docs say "sleeps, signals, and invokes happen through `ctx` inside a
  step".

  **Cursor skew.** `step()` took its cursor key before running the body, so a nested `ctx.sleep` /
  `ctx.signal` / `ctx.invoke` consumed the keys after it while the outer step's memo committed at the
  earlier one. The invocation that resumed the step wrote its memo, and the _next_ replay returned that
  memo without re-running the body — so the following call landed on the nested call's memo, raised
  `FlowDriftError`, parked, and re-claimed until the run dead-lettered as `RUN_ATTEMPTS_EXHAUSTED`. The
  run was unrecoverable: no redeploy fixes an already-skewed cursor. A call issued inside a step body
  now keys off that step (`s1.0`, `s1.1`) instead of the flat cursor, so the body can never shift the
  keys of anything after it. Flows that don't nest are unaffected — their keys are byte-identical.

  **Suspends ran the failure policy.** `runWithPolicy` passed the thrown control signal to `classify`
  and counted it against `retries`. A `classify` written the documented way — treat anything
  unrecognised as `permanent` — turned an ordinary `ctx.sleep` into a `StepFailedError` and failed the
  run; with `retries > 0` a normal park instead re-ran the whole step body after `retryDelayMs`,
  re-executing its side effects. Control signals now short-circuit both, matching the guard the
  executor already applies.

  Note the remaining property, now covered by a test: a step body containing a suspend re-runs **from
  the top** on resume, because the enclosing step's memo is only written once the body returns. Put
  side effects in their own `ctx.step` rather than alongside a nested sleep.

  **Upgrading with runs in flight:** a run that is parked _inside_ a nested `ctx.*` call when you deploy
  this resumes with the new key scheme and drift-parks — the guard catches it, it is not silently wrong,
  but it needs the usual drift recovery (redeploy the old body, or bump the flow version). Runs that
  never nest are unaffected: their keys are byte-identical. Drain nesting flows before upgrading if you
  can.

- cad0b53: Close two gaps on the remote-fed surfaces: signal payloads bypassed `maxPayloadBytes`, and the
  dashboard passed unvalidated input to the store.

  **`maxPayloadBytes` now covers signals.** It was applied at `submit` and `submitMany` only, so the
  one knob documented as "a runaway-payload guard" missed the other way a payload enters durable
  storage — and the signal path is the one fed by `@iterativeflow/webhooks` and the dashboard. A signal
  payload lands in the run's inbox and is re-read by every subsequent `loadRun`, so an oversized one is
  amplified on each replay; on DynamoDB it can wedge the run past the 400 KB item limit at a point
  where the caller can no longer intervene. Note this is a behaviour change for anyone already sending
  signal payloads larger than a configured cap: they will now be rejected, which is the point.

  **The dashboard validates before dispatching.** `?limit` went through `Math.min(Number(raw), 200)`,
  so `-1` survived and reaches SQLite as _no limit_ — paging the entire run table, inputs and outputs
  included, into one response — while `abc` yielded `NaN` and a driver error instead of a clamped
  value. It is now clamped to a positive integer. The signal route cast its JSON body without checking
  it, so a body with no `name` reached `postSignal` and surfaced as a NOT-NULL violation rather than a
  `400`.

  Also carries the "mutating routes are unauthenticated, mount behind your own auth and add CSRF" note
  from the dashboard README into the `createDashboard` JSDoc, so it reaches anyone wiring it from
  editor autocomplete rather than only those who read the README.

- 5c2c9d5: Make a filtered purge use an index instead of scanning the run table.

  Measured on Postgres 17 against 500k runs with 780 matching rows, using the shipped query: **217 ms
  and 38,181 rows discarded, down to 1.87 ms with none discarded.** Three changes, all needed together.

  - **A partial index with `status` in the key** — `(name, status, created_at) WHERE status IN
('done','failed','canceled')`, on Postgres and SQLite. The obvious index `(name, version,
created_at)` with the statuses only in the _predicate_ measured **worse than no index at all**: the
    predicate decides which rows are in the index, it does not let the planner seek one terminal
    status. Partial keeps live runs out entirely, so a row enters the index once, when it goes terminal,
    and the hot path never pays. MySQL has no partial indexes, so it is deliberately left alone rather
    than given a full index that would pay on every insert and transition.
  - **Statuses render as SQL literals, not binds.** A partial index is only usable when the planner can
    prove the query's predicate implies the index's, and it cannot do that through a bind parameter —
    under a generic plan (which Postgres switches to after roughly five executions of a prepared
    statement) it silently drops the index and scans everything. That is the nastiest shape of bug:
    fast in tests and for the first minutes after deploy, then quietly not. The values come from the
    engine's own closed enum via `purgeStatuses`, never from a caller, and the SQL backends already
    render status tuples as literals everywhere else — the purge path was the deviation.
  - **`ORDER BY` only when there is an age cutoff.** Without one the whole matched set is going
    eventually, so the order is arbitrary, and sorting forced a full read of every matching row before
    `LIMIT` could bound anything — which undercut the "repeat until `< limit`" contract by re-paying the
    scan on every batch. Backends already disagreed on the column (`created_at` vs `seq`) and
    conformance only ever asserted counts, so nothing promised an order.

- a6b79cc: Release-pipeline integrity: verify the version PR, protect the publish, pin the formatter, and fix
  the npm source links.

  - **The version commit is now verified.** CI ran on `main` and on pull requests, but a push made with
    `GITHUB_TOKEN` fires no workflow events at all, so the version PR's own checks never ran — the one
    commit that rewrites all 12 manifests, every changelog and the lockfile was the one commit nothing
    checked. Adding a push trigger for that branch does not help, for the same reason. The release job
    now installs from the regenerated lockfile and runs typecheck + build against the versioned tree
    before opening the PR, so a broken lockfile fails there instead of after merge.
  - **A publish can no longer be cancelled halfway.** Workflow-level `cancel-in-progress` covered the
    release job, so a second push during a release could interrupt `changeset publish` mid-loop and
    leave npm with a partial `fixed` version set — some packages at the new version depending on
    siblings that never published, and npm publishes are not revocable. Cancellation now applies to the
    test job only; the release job has its own non-cancelling group.
  - **`oxfmt` is pinned.** The pre-commit hook ran it via `npx` with no version and no lockfile entry,
    so every contributor fetched whatever was latest and could reformat the same file differently. It's
    now a pinned dev dependency invoked from the local binary, with `format` / `format:check` scripts
    and a CI check. Six files that had drifted are formatted.
  - **npm's source links resolve.** All 12 manifests pointed `repository.directory` at `v2/packages/*`,
    a path that doesn't exist in this repo, so every package's "source" link 404'd.
  - **The public-API gate can't be outgrown.** `api-snapshot.mjs` had a hardcoded 12-package list, so a
    13th package would ship with no `etc/*.api.md` and the `git diff --exit-code etc` check would pass
    because nothing was generated for it. The list is derived from the workspace now, and a missing
    `dist/` fails with a clear "run build first" instead of a raw ENOENT.

- 8a02c61: Fix: `postSignal` stayed idempotent only until the signal was consumed, on half the backends.

  `postSignal` promises "idempotent on `idempotencyKey` — a retried delivery lands once", and
  `@iterativeflow/webhooks` leans on it: its default key is `${event.id}:${runId}:${name}`, so a
  provider redelivering the same event is supposed to be a no-op.

  Postgres, MySQL, SQLite and MongoDB dedupe via a unique index **on the signal row itself**, and
  consuming a signal deleted that row — destroying the only record that the key had ever been seen. A
  redelivery after consumption therefore landed as a brand-new signal, and a flow that awaits the same
  signal more than once (a loop, a second gate) consumed it as genuine. Memory, Redis and DynamoDB keep
  a separate dedupe record and were always correct, so the guarantee was backend-dependent — and the
  two backends the docs push for production SQL were the unsafe ones.

  Consumption is now a soft mark (`signal.consumed`) rather than a delete, so the key survives for the
  life of the run and retention still reclaims it with everything else. The inbox read filters consumed
  rows, so nothing else changes.

  The conformance suite could not see this: it posted the same key twice back-to-back and never after
  consumption. It now has a `post → consume → re-post` case, which is what proves all eight backends
  agree.

  Schema: adds a `consumed` column to the `signal` table. `applySchema` adds it in place on Postgres,
  MySQL and SQLite (guarded, since MySQL and SQLite have no `ADD COLUMN IF NOT EXISTS` and it runs on
  every boot); MongoDB needs no migration. The generated drizzle schema mirrors the new column.

- 02147f8: Fix: a step that declares `timeoutMs` now holds its run's lease while it runs.

  A lease was renewed only when a step **committed**, so a single step longer than `leaseMs` could not
  renew: the lease expired mid-flight, a second worker claimed the same run, and the same step body
  executed concurrently. The memo stays exactly-once, so the _result_ was never wrong — the side effect
  ran twice. Reproduced on the memory backend with no crash involved, and reported from production as a
  run that sat `running` for ~4.5h across 25 attempts, re-running an expensive scan each time.

  Declaring `StepPolicy.timeoutMs` now also keeps the lease alive for as long as the step runs. The
  ceiling is the step's own declared timeout, so the engine invents no new number and no new knob: a
  step that overruns is still aborted, and its lease still lapses, so a wedged worker is reclaimable
  exactly as before. `leaseMs` no longer has to be hand-sized above the longest step times the batch
  size.

  A step with **no** `timeoutMs` is deliberately unchanged: there is no honest bound to renew to, and
  renewing without one would convert a hung step into a permanent stall — worse than today, since
  reclaim by another live worker is the only thing that currently recovers that case.

  Also documents, on `serverlessTick`, that a renewing step widens the worst-case strand of an
  un-executed batch tail to about twice `leaseMs` past a killed invocation.

- 2367975: Turn on the supply-chain cooldown that was configured but never active, and stop dev-installing a
  peer nothing imports.

  `pnpm-workspace.yaml` carried a `minimumReleaseAgeExclude` list with no `minimumReleaseAge` set, so
  the gate it assumed existed was off — a freshly published version of any dev dependency was
  installable the moment it appeared. Now set to a one-day cooldown, which is what the exclude list was
  written for.

  `@op-engineering/op-sqlite` is an optional peer of `@iterativeflow/sqlite`, and `autoInstallPeers`
  installed it anyway — pulling the whole React Native/Metro toolchain into every clone and CI run for
  a package nothing in this repo imports (the adapter is typed structurally and its tests emulate the
  driver). It is now in `ignoredOptionalDependencies`. Consumers who actually use op-sqlite are
  unaffected: the peer declaration is unchanged.

  Note the tree is only pruned on the next full lockfile resolution — the setting is recorded now, but
  the already-resolved entries stay until then.

## 2.3.0

### Minor Changes

- ff33a4b: New: `@iterativeflow/core/testing` — a virtual clock over the real engine, so a flow that sleeps for
  three days settles in a millisecond.

  Testing a durable flow meant hand-rolling a `Clock`, pumping `tickOnce` in a loop, and guessing how
  far to advance between ticks. `createTestHarness(backend, flows)` does that properly:

  ```ts
  const t = createTestHarness(createMemoryBackend(), [onboard]);
  const handle = await t.engine.submit(onboard, { userId: "u_1" });
  await t.advanceToNextWake(); // runs to the sleep, then jumps it
  await t.engine.signal(handle, "survey", { score: 9 });
  expect(await t.settle(handle)).toMatchObject({ status: "done" });
  ```

  - `settle(handle)` drives a run to its terminal outcome, jumping every sleep and retry backoff. When
    the run parks on something nothing will deliver, it throws naming what it waited on — a missing
    `engine.signal(...)` in the test is the usual cause — instead of hanging until the test times out.
  - `advanceToNextWake()` drains first, then jumps to the earliest pending deadline, so a freshly
    submitted run reaches its sleep before the clock is asked where to jump. `false` means nothing is
    scheduled.
  - `advance(ms)` and `drain()` for finer control; `now()` reads the virtual instant.

  It is a third entry point on `core` rather than a new package, and it takes any `Backend` — the
  in-memory one for unit tests, or a real database for an integration test that still doesn't wait out
  a sleep. Both loops are bounded and throw a diagnostic rather than spinning.

  Also fixes a latent snapshot-stability bug in `scripts/api-snapshot.mjs`: it normalized the content
  hash of the `id-<hash>` chunk only. A third entry makes tsdown emit further shared chunks, whose
  un-normalized hashes would have churned `etc/core.api.md` — and failed CI's `git diff --exit-code
etc` — on any private edit to core. Every chunk hash is now neutralized, in both filenames and import
  specifiers.

### Patch Changes

- d0dc42f: Fix: `engine.retry()` did nothing for a run that dead-lettered — the one population you retry.

  `retryRun` reset `status` and `error` but left `attempts` at its spent value. The executor checks the
  dead-letter cap on the next claim, _before_ running the body: `markRunning` bumps attempts past
  `maxAttempts` and the run is failed terminally with `RUN_ATTEMPTS_EXHAUSTED` without executing
  anything. So retrying a run that had used its retry budget reported `retried: true`, then silently
  re-failed it — with a different error code than the one it originally failed with.

  Retry now zeroes `attempts` in the same atomic write, on all 7 backends. This matches how
  `suspendRun` already treats the counter: a forward-progress park zeroes it too, because it counts
  _no-progress_ re-claims, not legitimate resumes.

  The old behavior was invisible because the regression test failed a run under `maxAttempts: 1` and
  then re-drove it under the default `10`. Production uses one policy for both. The new tests use a
  single policy end to end, plus a store-conformance case that pins the counter directly for every
  backend.

## 2.2.0

### Minor Changes

- f765a77: Filtered retention: `Store.deleteRuns(filter, limit)` + `engine.purge(filter, limit)`.

  `deleteRunsOlderThan` could only sweep by age, so the history a mass cancel leaves behind (thousands
  of `canceled` runs of one flow) sat inflating every dashboard count until the retention window caught
  up. `deleteRuns` narrows the same sweep by `before`, `name`, `version` and terminal `status`, in the
  same single transaction and the same cascade order, returning the count deleted so callers batch
  until `< limit`. `deleteRunsOlderThan` stays as the `{ before }` case — one delegating line per
  backend — and `engine.prune` is unchanged.

  The terminal-only guard is unconditional: the filter narrows within `done`/`failed`/`canceled` and
  can never widen onto a live run, and a filter with no predicate at all is refused, since "delete all
  history" should be spelled `{ before: new Date() }`.

  New on the backend SPI, in `#purge` and shaped after the existing `isOrphaned` / `orphanedRunsSql`
  pair: `purgeStatuses` (the terminal intersection + empty-filter refusal), `purgeMatcher` (the row
  predicate the scanning backends filter with) and `purgeWhereSql` (the same predicate as a `WHERE`
  body + binds for the SQL backends). `TERMINAL_STATUSES` moves next to `RUN_STATUSES` in `#types` so
  the new `TerminalStatus` type derives from it rather than restating the list.

  Implemented across all 7 backends and covered by a store conformance case.

- ce5765c: Fix: a run-less row now passes every name filter consistently — `claim` and the backlog readers agreed
  on nothing before.

  2.1.1 made a name-filtered `Queue.claim` lease run-less jobs so `runTick`'s gone-path acks them and
  they self-heal. The sibling readers were left on the old answer: `Queue.depth(now, names)`,
  `Timer.dueCount(now, names)` and both `pending_work(flow_names, as_of)` SQL functions still excluded
  them. So `engine.pendingWork(["a"])` reported 0 for a backlog its own workers would lease, and a
  name-sharded fleet scaled to zero on that number never woke to drain it — the exact failure the claim
  fix set out to close. Conformance pinned the contradiction rather than catching it: a run-less timer
  was asserted _excluded_ under a name filter while a run-less job was asserted _included_.

  One rule now, everywhere: a row whose run is gone is unownable, so it passes every name filter; an
  empty `names` means "this worker handles nothing" and matches nothing at all, run-less rows included.
  The empty-set case was itself divergent — sqlite/mysql/dynamodb short-circuited while
  postgres/memory/mongodb/redis leased the orphan — and the conformance case had no orphan in it to
  notice. Both invariants are now pinned for all 8 backends.

  `Timer.dueCount(now, names)` and `Queue.depth(now, names)` therefore count run-less rows where they
  did not before, so a sharded `engine.pendingWork(names)` can rise by the number of orphaned rows —
  which is the point: that work is claimable.

  Also: both `pending_work` functions drop their unconditional `LEFT JOIN run` for a `NOT EXISTS`, so
  the unfiltered call (the common single-shard case) no longer does a run lookup per job and per timer;
  the dead `?? ""` fallbacks in the dynamodb/mongodb/memory claim filters are gone; the mysql/mongodb
  testcontainer bootstrap is shared by both test files per package; and four inaccurate claims in
  `patterns/` are corrected — notably that MySQL's `applySchema` needs `CREATE ROUTINE`, and that KEDA's
  mongo scaler cannot run `pendingWorkPipeline` directly.

## 2.1.1

## 2.1.0

## 2.0.1

## 2.0.0

### Minor Changes

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

- d483f4f: Autoscaling backlog primitive, plus operability for rolling deploys and pooled/serverless databases.

  - **Autoscaling backlog.** `engine.pendingWork(names?)` returns claimable jobs + due timers + due
    crons as one number, served over HTTP at the dashboard's `GET /api/metrics`. Postgres also ships a
    `pending_work(flow_names, as_of)` SQL function so KEDA's Postgres scaler can read it directly,
    including scaling to and from zero. Counting due timers/crons (not just queued jobs) is what wakes a
    scaled-to-zero worker for a durable `ctx.sleep` or a cron.
  - **`engine.check()`** — a startup probe that throws a clear error if the backend schema is missing or
    unreachable, instead of the worker loop silently retrying query errors.
  - **`redeployParked` metric** — fires when a claimed run parks for `unknown_flow`/`flow_drift`, so a
    rolling deploy can alert on runs stuck waiting for a flow version that didn't come back.
  - **SQLite safe defaults.** `applySchema` now sets WAL, `busy_timeout`, and `synchronous=NORMAL` for a
    concurrent, durable file store. Opt out via `ApplySchemaOpts.pragmas` (Durable Objects, which manage
    their own durability, skip them automatically).
  - **Postgres autovacuum.** The high-churn `job` table is created with aggressive autovacuum so a queue
    workload doesn't bloat; set once on create, so a later operator `ALTER` is never reset.
  - **MySQL isolation.** READ COMMITTED is now set per transaction (safe behind a connection pooler).
    `mysqlPool(pool, { setIsolation: false })` skips it for PlanetScale/Vitess, where a server-default
    READ COMMITTED avoids tainting pooled connections.

- d35db90: Fan-out, structured concurrency, and an idempotency policy:

  - **Fan-out + join**: `ctx.invoke` now takes one child or many — `ctx.invoke([{ flow, input }, …])` spawns
    every child in parallel and joins on all of them, resolving with the outputs in order (per-spec typed).
    Children spawn in chunks, each an atomic memoized checkpoint, so a fan-out is crash-safe on every
    backend (no unrecoverable DynamoDB two-phase overflow).
  - **Fast-fail + first-class failure cascade**: if any fan-out child fails or is cancelled, the parent
    fails immediately and its still-running siblings are cancelled. More broadly, cancellation now cascades
    to non-terminal descendants on **any** non-success termination — an explicit `cancelRun` _and_ a plain
    failure (previously only explicit cancel cascaded; a failed parent left its children running).
  - **`onDuplicate` idempotency policy**: `submit(..., { idempotencyKey, onDuplicate })` — `"reuse"`
    (default) returns the existing run's handle on a key hit; `"error"` throws `DuplicateRunError`
    (code `RUN_DUPLICATE`) so an accidental double-submit surfaces instead of silently collapsing.

  See `docs/v2/CONTRACTS.md`.

- d35db90: Browser- and React-Native-ready core, plus an op-sqlite driver adapter for the SQLite backend.

  **Isomorphic core** — the three `node:`-only couplings are gone, so `@iterativeflow/core` (and every
  isomorphic backend) bundles for the browser and React Native with no Node polyfills:

  - `newId` uses the Web Crypto global (`globalThis.crypto.randomUUID()`) instead of `node:crypto`, and
    throws an actionable error (pass a custom `IdGen`) when a runtime lacks the global.
  - Trace/span id hashing uses a bundled synchronous SHA-256 instead of `node:crypto` — output is
    byte-identical (known-answer + `node:crypto` parity tests), and it only runs when a tracer is wired.
  - The resident-loop sleep is a Web-standard `setTimeout` + `AbortSignal` instead of
    `node:timers/promises` (same timer semantics; the abort listener is removed on resolve, no per-tick
    leak).

  Verified with an `esbuild --platform=browser` bundle of both core entrypoints. All three swaps behave
  identically at runtime; the only new requirement is a Web Crypto global (Node 20+ / browsers /
  polyfilled RN), escape-hatched by the injectable `IdGen`.

  **`@iterativeflow/sqlite`: op-sqlite adapter** — `opSqliteDb` / `createOpSqliteBackend` run the SQLite
  backend on [op-sqlite](https://op-engineering.github.io/op-sqlite) — one code path across React Native
  (native JSI) and the browser (wasm + OPFS) — reusing the whole backend and passing every conformance
  suite. Declared structurally (no dependency on op-sqlite; it's an optional peer beside `@libsql/client`).
  The adapter owns `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK` for deterministic commit-on-resolve /
  rollback-on-throw.

- d35db90: First-user field-report fixes (v2 on Lambda + DynamoDB):

  - **A `try/catch` around `ctx.*` is now safe.** `ctx.sleep` / `ctx.signal` / `ctx.invoke` suspend the
    run by _throwing_ a control signal; a `catch` that swallowed it used to commit the next checkpoint
    at the wrong cursor and drift the run permanently. The engine now re-propagates a swallowed suspend
    at the next `ctx.*` call (and when the body returns), so the suspend still reaches the engine and
    the run parks + resumes correctly — you no longer have to special-case control signals in your own
    error handling.
  - **`StepPolicy.classify` gains the attempt number** — `(error, attempt) => "transient" | "permanent"`
    — and is now documented: fail fast on permanent (4xx/validation) errors instead of burning the
    in-invocation + run-level retry budget.

- d35db90: Checkpoint-based lease renewal (first-user field report #4).

  A long or many-step run advanced its whole flow body under the single claim-time lease, so `leaseMs`
  had to exceed the longest run's wall-clock or a slow run got its lease stolen and double-executed. The
  executor now renews the lease (`queue.heartbeat`) as the run commits steps — best-effort, and only
  once the lease is half-consumed so quick steps don't each cost a heartbeat write. Long multi-step runs
  are safe instead of banned by the "`leaseMs` > longest run" convention. Backend-agnostic — uses the
  existing `heartbeat` port method; no backend changes.

- d35db90: Self-scheduling serverless (`nextWakeAt`) — first-user field-report feature.

  A cron-cadence serverless driver advances a `ctx.sleep(15s)` only at the cron floor (1 minute on
  AWS). Now `serverlessTick`'s `SweepResult` carries **`nextWakeAt`** — the earliest pending timer
  (sleep / retry / cron) after the tick drained the due ones — and **`engine.nextWakeAt()`** exposes
  the horizon standalone, both backed by a new **`Timer.nextDueAt(now)`** port method (one bounded read
  on each backend's due-ordered index, never a scan). A driver arms a one-shot (EventBridge Scheduler /
  SQS `DelaySeconds` / Step Functions `Wait`) for exactly `nextWakeAt` and pays nothing while idle, so
  cost scales with pending work instead of wall-clock. `nextWakeAt` is timer-only; signals and
  child-joins wake by a push on submit/signal. Additive — fixed-cadence drivers and `engine.run()` are
  unaffected.

- d35db90: Structured `TickResult` (first-user field report #5).

  `serverlessTick` / `tickOnce` / `engine.tick()` reported a bare status string per run, so a driver
  seeing `["flow_drift", "failed"]` had to query the store to learn WHICH run and WHY. `TickResult` is
  now `{ runId, status, error?, cursorKey? }` — a failed, retrying, or drifted tick carries the error
  (and, for a drift, the cursor key it drifted at), so a serverless `SweepResult` consumer can log/route
  it without touching the store. The status-string union is now exported as `TickStatus`.

  Note: this is a breaking shape change for code that compared a tick result as a string
  (`result === "done"`) — read `result.status` instead.

- d35db90: Flow-aware claiming — sharded workers only lease runs they can execute.

  `Queue.claim` (`ClaimOpts`) takes an optional `names?: readonly string[]`: the claim is restricted to
  runs whose flow `name` is in the set. `tickOnce` derives it automatically from the worker's registered
  flows, so `engine.run` / `serverlessTick` shard with zero config — a pod that registers a disjoint
  subset of flows never blind-claims a run it has no handler for.

  **Why:** with partitioned pods (an API pod dispatches many flows; each worker pod registers only a
  few), a blind claim leases a run for an unregistered flow, which parks `unknown_flow` BEFORE
  `markRunning` bumps `attempts` — so it never exhausts to a dead-letter and re-parks on `baseDelayMs`
  forever. Roughly one wrong-pod escape per claim cycle, no error logs; high-cadence flows never
  converge. Filtering the claim by registered name removes the bounce at the source.

  - `names` omitted ⇒ no filter (a monolith claims everything — unchanged behavior).
  - Matches on `name` only: a registered name at an unregistered _version_ still leases and then parks
    for redeploy — the intended rolling-deploy handoff, not a shard miss.
  - Every backend implements the filter (SQL `LEFT JOIN run`; memory/redis/mongodb/dynamodb look up the
    run's name), proven by a new `claimFilterConformance` case across all eight backends.

- d35db90: Consumer migration, schema-ownership, type-safety, replay-safety, and correctness work:

  - **Fix: the attempt cap no longer kills long-lived/looping runs.** `markRunning` bumps `attempts`
    on every claim, and each durable resume (a `ctx.sleep` wake, a signal, a sequential `ctx.invoke`)
    is a fresh claim — so any run dispatched more than `maxAttempts` (default 10) times was failed with
    `RUN_ATTEMPTS_EXHAUSTED` despite zero failures, contradicting the durable-sleep guarantee. Attempts
    now reset on forward-progress suspends (`sleeping`/`awaiting_signal`/`awaiting_child`) — the
    `suspendRun` write zeroes the dispatch counter in the same write for those statuses; the
    poison-pill cap still fires on no-progress re-claims.
  - **Robustness/perf**: the resident `engine.run()` loop routes background-tick rejections to a
    `metrics.tickError` hook instead of letting an unhandled rejection crash the process; `loadRun`
    (Postgres) and the `drainTimers`/`reconcile` re-enqueue loops now run their independent I/O in
    parallel; a new `store.loadRunRow` lets `invoke`/`result` read just the run row instead of the full
    snapshot; DynamoDB `orphanedRuns` derives its reconcilable set from `RECONCILABLE_STATUSES` (no
    per-backend drift).
  - **Tests**: a shared `engineConformance` suite now runs the composed engine behaviors
    (retry/dead-letter, signal resume, cancel cascade to grandchild depth) against all three backends,
    not just memory.

  - **Flow drift guard**: each step memo records the `kind:label` of the `ctx` call that made it; on
    replay the executor compares it to the call now issued at that cursor. A flow body reordered or
    refactored under a live run (without a `version` bump) is detected and, per `driftPolicy` on the
    engine (or overridden per-flow), parks the run recoverably (`flow_drift`, default) or fails it
    (`FLOW_DRIFT`). Restores v1's
    static drift detection as a runtime check. Adds a nullable `shape` column/attribute to the step memo
    in all three backends (additive; pre-existing memos skip the check). See `docs/v2/CONTRACTS.md`.

  - **Typed flows & signals** (restores v1 per-flow type-safety, adds typed signals): `submit` returns
    a `RunHandle<O, S>` so `result` recovers the flow's output type `O` (was `unknown`), and a flow's
    `signals` map types both `ctx.signal(name)` on the await side and `signal(handle, name, payload)` on
    the send side — a wrong signal name or payload is a compile error on both ends. A `signals` entry is
    any **Standard-Schema** validator (zod/valibot/arktype), just like `input`: the payload is validated
    (and parsed) as the flow consumes it, and a bad one fails the run. `signalType<T>()` is the
    type-only escape hatch. `RunHandle` is a `string`, so plain-string `result`/`signal` and stored run ids keep
    working. See `docs/v2/CONTRACTS.md`.
  - **DynamoDB consistency**: strongly-consistent reads on the durable decision path — the `loadRun`
    replay Query, the base-table point reads, and `childrenOf` (which drives the cancel cascade — a
    stale read there let a just-spawned child escape cancellation permanently). GSI reads stay
    eventually-consistent (CAS-guarded); observability scans stay eventual (no wasted RCU).

  - **`serverlessTick`** (core, plus `engine.serverlessTick`): one invocation fires due crons,
    reconciles orphans, drains due timers, and advances a batch — a cron-Lambda entrypoint with no
    resident daemon. A durable `ctx.sleep` survives across invocations. Size `leaseMs` ≤ the
    invocation timeout.
  - **DynamoDB `tableSpec` + `REQUIRED_IAM_ACTIONS`**: provision the table + GSI in your own IaC;
    the IAM list names `TransactWriteItems`/`ConditionCheckItem`, the two a CDK `grantReadWriteData`
    omits. `claim` now paginates the JOB partition so due jobs are not starved behind a backlog of
    leased/future-dated jobs.
  - **Postgres `drizzleSchema()` + `iterativeflow-pg-drizzle` bin**: emit a consumer-owned drizzle
    schema for typed reads, foreign keys to `workflow.run`, and your own drizzle-kit migrations —
    generated (not re-exported) so it targets your installed drizzle. Drift-tested against `ddl()`
    on real Postgres; verified on drizzle stable (`0.45`) and the `1.0` beta.

- d35db90: `pollTimeoutMs` — bound the resident loop's DB poll so a dead Postgres connection can't silently freeze it.

  The resident worker loop does one `await` per cycle on the DB poll (drain due timers + claim a batch). A
  dropped/black-holed connection — RDS failover, PgBouncer killing a pinned socket — leaves that query
  awaiting a dead socket forever: the process stays alive but stops doing work, with no error. `tickOnce` /
  `engine.run` now bound the poll with `pollTimeoutMs` (default 30s via `createEngine`; `0` disables — an
  in-memory backend never hangs). On timeout the poll rejects `PollTimeoutError`; the resident loop already
  catches tick errors, so it logs via `observe.metrics.tickError` and re-polls on a fresh pooled connection.
  Bounds the poll only, never step execution.

- d35db90: Close out the deferred parity items:

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

- d35db90: `ctx.signal(name, { timeoutMs })` — await a signal with a deadline.

  Resolves `{ received: true, payload }` if the signal arrives within `timeoutMs`, else `{ received: false }`.
  A plain `ctx.signal` wait parks until the signal arrives; the timed form can now give up. The timeout
  decision is **linearizable with the durable inbox**: `postSignal` bumps the run's dispatch version as it
  delivers, and the timeout commits under a `requireVersion` guard that write-conflicts on that same job row on
  every backend (SQL `FOR UPDATE`, redis Lua, mongo doc-conflict, dynamo `ConditionCheck`). So a signal
  delivered before the timeout commits always wins, and one that raced the deadline is re-consumed on the next
  tick instead of being silently dropped — no orphaned signals, on any of the 8 backends.

  New public surface: `SignalOutcome<T>`, `Outbox.requireVersion` (a checkpoint precondition), and
  `CheckpointResult` (checkpointStep's return type, which carries the guard result off the persisted-memo shape).

- d35db90: FlowError.cause capture + a Postgres classify preset (production field report — vod-media-convert).

  - **`FlowError.cause`** — `toFlowError` now walks a thrown error's `.cause` chain (bounded depth) and
    flattens it into the persisted error, so a wrapper like `DrizzleQueryError` (generic "Failed query:
    rollback", the real pg error on `.cause`) no longer reduces a run record to `[object Object]`. This
    removes the need for a `failNormalized`/`dbStep` workaround.
  - **`pgClassify`** (`@iterativeflow/postgres`) — a ready `StepPolicy.classify` preset: constraint, data,
    and syntax/access errors are permanent (fail fast), while connection drops, statement timeouts,
    deadlocks, and serialization failures stay transient (retry). Walks the `.cause` chain for the SQLSTATE.
  - **Docs** — an error-sink recipe (`observe.sink` capturing `FlowError.cause`), idempotent-step guidance,
    and a note that `maxAttempts` already bounds a stalled-step reclaim loop (no blind re-dispatch).

- d35db90: Audit sweep — correctness, type-safety, and naming consistency:

  - **Typed fan-out inputs**: `ctx.invoke([{ flow, input }, …])` now type-checks each child `input`
    against ITS own flow (was `any` on the many-form), inferred from a flow tuple so the joined
    outputs stay per-child typed. Replaces the spec-tuple-keyed `InvokeOutputs` with `FlowOutputs` +
    `InvokeSpecFor` on the public surface.
  - **DynamoDB `startManyRuns` batches atomic chunks**: the earlier per-run create (one write per run,
    unbounded fan-out on a large `submitMany`) is replaced by within-batch idempotency-key dedup +
    atomic `TransactWriteItems` chunks bounded by the 100-item cap, falling back to per-run create only
    for a chunk a concurrent creator races. Restores per-chunk all-or-none without regressing dedup.
  - **Renames (breaking)**: the type-only signal helper `type<T>()` → `signalType<T>()`; the batch-submit
    spec `SubmitItem` → `SubmitSpec`; the row-limit SPI param `max` → `limit` (`claim`, `dueBatch`,
    `orphanedRuns`, `dueCrons`, `reconcile`, `drainTimers`).
  - **Correctness**: the reconcile lost-parent-wake fires only on a _resolved_ fan-out join (fast-fail
    preserved) instead of any terminal child; cron no longer throws on a valid sparse schedule spanning
    a leap cycle.
  - **Cleanup**: removed the unreachable `failed_terminal` step status and the unused `Queue.release`;
    extracted the triplicated orphan predicate to one shared `isOrphaned`.

### Patch Changes

- d35db90: Declare `license: MIT` and the repository field in every package manifest — the alpha.1 tarballs showed as "Proprietary" on npm.
- d35db90: Recovery & operations guide (first-user field report #3).

  The field report asked for a supported heal/repair path for a stuck run, or at least documented
  recovery. Since a `try/catch` around `ctx.*` is now safe (field report #1), the main way a run drifted
  permanently is gone — so rather than a risky memo-clearing "heal" primitive, the recovery is composing
  the existing levers. `docs/v2/RECOVERY.md` is now the lever-by-scenario guide: `retry` for a transient
  failure, `park` + redeploy or a version bump for drift (keep old versions registered until in-flight
  runs drain), and `cancel` + a fresh submit (new idempotency key — re-using the key returns the existing
  run, not a fresh one) for an un-resumable run. Linked from the core README.

- d35db90: `@iterativeflow/sqlite`: the op-sqlite adapter now retries `BEGIN IMMEDIATE` on `SQLITE_BUSY`.

  Under concurrent writers, `BEGIN IMMEDIATE` can return `SQLITE_BUSY` when another writer holds the
  write lock. `opSqliteDb` now retries the acquisition with async exponential backoff (~10→160ms, 5
  attempts) before surfacing the error — rather than a `PRAGMA busy_timeout`, whose native blocking
  would freeze the JS thread on op-sqlite's sync (JSI) path. Only `BEGIN` is retried: once it holds the
  lock the statements inside the transaction don't contend. A non-busy error is never retried.

- d35db90: First public alpha of iterativeflow v2 — a ground-up durable-execution engine.

  - **Four-port architecture** (store / queue / timer / wakeup) with a transactional-outbox seam: every durable write commits its side-effects (child spawns, enqueues, timers, signal consumption) atomically. One durable write per step.
  - **Three backends against one conformance suite**: in-memory (reference), Postgres (`BEGIN…COMMIT`, `SKIP LOCKED`, proven under real concurrency), DynamoDB (single-table, `TransactWriteItems`, two-phase fan-out past the 100-item cap).
  - **Authoring**: imperative `defineFlow` + a fully-typed accumulator `builder`, per-step policy (retries, timeout, transient/permanent classification, AbortSignal), Standard-Schema input validation.
  - **Durable primitives**: steps with exactly-once memos, `sleep`/`sleepUntil`, child workflows via `ctx.invoke`, external signals via a durable inbox, idempotent submits, atomic batch dispatch, and Postgres transactional enqueue (`inTx`).
  - **Reliability**: run-level retry with backoff, dead-letter attempt cap, orphan reconciler, wake-survives-ack queue versioning, cancel with cascade, retry-a-failed-run preserving memos.
  - **Operations**: `createEngine` facade with a resident worker loop, cron (CAS single-fire, overlap-skip), `listRuns`/`status`/`health` query surface, gated durable event log + metrics hooks, and a mountable dashboard (`fetch` handler + self-contained UI).
  - **Split entries**: `@iterativeflow/core` for app authors, `@iterativeflow/core/backend` for backend implementors.

## 2.0.0-alpha.11

### Patch Changes

- 0101884: `@iterativeflow/sqlite`: the op-sqlite adapter now retries `BEGIN IMMEDIATE` on `SQLITE_BUSY`.

  Under concurrent writers, `BEGIN IMMEDIATE` can return `SQLITE_BUSY` when another writer holds the
  write lock. `opSqliteDb` now retries the acquisition with async exponential backoff (~10→160ms, 5
  attempts) before surfacing the error — rather than a `PRAGMA busy_timeout`, whose native blocking
  would freeze the JS thread on op-sqlite's sync (JSI) path. Only `BEGIN` is retried: once it holds the
  lock the statements inside the transaction don't contend. A non-busy error is never retried.

## 2.0.0-alpha.10

### Minor Changes

- f84d352: Browser- and React-Native-ready core, plus an op-sqlite driver adapter for the SQLite backend.

  **Isomorphic core** — the three `node:`-only couplings are gone, so `@iterativeflow/core` (and every
  isomorphic backend) bundles for the browser and React Native with no Node polyfills:

  - `newId` uses the Web Crypto global (`globalThis.crypto.randomUUID()`) instead of `node:crypto`, and
    throws an actionable error (pass a custom `IdGen`) when a runtime lacks the global.
  - Trace/span id hashing uses a bundled synchronous SHA-256 instead of `node:crypto` — output is
    byte-identical (known-answer + `node:crypto` parity tests), and it only runs when a tracer is wired.
  - The resident-loop sleep is a Web-standard `setTimeout` + `AbortSignal` instead of
    `node:timers/promises` (same timer semantics; the abort listener is removed on resolve, no per-tick
    leak).

  Verified with an `esbuild --platform=browser` bundle of both core entrypoints. All three swaps behave
  identically at runtime; the only new requirement is a Web Crypto global (Node 20+ / browsers /
  polyfilled RN), escape-hatched by the injectable `IdGen`.

  **`@iterativeflow/sqlite`: op-sqlite adapter** — `opSqliteDb` / `createOpSqliteBackend` run the SQLite
  backend on [op-sqlite](https://op-engineering.github.io/op-sqlite) — one code path across React Native
  (native JSI) and the browser (wasm + OPFS) — reusing the whole backend and passing every conformance
  suite. Declared structurally (no dependency on op-sqlite; it's an optional peer beside `@libsql/client`).
  The adapter owns `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK` for deterministic commit-on-resolve /
  rollback-on-throw.

## 2.0.0-alpha.9

### Minor Changes

- b8a9bb2: Flow-aware claiming — sharded workers only lease runs they can execute.

  `Queue.claim` (`ClaimOpts`) takes an optional `names?: readonly string[]`: the claim is restricted to
  runs whose flow `name` is in the set. `tickOnce` derives it automatically from the worker's registered
  flows, so `engine.run` / `serverlessTick` shard with zero config — a pod that registers a disjoint
  subset of flows never blind-claims a run it has no handler for.

  **Why:** with partitioned pods (an API pod dispatches many flows; each worker pod registers only a
  few), a blind claim leases a run for an unregistered flow, which parks `unknown_flow` BEFORE
  `markRunning` bumps `attempts` — so it never exhausts to a dead-letter and re-parks on `baseDelayMs`
  forever. Roughly one wrong-pod escape per claim cycle, no error logs; high-cadence flows never
  converge. Filtering the claim by registered name removes the bounce at the source.

  - `names` omitted ⇒ no filter (a monolith claims everything — unchanged behavior).
  - Matches on `name` only: a registered name at an unregistered _version_ still leases and then parks
    for redeploy — the intended rolling-deploy handoff, not a shard miss.
  - Every backend implements the filter (SQL `LEFT JOIN run`; memory/redis/mongodb/dynamodb look up the
    run's name), proven by a new `claimFilterConformance` case across all eight backends.

## 2.0.0-alpha.8

### Minor Changes

- 33d361c: `pollTimeoutMs` — bound the resident loop's DB poll so a dead Postgres connection can't silently freeze it.

  The resident worker loop does one `await` per cycle on the DB poll (drain due timers + claim a batch). A
  dropped/black-holed connection — RDS failover, PgBouncer killing a pinned socket — leaves that query
  awaiting a dead socket forever: the process stays alive but stops doing work, with no error. `tickOnce` /
  `engine.run` now bound the poll with `pollTimeoutMs` (default 30s via `createEngine`; `0` disables — an
  in-memory backend never hangs). On timeout the poll rejects `PollTimeoutError`; the resident loop already
  catches tick errors, so it logs via `observe.metrics.tickError` and re-polls on a fresh pooled connection.
  Bounds the poll only, never step execution.

## 2.0.0-alpha.7

## 2.0.0-alpha.6

## 2.0.0-alpha.5

### Minor Changes

- f5df1e8: `ctx.signal(name, { timeoutMs })` — await a signal with a deadline.

  Resolves `{ received: true, payload }` if the signal arrives within `timeoutMs`, else `{ received: false }`.
  A plain `ctx.signal` wait parks until the signal arrives; the timed form can now give up. The timeout
  decision is **linearizable with the durable inbox**: `postSignal` bumps the run's dispatch version as it
  delivers, and the timeout commits under a `requireVersion` guard that write-conflicts on that same job row on
  every backend (SQL `FOR UPDATE`, redis Lua, mongo doc-conflict, dynamo `ConditionCheck`). So a signal
  delivered before the timeout commits always wins, and one that raced the deadline is re-consumed on the next
  tick instead of being silently dropped — no orphaned signals, on any of the 8 backends.

  New public surface: `SignalOutcome<T>`, `Outbox.requireVersion` (a checkpoint precondition), and
  `CheckpointResult` (checkpointStep's return type, which carries the guard result off the persisted-memo shape).

## 2.0.0-alpha.4

### Minor Changes

- 3a1d828: FlowError.cause capture + a Postgres classify preset (production field report — vod-media-convert).

  - **`FlowError.cause`** — `toFlowError` now walks a thrown error's `.cause` chain (bounded depth) and
    flattens it into the persisted error, so a wrapper like `DrizzleQueryError` (generic "Failed query:
    rollback", the real pg error on `.cause`) no longer reduces a run record to `[object Object]`. This
    removes the need for a `failNormalized`/`dbStep` workaround.
  - **`pgClassify`** (`@iterativeflow/postgres`) — a ready `StepPolicy.classify` preset: constraint, data,
    and syntax/access errors are permanent (fail fast), while connection drops, statement timeouts,
    deadlocks, and serialization failures stay transient (retry). Walks the `.cause` chain for the SQLSTATE.
  - **Docs** — an error-sink recipe (`observe.sink` capturing `FlowError.cause`), idempotent-step guidance,
    and a note that `maxAttempts` already bounds a stalled-step reclaim loop (no blind re-dispatch).

## 2.0.0-alpha.3

### Minor Changes

- 5b07ed6: First-user field-report fixes (v2 on Lambda + DynamoDB):

  - **A `try/catch` around `ctx.*` is now safe.** `ctx.sleep` / `ctx.signal` / `ctx.invoke` suspend the
    run by _throwing_ a control signal; a `catch` that swallowed it used to commit the next checkpoint
    at the wrong cursor and drift the run permanently. The engine now re-propagates a swallowed suspend
    at the next `ctx.*` call (and when the body returns), so the suspend still reaches the engine and
    the run parks + resumes correctly — you no longer have to special-case control signals in your own
    error handling.
  - **`StepPolicy.classify` gains the attempt number** — `(error, attempt) => "transient" | "permanent"`
    — and is now documented: fail fast on permanent (4xx/validation) errors instead of burning the
    in-invocation + run-level retry budget.

- acbe2bb: Checkpoint-based lease renewal (first-user field report #4).

  A long or many-step run advanced its whole flow body under the single claim-time lease, so `leaseMs`
  had to exceed the longest run's wall-clock or a slow run got its lease stolen and double-executed. The
  executor now renews the lease (`queue.heartbeat`) as the run commits steps — best-effort, and only
  once the lease is half-consumed so quick steps don't each cost a heartbeat write. Long multi-step runs
  are safe instead of banned by the "`leaseMs` > longest run" convention. Backend-agnostic — uses the
  existing `heartbeat` port method; no backend changes.

- 2257a3e: Self-scheduling serverless (`nextWakeAt`) — first-user field-report feature.

  A cron-cadence serverless driver advances a `ctx.sleep(15s)` only at the cron floor (1 minute on
  AWS). Now `serverlessTick`'s `SweepResult` carries **`nextWakeAt`** — the earliest pending timer
  (sleep / retry / cron) after the tick drained the due ones — and **`engine.nextWakeAt()`** exposes
  the horizon standalone, both backed by a new **`Timer.nextDueAt(now)`** port method (one bounded read
  on each backend's due-ordered index, never a scan). A driver arms a one-shot (EventBridge Scheduler /
  SQS `DelaySeconds` / Step Functions `Wait`) for exactly `nextWakeAt` and pays nothing while idle, so
  cost scales with pending work instead of wall-clock. `nextWakeAt` is timer-only; signals and
  child-joins wake by a push on submit/signal. Additive — fixed-cadence drivers and `engine.run()` are
  unaffected.

- 12f3baa: Structured `TickResult` (first-user field report #5).

  `serverlessTick` / `tickOnce` / `engine.tick()` reported a bare status string per run, so a driver
  seeing `["flow_drift", "failed"]` had to query the store to learn WHICH run and WHY. `TickResult` is
  now `{ runId, status, error?, cursorKey? }` — a failed, retrying, or drifted tick carries the error
  (and, for a drift, the cursor key it drifted at), so a serverless `SweepResult` consumer can log/route
  it without touching the store. The status-string union is now exported as `TickStatus`.

  Note: this is a breaking shape change for code that compared a tick result as a string
  (`result === "done"`) — read `result.status` instead.

### Patch Changes

- 539a1c2: Recovery & operations guide (first-user field report #3).

  The field report asked for a supported heal/repair path for a stuck run, or at least documented
  recovery. Since a `try/catch` around `ctx.*` is now safe (field report #1), the main way a run drifted
  permanently is gone — so rather than a risky memo-clearing "heal" primitive, the recovery is composing
  the existing levers. `docs/v2/RECOVERY.md` is now the lever-by-scenario guide: `retry` for a transient
  failure, `park` + redeploy or a version bump for drift (keep old versions registered until in-flight
  runs drain), and `cancel` + a fresh submit (new idempotency key — re-using the key returns the existing
  run, not a fresh one) for an un-resumable run. Linked from the core README.

## 2.0.0-alpha.2

### Minor Changes

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

- 3377316: Fan-out, structured concurrency, and an idempotency policy:

  - **Fan-out + join**: `ctx.invoke` now takes one child or many — `ctx.invoke([{ flow, input }, …])` spawns
    every child in parallel and joins on all of them, resolving with the outputs in order (per-spec typed).
    Children spawn in chunks, each an atomic memoized checkpoint, so a fan-out is crash-safe on every
    backend (no unrecoverable DynamoDB two-phase overflow).
  - **Fast-fail + first-class failure cascade**: if any fan-out child fails or is cancelled, the parent
    fails immediately and its still-running siblings are cancelled. More broadly, cancellation now cascades
    to non-terminal descendants on **any** non-success termination — an explicit `cancelRun` _and_ a plain
    failure (previously only explicit cancel cascaded; a failed parent left its children running).
  - **`onDuplicate` idempotency policy**: `submit(..., { idempotencyKey, onDuplicate })` — `"reuse"`
    (default) returns the existing run's handle on a key hit; `"error"` throws `DuplicateRunError`
    (code `RUN_DUPLICATE`) so an accidental double-submit surfaces instead of silently collapsing.

  See `docs/v2/CONTRACTS.md`.

- f7bf20f: Consumer migration, schema-ownership, type-safety, replay-safety, and correctness work:

  - **Fix: the attempt cap no longer kills long-lived/looping runs.** `markRunning` bumps `attempts`
    on every claim, and each durable resume (a `ctx.sleep` wake, a signal, a sequential `ctx.invoke`)
    is a fresh claim — so any run dispatched more than `maxAttempts` (default 10) times was failed with
    `RUN_ATTEMPTS_EXHAUSTED` despite zero failures, contradicting the durable-sleep guarantee. Attempts
    now reset on forward-progress suspends (`sleeping`/`awaiting_signal`/`awaiting_child`) — the
    `suspendRun` write zeroes the dispatch counter in the same write for those statuses; the
    poison-pill cap still fires on no-progress re-claims.
  - **Robustness/perf**: the resident `engine.run()` loop routes background-tick rejections to a
    `metrics.tickError` hook instead of letting an unhandled rejection crash the process; `loadRun`
    (Postgres) and the `drainTimers`/`reconcile` re-enqueue loops now run their independent I/O in
    parallel; a new `store.loadRunRow` lets `invoke`/`result` read just the run row instead of the full
    snapshot; DynamoDB `orphanedRuns` derives its reconcilable set from `RECONCILABLE_STATUSES` (no
    per-backend drift).
  - **Tests**: a shared `engineConformance` suite now runs the composed engine behaviors
    (retry/dead-letter, signal resume, cancel cascade to grandchild depth) against all three backends,
    not just memory.

  - **Flow drift guard**: each step memo records the `kind:label` of the `ctx` call that made it; on
    replay the executor compares it to the call now issued at that cursor. A flow body reordered or
    refactored under a live run (without a `version` bump) is detected and, per `driftPolicy` on the
    engine (or overridden per-flow), parks the run recoverably (`flow_drift`, default) or fails it
    (`FLOW_DRIFT`). Restores v1's
    static drift detection as a runtime check. Adds a nullable `shape` column/attribute to the step memo
    in all three backends (additive; pre-existing memos skip the check). See `docs/v2/CONTRACTS.md`.

  - **Typed flows & signals** (restores v1 per-flow type-safety, adds typed signals): `submit` returns
    a `RunHandle<O, S>` so `result` recovers the flow's output type `O` (was `unknown`), and a flow's
    `signals` map types both `ctx.signal(name)` on the await side and `signal(handle, name, payload)` on
    the send side — a wrong signal name or payload is a compile error on both ends. A `signals` entry is
    any **Standard-Schema** validator (zod/valibot/arktype), just like `input`: the payload is validated
    (and parsed) as the flow consumes it, and a bad one fails the run. `signalType<T>()` is the
    type-only escape hatch. `RunHandle` is a `string`, so plain-string `result`/`signal` and stored run ids keep
    working. See `docs/v2/CONTRACTS.md`.
  - **DynamoDB consistency**: strongly-consistent reads on the durable decision path — the `loadRun`
    replay Query, the base-table point reads, and `childrenOf` (which drives the cancel cascade — a
    stale read there let a just-spawned child escape cancellation permanently). GSI reads stay
    eventually-consistent (CAS-guarded); observability scans stay eventual (no wasted RCU).

  - **`serverlessTick`** (core, plus `engine.serverlessTick`): one invocation fires due crons,
    reconciles orphans, drains due timers, and advances a batch — a cron-Lambda entrypoint with no
    resident daemon. A durable `ctx.sleep` survives across invocations. Size `leaseMs` ≤ the
    invocation timeout.
  - **DynamoDB `tableSpec` + `REQUIRED_IAM_ACTIONS`**: provision the table + GSI in your own IaC;
    the IAM list names `TransactWriteItems`/`ConditionCheckItem`, the two a CDK `grantReadWriteData`
    omits. `claim` now paginates the JOB partition so due jobs are not starved behind a backlog of
    leased/future-dated jobs.
  - **Postgres `drizzleSchema()` + `iterativeflow-pg-drizzle` bin**: emit a consumer-owned drizzle
    schema for typed reads, foreign keys to `workflow.run`, and your own drizzle-kit migrations —
    generated (not re-exported) so it targets your installed drizzle. Drift-tested against `ddl()`
    on real Postgres; verified on drizzle stable (`0.45`) and the `1.0` beta.

- dc2b059: Close out the deferred parity items:

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

- 11d3aa2: Audit sweep — correctness, type-safety, and naming consistency:

  - **Typed fan-out inputs**: `ctx.invoke([{ flow, input }, …])` now type-checks each child `input`
    against ITS own flow (was `any` on the many-form), inferred from a flow tuple so the joined
    outputs stay per-child typed. Replaces the spec-tuple-keyed `InvokeOutputs` with `FlowOutputs` +
    `InvokeSpecFor` on the public surface.
  - **DynamoDB `startManyRuns` batches atomic chunks**: the earlier per-run create (one write per run,
    unbounded fan-out on a large `submitMany`) is replaced by within-batch idempotency-key dedup +
    atomic `TransactWriteItems` chunks bounded by the 100-item cap, falling back to per-run create only
    for a chunk a concurrent creator races. Restores per-chunk all-or-none without regressing dedup.
  - **Renames (breaking)**: the type-only signal helper `type<T>()` → `signalType<T>()`; the batch-submit
    spec `SubmitItem` → `SubmitSpec`; the row-limit SPI param `max` → `limit` (`claim`, `dueBatch`,
    `orphanedRuns`, `dueCrons`, `reconcile`, `drainTimers`).
  - **Correctness**: the reconcile lost-parent-wake fires only on a _resolved_ fan-out join (fast-fail
    preserved) instead of any terminal child; cron no longer throws on a valid sparse schedule spanning
    a leap cycle.
  - **Cleanup**: removed the unreachable `failed_terminal` step status and the unused `Queue.release`;
    extracted the triplicated orphan predicate to one shared `isOrphaned`.

### Patch Changes

- a624058: Declare `license: MIT` and the repository field in every package manifest — the alpha.1 tarballs showed as "Proprietary" on npm.

## 2.0.0-alpha.1

### Patch Changes

- First public alpha of iterativeflow v2 — a ground-up durable-execution engine.
  - **Four-port architecture** (store / queue / timer / wakeup) with a transactional-outbox seam: every durable write commits its side-effects (child spawns, enqueues, timers, signal consumption) atomically. One durable write per step.
  - **Three backends against one conformance suite**: in-memory (reference), Postgres (`BEGIN…COMMIT`, `SKIP LOCKED`, proven under real concurrency), DynamoDB (single-table, `TransactWriteItems`, two-phase fan-out past the 100-item cap).
  - **Authoring**: imperative `defineFlow` + a fully-typed accumulator `builder`, per-step policy (retries, timeout, transient/permanent classification, AbortSignal), Standard-Schema input validation.
  - **Durable primitives**: steps with exactly-once memos, `sleep`/`sleepUntil`, child workflows via `ctx.invoke`, external signals via a durable inbox, idempotent submits, atomic batch dispatch, and Postgres transactional enqueue (`inTx`).
  - **Reliability**: run-level retry with backoff, dead-letter attempt cap, orphan reconciler, wake-survives-ack queue versioning, cancel with cascade, retry-a-failed-run preserving memos.
  - **Operations**: `createEngine` facade with a resident worker loop, cron (CAS single-fire, overlap-skip), `listRuns`/`status`/`health` query surface, gated durable event log + metrics hooks, and a mountable dashboard (`fetch` handler + self-contained UI).
  - **Split entries**: `@iterativeflow/core` for app authors, `@iterativeflow/core/backend` for backend implementors.
