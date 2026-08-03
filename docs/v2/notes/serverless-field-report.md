# Field report: iterativeflow v2 on Lambda + EventBridge + DynamoDB

- **Status:** Field notes (not a spec). From driving COLD MediaLive channel applies
  (stop → poll-until-IDLE → update → start) on a real control plane: AWS Lambda (oRPC API +
  a tick Lambda), EventBridge `rate(1 min)`, `@iterativeflow/dynamodb`.
- **Scope:** `core` + `@iterativeflow/dynamodb` + a missing `docs/serverless` story. Pairs the
  concrete pain with the two standalone proposals it motivated:
  [self-scheduling `serverlessTick`](./self-scheduling-serverless-tick.md) and
  [a time-ordered run index](./time-ordered-run-index.md).

The library **worked** — durable stop/update/start survived crashes and resumed correctly once the
edges below were understood. This is a report on the edges, ranked by how much they bit.

## Sharp edges hit (ranked)

### 1. A user `try/catch` can swallow a suspend → permanent FlowDrift

The single worst footgun. `ctx.sleep` suspends by **throwing** `SleepSignal`. The apply flow wrapped
its stop→wait→update in a `try/catch` to guarantee an always-restart on failure:

```ts
try {
  await ctx.step("stop", ...);
  for (;;) { await ctx.step(`await-idle-${i}`, ...); await ctx.sleep(15_000); } // ← throws SleepSignal
  await ctx.step("update", ...);
} catch (err) { failure = err; }          // ← SWALLOWS the suspend
if (wasRunning) await ctx.step("start", ...); // ← runs early, at the WRONG cursor
if (failure) throw failure;
```

The swallowed `SleepSignal` fell through to `ctx.step("start")`, which committed `step:start` at the
**positional cursor** the poll loop's next `await-idle` belonged to. On resume the shapes no longer
matched → `FlowDriftError` on every wake, forever. It also fired `StartChannel` mid-STOP.

`isControlSignal(err)` is the fix (re-throw it before handling real errors), and once known it's a
one-liner. But it's **opt-in tribal knowledge** — nothing stops a normal-looking `try/catch` from
bricking a run. Wishlist, in order of preference:

- Make a swallowed suspend **impossible**: the control signal is a branded throwable the executor
  re-raises even if caught (a `finally`-based or generator-based suspend rather than a plain `throw`),
  so `ctx.sleep` inside a `try` is simply safe.
- Failing that, a **dev-mode assertion / lint**: warn when a `ctx.*` call sits inside a `try` whose
  `catch` doesn't re-throw control signals.
- At minimum, put this at the top of the replay-semantics docs with this exact example.

### 2. No deterministic-vs-transient failure classification

`UpdateChannel` with an invalid patch returns a 4xx that will fail **identically** on every retry —
but the run retried it to `maxAttempts` (10×) before failing, minutes of wasted re-drives with the
channel parked. The engine retries anything that isn't `StepFailedError`; it can't tell a permanent
4xx from a transient 429/5xx.

I worked around it by classifying `httpStatusCode` (4xx except 409/429) and re-throwing as
`StepFailedError`. Wish: first-class support —

```ts
ctx.step("update", fn, { retryable: (err, attempt) => isTransient(err) })
// or a well-known `PermanentError` the executor never retries (symmetry with StepFailedError).
```

### 3. A poisoned run has no recovery path

The cursor-divergence from #1 never self-heals. `park-for-redeploy` only helps a **version bump** (new
code shape), not a genuine cursor mismatch — the run just sits parked / retrying until it dies at
`maxAttempts`. There's no "reset to last-good cursor" or "heal" primitive; the operator's only lever is
`cancel`. For a control plane that deploys often, a supported heal/repair path (or at least a documented
"cancel + resubmit with the same idempotency key is the recovery") would help.

### 4. Lease with no heartbeat is a fragile tuning band

`leaseMs` must **exceed the longest single step's wall-clock** AND be **≤ the tick Lambda timeout** — or
a killed invocation strands the run until the oversized lease expires, or a slow step gets its lease
stolen and double-executes. That's a narrow band to hit by hand, and it silently couples two unrelated
numbers (step latency, Lambda timeout). Wish: **checkpoint-based lease renewal** — extend the lease on
each committed step — so long steps are safe instead of banned by convention. (We kept every step to one
quick AWS call and pushed all waiting into `ctx.sleep`, which is the right shape — but the lib made us
discover that rule the hard way.)

### 5. Opaque failures in `SweepResult`

The tick returns `results: ["flow_drift", "failed"]` — no run id, no error, no cursor. Diagnosing meant
querying the `RUN#` partition and reading `STEP#s0…sN` items by hand to reconstruct the chain. Wish: each
`TickResult` carries `{ runId, status, error?, cursorKey? }` so a driver can log *why* a run drifted/failed
without touching the store.

### 6. IAM: `grantReadWriteData` omits the atomic-write actions

The checkpoint uses `TransactWriteItems` + `ConditionCheckItem`, which CDK's `grantReadWriteData` does
**not** include. Atomic writes fail with an opaque AccessDenied until you grant `REQUIRED_IAM_ACTIONS`
explicitly. It's exported and documented — but the default-grant trap catches you first. Worth a loud
callout in the dynamodb README right next to the table spec.

### 7. Run ordering is per-instance (see the standalone note)

`nextSeq` is a module-level counter that resets to `0` on every Lambda cold start, so `listRuns` order is
arbitrary across instances. We re-sort each page by `createdAt` client-side as an interim.
→ [time-ordered-run-index.md](./time-ordered-run-index.md).

## The serverless execution model I'd want

Today's serverless story is "one bounded `serverlessTick` per invocation; an external cron wakes the
next." That's cron-driven — we pay a tick every minute forever, and a `ctx.sleep(15s)` still advances at
the 1-minute cron floor. The model I'd want is **event-driven — cost scales with pending work, not
wall-clock**:

- **submit / signal → push a wake** (invoke the tick once) → starts in ~1s. We hand-rolled this as a
  fire-and-forget `kickTick`; it should be a first-class driver concept, not per-deployer glue.
- **timer (sleep / retry) → the tick returns `nextWakeAt`** → the driver arms a one-shot (EventBridge
  Scheduler / SQS `DelaySeconds` / Step Functions `Wait`) and resumes *exactly* then, at any granularity.
  → [self-scheduling-serverless-tick.md](./self-scheduling-serverless-tick.md).
- **nothing pending → the process dies.** Only a low-frequency heartbeat remains as the reconcile/orphan
  safety net.
- All wrapped in a **`ServerlessDriver` abstraction with per-platform adapters** (EventBridge one-shot,
  SQS delay, …), so a deployer supplies "here's my one-shot scheduler" and gets the whole loop — instead
  of `kickTick` + a fixed `rate(1 min)` rule assembled by hand.

Net: idle deployments pay ~nothing, new work starts sub-second, timers resume precisely — versus today's
always-on per-minute tick that still eats cron latency on sleeps.

## Priority for a control-plane user

If only some of this ships, this is the order that would have saved us the most pain:

1. **#1** (suspend can't be swallowed) — it's a data-corruption footgun, not a UX nit.
2. **`nextWakeAt`** + push-on-submit — turns the whole cost/latency model around.
3. **#2** (permanent-error classification) — trivial to add, saves every AWS-mutating flow from
   reinventing fail-fast.
4. **time-ordered index** — small backend change, fixes every list consumer.
5. **#4 / #5** (lease renewal, structured tick results) — quality-of-life, unblock longer steps and
   real observability.

## Resolution — all 8 shipped (alpha.3, unreleased)

Every ranked edge landed, each conformance-backed and `/simplify`'d:

| Item | What shipped | Where |
| --- | --- | --- |
| #1 | A `try/catch` around `ctx.*` is now safe — a swallowed suspend re-propagates at the next `ctx.*` (and post-return), so it can't brick a run. This also removed the main permanent-drift cause. | `context.ts` `arm`/`suspend` holder, `executor.ts` |
| #2 | `StepPolicy.classify(error, attempt) → "transient" \| "permanent"` for first-class fail-fast. | `context.ts`, core README |
| #3 | Recovery is composing existing levers (#1 killed the poison source), documented lever-by-scenario. | `docs/v2/RECOVERY.md` |
| #4 | Checkpoint-based lease renewal — the executor renews on committed steps (and fan-out chunks), half-consumed guard. | `executor.ts` `renewLease`, `context.ts` `onStepCommit` |
| #5 | `TickResult { runId, status, error?, cursorKey? }` — failed/retrying/drifted ticks carry the error. | `executor.ts`, `worker.ts` `SweepResult` |
| #6 | IAM `> [!WARNING]` — `grantReadWriteData` omits the transactional actions; grant `REQUIRED_IAM_ACTIONS`. | `dynamodb/README.md` |
| #7 | Time-ordered run index — `gsi2sk = createdAt#seq`, opaque cursor. | `dynamodb/{statements,store,schema}.ts` |
| nextWakeAt | `serverlessTick` returns `nextWakeAt`; `Timer.nextDueAt` across all 8 backends → self-scheduling one-shot. | `worker.ts`, `ports/timer.ts`, every backend |
