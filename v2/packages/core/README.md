# @iterativeflow/core

Durable workflow engine for TypeScript. Write a flow as an ordinary async
function; it survives process crashes, retries failed steps, sleeps for days,
and resumes deterministically by replaying memoized steps. Backend-agnostic —
run it on Postgres, DynamoDB, or in-memory behind one interface.

Part of [iterativeflow](https://github.com/ahmedrowaihi/iterativeflow) v2 (`2.0.0-alpha.2`).

```bash
npm install @iterativeflow/core @iterativeflow/memory
```

`core` is the engine; you pair it with a backend package
([`@iterativeflow/memory`](../memory), [`@iterativeflow/postgres`](../postgres),
or [`@iterativeflow/dynamodb`](../dynamodb)).

## Quick start

```ts
import { createEngine, defineFlow } from "@iterativeflow/core";
import { createMemoryBackend } from "@iterativeflow/memory";

const double = defineFlow<{ x: number }, number>({
  name: "double",
  version: 1,
  run: async (ctx, input) => ctx.step("d", () => input.x * 2),
});

const engine = createEngine(createMemoryBackend(), [double]);

const handle = await engine.submit(double, { x: 21 });
const stop = engine.run(); // resident worker loop; returns a stop fn
const res = await engine.result(handle, { timeoutMs: 5_000 });
await stop();
// res.output === 42
```

`submit` returns a `RunHandle` (a branded run id) you pass straight to
`result`, `status`, or `signal`.

Swap `createMemoryBackend()` for `createPgBackend(...)` or
`createDynamoBackend(...)` — nothing else changes.

## The flow context

Inside `run`, `ctx` is the durable surface. Every call is a checkpoint:

- `ctx.step(label, fn)` — run `fn` once; its result is memoized and replayed on
  every subsequent attempt. The unit of at-least-once execution.
- `ctx.sleep(ms)` — suspend and resume after a durable timer.
- `ctx.signal(name)` — park until an external `signalRun` delivers a typed payload.
- `ctx.signal(name, { timeoutMs })` — the same wait with a deadline; resolves
  `{ received: true, payload }` if it arrives in time, else `{ received: false }`.
  A signal delivered before the timeout commits always wins (linearizable with the
  durable inbox), so a late-arriving signal is never silently dropped.
- `ctx.invoke(flow, input)` — run a child flow and await its output.
- `ctx.invoke([{ flow, input }, …])` — fan out to many children in parallel and
  join their outputs in order. A child failure fast-fails the parent and cancels
  the running siblings.

Child flows form a tree: when a run terminates without success, cancellation
cascades to its descendants (structured concurrency).

### Step policy — retries, timeout, fail-fast

`ctx.step(label, fn, policy)` takes an optional `StepPolicy`:

```ts
await ctx.step("charge", chargeCard, {
  retries: 3, // in-invocation retries before the durable run-level retry
  retryDelayMs: 200,
  timeoutMs: 30_000, // abort fn (via its AbortSignal) if it runs longer
  // Fail fast on permanent errors instead of burning the retry budget: a `permanent`
  // verdict fails the step (and run) immediately; `transient` retries as configured.
  classify: (err, attempt) => (isHttp4xx(err) ? "permanent" : "transient"),
});
```

`classify` is how you make a 4xx/validation error stop retrying while a 5xx/timeout keeps retrying —
no need to hand-roll a wrapper that re-throws as a terminal error. For Postgres,
`@iterativeflow/postgres` ships a ready preset: `classify: pgClassify` fails fast on the deterministic
errors (bad data, bad SQL, not-null/check violations) and keeps retrying connection drops, statement
timeouts, deadlocks, serialization failures, and foreign-key/unique races.

When a step throws, the persisted `FlowError` captures `{ code, message, stack, cause }` — and `cause`
is the flattened `.cause` chain, so a driver that wraps the real error (e.g. a `DrizzleQueryError`
whose message is a generic `Failed query: rollback` with the pg detail on `.cause`) no longer loses
the actual failure.

> **A `try/catch` around `ctx.*` is safe.** `ctx.sleep`/`signal`/`invoke` suspend the run by
> _throwing_ a control signal; even if your `catch` swallows it, the engine re-propagates the suspend
> at the next `ctx.*` call (or when the body returns), so the run parks and resumes correctly. Wrap
> your real error handling however you like — you don't have to special-case control signals.

## Typed contracts

`submit` returns a `RunHandle<Output, Signals>`; `result` recovers the output
type; signals are typed on both the `defineFlow` declaration and the
`signalRun` call. See [docs/v2/CONTRACTS.md](../../../docs/v2/CONTRACTS.md).

## Determinism & drift

Replay assumes the flow body is stable. Each step memo records a shape
fingerprint; if a redeploy reorders or refactors the body, replay detects the
drift and applies the flow's `driftPolicy` (park or fail) instead of running the
wrong step. Keep step order and labels stable across deploys.

When a run does get stuck — a transient failure, a drift, or an un-resumable run
— [docs/v2/RECOVERY.md](../../../docs/v2/RECOVERY.md) is the lever-by-scenario
guide: `retry`, `cancel` + fresh submit, and the version-migration pattern.

## Serverless

Beyond the resident `engine.run()` loop, `serverlessTick` drives one bounded
claim+reconcile cycle per invocation for Lambda/Vercel/Cron.

Rather than a fixed cron cadence, **self-schedule**: each tick returns
`nextWakeAt` — the earliest pending timer (sleep / retry / cron) — so the driver
arms a one-shot for exactly then (EventBridge Scheduler / SQS `DelaySeconds` /
Step Functions `Wait`) and pays nothing while idle, resuming on time at any
granularity instead of at the cron floor:

```ts
const { nextWakeAt } = await engine.serverlessTick();
if (nextWakeAt) await scheduleOneShot(nextWakeAt);
// else: nothing pending — exit; a push on submit/signal starts the next cycle.
```

`engine.nextWakeAt()` exposes the horizon standalone. `nextWakeAt` covers timers
only; signal- and child-waits resume via a push when the event arrives. See
[docs/v2/MIGRATION.md](../../../docs/v2/MIGRATION.md).

## Backend authoring

To add a substrate, implement the four ports (store/queue/timer/wakeup) from
`@iterativeflow/core/backend` and pass the shared suites in
[`@iterativeflow/conformance`](../conformance).
