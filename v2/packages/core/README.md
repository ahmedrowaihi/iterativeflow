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
- `ctx.invoke(flow, input)` — run a child flow and await its output.
- `ctx.invoke([{ flow, input }, …])` — fan out to many children in parallel and
  join their outputs in order. A child failure fast-fails the parent and cancels
  the running siblings.

Child flows form a tree: when a run terminates without success, cancellation
cascades to its descendants (structured concurrency).

## Typed contracts

`submit` returns a `RunHandle<Output, Signals>`; `result` recovers the output
type; signals are typed on both the `defineFlow` declaration and the
`signalRun` call. See [docs/v2/CONTRACTS.md](../../../docs/v2/CONTRACTS.md).

## Determinism & drift

Replay assumes the flow body is stable. Each step memo records a shape
fingerprint; if a redeploy reorders or refactors the body, replay detects the
drift and applies the flow's `driftPolicy` (park or fail) instead of running the
wrong step. Keep step order and labels stable across deploys.

## Serverless

Beyond the resident `engine.run()` loop, `serverlessTick` drives one bounded
claim+reconcile cycle per invocation for Lambda/Vercel/Cron. See
[docs/v2/MIGRATION.md](../../../docs/v2/MIGRATION.md).

## Backend authoring

To add a substrate, implement the four ports (store/queue/timer/wakeup) from
`@iterativeflow/core/backend` and pass the shared suites in
[`@iterativeflow/conformance`](../conformance).
