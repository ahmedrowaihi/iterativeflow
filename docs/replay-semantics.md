# Replay semantics

How runs survive crashes, what re-executes when, and what doesn't. The model that makes durable workflows possible — and the few sharp edges it leaves behind.

## The core model

A flow body is a regular async TypeScript function. The engine wraps every call to `ctx.step` / `ctx.sleep` / `ctx.signal` / `ctx.invoke` so that:

1. **Reaching the call for the first time** persists a row (step result, timer, signal arm, child run) to Postgres.
2. **Crashing or suspending** before the call returns leaves that row in place.
3. **Resuming** re-executes the flow body from the top. Calls that already have a stored row short-circuit, returning the persisted value without running the underlying code.

This is what makes a `.sleep("3d")` actually work. The body throws a `FlowSuspend`, the engine schedules a wake, the worker is freed, and three days later a (possibly different) worker resumes by re-running the body — every `ctx.step` before the sleep returns its memoized result without re-executing, the sleep itself sees a fired timer and returns instantly, and execution continues past the sleep.

## Memoization

**Step results are memoized by `(runId, cursor_key)`.** Once a step's `status = 'ok'` row exists, the step body is **never re-executed for that run**. Even if you deploy new code, the resumed run uses the OLD result.

<!-- doc-check: skip — illustrative; assumes external `generateDraft`/`publish` -->

```ts
flow("publish")
  .step("draft", () => generateDraft()) // memoized after first success
  .sleep("1d")
  .step("ship", ({ input }) => publish(input))
  .output(({ input }) => input)
  .build();
```

If you redeploy `generateDraft()` while a run is sleeping, the resumed run uses the OLD draft. Bumping `.version(N)` forks the registry — new runs use the new code; in-flight runs keep using the version they started on.

**This is the most surprising semantic in the library.** If you need a step to re-run with new code, either:

- Bump the flow's `.version(N)` (new runs only — in-flight runs are unaffected)
- Cancel the in-flight runs and restart them (`engine.cancel` → `handle.start` with a fresh input)

## What re-executes on resume

The top of the flow body runs again on every wake. The engine doesn't keep a program counter; it re-runs and uses storage as the cache. Practical implications:

- `ctx.step("X", fn)` first occurrence: runs `fn`, stores `{ status: ok, result }`. Subsequent passes: short-circuits with the stored `result`.
- `ctx.sleep(duration)` first occurrence: creates a timer row, throws `FlowSuspend({ wakeAt })`. On wake: sees `firedAt` set, returns instantly.
- `ctx.signal(name)` first occurrence: arms or consumes the signal row, throws `FlowSuspend` if not delivered. On wake (after `engine.signal(...)` delivered it): sees `delivered: true`, returns payload.
- `ctx.invoke(handle, input)` first occurrence: creates the child run row, throws `FlowSuspend`. On wake (after child terminated): looks up child output, returns it.

**Code at the top level of the body — outside any `ctx.step` — runs on every resume.** A `console.log("starting")` at the top runs on every wake. A `Date.now()` produces a new value every wake. Any side effect at the top level happens multiple times.

The rule: **wrap anything with side effects or non-determinism in `ctx.step`.**

## Non-determinism traps

<!-- doc-check: skip — paired BAD/GOOD illustrative snippet, intentionally incomplete -->

```ts
// BAD — Date.now() changes on every resume
const def = flow("badge")
  .step("issue", ({ input }) => issue(input, Date.now()))
  ...

// GOOD — capture once in a step
const def = flow("badge")
  .step("now", () => Date.now())
  .step("issue", ({ input }) => issue(input))
  ...
```

Anything non-deterministic at the top level (or inside another step's body that happens to escape into the channel) makes the cursor produce different keys on different runs — which surfaces as `REPLAY_NON_DETERMINISTIC` or as silent inconsistency.

The cursor itself is deterministic — it counts occurrences of each base name. Different number of `ctx.step("fetch", ...)` calls across resumes = drift = compat-check failure on the next resume.

## Mutation traps

A step that mutates outer state is replay-unsafe:

<!-- doc-check: skip — illustrative BAD pattern; intentionally incomplete -->

```ts
let total = 0;                            // outer state

const def = flow("sum")
  .step("incr", () => { total += 1; })    // mutation only happens on FIRST run
  .sleep("1m")
  .step("read", () => total)              // resumed run: total is whatever it was
  ...
```

On the resumed run, `total` is whatever the new process's module-level value happens to be (probably `0`). The first step short-circuits via memoization, so the mutation doesn't re-fire. The "read" step sees `0`.

Return the value instead of mutating outer state:

<!-- doc-check: skip — illustrative GOOD pattern; intentionally incomplete -->

```ts
const def = flow("sum")
  .step("incr", () => 1)
  .sleep("1m")
  .step("read", ({ input }) => input)
  ...
```

## `AbortSignal` and step timeouts

Step functions receive `{ input, signal, attempt }`. The `signal` is wired to:

- `StepOpts.timeoutMs` (or `EngineOpts.limits.defaultStepTimeoutMs` as fallback)
- `engine.cancel(runId)` (which propagates through `cancelCascade` to any in-flight descendants)

<!-- doc-check: skip — partial builder chain -->

```ts
.step("fetch", async ({ signal }) => {
  const res = await fetch(url, { signal });   // honors the abort
  return res.json();
}, { timeoutMs: 30_000 })
```

If a step ignores `signal` (e.g. uses a fetch library that doesn't accept one), the engine still throws `step "name" exceeded timeoutMs=...` on time — but the underlying work continues, holding sockets, DB leases, file descriptors. Honor the signal.

## Error code stability

`FLOW_ERROR_CODES` is the stable contract. Error **messages** are advisory and may be refined between patch releases — they are NOT part of the SemVer contract. Treatment:

- **Alert / branch / persist on `error.code`.** Always stable.
- **Log `error.message`.** May change between versions; don't rely on its exact text for automation.

If you need a custom code from inside a step:

```ts
import { FlowRuntimeError } from "iterativeflow";

throw new FlowRuntimeError({
  code: "PAYMENT_DECLINED",
  message: "card was declined",
  nonRetryable: true,
});
```

User-defined codes are free-form strings. The engine doesn't validate them; they ride through `runs.error.code` and show up in `step_failed` / `failed` events.

## Determinism contract for parallel steps

`Promise.all([ctx.step("a", fnA), ctx.step("b", fnB)])` works because the cursor is incremented synchronously at the moment each `ctx.step` is called. Each branch gets a distinct key.

But: `Promise.all([ctx.step("X", fn), ctx.step("X", fn)])` is **order-dependent**. The first synchronous call to `ctx.step("X", ...)` claims key `X`; the second claims `X:1`. Which one wins depends on synchronous call order, NOT on which promise resolves first. As long as the workflow body itself is deterministic about ORDER of calls, the result is consistent.

The safe pattern: **unique names per parallel branch.**

<!-- doc-check: skip — inside-body snippet (no surrounding ctx / httpGet binding) -->

```ts
// SAFE
const [a, b] = await Promise.all([
  ctx.step("fetch-a", () => httpGet(urlA)),
  ctx.step("fetch-b", () => httpGet(urlB)),
]);

// UNSAFE-LOOKING but actually fine (cursor assigns X and X:1 in source order)
const [a, b] = await Promise.all([
  ctx.step("fetch", () => httpGet(urlA)),
  ctx.step("fetch", () => httpGet(urlB)),
]);
// Replay assigns the same keys in the same order; result memoization makes
// this stable as long as source order doesn't change between deploys.
```

Source code reorderings between deploys (e.g. swapping the two `ctx.step` calls) DO change which key each branch gets — same `REPLAY_NON_DETERMINISTIC` failure mode as any other shape change. Bump `.version(N)`.

## Compat checks: what's caught, what isn't

On every resume, the engine compares the persisted snapshot against the registered flow graph:

- ✅ Step / sleep / signal removed from the graph → `REPLAY_INCOMPATIBLE_VERSION`
- ✅ Step renamed → `REPLAY_INCOMPATIBLE_VERSION`
- ✅ Kind switched (e.g. step `"sleep"` → `ctx.sleep()`) → `REPLAY_INCOMPATIBLE_VERSION`
- ✅ Occurrence count for a base name shrunk → `REPLAY_NON_DETERMINISTIC`
- ✅ Rename or kind change inside a loop body → `REPLAY_INCOMPATIBLE_VERSION`
- ❌ Code inside a step body changed → NOT caught (memoized; old result used)
- ❌ Input schema tightened → NOT caught on resumes (only on `handle.start`); old runs keep their input

Bumping `.version(N)` is your single tool for "I changed something inside the body and need new runs to use the new code." The plan doc has more on the snapshot/replay contract.

## Recovery model

Three things can go wrong with an in-flight run:

1. **Worker crashes mid-step.** The step row is in `status: running`. The run row's `updated_at` stops advancing. The reconciler cron picks it up after `runningStuckMs` (default 10 min) and re-enqueues. On resume, the failed step re-runs (memo only fires for `status: ok` or `status: failed_terminal`).
2. **Worker crashes mid-suspend-write.** The transactional outbox (`storage.signalHook`, `claimRun`, the suspend handler) commits the state change + enqueue atomically. Either both happen or neither — the reconciler will catch the "neither" case.
3. **Worker dies between mark and notify.** The state is persisted; the in-process LISTEN waiters don't fire, but the row update IS visible. `handle.result()` falls back to row-polling when LISTEN is degraded.

The reconciler is the safety net. It scans for runs whose `updated_at` is past the grace window AND are in a resumable state (`pending` with no fire, `sleeping` with a due timer, `awaiting_signal` with a delivered signal, `running` past the stuck threshold). Anything stranded gets re-enqueued.

## What's NOT durable

- **In-memory state** between `ctx.step` calls — not persisted. Top-level variables get re-derived from `ctx.step` returns on every resume.
- **Closures over outer scope** — same. Capture into the channel via step returns.
- **Imports from modules with side effects** — module top-level code runs at process start, not on resume.
- **External system state** — if a step makes an HTTP call and stores `result: "ok"`, the engine knows the call succeeded. It does NOT know whether the external system actually did the right thing. That's your problem to design around (idempotency keys to the external API, dedup at the target, etc).

## See also

- [`docs/signals.md`](./signals.md) — signal lifecycle, single-consumer semantics, delivery results
- README's "Defaults you should know" section — operator-facing knobs and traps
