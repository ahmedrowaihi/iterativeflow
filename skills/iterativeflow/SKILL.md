---
name: iterativeflow
description: >-
  Write and operate durable, resumable workflows with the `iterativeflow` npm
  library (durable execution on your own Postgres via graphile-worker +
  drizzle-orm). Use when building or debugging multi-step processes that must
  survive crashes, suspend for hours/days, retry steps, wait on external signals
  (webhooks / human approval), sleep, or fan out to child flows. Triggers:
  `flow(...)`, `createEngine`, `ctx.step`, `ctx.signal`, `ctx.sleep`,
  `ctx.invoke`, `engine.signal`, `defineFlow`, `defineContract`, "durable
  workflow", "resume after crash", "saga", "long-running job", or any code
  importing `iterativeflow`.
---

# iterativeflow

Durable, iterative flows on your own Postgres. Same idea as Temporal /
Trigger.dev (write a flow as code, suspend for hours or days, survive crashes)
but it runs **inside your Node app** on [graphile-worker](https://worker.graphile.org)

- [drizzle-orm](https://orm.drizzle.team). No separate service. Node ≥ 20.

A flow is a **linear chain** of nodes (`.step` → `.sleep` → `.signal` → `.invoke`
→ `.output`, with optional `.loop`). A run of that flow lives in Postgres; the
worker fleet can crash, deploy, or be killed — when a timer fires or a signal
arrives, the run **replays from the top** and resumes from where it left off.

Schemas use [Standard Schema](https://standardschema.dev) — any compliant
validator works (zod, valibot, arktype, …).

## The one mental model that prevents every bug

**The flow body re-executes from the top on every resume.** Only `.step` results
are persisted. On replay each node looks itself up by **cursor key** (position +
declared name) and short-circuits if already recorded:

- a completed **step** returns its stored result — **the fn never runs again**
- a fired **sleep** / consumed **signal** is skipped
- everything **between** nodes (top-level glue code) **runs every single time**

So: **all side effects and all non-determinism must live inside a `.step()` fn.**
There is no other safe place. The node builder makes this structural — there is
no "between nodes" space to put work.

```
input ─► step('a') ─► step('b') ─► sleep ─► signal ─► step('c') ─► output
   I        I→A          A→B       B(pass)   B→P        P→C          C→O
```

Each step fn receives `{ input, signal, attempt }` and returns the next channel
value. `sleep` is transparent (channel unchanged). `signal` **replaces** the
channel with the delivered payload — or with `merge(input, payload)` if you pass
a merge fn (that's how you carry an earlier value past a signal).

## Setup (once per project)

```bash
npm install iterativeflow drizzle-orm graphile-worker pg
npx iterativeflow generate-schema           # writes ./iterativeflow-schema.ts
```

The generated schema file is typed against **your** drizzle-orm. Add it to
`drizzle.config.ts` (`schema: ["./db/your-schema.ts", "./iterativeflow-schema.ts"]`),
then install **both** schemas:

```bash
node -e "import('graphile-worker').then(m => m.migrate({ pgPool: pool }))"  # graphile_worker.*
npx drizzle-kit generate && npx drizzle-kit migrate                         # workflow.*
# or: psql -f node_modules/iterativeflow/migrations/0000_init.sql
```

Re-run `generate-schema` after upgrading iterativeflow. If you rename tables /
`pgSchema` in the generated file, pass `createEngine({ tables: flowTables })` —
otherwise the engine queries the default `workflow.*` and your renames break it.

## Hello flow

```ts
import { Pool } from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { createEngine, flow } from "iterativeflow";
import { z } from "zod";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const db = drizzle(pool);
const engine = createEngine({ db, pool });

const onboard = flow("onboard")
  .version(1)
  .input(z.object({ userId: z.string() }))
  .step("account", ({ input, signal }) => createAccount(input.userId, { signal }))
  .sleep("3d")
  .signal("survey", { schema: z.object({ score: z.number() }) })
  .output(({ input }) => ({ score: input.score }))
  .build();

const handle = engine.register(onboard); // BEFORE listen()
await engine.listen();

const { runId } = await handle.start({ userId: "u_1" });

// 3 days later, from a webhook:
const result = await engine.signal(runId, "survey", { score: 9 });
// result.kind: "delivered" | "buffered" | "duplicate" | "expired"

const out = await handle.result(runId); // resolves when the run is terminal
```

## API cheat-sheet

**Builder** — `flow(name | contract)` then chain, end with `.output(fn).build()`:
`.version(N)` · `.input(schema)` · `.step(name, fn, opts?)` · `.sleep(duration)` ·
`.signal(name, opts?, merge?)` · `.loop({ until }, sub => …)` · `.output(fn)` · `.build()`

**Inside a step / `defineFlow` body** (`FlowContext`):

- `ctx.step(name, fn, opts?)` — memoized side-effecting unit
- `ctx.sleep(duration)` — durable pause (`"3d"`, `"500ms"`, or ms number)
- `ctx.signal<T>(name, opts?)` — suspend until `engine.signal` delivers
- `ctx.invoke(handle, input, opts?)` — start a child flow, suspend until it terminates
- `ctx.log(msg, payload?)` · `ctx.runId` · `ctx.attempt`

**Engine** — `createEngine({ db, pool, … })`:
`register(def)→handle` · `enqueueHandle(contract)→handle` · `enqueue(name, version, input, opts?)` ·
`defineCron(spec)` · `listen()` · `stop()` · `attachShutdownSignals()` ·
`signal(runId, name, payload?)` · `cancel(runId, reason?)` · `retry(runId)` ·
`status(runId)` · `health()` · `listRuns({ tag, status, since })` ·
`pruneRuns({ olderThan })` · `pruneEvents({ olderThan })`

**Handle** — `start(input, opts?)→{runId, status}` · `output(runId)` ·
`result(runId, {timeoutMs?})` (blocks until terminal via LISTEN/NOTIFY) ·
`wait(runId, { until: { step } | { signal }, timeoutMs? })`

**`StartOpts`**: `idempotencyKey` · `priority` · `delay` · `tags`.
**`StepOpts`**: `retries` (default `0`) · `timeoutMs` · `baseBackoffMs` · `capBackoffMs` ·
`backoff` · `classify: (err) => "transient" | "permanent"`.
**`SignalOpts`**: `schema` · `timeout`.

## Load-bearing rules (read before writing any flow)

1. **Steps are memoized forever.** Once a step result is stored, the body never
   re-runs for that `runId`. A code change to a step body → resumed runs use the
   OLD result. **Bump `.version(N)` and register both versions** to get new code;
   old runs drain on the old version.
2. **Top-level glue re-runs on every resume.** Never put a side effect, an API
   call, a DB write, `Date.now()`, `Math.random()`, or `crypto.randomUUID()` at
   the top level — wrap it in `ctx.step("now", () => Date.now())` to memoize it.
3. **Honor `AbortSignal`.** Every step fn gets `{ input, signal, attempt }`. Pass
   `signal` to `fetch` / `undici` / `pg` / `openai`. A step that ignores it keeps
   running after timeout/cancel — the engine throws on time but the work doesn't
   stop. `engine.cancel(runId)` propagates the abort.
4. **Steps are at-least-once. Keep bodies idempotent.** A crash between the side
   effect and the `finishStep` commit re-runs the body. Use idempotency tokens
   on external calls; `INSERT … ON CONFLICT DO NOTHING` for writes.
5. **Steps don't retry by default (`retries: 0`).** Opt in per step. Set
   `timeoutMs` on every I/O step — a hung fn pins a worker slot forever.
6. **Keep step results small.** Every result is loaded into RAM on every replay.
   Store pointers (IDs, S3 keys), not blobs.
7. **`engine.signal(runId, name, payload)` is single-consumer, not pub/sub.**
   Each call delivers to one armed `ctx.signal` (or buffers for the first arm).
   1000 runs awaiting the same name → 1000 calls. Validate the payload in your
   webhook handler before calling — bad payloads currently fail the run.
8. **`merge` / `output` fns must be pure** — they re-run on replay.
9. **No `ctx.sleep` / `ctx.signal` / `ctx.invoke` inside a step fn** → throws
   `STEP_INVALID_AWAIT`. They are top-level only.
10. **Idempotency keys are scoped to `(name, version, key)`.** Same key → same
    `runId` returned. Bumping `.version(N)` lets the same key start a fresh run.
    Prefix for multi-tenant: `idempotencyKey: \`\${tenantId}:\${requestId}\``.
11. **The pool is yours.** `engine.stop()` does NOT call `pool.end()`. In
    shutdown: `engine.stop()` then `pool.end()`.
12. **Error `code` is stable across patches; `message` is not.** Alert on `code`,
    log the `message`. Throw `new FlowRuntimeError({ code, message, nonRetryable: true })`
    to skip retries on a permanent failure.

## Defaults that surprise people

`worker.concurrency` = `5` · `limits.defaultStepTimeoutMs` = `undefined` (can hang
forever) · `limits.*Bytes` = `undefined` (no payload cap) · `retention` = **off**
(tables grow forever — opt in) · cron `timezone` = `UTC` ·
`limits.maxRunAttempts` = `100` (poison pills die with `RUN_ATTEMPTS_EXHAUSTED`) ·
`limits.maxInvokeDepth` = `10` · `limits.maxChildrenPerRun` = `1000`.

## Lifecycle ordering (hard constraints)

- Install `graphile_worker` schema **before** `engine.listen()`.
- `register` all flows and `defineCron` all specs **before** `listen()` — the
  task list is fixed at `listen()`; late calls throw.
- A process **claims only the flows it registered.** Split workers by registering
  a subset; an enqueue-only API registers flows for typed `.start` handles but
  never calls `listen()`.
- Pool size ≥ `concurrency + handles awaiting result() + reconciler headroom`.

## Versioning decision rule

- Logic-only fix **inside** a step's fn → **keep the version**.
- Shape change (add / remove / rename / reorder a node, loop count) → **bump
  `.version(N)`, register both**. Editing the graph in place makes resumed runs
  fail loudly: `REPLAY_INCOMPATIBLE_VERSION` (recorded step gone) or
  `REPLAY_NON_DETERMINISTIC` (same-name step occurrence count shifted). Never
  silent corruption.

## Builder vs `defineFlow`

Prefer the **builder** — its replay-compat guard catches shape drift (including
renames inside `.loop` bodies). Drop to **`defineFlow({ name, version, body })`**
only for shapes the builder can't express (dynamic step names, computed graphs,
`while(true)` chat loops). You get full JS control flow and the same
`ctx.step/sleep/signal/invoke`, but **no compat guard** — you own determinism.

## Enqueue-only processes (flow contracts)

An API that only _starts_ flows shouldn't import the flow body and its heavy
deps. Split a light **contract** from the heavy **implementation**:

```ts
// contract.ts — light, imported by the API
export const cloneContract = defineContract<{ mediaId: string }, { status: "done" }>({
  name: "clone-media",
  version: 1,
  input: z.object({ mediaId: z.string() }),
});
// flow.ts — heavy, lives with the worker
export const cloneFlow = flow(cloneContract)
  .step("copy", ({ input }) => copyWithNodeAv(input.mediaId))
  .output(() => ({ status: "done" as const }))
  .build();

await engine.enqueueHandle(cloneContract).start({ mediaId }); // API: typed, no listen()
engine.register(cloneFlow);
await engine.listen(); // worker: claims the run
```

Both sides agree on `clone-media@1` and its I/O via the shared contract — a
mismatch is a compile error. `engine.enqueue(name, version, input)` is the
untyped escape hatch.

## Failure-mode reference

| Symptom                                                    | Cause                                         | Fix                                          |
| ---------------------------------------------------------- | --------------------------------------------- | -------------------------------------------- |
| Run stuck `pending` after `start()`                        | `graphile_worker` schema missing or no worker | `migrate({ pgPool })`; call `listen()`       |
| `FLOW_UNKNOWN` on resume                                   | replica missing the `name@version`            | register on every replica                    |
| `REPLAY_INCOMPATIBLE_VERSION` / `REPLAY_NON_DETERMINISTIC` | graph edited without bumping version          | restore the node OR publish v2 alongside     |
| `STEP_INVALID_AWAIT`                                       | `ctx.sleep/signal/invoke` inside a step fn    | move to top-level                            |
| `SIGNAL_TIMEOUT`                                           | signal `timeout` elapsed                      | operator decision; engine did its job        |
| `SIGNAL_PAYLOAD_INVALID`                                   | stored payload fails current schema           | widen schema or validate at webhook          |
| `SCHEMA_MISMATCH`                                          | DB schema ≠ engine's expected version         | `drizzle-kit generate && migrate`            |
| `INVOKE_DEPTH_EXCEEDED` / `INVOKE_FANOUT_EXCEEDED`         | invoke tree exceeded caps                     | restructure or raise `limits.*`              |
| Worker slot stuck forever                                  | step fn hung without `timeoutMs`              | set `StepOpts.timeoutMs`                     |
| `events` table growing unbounded                           | no retention                                  | set `EngineOpts.retention` or a pruning cron |

## What you DON'T get

Exactly-once (it's at-least-once) · automatic whole-run retry (failed = terminal)
· branching combinators in the builder (linear chains; branch via a step return
or `defineFlow`) · per-flow concurrency limits (one global `concurrency`).

## Deeper references (in the package repo)

`docs/guide.md` (lifecycle, durability, reconciler, full `EngineOpts` reference) ·
`docs/replay-semantics.md` (cursor keys, memoization, non-determinism traps) ·
`docs/signals.md` (delivery kinds, single-consumer semantics) ·
`docs/adr/` (per-flow task routing, enqueue-only handles) · `CONTEXT.md` (glossary).
