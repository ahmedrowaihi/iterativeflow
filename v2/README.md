# iterativeflow v2

[![core](https://img.shields.io/npm/v/@iterativeflow/core?label=%40iterativeflow%2Fcore&labelColor=171717&color=FF570A)](https://www.npmjs.com/package/@iterativeflow/core)
[![license](https://img.shields.io/npm/l/@iterativeflow/core?labelColor=171717&color=FF570A)](../LICENSE)

Durable, backend-agnostic workflows for TypeScript.

Write a flow as an ordinary async function; it survives process crashes, retries failed steps, sleeps
for days, and resumes deterministically by replaying memoized steps. The same engine runs behind a
four-port `Backend` interface — **Postgres, SQLite, MySQL, MongoDB, Redis, DynamoDB, Cloudflare
Durable Objects, or in-memory** — resident or serverless. Published under the `@iterativeflow/*@2.0.0-alpha` scope.

> This is the v2 rewrite. The v1 API (`flow().step()` on graphile-worker) is unchanged and still
> shipped as [`iterativeflow`](../README.md).

```ts
import { createEngine, defineFlow } from "@iterativeflow/core";
import { createPgBackend, pgPool } from "@iterativeflow/postgres";
import { Pool } from "pg";

const onboard = defineFlow<{ userId: string }, { score: number }>({
  name: "onboard",
  version: 1,
  run: async (ctx, input) => {
    await ctx.step("create-account", () => createAccount(input.userId));
    await ctx.sleep(3 * 24 * 60 * 60_000); // 3 days, durable
    const survey = await ctx.signal<{ score: number }>("survey");
    return { score: survey.score };
  },
});

const engine = createEngine(createPgBackend(pgPool(new Pool())), [onboard]);
const stop = engine.run(); // resident worker loop; returns a stop fn

const handle = await engine.submit(onboard, { userId: "u_1" });
// 3 days later, from a webhook:
await engine.signal(handle, "survey", { score: 9 });
const { output } = await engine.result(handle); // { score: 9 }
await stop();
```

That run lives in your backend for three days. Workers can crash, deploys can roll, the process can be
killed and restarted — when the timer fires, the flow resumes from where it left off, replaying the
memoized `create-account` step instead of re-running it.

- **Steps** memoized by `(runId, cursor)` — `ctx.step(label, fn)`, the unit of at-least-once execution
- **Sleeps** and external **signals** lasting days — `ctx.sleep(ms)` / `ctx.signal(name)`
- **`ctx.invoke(child, input)`** for child flows, and `ctx.invoke([…])` for parallel fan-out + join
- **Typed contracts** — `submit` returns a `RunHandle<Output, Signals>`; outputs and signal payloads
  are typed end to end
- **At-least-once** via a transactional outbox committed with each step; a reconciler re-drives
  anything stranded by a crash
- **Serverless or resident** — `engine.run()` for a worker loop, or `serverlessTick` for one bounded
  cycle per Lambda/Vercel/Cron invocation
- **Structured concurrency** — a run that terminates without success cancels its descendants

## Packages

| Package                                                      | What it is                                                                                                    |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| [`@iterativeflow/core`](packages/core)                       | The engine — `defineFlow`, `ctx`, `createEngine`, replay, outbox. Pair it with one backend.                   |
| [`@iterativeflow/memory`](packages/memory)                   | In-memory backend — tests, dev, single process.                                                               |
| [`@iterativeflow/postgres`](packages/postgres)               | Postgres backend — transactional outbox, optional `LISTEN/NOTIFY` push, serverless-friendly. **The default.** |
| [`@iterativeflow/sqlite`](packages/sqlite)                   | SQLite backend (`@libsql/client`) — embedded, Turso, or a single-node service.                                |
| [`@iterativeflow/mysql`](packages/mysql)                     | MySQL/InnoDB backend — `FOR UPDATE SKIP LOCKED` claims.                                                       |
| [`@iterativeflow/mongodb`](packages/mongodb)                 | MongoDB backend — multi-document transactional outbox (replica set required).                                 |
| [`@iterativeflow/redis`](packages/redis)                     | Redis backend — Lua-scripted outbox, single-node.                                                             |
| [`@iterativeflow/dynamodb`](packages/dynamodb)               | DynamoDB backend — single-table, `TransactWriteItems` outbox, serverless AWS.                                 |
| [`@iterativeflow/durable-objects`](packages/durable-objects) | Run the engine **inside a Cloudflare Durable Object** on its built-in SQLite — no external DB.                |
| [`@iterativeflow/webhooks`](packages/webhooks)               | Inbound edge — verify a signed provider webhook and deliver it as a durable signal a parked flow `await`s.    |
| [`@iterativeflow/dashboard`](packages/dashboard)             | Dependency-free ops UI (runs list, detail, cancel/retry/signal) as a fetch handler.                           |
| [`@iterativeflow/conformance`](packages/conformance)         | The shared suites every backend must pass — the executable definition of a correct backend.                   |

Every backend implements the same four ports and passes the same nine conformance suites, so swapping
one for another changes only the `create*Backend(...)` call.

### Which backend?

- **`memory`** — tests, examples, a single-process app.
- **`postgres`** — the default: strong consistency, push completion via `LISTEN/NOTIFY`, works on
  serverless/pooled Postgres.
- **`sqlite`** — embedded or edge, one file or Turso/libsql; zero server.
- **`durable-objects`** — the edge, strongly consistent per object, no external database.
- **`redis`** — low-latency, single-node (Valkey/Dragonfly work too).
- **`mysql` / `mongodb` / `dynamodb`** — when that store is already your stack.

## Install & quick start

```bash
npm install @iterativeflow/core @iterativeflow/memory
```

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
const stop = engine.run();
const res = await engine.result(handle, { timeoutMs: 5_000 });
await stop();
// res.output === 42
```

Swap `createMemoryBackend()` for `createPgBackend(...)`, `createSqliteBackend(...)`, etc. — nothing
else changes. Each backend's README covers its connection setup and `applySchema`.

## The flow context

Inside `run`, `ctx` is the durable surface; every call is a checkpoint:

- `ctx.step(label, fn)` — run `fn` once; memoize its result and replay it on every later attempt.
- `ctx.sleep(ms)` — suspend and resume after a durable timer.
- `ctx.signal(name)` — park until an external `engine.signal(runId, name, payload)` (or
  [`@iterativeflow/webhooks`](packages/webhooks)) delivers a typed payload.
- `ctx.invoke(flow, input)` — run a child flow and await its output; pass an array to fan out in
  parallel and join outputs in order.

Replay assumes the body is stable: each step memo records a shape fingerprint, and a redeploy that
reorders the body triggers the flow's `driftPolicy` (park or fail) instead of running the wrong step.
Keep step order and labels stable across deploys; bump `version` when the body changes meaningfully.

### Builder (fluent, typed)

Prefer a chain? `builder` compiles to the same `ctx.step` — each `.step` result is added to a typed
accumulator (`acc.account`, `acc.survey`) that later steps and the output projection can read. Sleeps,
signals, and invokes happen through `ctx` inside a step (there are no separate chain nodes for them):

```ts
import { builder } from "@iterativeflow/core";

const onboard = builder<{ userId: string }>("onboard", 1)
  .step("account", (acc) => createAccount(acc.input.userId))
  .step("survey", async (_acc, ctx) => {
    await ctx.sleep(3 * 24 * 60 * 60_000); // 3 days
    return ctx.signal<{ score: number }>("survey");
  })
  .output((acc) => ({ score: acc.survey.score }));
```

## Docs

- [ARCHITECTURE](../docs/v2/ARCHITECTURE.md) — the four ports + transactional outbox
- [CONTRACTS](../docs/v2/CONTRACTS.md) — typed flows & signals
- [MIGRATION](../docs/v2/MIGRATION.md) — schema ownership, resident vs. serverless
- [PARITY](../docs/v2/PARITY.md) — v1 → v2 feature parity

## License

MIT
