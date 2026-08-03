# Cloudflare Durable Objects

`@iterativeflow/durable-objects` runs the engine inside a Cloudflare Durable Object, on the DO's SQLite
storage. It's the same SQLite backend as [sqlite](./sqlite.md), adapted to `ctx.storage.sql`. Use it to
run durable workflows at the edge, one engine per Durable Object.

Each Durable Object is **single-writer** and handles one request at a time. That's a natural fit for the
engine — no lock contention — but it means one DO is one engine over one small database. Scale by
sharding work across many DOs (one per id), not by adding workers to one DO.

## Requirements

- A Durable Object with SQLite storage enabled (a `new_sqlite_classes` migration in `wrangler.toml`).
- No peer dependencies — it runs on the Workers runtime.

## Install

```bash
npm install @iterativeflow/durable-objects @iterativeflow/core
```

## Set up and run inside the DO

Apply the schema once (in the constructor or a migration), then drive the engine from a method. DO
storage manages its own durability, so the file-store PRAGMAs are skipped automatically.

```ts
// MyFlowObject.ts
import { createDurableObjectBackend, applySchema } from "@iterativeflow/durable-objects";
import { createEngine, defineFlow, serverlessTick, registry } from "@iterativeflow/core";

const greet = defineFlow({
  name: "greet",
  version: 1,
  run: async (ctx, input: { name: string }) => `hi ${input.name}`,
});

export class MyFlowObject {
  private backend;
  private engine;

  constructor(private ctx: DurableObjectState) {
    this.backend = createDurableObjectBackend(ctx.storage.sql);
    this.engine = createEngine(this.backend, [greet]);
    ctx.blockConcurrencyWhile(async () => {
      await applySchema(ctx.storage.sql); // once, before the first request
    });
  }

  async submit(name: string) {
    return this.engine.submit(greet, { name });
  }

  // Drive one pass on an alarm; use nextWakeAt to set the next alarm.
  async alarm() {
    const { nextWakeAt } = await serverlessTick(this.backend, registry([greet]), {
      batchMax: 16,
      leaseMs: 30_000,
    });
    if (nextWakeAt) await this.ctx.storage.setAlarm(nextWakeAt);
  }
}
```

Don't run a resident `engine.run()` loop in a DO — DOs are request/alarm-driven. Drive the engine with
`serverlessTick` from a DO alarm, and reschedule the alarm from `nextWakeAt`.

## Operating in production

### One DO, one engine

A Durable Object serializes its requests, so claims never contend — but a single DO is a single writer
over its own storage. Partition your workload across DOs by id (for example, one DO per tenant or per
workflow family). There is no cross-DO claiming; each DO owns its own runs.

### Atomicity note

The engine commits each step and its side effects together. On DO SQLite, if a write throws part-way
through and your code catches it, the all-or-nothing guarantee is weaker than on the server backends
(which use explicit transactions). An uncaught error still rolls back the whole DO invocation, which
covers the common case. Keep step side effects idempotent, as everywhere.

### Retention

Prune terminal runs on a schedule (from an alarm): `await engine.prune(7 * 24 * 60 * 60 * 1000)`.

### Health checks

`engine.health()` returns run counts per status; `engine.liveness()` returns the dispatch backlog and
oldest-claimable age. Expose them from a DO method if you want to scrape them.

## Troubleshooting

- **`no such table` on the first request.** `applySchema` didn't run before the request — call it in
  `blockConcurrencyWhile` in the constructor.
- **Work doesn't advance on its own.** A DO only runs when called or on an alarm. Set an alarm from
  `nextWakeAt` after each `serverlessTick`.

## Other backends

Postgres, MySQL, SQLite, MongoDB, DynamoDB, and Redis each have their own guide in this folder. This
backend shares the SQLite engine — see [sqlite](./sqlite.md). Cross-cutting topics live in
[deployment](../deployment.md).
