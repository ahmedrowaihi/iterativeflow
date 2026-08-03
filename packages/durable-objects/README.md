# @iterativeflow/durable-objects

Run [iterativeflow](https://github.com/ahmedrowaihi/iterativeflow) **inside a Cloudflare Durable
Object**, on the DO's built-in SQLite storage. It's the `@iterativeflow/sqlite` backend driven through
a thin adapter over `ctx.storage.sql` — so a single DO becomes a self-contained, strongly-consistent
durable-execution engine at the edge, no external database.

```ts
import { createDurableObjectBackend, applySchema } from "@iterativeflow/durable-objects";
import { createEngine } from "@iterativeflow/core";

export class WorkflowDO {
  #engine;
  constructor(ctx: DurableObjectState) {
    ctx.blockConcurrencyWhile(() => applySchema(ctx.storage.sql));
    this.#engine = createEngine(createDurableObjectBackend(ctx.storage.sql), [
      /* your flows */
    ]);
  }
  // drive the engine from an alarm() handler or fetch()
}
```

## Notes

- **No dependencies beyond `@iterativeflow/core` + `@iterativeflow/sqlite`** — the `SqlStorage` type is
  structural, so you don't need `@cloudflare/workers-types` at build.
- **Consistency model.** `ctx.storage.sql` is synchronous and a DO serves one request at a time, so a
  Store method runs to completion without another interleaving — single-writer-safe by construction. A
  DO commits an invocation's writes on return and rolls them back on an **uncaught** throw. DO SQLite
  forbids a manual `BEGIN`/`COMMIT`, so the transactional-outbox's all-or-nothing guarantee under a
  _caught, mid-write_ storage error is weaker here than in the SQL backends' explicit transactions.
  In practice storage writes to a local DO don't fail mid-outbox, so the happy path is atomic — and
  the real DO's invocation rollback is _stronger_ than the equivalence used to test this. It passes
  the same nine conformance suites as every other backend (verified against Node's synchronous
  `node:sqlite`, which matches `ctx.storage.sql`'s shape); a `transactionSync` path to also cover a
  caught mid-outbox fault is a possible refinement.
- **Wakeup** is in-process (the DO itself); wire completion latency to an `alarm()` cadence.
- One DO = one engine instance. Shard workflows across DOs by id for horizontal scale.
