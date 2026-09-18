---
"@iterativeflow/core": major
---

Typed values are now checked values, and a step is a leaf. Breaking changes, with what to do:

- **A step body must not call `ctx`.** Nested calls were keyed under whichever step had started most
  recently, so steps run in `Promise.all` could replay against the wrong memo. Move durable waits
  into the flow body, and nested durable work into a child flow with `ctx.invoke`. A body that still
  calls `ctx` parks the run as drifted on replay. Runs in flight that already made nested calls will
  park the same way; let them finish before upgrading.
- **`signalType<T>()` is removed.** Declare each signal with a Standard Schema (zod, valibot, arktype…):
  `signals: { approve: z.object({ by: z.string() }) }`. The payload is validated when the flow consumes it.
- **`result`, `engine.result` and `settle` return `output: unknown`** unless you pass the flow's output
  schema: `engine.result(handle, { output: OrderSchema })`. With a schema, the output is validated
  and typed.
- **`builder()` and `FlowBuilder` are removed.** Write the same steps as a `defineFlow` body.
- **Dashboard:** signals are delivered with `POST /api/runs/:id/signals/:name`. The request body is the
  payload, and an optional `Idempotency-Key` header dedupes. `POST /api/runs/:id/signal` is gone.
- **Custom SQL drivers:** `Sql.query` on Postgres, MySQL and SQLite is no longer generic and returns
  `SqlRow[]`, which the backend decodes and checks column by column. MySQL and Postgres export
  `SqlParam`.
- **DynamoDB:** the client you pass must be a `DynamoDBDocumentClient` (its typed `send`), not any
  object with a `send` method.
- **Backend authors:** `StepOutcome.shape` is renamed `call`. `run.failed` events always carry
  `{ error }`, and `EventData` maps each event type to its data. `@iterativeflow/core/backend` adds
  `Json`, `durable`, and shared decoders for stored values (`decodeFlowError`, `decodeTags`,
  `decodeOneOf` with `CRON_OVERLAPS` / `STEP_STATUSES`).

Fixes:

- **Parallel branches no longer lose work.** In `Promise.all`, a branch still starts when a sibling
  suspends first, and the run wakes at the earliest sleep or deadline of any branch, not only the
  first to suspend.
- **In-memory backend:** a run's output now round-trips through JSON like on every other backend, so
  a `Date` comes back as a string in tests too.

On MongoDB, Redis and DynamoDB, steps saved before the upgrade have no `call` field, so the drift
check skips them. Steps saved after the upgrade are checked as before.
