---
"@iterativeflow/core": minor
"@iterativeflow/dynamodb": minor
"@iterativeflow/postgres": minor
---

Consumer migration, schema-ownership, and type-safety work:

- **Typed flows & signals** (restores v1 per-flow type-safety, adds typed signals): `submit` returns
  a `RunHandle<O, S>` so `result` recovers the flow's output type `O` (was `unknown`), and a flow's
  `signals` map types both `ctx.signal(name)` on the await side and `signal(handle, name, payload)` on
  the send side — a wrong signal name or payload is a compile error on both ends. A `signals` entry is
  any **Standard-Schema** validator (zod/valibot/arktype), just like `input`: the payload is validated
  (and parsed) as the flow consumes it, and a bad one fails the run. `type<T>()` is the type-only
  escape hatch. `RunHandle` is a `string`, so plain-string `result`/`signal` and stored run ids keep
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
