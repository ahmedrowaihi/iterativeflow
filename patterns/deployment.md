# Deployment

How to run iterativeflow in production: how workers run, what databases and connection pools it
works with, and what to watch for when you run more than one worker.

## How workers run

There are two ways to run a worker.

- **Resident loop** (`engine.run()`): a long-running process that keeps claiming and running work. It
  also runs `reconcile` and due crons every few seconds (`maintenanceMs`, default 5s). Use it for
  long-lived workers on VMs, containers, or Kubernetes.
- **One-shot** (`serverlessTick()`): does a single pass per call — run due crons, reconcile, fire due
  timers, then claim and run a batch — and returns `nextWakeAt`, the next time work is due. Use it for
  Lambda, Cloud Functions, or a cron-triggered container.

A run started under one can finish under the other.

## Databases and connection pools

The SQL backends work behind connection poolers and on serverless databases.

- **No prepared statements.** Postgres uses `$1` params, MySQL uses `?`. Nothing is pinned to a
  connection, so transaction-mode pooling (RDS Proxy, PgBouncer, PlanetScale) works.
- **A connection is held only for one step's write**, not for the whole step. A low connection limit
  (Aurora Serverless v2, Neon) is fine.
- **Completion is poll-based.** When a run finishes it nudges anything waiting on it, but a missed
  nudge just costs one extra poll; it never affects correctness. Postgres `LISTEN/NOTIFY` is optional
  and holds a dedicated connection, so don't turn it on behind a transaction pooler — the polling
  default is enough there.
- **`pollTimeoutMs`** (default 30s) caps how long a claim poll waits, so a suspended or stalled
  connection is dropped and retried instead of freezing the loop. It bounds the poll only, not step
  execution; a hung write is recovered by the driver's own timeout and by lease expiry.

| Database setup                                           | Works       | Notes                                                                                                                                                                                     |
| -------------------------------------------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Postgres behind RDS Proxy / PgBouncer (transaction mode) | Yes         | Keep `LISTEN/NOTIFY` off this path.                                                                                                                                                       |
| Neon / Aurora Serverless v2                              | Yes         | Cold starts are absorbed by `pollTimeoutMs`. A Neon HTTP-driver adapter would fit the `Sql` interface but isn't included.                                                                 |
| MySQL behind a pooler (PlanetScale)                      | Yes         | Isolation is set per transaction, so a pooler that swaps connections can't drop it.                                                                                                       |
| Redis                                                    | Socket only | Uses `ioredis` (pipelines, Lua). A REST Redis such as Upstash REST isn't supported.                                                                                                       |
| DynamoDB (on-demand)                                     | Yes         | Retries come from the AWS SDK; there's no extra backoff, and claim/depth scan the partition, so watch throttling under heavy load.                                                        |
| MongoDB Atlas                                            | Yes         | Needs a replica set.                                                                                                                                                                      |
| Cloudflare Durable Objects                               | Yes         | One request at a time. If a write throws mid-way and is caught, the all-or-nothing guarantee is weaker than on the SQL backends; an uncaught error still rolls back the whole invocation. |

## Clocks and leases

Lease expiry and timer/cron due times use each worker's own clock, not the database's. With more than
one worker this matters:

- **Keep the workers' clocks in sync (NTP).** If one worker's clock runs ahead, it can decide a peer's
  lease has expired and re-claim a run that's still going. The run isn't corrupted — a step runs at
  most once because of its memo — but the re-run work is wasted.
- **Set `leaseMs` above your longest step plus the skew you expect.** There's no heartbeat inside a
  single step, so the lease has to cover the whole step.
- A single worker, or `serverlessTick`, has no peer, so none of this applies.

If you run a large fleet you can't keep in sync, you'd want lease times to come from the database
instead of the worker. That isn't built yet (it would change the clock model that keeps the engine
testable and able to run in the browser). Open an issue if you need it.

## Scaling to zero

The library gives you the numbers; you wire up the scaling.

- `engine.pendingWork(names?)` returns one number: claimable jobs plus due timers plus due crons. It's
  served at `GET /api/metrics` (with an optional `?name=` filter). Point a KEDA `metrics-api` scaler at
  it, or point KEDA's Postgres scaler at the `pending_work(flow_names, as_of)` SQL function. Counting
  due timers and crons, not just queued jobs, is what lets a worker scaled to zero wake for a
  `ctx.sleep` or a cron.
- `engine.nextWakeAt()` (also on the `serverlessTick` result) returns the next time work is due. Use it
  to schedule a single wake-up — SQS `DelaySeconds`, EventBridge, a Step Functions `Wait` — instead of
  polling on a timer. Signals and child completions wake a run directly, so they aren't part of this.

The scaling setup itself (SQS, EventBridge, KEDA) is yours; it depends on your platform.

## Running many workers

- **Shard by flow name.** A worker only claims runs for the flows it registered (`ClaimOpts.names`,
  taken from its registry). Run different sets of flows on different workers against one database and
  each picks up only its own. If a worker sees a run for a flow it has by name but not at that version,
  it leases the run and parks it until the right version deploys (the rolling-deploy handoff).

  MySQL note: the flow name lives on the `run` table, which the claim joins in. MySQL locks the
  eligible `job` rows before applying the name filter, so the first worker to claim briefly locks rows
  another shard wants; they turn up on that shard's next poll. Nothing is claimed twice or by the wrong
  shard, but a sharded MySQL pool sees more claim contention than Postgres, which only locks the rows
  that match the name.

- **One database per instance.** There's no built-in way to split runs across databases or tenants; one
  backend is one schema on one database (the Postgres `schema` option just picks the table namespace).
  To go past one database, run separate engine instances, each with its own — one per shard, or one
  Durable Object per id. Many workers, including a mix of Go and TypeScript, can share a single Postgres.

## Rolling deploys and shutdown

During a rolling deploy — some pods on the old version, some on the new — the engine's job is to lose
nothing. It does, if you follow a few rules.

- **Shut down gracefully.** Wire `SIGTERM` to the stop function `engine.run()` returns. It stops
  claiming new work and waits for the in-flight batch to reach its next durable checkpoint, then
  returns. Set the pod's `terminationGracePeriodSeconds` above your longest step so Kubernetes doesn't
  `SIGKILL` a worker mid-step.
- **A hard kill is safe, just slower.** If a pod is killed mid-run, its lease is never released, so
  another pod reclaims the run after `leaseMs` and replays it from the last checkpoint. Nothing is lost,
  but the run stalls up to `leaseMs` and the interrupted step runs again — keep step side effects
  idempotent.
- **Bump the flow version for any change to a flow's structure, and keep the old version deployed until
  its runs drain.** A worker that claims a run for a version it doesn't have parks the run and waits — it
  never fails or double-runs it. But if you remove the old version while old runs are in flight, those
  runs park until that version is back (no loss, just stuck). Watch the `redeployParked` metric during a
  rollout: a steady stream means runs are waiting for a version that didn't return.
- **Fail fast on a bad config.** Call `engine.check()` at startup (from a readiness probe). It throws a
  clear error if the schema is missing or the database is unreachable, instead of the worker loop quietly
  retrying query errors.
- **Schema changes are additive.** `applySchema` only adds tables and columns, never drops or renames, so
  an old pod keeps working against the new schema during the rollout, and an app rollback needs no schema
  rollback.

Size `leaseMs` above your longest step plus the clock skew you expect — see [Clocks and
leases](#clocks-and-leases).
