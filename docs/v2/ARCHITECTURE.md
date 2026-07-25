# iterativeflow v2 — Architecture (north star)

> Status: **original design manifesto (pre-build), with concrete facts refreshed.** This captured
> the committed direction before v2 shipped; the design prose is kept as the historical north star,
> but the backend list, roadmap (§9), and once-open decisions have been updated to what shipped. For
> the current picture, the authoritative docs are the **[v2 README](../../v2/README.md)** (quick
> start + package table), [`PARITY.md`](./PARITY.md) (the v1→v2 parity ledger), and
> [`CONTRACTS.md`](./CONTRACTS.md) (typed flows/signals + the drift guard).
>
> What actually shipped vs. this manifesto: **eight backends**, not the three sketched here (memory,
> postgres, dynamodb, redis, sqlite, mysql, mongodb, durable-objects), plus `@iterativeflow/webhooks`
> and `@iterativeflow/dashboard`; the **node-graph builder + static determinism guard was dropped**
> for a **linear builder + imperative `defineFlow`** guarded at **runtime** (the drift guard,
> `CONTRACTS.md`); and the "crash/partition harness" + "time-skip testing" below are **not yet built**
> (crashes are simulated ad-hoc in tests). Port sketches in §4 are illustrative — the shipped port
> interfaces live in `packages/core/src/ports/`.

## 1. Why v2

v1 is the right _paradigm_ (memoized-step / checkpoint durable execution) paying the
wrong _price_ and coupled to the wrong _substrate_. v2 keeps the paradigm and the
genuine goodies, and forks the rest from the best in the field. Three goals, in order:

- **Free** — no `graphile-worker` hard dependency, no single-database assumption, no
  resident-process requirement. The core depends on **nothing** but four small ports.
- **Performant** — **1 durable write per step** (0 for DB-touching steps), audit off the
  hot path, batch on the resident path, **connectionless** on the serverless path.
- **Flexible** — the _same_ authoring code runs resident (Postgres), serverless-Postgres
  (pooler-safe), or Lambda-native (DynamoDB). The user picks a **profile**, not a rewrite.

Non-goal: reinventing Temporal. We compete on **code-first TS DX**, **self-hostable /
portable**, and **cheap at high step counts** — not feature-parity with a cluster.

## 2. Keep vs discard (be explicit)

**Keep (v1's real goodies):**

- The **node-graph builder + single typed channel** — it _structurally_ corrals
  non-determinism into `.step()`, giving safe imperative-feeling flows without a
  Temporal-style sandbox. This is our unfair advantage.
- **Transactional-outbox discipline** — state + dispatch commit atomically.
- **Durable deadlines as first-class timers** (the retry-timer we shipped in 5.5.2
  generalizes to _all_ waits — sleep, retry, signal-timeout).
- **Memoized-step recovery** — read the last completed step, don't replay full history.

**Discard:**

- `graphile-worker` as a hard dependency.
- Postgres-only assumptions baked into the core.
- LISTEN/NOTIFY as _the_ completion mechanism (dies behind RDS Proxy / PgBouncer-txn).
- The 5-round-trip-per-step write path.

## 3. The core

A backend-free durable-execution engine that speaks **only** to the four ports below.
It owns: the run state machine, step memoization + cursor sequencing, the flow builder,
the replay loop, retry/backoff policy, and the reconciler. It knows nothing about
Postgres, DynamoDB, graphile, HTTP, or Lambda.

```
        authoring (builder / defineFlow)
                     │
            ┌────────▼─────────┐
            │  durable core     │  state machine · memoization · replay · reconcile
            └─┬───┬────┬────┬──┘
              │   │    │    │
     Store  Queue  Timer  Wakeup     ← four ports (backend-specific)
```

## 4. The four ports (contracts)

Each port is small and orthogonal. Backends implement each with the _best primitive
available_, not a lowest-common-denominator. Sketches (final types live in `src`):

### 4.1 Store — durable checkpoint

```ts
interface Store {
  /** Insert the run if new; return existing on idempotency-key hit. Idempotent. */
  startRun(spec: RunSpec): Promise<{ runId: string; created: boolean; status: RunStatus }>;
  /** Load the memo needed to replay: completed steps, timers, signals. One read. */
  loadRun(runId: string): Promise<RunSnapshot | undefined>;
  /**
   * Checkpoint a step's TERMINAL outcome — the single durable write per step.
   * Conditional/idempotent: a second writer for the same (runId, cursorKey) is a no-op
   * and returns the stored outcome. PG: INSERT … ON CONFLICT. Dynamo: PutItem(cond).
   */
  checkpointStep(c: StepCheckpoint): Promise<StepOutcome>;
  /**
   * Atomic state-transition + outbox in ONE commit (the transactional outbox).
   * Optional `withTx` lets a DB-touching step free-ride the user's own transaction
   * → zero extra round-trips (the DBOS trick).
   */
  commit(txn: StateTxn, withTx?: unknown): Promise<void>;
  markTerminal(runId: string, outcome: RunOutcome): Promise<void>;
}
```

Invariant: **one durable write per step** (the checkpoint). `startStep`/`step_started`
events are gone; run-level attempt-bounding replaces the per-step start marker.

### 4.2 Queue — claim / lease (this is where graphile goes)

```ts
interface Queue {
  enqueue(runId: string, opts?: { runAt?: Date; priority?: number }): Promise<void>;
  /** Lease up to `max` due runs to this worker for `leaseMs`. Batchable. */
  claim(opts: { max: number; leaseMs: number }): Promise<Lease[]>;
  heartbeat(lease: Lease): Promise<void>; // extend the lease during a long step
  ack(lease: Lease): Promise<void>; // done; release
}
```

- **Universal impl — lease-CAS**: conditional update `owner`/`leaseExpiry` (works on any
  store, incl. DynamoDB). A crashed worker's lease expires → another claims. No
  `SKIP LOCKED` required.
- **PG fast-path** — `SELECT … FOR UPDATE SKIP LOCKED` + batched claim (River-style) for
  built-in fan-out.
- **Dynamo** — Streams→Lambda (event-driven) or lease-CAS over a `due` GSI.
  The lease-CAS contract is the floor that frees us from any specific queue engine.

### 4.3 Timer — durable deadlines

```ts
interface Timer {
  schedule(runId: string, fireAt: Date): Promise<void>; // sleep / retry / signal-timeout
  dueBatch(now: Date, max: number): Promise<string[]>; // runs whose deadline passed
  cancel(runId: string): Promise<void>;
}
```

PG: `fireAt` partial index (what we already do). Dynamo: EventBridge Scheduler per-run,
or a `fireAt` GSI + poller. **Every wait is a durable deadline** — the reconciler
reconciles against timers, never against a queue property.

### 4.4 Wakeup — completion signalling (poll-first)

```ts
interface Wakeup {
  awaitTerminal(runId: string, timeoutMs: number): Promise<RunOutcome>; // poll or push
  notifyTerminal?(runId: string): Promise<void>; // optional fast-path emit
}
```

- **Default — poll**: indexed, backoff, pooler-safe, works everywhere.
- **Optional push** — NOTIFY on a _dedicated_ direct connection, or Dynamo Streams. A
  latency optimization, **never** a correctness dependency.

## 5. Recovery & determinism

Memoized-step, read-last-completed (the cheap camp — DBOS/Restate/Hatchet). On resume:
`loadRun` → re-enter the body → completed steps short-circuit from the memo. The
**builder guarantees** non-determinism lives inside `.step()`, so the between-steps
determinism tax that bites free-form imperative engines doesn't apply to builder flows.
The imperative `defineFlow` escape hatch carries an **explicit determinism contract**
(documented + an optional lint rule), because we won't ship a Node sandbox and CRIU is
too heavy for a self-hostable lib.

## 6. Deployment profiles (same core)

| Profile                  | Store                                       | Queue               | Timer                 | Wakeup                 |
| ------------------------ | ------------------------------------------- | ------------------- | --------------------- | ---------------------- |
| **resident** (self-host) | Postgres                                    | SKIP LOCKED + batch | `fireAt` index        | poll + optional NOTIFY |
| **serverless-postgres**  | Postgres (pooler-safe: unnamed params)      | lease-CAS           | `fireAt` index        | poll                   |
| **lambda-native**        | DynamoDB (cond writes / TransactWriteItems) | Streams / lease-CAS | EventBridge Scheduler | poll / Streams         |
| **memory** (tests)       | in-memory                                   | in-memory           | in-memory             | in-memory              |

## 7. DX

- **Linear builder + imperative `defineFlow`** both ship as first-class authoring APIs.
  Determinism is guarded at **runtime** by the drift guard (`CONTRACTS.md`), not
  structurally by a node graph (the original graph builder was dropped — see `PARITY.md`).
- **Typed signals** (shipped) — Standard-Schema payload contracts, typed on both ends.
- **Time-skip testing** (Temporal's test env) — advance timers in-memory, assert outcomes
  without wall-clock waits. **Not yet built** — backlog.

## 8. Correctness — how we don't cut corners (this is architectural, not a vibe)

The single biggest guardrail against "an agent missed a detail / skipped a guarantee /
left dirty work" is that **durability is defined as an executable conformance suite, and
every backend must pass it.**

1. **Invariants are written first, as tests.** Before a port impl is written, its
   invariants exist as a `conformance/` test module the impl is run against:
   - a step runs **exactly once** across a simulated crash between fn-return and checkpoint;
   - a completed step is **never re-run** on resume (memoized);
   - a `failed_terminal` step is **never retried**;
   - a lease **expires** and re-claims after a worker "crash" (no double-active-claim);
   - a timer **fires once**, at/after `fireAt`, and is **consumed** on claim;
   - `startRun` is **idempotent** on the idempotency key;
   - the transactional outbox is **all-or-nothing** (state + enqueue land together);
   - concurrent claims of the same run yield **exactly one** winner.
2. **Every backend runs the SAME suite** — all eight (memory, postgres, dynamodb, redis,
   sqlite, mysql, mongodb, durable-objects). An impl is not "done" until green on the full
   conformance suite (memory/sqlite/durable-objects via a fake clock; the rest via real
   containers). This mechanically prevents a backend from silently dropping a guarantee.
3. **Definition of Done per change (machine-checkable, no exceptions):**
   `tsc` clean · lint · format · unit + **behavior** tests (drive the real path, not
   types) · the change is **verified by running the affected flow**, not just compiled ·
   no dead code / plumb-fit / drive-by reformat · API report regenerated · changeset.
4. **Adversarial review before merge** — the `/code-review` (bugs) + `/simplify`
   (reuse/altitude/dead-code) passes, ideally multi-agent, on every non-trivial change.
   Findings are verified, not taken on faith.
5. **Fault-injection** (backlog, not yet built) — the intent is an in-memory crash/partition
   harness so resume-correctness is exercised on _every_ run. Today crashes are simulated
   ad-hoc in the tests (e.g. manual `markRunning` bumps, injected `Doc`/`send` faults).

The rule: **no code without a written contract + invariant test; nothing merges that
isn't specced, conformance-green, behavior-verified, and adversarially reviewed.**

## 9. Backends & roadmap

_Shipped at `2.0.0-alpha.2`:_ **eight** backends behind the ports —
**memory** (reference/oracle), **postgres**, **dynamodb**, **redis**, **sqlite**, **mysql**,
**mongodb**, and **durable-objects** (the SQLite backend inside a Cloudflare Durable Object). Each
passes the same nine conformance suites — the port split held: `Store` abstracts cleanly while
`Queue`/`Timer`/`Wakeup` are purpose-built per backend. Alongside the backends: `@iterativeflow/webhooks`
(inbound signed-webhook → durable signal), `@iterativeflow/dashboard` (ops UI), and
`@iterativeflow/conformance` (the shared suites).

## 10. Versioning & migration — the upgrade must feel like a promotion

**Guiding principle: a v1 user reading the v2 changelog should feel _stronger_, never
degraded.** Breaking is permitted only when the break _buys_ the user something bigger,
and never costs them a goodie they relied on. Concretely:

- **No goodie left behind.** Maintain a **parity matrix**: every v1 capability (builder,
  typed channel, invoke, signals, sleep, idempotency, tags, reconciler, retention,
  transactional enqueue, `startMany`) is listed with its v2 status — _kept · improved ·
  replaced-by-better_. Nothing is silently dropped; a "removed" row must name the
  superior replacement.
- **Every break buys a named win.** Each breaking change in the changelog/guide states
  it as a trade: _"`results:'poll'` is gone → replaced by first-class poll that also
  works behind RDS Proxy."_ No gratuitous breaks; if a break has no upside for the user,
  it doesn't ship.
- **Codemod + compat shim.** Ship an automated codemod for mechanical API changes and a
  thin v1-compat adapter so an existing flow runs on v2 with minimal edits — the break is
  a guided step, not a cliff.
- **Runnable upgrade guide.** A real before/after migration of a non-trivial example
  flow, not prose. The reader sees their own shape translated.
- **Deprecation runway.** 5.x stays maintained through the transition; v2.0 ships _with_
  the guide + codemod, not ahead of them.

Mechanics:

- `iterativeflow@2` is a new major; 5.x continues on maintenance.
- The Store schema is new (no in-place 5.x→2 data migration promised at v2.0); a
  data-migration tool is a post-2.0 consideration and, if a user needs it, must not lose
  in-flight runs.

## Resolved decisions (were open at design time)

- **Package layout → monorepo.** Shipped as separate `@iterativeflow/*` packages (`core`, one per
  backend, `webhooks`, `dashboard`, `conformance`), so backends are opt-in deps.
- **`defineFlow` ships now.** Both authoring APIs are first-class: the fluent `builder()` and the
  imperative `defineFlow` (both exported from `@iterativeflow/core`).
- Lease/heartbeat defaults and the batch-claim knob are settled in each backend's `Queue` impl.
