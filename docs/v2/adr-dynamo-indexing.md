# ADR (v2) — DynamoDB access paths: overload the single GSI, don't full-table Scan

- **Status:** Accepted — implemented. `dueCrons` and `childrenOf` now Query the overloaded `gsi1` (`CRON_DUE` / `CHILD#<parent>` partitions); `orphanedRuns`/`listRuns`/`runStats` unchanged per the decisions below.
- **Date:** 2026-07-24
- **Deciders:** iterativeflow maintainers
- **Scope:** `@iterativeflow/dynamodb` only. The Postgres backend already serves these paths with indexes; this closes the complexity-class divergence.

## Context

Several DynamoDB store operations are **full-table `Scan`s** that filter
client-side — O(table) cost that grows with never-deleted run history, where the
Postgres backend uses an indexed `WHERE`/`GROUP BY` for the same port contract.
The scan sites (`v2/packages/dynamodb/src/store.ts`):

| Op                      | Cost today                                       | On the hot path?                                |
| ----------------------- | ------------------------------------------------ | ----------------------------------------------- |
| `orphanedRuns`          | **3** scans (`run`, `job`, `timer`)              | yes — every `reconcile`                         |
| `dueCrons`              | 1 scan filtered to `type="cron"`                 | yes — every cron sweep                          |
| `childrenOf`            | 1 **consistent** scan, filtered by `parentRunId` | yes — **once per node of every cancel cascade** |
| `listRuns` / `runStats` | 1 scan per page / per call                       | no — cold / admin                               |

`serverlessTick` therefore scans the table ~4× per invocation regardless of how
little is actually due — a table with millions of historical runs pays
proportional RCU and latency on every tick. This is a hard scaling ceiling.

### The constraint that shapes the fix

The DynamoDB schema is a deliberate, documented shape: **one table, one GSI
(`gsi1`)**. `gsi1` is already **overloaded** — it multiplexes two independent
access paths by namespacing its partition key with a constant:

- `gsi1pk = JOB_GSI_PK` → claimable jobs, `Query gsi1pk = :job`
  (`queue.ts:39`)
- `gsi1pk = TIMER_GSI_PK`, `gsi1sk = pad(fireAt)` → due timers,
  `Query gsi1pk = :tp AND gsi1sk <= :now` (`timer.ts:39`)

Each is a **sparse, constant-partition, sorted-range** index living in the one
GSI. New access paths can join `gsi1` the same way — a new partition-key
namespace, no new index. **The fix stays within the one-GSI decision.**

The load-bearing wrinkle: a GSI is **eventually consistent**, but `childrenOf`
feeds the cancel cascade off a **strongly-consistent** scan today.

## Decision

Serve the two hot, safe paths by overloading `gsi1`; gate the hard one
(`orphanedRuns`) off the per-invocation path instead of indexing it; defer the
cold paths.

### 1. `dueCrons` → `gsi1` due-partition (safe win)

Cron items set `gsi1pk = CRON_DUE_GSI_PK` (a new constant), `gsi1sk =
pad(nextRunAt)`. `dueCrons` becomes `Query gsi1pk = :cd AND gsi1sk <= :now` —
the exact shape the timer path already uses.

**Consistency:** fine. `advanceCron` is CAS-guarded, so a stale/duplicate due
read can't double-fire — the same guarantee that already makes the timer GSI
safe. `nextRunAt` is a single-item attribute already written by
`upsertCron`/`advanceCron`; adding the two `gsi1` attributes is part of that same
write.

### 2. `childrenOf` → `gsi1` parent-partition (the consistency call)

Child run items set `gsi1pk = CHILD#<parentRunId>`, `gsi1sk = pad(seq)`. Root
runs (no parent) get no `gsi1` entry — sparse. `childrenOf` becomes `Query
gsi1pk = :child` instead of a consistent full scan, dropping cancel-cascade cost
from O(table)-per-node to O(children)-per-node.

**Consistency — the decision:** accept the eventually-consistent Query. The GSI
lag window (a child spawned microseconds before the parent's cascade reads
`childrenOf` might not be indexed yet, so the push cascade could miss it) is
**already closed by two existing backstops**, both covered by
`engineConformance`:

- **Pull-check self-cancel** — on dispatch, a run whose `parentRunId` is
  terminally failed/canceled cancels itself (`executor.ts`). A child the push
  cascade missed cancels itself the moment it is next claimed.
- **Reconcile sweep** — `reconcile` re-drives a non-terminal run whose parent
  terminally failed/canceled, which then hits the pull-check. This proactively
  reaches even a _sleeping_ missed child (thread-4 fix).

So the cascade is defense-in-depth: the push (`childrenOf` + cancel) is the fast
path, and pull + reconcile are the completeness guarantee. Eventual consistency
on `childrenOf` degrades the push's promptness, never its correctness — the same
"documented residual race, backstopped elsewhere" posture as the JOB/TIMER GSIs.
This is acceptable **only because** those two backstops exist; if either is
removed, `childrenOf` must revert to a consistent read.

### 3. `orphanedRuns` → gate off the hot path, don't index (for now)

Orphan detection is a set difference: "non-terminal run with **no** job and
**no** timer." The absence is the signal, and you cannot cheaply index an
absence. A sparse "active" membership on run items would shrink the dominant
`run` scan (run history grows unbounded; the live set is bounded by
concurrency), **but** a run item's single `gsi1` slot is already claimed by the
`CHILD#` membership in decision 2 — a run can't be in two `gsi1` partitions at
once. Indexing orphans would require a **second GSI** (`gsi2`), which breaks the
one-GSI decision.

Decision: **don't index; change the cadence.** `orphanedRuns` is a crash-recovery
sweep, not a per-run-critical path. Run `reconcile` on a slow cadence (the
resident loop already separates `maintenanceMs` from `tickMs`); for serverless,
drive it every Nth `serverlessTick` (or a separate scheduled invocation) rather
than every tick. The scans stay O(table) but fire rarely, and the correctness of
recovery is unchanged. Revisit a `gsi2` "active-runs" index only if reconcile
cost is measured to bite.

### 4. `listRuns` / `runStats` → a second GSI (`gsi2`), constant `RUN` partition

Cold admin paths, but a full-table `Scan` per page / per call is still the worst
complexity-class gap. They get a **second GSI**, `gsi2pk = "RUN"` (a single
constant partition) sorted by `gsi2sk = pad(seq)`, written **once at run creation**
and never touched again. Deliberately chosen over a status-partitioned index: a
`STATUS#<status>` partition would have to be rewritten on **every status
transition** — including `markRunning`, on the hot claim path — to speed up a cold
read. `listRuns` becomes a bounded descending-`seq` Query (one index page when
unfiltered; a few pages under a selective filter, never the whole table);
`runStats` is a Query over `gsi2` projecting only `status` (a Query, not a
full-item Scan). A single write-partition is fine at normal run-creation rates and
upgrades to a sharded `RUN#<seq mod N>` layout later without changing the reads.

This is the one place the design accepts a **second** GSI. The one-GSI preference
held for the hot paths (decisions 1–3 overload `gsi1`); the cold list/stat paths
justify `gsi2` rather than an unbounded Scan.

## Consequences

**Write cost.** `gsi1` uses `ProjectionType: ALL`, so an indexed item's write
also writes its projection. Cron items (rare) and child-run items (every child
state transition) now carry two extra `gsi1` attributes. Child runs already
rewrite their run item on each transition; the delta is index WCU proportional
to item size, not an extra round-trip. Root-run writes are unchanged (sparse —
no `gsi1` entry).

**Migration.** Decisions 1–2 add partition-key _values_ to the existing `gsi1` —
no index creation. Decision 4 adds `gsi2` to `tableSpec` / `ensureTable`: a fresh
table gets it at create time; an existing table needs a one-time `UpdateTable`
(DynamoDB backfills the new index online). Existing rows written before any of
these lack the corresponding `gsi1`/`gsi2` attributes and won't appear in the new
Queries until rewritten — acceptable for a fresh `2.0.0-alpha`; a one-time
`UpdateItem` backfill sweep is available for a live table.

**`REQUIRED_IAM_ACTIONS`** already grants `Query` on `index/*`; both GSIs are
covered, no IAM change.

**What did _not_ change.** The atomic `TransactWriteItems` / CAS paths are
untouched — every decision only adds attributes to items those writes already
touch, or (for `gsi2`) sets them once at creation.

## Alternatives considered

- **A status-partitioned `gsi2` (`STATUS#<status>`) for `listRuns`/`runStats`.**
  Rejected: it would rewrite the index on every status transition — including
  `markRunning` on the hot claim path — to serve a cold read. The constant-`RUN`
  partition (decision 4) writes `gsi2` once at creation instead. A sharded
  `RUN#<seq mod N>` variant is the escalation if one write-partition ever throttles.
- **A `gsi2` for orphan detection (active runs).** Still rejected: `orphanedRuns`
  is cadence-gated (decision 3), so it doesn't justify indexing an absence.
- **Keep `childrenOf` strongly consistent via scan.** Rejected: O(table) per
  cascade node is the worst offender, and the pull+reconcile backstops already
  make an eventually-consistent read safe.
- **Maintain `runStats` as counters.** Rejected: write-amplifies every run
  transition to fix a cold admin call.

## STOP / revisit triggers

- If the executor pull-check **or** the reconcile sweep is ever removed,
  decision 2 is invalid — `childrenOf` must go back to a consistent read.
- If a measured reconcile cost forces indexing orphans, that's the `gsi2`
  escalation and a new ADR.
