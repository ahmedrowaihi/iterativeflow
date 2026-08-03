# Study: an all-Redis backend (Valkey / Dragonfly)

Design study for a Redis-family backend implementing the four v2 ports (Store / Queue / Timer /
Wakeup) against the existing conformance suites. **Built** — shipped as `@iterativeflow/redis`,
green on all nine conformance suites against real Valkey. This records the design decisions.

## Scope and tier

This is the **all-Redis** backend (design "A"): state _and_ dispatch live in one Redis-compatible
store. Durability = the store's persistence config (AOF `everysec`/`always`). It is the **fast,
loss-tolerant** tier — a full wipe loses state, because Redis _is_ the source of truth. The
pg-truth + Redis-accelerator **hybrid** (design "B") is a separate effort, deferred; see the closing
note on why the hybrid is harder.

## Framing: dispatch is solved, the durable Store is the novel part

Every Redis queue (BullMQ, Sidekiq, the antirez reliable-queue pattern) is a **job queue**, not
durable execution — none has memoized-step replay. So Queue / Timer / Wakeup are well-trodden and we
steal them wholesale. The **durable Store + the transactional outbox** has no strong Redis precedent
(Temporal / DBOS / Restate use their own logs) — that is the part to design carefully. The good news:
Redis makes it _easier_ than Postgres, because a Lua script is atomic by Redis's single-threaded
execution, so the whole outbox commit is one `EVALSHA` with no transaction machinery.

## Why Redis fits the model cleanly

| Concern                                                     | Postgres today                  | Redis-family                         |
| ----------------------------------------------------------- | ------------------------------- | ------------------------------------ |
| Outbox atomicity (step + spawns + enqueues + timers + join) | one `BEGIN…COMMIT`              | one Lua script                       |
| Timer                                                       | `timer` table + index scan      | sorted set by `fireAt` (native)      |
| Wakeup                                                      | `LISTEN/NOTIFY` (pins a conn)   | pub/sub (native, no pinning)         |
| Retention                                                   | `deleteRunsOlderThan` sweep     | `EXPIRE` on terminal run keys (free) |
| Join countdown                                              | `arriveAtJoin` UPDATE…RETURNING | `DECR` a counter (O(1))              |
| Claim                                                       | `SKIP LOCKED` batch             | Lua lease-CAS on a sorted set        |

## Store

A run is **split** across a few keys under a `{runId}` hash-tag (see Cluster below), not one giant
object — so the hot run-row read stays cheap and steps scale independently of it:

- `run:{id}` — **hash**: the run row (`status`, `input`, `output`, `error`, `attempts`, `depth`,
  `createdAt`, `parentRunId`, `parentCursorKey`, `joinRemaining`).
- `steps:{id}` — **hash**: cursor key → memo JSON (`HGETALL` on replay). Kept off the run row so a
  10k-step flow doesn't bloat the row read.
- `inbox:{id}` — **list** (or stream): delivered signals awaiting a `ctx.signal` consume.
- `join:{id}` — the fan-out countdown counter (may live as a `run:` hash field instead).

Rejected: co-locating steps as fields of `run:{id}` (the "one object"). It reads in one shot but an
unbounded hash for a many-step flow means a huge `HGETALL` that blocks the single thread. The split
keeps the common path (load run row) O(1) and pays for steps only when replaying.

`listRuns` / `runStats` need explicit index structures (Redis has no secondary index): a
`runs:byStatus:{status}` set or a `runs:bySeq` sorted set maintained inside the same Lua writes.

## The Lua outbox (the crux)

`checkpointStep(c, fx)` becomes **one script** that, atomically:

1. First-writer-wins guard: `HSETNX steps:{id} {cursor} {memo}` — if it already exists, return the
   stored memo (idempotent replay; never double-spawns).
2. For each `fx.spawn`: create the child `run:{childId}` hash + `ZADD` it onto the queue.
3. Arm the join: `SET join:{id} {count}` (fan-out) or `HSET run:{id} joinRemaining {count}`.
4. `fx.enqueue`: `ZADD` the run onto the queue.
5. `fx.timers`: `ZADD timers {fireAt} {runId}` + drop the delay marker.
6. `fx.cancelTimers`: `ZREM timers {runId}`.
7. `fx.consumeSignals`: `LREM inbox:{id} …`.

All keys must share one slot — see Cluster. `FAN_OUT_CHUNK` (40) still applies as a _latency_ bound
(a Lua script blocks the single thread while it runs), not a hard cap like Dynamo's 100-item limit.

## Queue — sorted-set lease-CAS (first cut)

Maps most directly to the existing `queueConformance`, including the subtle version-bump test.

- `queue` — **sorted set**: member = `runId`, score = `priority * 2^k + runAt` (priority-then-time).
- Lease state as fields on `run:{id}` (or a parallel hash): `leaseToken`, `leaseExpires`, `version`.
- **claim** (Lua): `ZRANGEBYSCORE queue -inf {now-encoded} LIMIT 0 n`, filter to unleased-or-expired
  (`leaseExpires` absent or `≤ now`), `SET` token + `leaseExpires = now+leaseMs`, return leases.
- **heartbeat** (Lua): CAS on token + `leaseExpires > now` → extend; else error. Mirrors pg.
- **ack** (Lua): token + unexpired + `version` unchanged → `ZREM` (delete); `version` changed
  (a wake raced the ack) → **release** (clear lease, reset score) not delete. This is the
  ack-survives-wake invariant, expressed as a version field + Lua CAS.
- **enqueue** upsert: `ZADD` keyed by `runId`; `HINCRBY … version` so a mid-lease wake bumps it.
- **depth**: `ZCARD` + a `ZCOUNT`/scan for claimable/oldest (like the memory/Dynamo path).

**Crash recovery is free**: a lease with `leaseExpires ≤ now` is directly re-claimable — no active
list, no stalled-check. BullMQ needs a two-pass stalled sweep only because it locks with a separate
_key_; our expiry-as-score model doesn't. (BullMQ's actual lease is just
`SET {job}:lock {token} PX {dur}`, renewed by a token-checked `PEXPIRE` — same shape, minus the sweep.)

Alternatives considered: BullMQ's **list + lock** (loses priority unless you bolt on a prioritized
ZSET like BullMQ does) and **Redis Streams** (see appendix).

## Timer — sorted set + marker

- `timers` — sorted set, score = `fireAt`. `dueBatch` = `ZRANGEBYSCORE timers -inf now LIMIT 0 n`
  then `ZREM`, inside one Lua call so two workers can't double-promote.
- **Push**: BullMQ's marker trick — workers `BZPOPMIN` a marker key so they block until the next
  timer is due instead of polling. This is `Queue.waitForWork`, natively, no pg LISTEN.

## Wakeup — pub/sub

- `wakeup.signal(runId)` → `PUBLISH wake:{runId} ""` (or a shared channel with a runId payload).
- `wakeup.wait(runId, ms)` → `SUBSCRIBE` with a timeout; the completion trigger is a `PUBLISH` in the
  terminal-write Lua. Strictly better than pg `LISTEN/NOTIFY` — no connection pinning, no RDS-Proxy
  problem (that whole class of caveat disappears on Redis).

## Fan-out / join — a counter, validated by BullMQ Flows

BullMQ Flows are exactly our countdown-join. Their transition (quoting the actual Lua):

```lua
local result = rcall("SREM", parentDependenciesKey, jobKey)
if result > 0 then
  local pendingDependencies = rcall("SCARD", parentDependenciesKey)
  -- pendingDependencies == 0 → ZREM waiting-children, RPUSH parent to wait
```

They use a _set_ (`SREM` + `SCARD`) plus a `:processed` hash (child → return value). We use a
**counter** (`arriveAtJoin` = `DECR join:{id}`, wake at zero) and read child outputs from the child
run rows via `loadRunRows` — so we don't need the `:processed` hash. Fast-fail (a failed/canceled
child cascades to the parent) mirrors BullMQ's `failParentOnFailure`. The counter is the natural
Redis choice; the set is only worth it if we ever want the parent to read child results without
touching child rows.

## Retention — TTL

Set `EXPIRE` (a few fields, or the whole `{id}` key group) when a run goes terminal, in the
terminal-write Lua. Redis evicts old terminal runs itself — no `deleteRunsOlderThan` sweep. We can
still expose explicit `prune` for parity, but TTL is the idiomatic path and it's free.

## Key layout and the Cluster caveat

Per-run keys hash-tag by `{runId}` so `run:{id}`, `steps:{id}`, `inbox:{id}`, `join:{id}` co-locate
on one slot (a Lua script requires all keys on one slot). The problem: the **outbox** script touches
both the run keys _and_ the shared `queue`/`timers` keys → cross-slot. Options:

- **Single-node Valkey / Dragonfly** (the first target): non-issue. Dragonfly is multi-threaded and
  big-value friendly — a strong fit for the high-throughput tier.
- **Redis Cluster**: tag the shared dispatch keys by queue name and accept that state + dispatch for
  a queue live on one slot (BullMQ's approach; limits sharding to per-queue). Decide before building.

## Durability and the wipe question

- Configure AOF `appendfsync everysec` (≤1s loss window) or `always` (durable, slower). Document it
  loudly — this tier's durability _is_ that setting.
- A full wipe = total loss, because Redis is the source of truth here (unlike the hybrid, where pg
  survives). That's the tier's explicit trade.

## Appendix: the Redis Streams alternative

Streams (`XREADGROUP` / `XACK` / `XAUTOCLAIM`) are the most _robust_ at-least-once option:

- `XREADGROUP GROUP g c …` claims never-delivered entries; the Pending Entries List (PEL) tracks
  in-flight work; `XACK` completes.
- `XAUTOCLAIM key g c min-idle-time` transfers ownership of entries idle longer than `min-idle-time`
  — automatic crashed-consumer recovery, no manual stalled-check. Claiming resets idle time, so only
  one consumer wins. The PEL delivery counter gives poison detection (claim count > N → dead-letter).

Why _not_ for the first cut: streams are insertion-time-ordered (**no native priority**), and their
redelivery is idle-time-based, which doesn't map to our version-bump "ack survives a mid-lease wake"
conformance test. Revisit if we relax priority or model the wake differently.

## Definition of done

The backend is correct by the same bar as pg/Dynamo when it passes the **same four conformance
suites** (`storeConformance`, `queueConformance`, `timerConformance`, `wakeupConformance`) plus
`engineConformance`. First target: single-node Valkey/Dragonfly; note the Cluster hash-tag constraint
for later.
