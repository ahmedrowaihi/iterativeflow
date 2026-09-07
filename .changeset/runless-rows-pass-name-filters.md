---
"@iterativeflow/core": minor
---

Fix: a run-less row now passes every name filter consistently — `claim` and the backlog readers agreed
on nothing before.

2.1.1 made a name-filtered `Queue.claim` lease run-less jobs so `runTick`'s gone-path acks them and
they self-heal. The sibling readers were left on the old answer: `Queue.depth(now, names)`,
`Timer.dueCount(now, names)` and both `pending_work(flow_names, as_of)` SQL functions still excluded
them. So `engine.pendingWork(["a"])` reported 0 for a backlog its own workers would lease, and a
name-sharded fleet scaled to zero on that number never woke to drain it — the exact failure the claim
fix set out to close. Conformance pinned the contradiction rather than catching it: a run-less timer
was asserted _excluded_ under a name filter while a run-less job was asserted _included_.

One rule now, everywhere: a row whose run is gone is unownable, so it passes every name filter; an
empty `names` means "this worker handles nothing" and matches nothing at all, run-less rows included.
The empty-set case was itself divergent — sqlite/mysql/dynamodb short-circuited while
postgres/memory/mongodb/redis leased the orphan — and the conformance case had no orphan in it to
notice. Both invariants are now pinned for all 8 backends.

`Timer.dueCount(now, names)` and `Queue.depth(now, names)` therefore count run-less rows where they
did not before, so a sharded `engine.pendingWork(names)` can rise by the number of orphaned rows —
which is the point: that work is claimable.

Also: both `pending_work` functions drop their unconditional `LEFT JOIN run` for a `NOT EXISTS`, so
the unfiltered call (the common single-shard case) no longer does a run lookup per job and per timer;
the dead `?? ""` fallbacks in the dynamodb/mongodb/memory claim filters are gone; the mysql/mongodb
testcontainer bootstrap is shared by both test files per package; and four inaccurate claims in
`patterns/` are corrected — notably that MySQL's `applySchema` needs `CREATE ROUTINE`, and that KEDA's
mongo scaler cannot run `pendingWorkPipeline` directly.
