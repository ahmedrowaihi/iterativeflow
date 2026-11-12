# @iterativeflow/conformance

The backend-authoring kit for [iterativeflow](https://github.com/ahmedrowaihi/iterativeflow)
v2. To add a substrate, implement the four ports (store/queue/timer/wakeup) from
`@iterativeflow/core/backend` and prove them against these shared suites — the
same tests every built-in backend passes. A real serialization or consistency
regression can't hide behind a backend's own tests.

```bash
npm install -D @iterativeflow/conformance @iterativeflow/core
```

```ts
import { backendConformance } from "@iterativeflow/conformance";
import { createMyBackend } from "./backend";

backendConformance("my-backend", () => createMyBackend(), { concurrentWriters: true });
```

`backendConformance` registers every suite below (vitest `describe`/`it`), so call
it at the top level of a test file. `makeBackend` must return a fresh `Backend`
(or a `Promise` of one) per case. Set `concurrentWriters: false` for a
single-writer substrate (one process, one file, one Durable Object) — it skips
the concurrent-claim suite.

## Suites

Each suite is also exported on its own. The port suites — `storeConformance`,
`queueConformance`, `timerConformance`, `wakeupConformance`, `cronConformance` —
take a factory for that one port (`() => Store`, `() => Queue`, …); every other
suite takes a `Backend` factory.

- **Port suites** — each port's contract in isolation (`cronConformance` covers
  the Store's cron methods).
- `outboxConformance` — the atomic multi-port checkpoint write.
- `signalConformance`, `reconcileConformance` — the durable behaviors built on the ports.
- `engineConformance` — the composed engine (retry/dead-letter, signal resume,
  cancel cascade, fan-out, drift) end-to-end on your backend.
- `claimFilterConformance`, `shardedClaimConformance` — flow-name-filtered claims,
  and concurrent claims across a fleet (`concurrentWriters` backends only).
- `pendingWorkConformance` — the autoscaling backlog reads (`Queue.depth`,
  `Timer.dueCount`, `Store.dueCronCount`) that `engine.pendingWork` composes.
