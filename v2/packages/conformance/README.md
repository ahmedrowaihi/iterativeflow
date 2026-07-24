# @iterativeflow/conformance

The backend-authoring kit for [iterativeflow](https://github.com/ahmedrowaihi/iterativeflow)
v2. To add a substrate, implement the four ports (store/queue/timer/wakeup) from
`@iterativeflow/core/backend` and prove them against these shared suites — the
same tests the built-in memory, Postgres, and DynamoDB backends pass. A real
serialization or consistency regression can't hide behind a backend's own tests.

```bash
npm install -D @iterativeflow/conformance @iterativeflow/core
```

```ts
import { storeConformance, engineConformance } from "@iterativeflow/conformance";
import { createMyBackend } from "./backend";

storeConformance("my-backend", () => createMyBackend());
engineConformance("my-backend", () => createMyBackend());
```

Each suite registers `describe`/`it` blocks (vitest), so call them at the top
level of a test file.

## Suites

- **Port suites** — `storeConformance`, `queueConformance`, `timerConformance`,
  `wakeupConformance`: each port's contract in isolation.
- `outboxConformance` — the atomic multi-port checkpoint write.
- `signalConformance`, `reconcileConformance`, `cronConformance` — the durable
  behaviors built on the ports.
- `engineConformance` — the composed engine (retry/dead-letter, signal resume,
  cancel cascade, fan-out, drift) end-to-end on your backend.

Every suite takes `(label, makeBackend)` where `makeBackend` returns a fresh
`Backend` (or a `Promise` of one) per case.
