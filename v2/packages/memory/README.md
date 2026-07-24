# @iterativeflow/memory

In-memory [`Backend`](../core) for [iterativeflow](https://github.com/ahmedrowaihi/iterativeflow)
v2. Zero setup, no external service — the four ports (store/queue/timer/wakeup)
live in process memory. Ideal for tests, examples, and single-process
development. State does not survive a restart; use
[`@iterativeflow/postgres`](../postgres) or
[`@iterativeflow/dynamodb`](../dynamodb) for durability.

```bash
npm install @iterativeflow/memory @iterativeflow/core
```

```ts
import { createEngine, defineFlow } from "@iterativeflow/core";
import { createMemoryBackend } from "@iterativeflow/memory";

const engine = createEngine(createMemoryBackend(), [myFlow]);
```

`createMemoryBackend()` returns a full `Backend`. The per-port constructors
(`createMemoryStore`, `createMemoryQueue`, `createMemoryTimer`,
`createMemoryWakeup`) are exposed for isolated port testing.
