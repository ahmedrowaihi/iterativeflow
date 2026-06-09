---
"iterativeflow": major
---

Group `EngineOpts` into descriptive config blocks.

The flat options bag is replaced with four nested groups so related settings live together and each group's defaults are documented on the hover. Switchable subsystems (`reconciler`, `retention`) take `false | { … }`; always-on tuning (`worker`, `limits`) takes `{ … }`. New: `reconciler.schedule` lets you change the sweep cadence (previously hardcoded to every minute).

Migration:

| Before (v3)               | After (v4)                    |
| ------------------------- | ----------------------------- |
| `workerSchema`            | `worker.schema`               |
| `concurrency`             | `worker.concurrency`          |
| `pollInterval`            | `worker.pollInterval`         |
| `enqueue`                 | `worker.enqueue`              |
| `disableReconciler: true` | `reconciler: false`           |
| `reconcilerGraceMs`       | `reconciler.graceMs`          |
| `runningStuckMs`          | `reconciler.runningStuckMs`   |
| `maxRunAttempts`          | `limits.maxRunAttempts`       |
| `defaultStepTimeoutMs`    | `limits.defaultStepTimeoutMs` |

`retention` and `limits` (size caps) keep their fields; `limits` now also holds `maxRunAttempts` and `defaultStepTimeoutMs`.

```ts
// before
createEngine({
  db,
  pool,
  workerSchema: "gw",
  concurrency: 10,
  disableReconciler: true,
  maxRunAttempts: 50,
});

// after
createEngine({
  db,
  pool,
  worker: { schema: "gw", concurrency: 10 },
  reconciler: false,
  limits: { maxRunAttempts: 50 },
});
```
