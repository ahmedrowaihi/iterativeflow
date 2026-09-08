---
"@iterativeflow/core": minor
---

Type-check flow names in engine filters. `Flow` now carries its `name` as a literal type parameter, and `createEngine` infers the union of the names it was given, so `engine.cancelMany({ name: "onbaord" })`, `purge`, `listRuns`, `retryMany` and `pendingWork` reject an unregistered name at compile time instead of silently matching nothing. A typo in a filter is the one mistake the runtime cannot report — every backend answers it with "0 runs matched", which is indistinguishable from a correct filter over an empty set.

Additive: `RunFilter`, `PurgeFilter`, `Flow` and `Engine` all default their new parameter to `string`, so the `Store` port, all eight backends, and any code holding an `Engine` are unchanged.
