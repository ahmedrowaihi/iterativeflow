---
"iterativeflow": minor
---

`engine.status()` and `engine.listRuns()` now return rows with your drizzle-inferred types instead of `unknown`. `Engine`, `EngineOpts`, `RunDetail`, and `ListRunsPage` are generic over `T extends FlowTables` with a sensible default. Pass `tables` from your generated schema and any custom columns you added flow through end-to-end — without `tables`, the rows reflect the engine's internal table shape.

Also exports the row types (`RunRow`, `StepRow`, `TimerRow`, `SignalRow`, `EventRow`), `DefaultFlowTables`, and the `Row<T>` helper.
