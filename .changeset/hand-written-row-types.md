---
"iterativeflow": patch
---

Fix `engine.status()` and `engine.listRuns()` rows showing every column as `unknown` on the consumer side. The root cause: `RunRow` (and friends) were `typeof runs.$inferSelect`, which carries drizzle's column brand into the bundled `.d.ts`. The bundle re-renders drizzle under a vendored namespace, so a consumer's drizzle copy can't dereference the brand — every per-column inference collapses to `unknown`.

`RunRow`, `StepRow`, `TimerRow`, `SignalRow`, `EventRow` are now hand-written interfaces with concrete field types (`id: string`, `status: RunStatus`, `createdAt: Date`, `error: FlowError | null`, `tags: string[] | null`, jsonb columns as `unknown`, etc.). A compile-time equivalence check pins each interface to drizzle's `$inferSelect` of the runtime table, so a column rename or type change here fails the build instead of drifting.

No runtime change; structurally identical shapes — consumers just get usable types in `engine.status().run.name` etc. without casts.
