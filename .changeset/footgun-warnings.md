---
"iterativeflow": patch
---

Three more boot-time footgun warnings + structural cleanup.

**Warnings** (operator-tunable defaults that silently bite under load):

- `flow.config.unbounded_step_timeout` — no `defaultStepTimeoutMs` set; a hung step pins a worker slot indefinitely. Set `defaultStepTimeoutMs` (or pass `StepOpts.timeoutMs` on every step).
- `flow.config.no_retention` — no `retention` configured; `workflow.events` and terminal `workflow.runs` grow forever. Set `EngineOpts.retention` or run your own prune cron.
- (already shipped last patch) `flow.config.stuck_shorter_than_step_timeout` — reconciler would resurrect a still-running step.

**Stderr fallback for warnings.** When `EngineOpts.logger` isn't provided, the engine now uses a logger that pipes `warn`/`error` to `process.stderr` (debug/info stay silent). Previously the default was a full noop — boot validators warned into the void. Consumers who genuinely want silence still get it by passing their own no-op logger.

**Internal restructure.** Extracted `src/engine/internal-crons.ts` (reconciler + retention cron builders) and `src/engine/loggers.ts` (fallback + console presets). `engine.ts` 464 → 413 lines; `createEngine` reads more linearly. Default magic numbers consolidated into named constants, using the existing `toMs("1m")` / `toMs("10m")` duration helpers for self-documenting time values.
