# iterativeflow v2 — contributor conventions

## Comments

Near-zero inline comments. The public boundary carries words; internal code speaks for itself.

- **Public exports** (an `index.ts` surface, the `/backend` SPI, the port interfaces in
  `core/src/ports/*`) — JSDoc, one-line summary minimum. Add `@param`/`@returns`/`@throws` only when
  non-obvious from the types.
- **Internal code** — no JSDoc. `/** @internal */` on cross-file helpers that happen to be exported.
- **Inline `//`** — only for a **WHY** the code can't state itself: a non-obvious reason, an ordering
  or first-writer constraint, a foot-gun, a correctness/security invariant. Never restate the line,
  narrate a language feature, repeat a type, or explain what a well-named function does.
- **No journey narration** (why-I-wrote-it-this-way, design-history) and **no ADR references** in
  code — that belongs in `docs/`.
- **Ports own the contract.** A behaviour documented on a `core/src/ports/*` interface is NOT
  re-explained at each of the 8 backends' implementations — the backend just implements it.
- **Tests** — the test name carries the intent. Comment only a subtle timing/ordering dependency, a
  cast, or a regression's linked cause.

## Backends

Each backend under `packages/*` implements the four ports (Store/Queue/Timer/Wakeup) + the
transactional outbox against its own primitive, independently — only the `@iterativeflow/conformance`
suites are shared. Duplicated port _logic_ that must stay identical (e.g. the orphan predicate) is
consolidated in `core` (`orphanedRunsSql`, `isOrphaned`); small per-backend row/codec boilerplate is
allowed to stay local. Every backend must pass all nine conformance suites.

## Verify

`pnpm run typecheck && pnpm run lint && pnpm test` (backend suites need Docker: postgres, redis,
mysql, mongodb, dynamodb; memory/sqlite/durable-objects/core/webhooks need none). `pnpm run api:check`
regenerates and diffs the `etc/*.api.md` public-surface snapshots.
