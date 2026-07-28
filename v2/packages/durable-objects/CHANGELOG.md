# @iterativeflow/durable-objects

## 2.0.0-alpha.4

### Patch Changes

- Updated dependencies [3a1d828]
  - @iterativeflow/core@2.0.0-alpha.4
  - @iterativeflow/sqlite@2.0.0-alpha.4

## 2.0.0-alpha.3

### Patch Changes

- Updated dependencies [5b07ed6]
- Updated dependencies [acbe2bb]
- Updated dependencies [2257a3e]
- Updated dependencies [539a1c2]
- Updated dependencies [12f3baa]
  - @iterativeflow/core@2.0.0-alpha.3
  - @iterativeflow/sqlite@2.0.0-alpha.3

## 2.0.0-alpha.2

### Minor Changes

- f191234: New: **`@iterativeflow/durable-objects`** — run iterativeflow inside a Cloudflare Durable Object on
  its built-in SQLite storage. It's the `@iterativeflow/sqlite` backend driven through a thin `Sql`
  adapter over `ctx.storage.sql` (`createDurableObjectBackend(storage)` + `applySchema`), so one DO
  becomes a self-contained, strongly-consistent durable-execution engine at the edge with no external
  database. No dependency beyond core + sqlite (the `SqlStorage` type is structural). Passes the same
  nine conformance suites as every other backend, verified against Node's synchronous `node:sqlite`,
  which matches the DO storage shape. A DO serves one request at a time (single-writer by
  construction); the outbox relies on the DO's invocation-level atomicity rather than a manual
  transaction, which DO SQLite forbids.

### Patch Changes

- Updated dependencies [e1ef077]
- Updated dependencies [3377316]
- Updated dependencies [a624058]
- Updated dependencies [f7bf20f]
- Updated dependencies [dc2b059]
- Updated dependencies [7a32846]
- Updated dependencies [11d3aa2]
  - @iterativeflow/core@2.0.0-alpha.2
  - @iterativeflow/sqlite@2.0.0-alpha.2
