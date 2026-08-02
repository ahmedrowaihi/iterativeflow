---
"@iterativeflow/core": minor
---

Browser- and React-Native-ready core, plus an op-sqlite driver adapter for the SQLite backend.

**Isomorphic core** — the three `node:`-only couplings are gone, so `@iterativeflow/core` (and every
isomorphic backend) bundles for the browser and React Native with no Node polyfills:

- `newId` uses the Web Crypto global (`globalThis.crypto.randomUUID()`) instead of `node:crypto`, and
  throws an actionable error (pass a custom `IdGen`) when a runtime lacks the global.
- Trace/span id hashing uses a bundled synchronous SHA-256 instead of `node:crypto` — output is
  byte-identical (known-answer + `node:crypto` parity tests), and it only runs when a tracer is wired.
- The resident-loop sleep is a Web-standard `setTimeout` + `AbortSignal` instead of
  `node:timers/promises` (same timer semantics; the abort listener is removed on resolve, no per-tick
  leak).

Verified with an `esbuild --platform=browser` bundle of both core entrypoints. All three swaps behave
identically at runtime; the only new requirement is a Web Crypto global (Node 20+ / browsers /
polyfilled RN), escape-hatched by the injectable `IdGen`.

**`@iterativeflow/sqlite`: op-sqlite adapter** — `opSqliteDb` / `createOpSqliteBackend` run the SQLite
backend on [op-sqlite](https://op-engineering.github.io/op-sqlite) — one code path across React Native
(native JSI) and the browser (wasm + OPFS) — reusing the whole backend and passing every conformance
suite. Declared structurally (no dependency on op-sqlite; it's an optional peer beside `@libsql/client`).
The adapter owns `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK` for deterministic commit-on-resolve /
rollback-on-throw.
