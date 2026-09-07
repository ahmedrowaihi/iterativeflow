---
"@iterativeflow/core": patch
---

Documentation accuracy pass, plus three small hardening fixes.

The reference docs the package READMEs link to (`CONTRACTS.md`, `RECOVERY.md`, `MIGRATION.md`,
`ARCHITECTURE.md`, `PARITY.md`) are now in the repository. `.gitignore` excluded all of `docs/`, so
those links 404'd for every reader — the design notes, ADR drafts and docs plan stay local, which is
what the rule was for.

Corrected against the source: `result()` waits **indefinitely** without `timeoutMs` (and the README's
headline example now passes one); Postgres has shipped cross-process push, so `createLocalWakeup` no
longer calls it "a future opt-in"; Redis is single-node, not "or a Cluster" — the outbox Lua spans
keys and a Cluster fails with `CROSSSLOT`; MongoDB's dedup indexes are partial, not sparse; DynamoDB
needs **two** GSIs, not one; `inTx` exists for MongoDB too; the `maxFanOut` (10 000) and `maxDepth`
(32) caps are documented where they're declared; the conformance suite list is complete (12, not 9,
with the single-writer exemption stated); the React Native example passes the right argument; and the
core README no longer advertises an alpha version.

Hardening: `createPgListener` validates its schema identifier like every other interpolation site in
the package; the dashboard's HTML escaper covers quotes and backticks, and its two inline `onclick`
handlers are delegated listeners, so no value derived from a run id can reach a script context.

Also adds `CONTRIBUTING.md` (Node 22.5+, corepack, the Docker-optional `SKIP_TESTCONTAINERS=1` path
that was documented only inside `lefthook.yml`) and an `engines` floor to all 12 manifests, so a Node
mismatch is a clear message instead of a stack trace.
