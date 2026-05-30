---
"iterativeflow": patch
---

Docs and internal hardening:

- Rewrote `docs/guide.md` to v2 vocabulary (`signal` not `hook`, `FlowHandle` not `WorkflowHandle`, `engine.listen()` not `engine.start()`, etc.). Multiple v1 names were sitting in published docs after the v2 rename — they're gone now.
- Fixed a stray `// hook's timeout fired` comment in `README.md`.
- Added a compiled-docs gate: `scripts/extract-doc-examples.mjs` pulls every ` ```ts ` block from `README.md` + `docs/*.md` into `tests/docs-examples/`, and `npm run docs:check` typechecks them against the local source. Wired into the pre-push hook and CI. Blocks that aren't standalone (signature listings, partial chains) are marked with `<!-- doc-check: skip -->`.
- Added a replay corpus: `tests/replay-corpus/*.json` are captured suspension-state snapshots (`sleep-suspended`, `signal-suspended`, `completed-run`), and `tests/replay-corpus/corpus.test.ts` re-inserts each one against a fresh pglite and replays via `playRunAttempt` to verify the run reaches the documented terminal state. Regenerate the corpus deliberately via `npm run corpus:capture` when the storage shape changes.
