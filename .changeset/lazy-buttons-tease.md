---
"iterativeflow": minor
---

Add `iterativeflow/dashboard` — a mountable observability UI for flows.

`createFlowsDashboard({ engine })` returns a WHATWG fetch handler that serves
a JSON API plus a single self-contained HTML page (no framework, no build
step, zero new dependencies): a runs list with status/name/tag filters and
keyset pagination, a run detail view (steps, sleeps, signals, capped
input/output payloads), a per-process health strip, and cancel/retry actions
with confirmation. It consumes only the public Engine API, mounts at any
path behind the host app's auth, and requires `content-type:
application/json` on mutations. Ships with a `jsonCap` option bounding how
much of any jsonb payload reaches the browser.
