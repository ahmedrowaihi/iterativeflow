---
"iterativeflow": patch
---

Internal: harden the Postgres-backed test suite against connection-shutdown races. Every test pool now attaches an idle-client error handler, so a `57P01` ("terminating connection due to administrator command") when the server is torn down no longer surfaces as an unhandled error and fails an otherwise-green suite. No runtime or API changes.
