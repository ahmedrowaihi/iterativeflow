---
"@iterativeflow/core": patch
---

`@iterativeflow/sqlite`: the op-sqlite adapter now retries `BEGIN IMMEDIATE` on `SQLITE_BUSY`.

Under concurrent writers, `BEGIN IMMEDIATE` can return `SQLITE_BUSY` when another writer holds the
write lock. `opSqliteDb` now retries the acquisition with async exponential backoff (~10→160ms, 5
attempts) before surfacing the error — rather than a `PRAGMA busy_timeout`, whose native blocking
would freeze the JS thread on op-sqlite's sync (JSI) path. Only `BEGIN` is retried: once it holds the
lock the statements inside the transaction don't contend. A non-busy error is never retried.
