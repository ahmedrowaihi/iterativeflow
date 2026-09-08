---
"@iterativeflow/core": patch
---

Release-pipeline integrity: verify the version PR, protect the publish, pin the formatter, and fix
the npm source links.

- **The version commit is now verified.** CI ran on `main` and on pull requests, but a push made with
  `GITHUB_TOKEN` fires no workflow events at all, so the version PR's own checks never ran — the one
  commit that rewrites all 12 manifests, every changelog and the lockfile was the one commit nothing
  checked. Adding a push trigger for that branch does not help, for the same reason. The release job
  now installs from the regenerated lockfile and runs typecheck + build against the versioned tree
  before opening the PR, so a broken lockfile fails there instead of after merge.
- **A publish can no longer be cancelled halfway.** Workflow-level `cancel-in-progress` covered the
  release job, so a second push during a release could interrupt `changeset publish` mid-loop and
  leave npm with a partial `fixed` version set — some packages at the new version depending on
  siblings that never published, and npm publishes are not revocable. Cancellation now applies to the
  test job only; the release job has its own non-cancelling group.
- **`oxfmt` is pinned.** The pre-commit hook ran it via `npx` with no version and no lockfile entry,
  so every contributor fetched whatever was latest and could reformat the same file differently. It's
  now a pinned dev dependency invoked from the local binary, with `format` / `format:check` scripts
  and a CI check. Six files that had drifted are formatted.
- **npm's source links resolve.** All 12 manifests pointed `repository.directory` at `v2/packages/*`,
  a path that doesn't exist in this repo, so every package's "source" link 404'd.
- **The public-API gate can't be outgrown.** `api-snapshot.mjs` had a hardcoded 12-package list, so a
  13th package would ship with no `etc/*.api.md` and the `git diff --exit-code etc` check would pass
  because nothing was generated for it. The list is derived from the workspace now, and a missing
  `dist/` fails with a clear "run build first" instead of a raw ENOENT.
