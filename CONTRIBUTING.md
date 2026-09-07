# Contributing

## Prerequisites

- **Node 22.5 or newer.** The published packages run on Node 20, but the test tooling does not:
  `packages/durable-objects` runs against the built-in `node:sqlite` (added in 22.5), and the
  testcontainers/undici stack needs 22+.
- **pnpm** via corepack — the version is pinned in `package.json`:
  ```bash
  corepack enable
  ```
- **Docker**, optional. Postgres, MySQL, MongoDB, Redis and DynamoDB Local start themselves through
  testcontainers. Without Docker, see below.

```bash
pnpm install
```

## Verify

The same four gates CI runs, in the order that fails fastest:

```bash
pnpm run typecheck
pnpm run lint
pnpm run format:check
pnpm test
```

If you changed the public API of any package, regenerate the committed surface and commit it — CI
gates on `git diff --exit-code etc`:

```bash
pnpm run api:check
```

### Without Docker

```bash
SKIP_TESTCONTAINERS=1 pnpm test
```

That skips the five server backends and runs memory, sqlite, durable-objects, core, dashboard and
webhooks. It is what the pre-push hook uses. **CI always runs the full suite** — a backend change
needs Docker locally, or the CI run on your PR.

## Conventions

`AGENTS.md` is the short version and takes precedence: near-zero inline comments, JSDoc on the public
boundary only, ports own the contract, and every backend passes every conformance suite.

Two rules worth repeating because they are the most common review comments:

- **A new field on an interface whose siblings are bare stays bare.** The reason goes in the JSDoc on
  the type, not stacked above the field.
- **Cross-backend behavior belongs in `packages/conformance`**, so all 8 backends prove it. Core-only
  logic gets a memory engine test. Write the test to force the hard case — the race, the boundary,
  the exhaustion path.

## Changesets

Every user-visible change needs one:

```bash
pnpm changeset
```

All `@iterativeflow/*` packages version together (`fixed`), so one changeset with the lead package's
bump is enough. Say what changed and why, not just what.
