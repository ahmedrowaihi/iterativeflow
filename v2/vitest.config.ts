import { resolve } from "node:path";
import { defineConfig } from "vitest/config";

// Isolated from the v1 suite (root config only globs src/ + tests/). Path aliases mirror
// tsconfig so cross-package imports resolve without a workspace install.
const pkg = (name: string): string => resolve(__dirname, `packages/${name}/src/index.ts`);

export default defineConfig({
  resolve: {
    // Ordered: the `/backend` subpath must match before the bare `@iterativeflow/core` prefix.
    alias: [
      {
        find: "@iterativeflow/core/backend",
        replacement: resolve(__dirname, "packages/core/src/backend.ts"),
      },
      { find: "@iterativeflow/core", replacement: pkg("core") },
      { find: "@iterativeflow/conformance", replacement: pkg("conformance") },
      { find: "@iterativeflow/memory", replacement: pkg("memory") },
      { find: "@iterativeflow/postgres", replacement: pkg("postgres") },
      { find: "@iterativeflow/dynamodb", replacement: pkg("dynamodb") },
    ],
  },
  test: {
    globals: true,
    environment: "node",
    root: __dirname,
    include: ["packages/*/src/**/*.test.ts"],
    coverage: {
      provider: "v8",
      include: ["packages/*/src/**"],
      exclude: ["packages/*/src/**/*.test.ts", "packages/conformance/src/**"],
      // A floor on the backend-agnostic engine, not a target — raise it as the
      // cross-backend suites grow. Backends need Docker, so their lines don't
      // count toward this gate.
      thresholds: {
        "packages/core/src/**": { lines: 70, functions: 70 },
      },
    },
  },
});
