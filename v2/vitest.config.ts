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
  },
});
