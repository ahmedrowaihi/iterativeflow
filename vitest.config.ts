import { resolve } from "node:path";
import { defineConfig } from "vitest/config";
import tsconfig from "./tsconfig.json" with { type: "json" };

export default defineConfig({
  resolve: {
    // Longest first: `@iterativeflow/core/backend` must match before the bare `@iterativeflow/core`.
    alias: Object.entries(tsconfig.compilerOptions.paths)
      .sort(([a], [b]) => b.length - a.length)
      .map(([find, [target]]) => ({ find, replacement: resolve(import.meta.dirname, target) })),
  },
  test: {
    environment: "node",
    root: import.meta.dirname,
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
