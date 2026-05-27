import { defineConfig } from "tsdown";

export default defineConfig({
  entry: ["src/index.ts", "src/storage/schema.ts", "src/storage/relations.ts"],
  format: ["esm"],
  dts: true,
  clean: true,
  sourcemap: true,
  // Peers + node builtins stay external; drizzle-kit is test-only and must
  // never be pulled into the published bundle.
  external: ["drizzle-orm", "graphile-worker", "pg", "drizzle-kit"],
});
