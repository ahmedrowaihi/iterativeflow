import { defineConfig } from "tsdown";

export default defineConfig({
  entry: ["src/index.ts", "src/cli.ts"],
  format: ["esm"],
  dts: true,
  clean: true,
  sourcemap: true,
  external: [
    /^drizzle-orm(?:\/|$)/,
    /^drizzle-kit(?:\/|$)/,
    /^graphile-worker(?:\/|$)/,
    /^pg(?:\/|$)/,
  ],
});
