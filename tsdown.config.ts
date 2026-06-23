import { defineConfig } from "tsdown";

export default defineConfig({
  entry: ["src/index.ts", "src/cli.ts", "src/serverless.ts", "src/pgmq.ts"],
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
