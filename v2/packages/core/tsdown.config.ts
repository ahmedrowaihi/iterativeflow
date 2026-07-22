import { defineConfig } from "tsdown";

export default defineConfig({
  entry: ["src/index.ts", "src/backend.ts"],
  format: ["esm"],
  dts: true,
  clean: true,
});
