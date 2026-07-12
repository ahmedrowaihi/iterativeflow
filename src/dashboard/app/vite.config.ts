import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import preact from "@preact/preset-vite";
import tailwindcss from "@tailwindcss/vite";
import franken from "franken-ui/plugin-vite";
import { defineConfig } from "vite";

const here = dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  root: here,
  base: "./",
  resolve: {
    alias: {
      "@": here,
      react: "preact/compat",
      "react-dom": "preact/compat",
      "react/jsx-runtime": "preact/jsx-runtime",
    },
  },
  plugins: [preact(), franken({ preflight: false, layer: true }), tailwindcss()],
  build: {
    outDir: join(here, "..", "..", "..", "dist", "dashboard"),
    emptyOutDir: true,
    chunkSizeWarningLimit: 2000,
  },
});
