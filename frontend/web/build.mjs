import { build } from "esbuild";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const webDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(webDir, "../..");

await build({
  entryPoints: [resolve(webDir, "src/main.ts")],
  outfile: resolve(repoRoot, "static/web/app.js"),
  bundle: true,
  format: "iife",
  platform: "browser",
  target: "es2021",
  define: {
    "process.env.NODE_ENV": '"production"',
  },
  minify: true,
  sourcemap: false,
  loader: {
    ".css": "css",
  },
});
