import { build } from "esbuild";

await build({
  entryPoints: ["frontend/web/src/main.ts"],
  outfile: "static/web/app.js",
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
