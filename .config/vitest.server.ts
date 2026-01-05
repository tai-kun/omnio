import * as v from "valibot";
import { defineConfig } from "vitest/config";
import isDebugMode from "./_is-debug-mode.js";

export default defineConfig({
  esbuild: {
    target: [
      "es2020",
      "node22",
    ],
  },
  define: {
    __DEBUG__: String(isDebugMode()),
    __CLIENT__: "false",
    __SERVER__: "true",
    __FILE_SYSTEM__: JSON.stringify(v.parse(
      v.union([
        v.literal("memory"),
        v.literal("local"),
      ]),
      process.env["FILE_SYSTEM"],
    )),
  },
  test: {
    include: [
      "tests/**/*.test.ts",
    ],
    exclude: [
      "tests/**/*.client.test.ts",
    ],
  },
});
