import { playwright } from "@vitest/browser-playwright";
import * as v from "valibot";
import { defineConfig } from "vitest/config";
import isDebugMode from "./_is-debug-mode.js";

export default defineConfig({
  esbuild: {
    target: "es2020",
  },
  define: {
    __DEBUG__: String(isDebugMode()),
    __CLIENT__: "true",
    __SERVER__: "false",
    __FILE_SYSTEM__: JSON.stringify(v.parse(
      v.union([
        v.literal("memory"),
        v.literal("opfs"),
      ]),
      process.env["FILE_SYSTEM"],
    )),
  },
  test: {
    include: [
      "tests/**/*.test.ts",
    ],
    exclude: [
      "tests/**/*.server.test.ts",
    ],
    browser: {
      provider: playwright({
        contextOptions: {
          permissions: [
            "storage-access",
          ],
        },
      }),
      enabled: true,
      headless: true,
      instances: [
        { browser: "chromium" },
      ],
    },
  },
  optimizeDeps: {
    exclude: [
      "@duckdb/node-api",
    ],
  },
});
