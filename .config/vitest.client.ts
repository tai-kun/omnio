import { defineConfig } from "vitest/config";

export default defineConfig({
  esbuild: {
    target: "safari15",
  },
  optimizeDeps: {
    exclude: [
      "@duckdb/node-api",
      "memfs",
    ],
  },
  define: {
    __DEBUG__: ["DEBUG", "RUNNER_DEBUG", "ACTIONS_RUNNER_DEBUG", "ACTIONS_STEP_DEBUG"]
      .some(k => ["1", "true"].includes(process.env[k]?.toLowerCase()!))
      .toString(),
    __CLIENT__: "true",
    __SERVER__: "false",
  },
  test: {
    include: [
      "tests\/**\/*.test.ts",
    ],
    exclude: [
      "tests\/**\/*.server.test.ts",
    ],
    browser: {
      provider: "playwright",
      enabled: true,
      headless: true,
      instances: [
        {
          browser: "chromium",
          context: {
            permissions: [
              "storage-access",
            ],
          },
        },
      ],
    },
  },
});
