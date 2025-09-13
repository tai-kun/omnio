import { defineConfig } from "vitest/config";

export default defineConfig({
  esbuild: {
    target: "node22",
  },
  define: {
    __DEBUG__: ["DEBUG", "RUNNER_DEBUG", "ACTIONS_RUNNER_DEBUG", "ACTIONS_STEP_DEBUG"]
      .some(k => ["1", "true"].includes(process.env[k]?.toLowerCase()!))
      .toString(),
    __CLIENT__: "false",
    __SERVER__: "true",
  },
  test: {
    include: [
      "**\/*.test.ts?(x)",
    ],
    exclude: [
      "**\/*.client.test.ts?(x)",
      ".temp/**",
    ],
  },
});
