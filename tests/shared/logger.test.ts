import { test } from "vitest";
import { LogLevel } from "../../src/shared/logger.js";

test("LogLevel の値はすべて異なる", ({ expect }) => {
  const uniqueKeyCount = Object.keys(LogLevel).length;
  const uniqueValueCount = new Set(Object.values(LogLevel)).size;

  expect(uniqueKeyCount).toBe(uniqueValueCount);
});
