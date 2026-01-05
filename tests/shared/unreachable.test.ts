import { expect, test } from "vitest";
import { UnreachableError } from "../../src/shared/errors.js";
import unreachable from "../../src/shared/unreachable.js";

test("引数なしで呼び出された場合、必ずエラーを投げる", () => {
  expect(() => unreachable()).toThrow(UnreachableError);
});

test("値を伴って呼び出された場合、必ずエラーを投げる", () => {
  const value = "予期せぬ文字列";

  expect(() => unreachable(value as never)).toThrow(UnreachableError);
});
