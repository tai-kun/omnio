import { test } from "vitest";
import isError from "../../src/shared/is-error.js";

test("現在のプロセスが作成した Error オブジェクトを入力すると true を返す", ({ expect }) => {
  expect(isError(new Error())).toBe(true);
});
