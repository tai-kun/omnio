import { test } from "vitest";
import objectPathInternalUse from "../../src/shared/_object-path-internal-use.js";

test("真偽値の enable プロパティーのみを持ち、初期値は false", ({ expect }) => {
  expect(objectPathInternalUse).toStrictEqual({
    enable: false,
  });
});
