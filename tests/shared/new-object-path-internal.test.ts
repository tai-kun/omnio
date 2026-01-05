import { test } from "vitest";
import newObjectPathInternal from "../../src/shared/new-object-path-internal.js";
import ObjectPath from "../../src/shared/object-path.js";

test("検証済みの信頼できる値を使って新しい `ObjectPath` を作成", ({ expect }) => {
  expect(newObjectPathInternal(new Uint8Array(), "")).toBeInstanceOf(ObjectPath);
});
