import { test } from "vitest";
import isArrayBuffer from "../../src/shared/_is-array-buffer.js";

test("ArrayBuffer インスタンスには true を返す", ({ expect }) => {
  expect(isArrayBuffer(new ArrayBuffer(8))).toBe(true);
});

test("TypedArray には false を返す", ({ expect }) => {
  const buffer = new ArrayBuffer(8);

  expect.soft(isArrayBuffer(new Uint8Array(buffer))).toBe(false);
  expect.soft(isArrayBuffer(new Int32Array(buffer))).toBe(false);
  expect.soft(isArrayBuffer(new Float64Array(buffer))).toBe(false);
});

test("DataView には false を返す", ({ expect }) => {
  expect(isArrayBuffer(new DataView(new ArrayBuffer(8)))).toBe(false);
});

test.skipIf(typeof SharedArrayBuffer === "undefined")(
  "SharedArrayBuffer は false を返す",
  ({ expect }) => {
    expect(isArrayBuffer(new SharedArrayBuffer(8))).toBe(false);
  },
);
