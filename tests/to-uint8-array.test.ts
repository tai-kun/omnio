import { test } from "vitest";
import toUint8Array, { type Uint8ArraySource } from "../src/to-uint8-array.js";

test("文字列を Uint8Array に変換する", ({ expect }) => {
  const input: Uint8ArraySource = "Hello, World!";
  const result = toUint8Array(input);

  expect(result).toBeInstanceOf(Uint8Array);
  expect(result).toStrictEqual(new TextEncoder().encode(input));
});

test("Uint8Array をそのまま返す", ({ expect }) => {
  const input: Uint8ArraySource = new Uint8Array([1, 2, 3]);
  const result = toUint8Array(input);

  expect(result).toBe(input);
});

test("ArrayBufferView を Uint8Array に変換する", ({ expect }) => {
  const input: Uint8ArraySource = new Int8Array([1, 2, 3]);
  const result = toUint8Array(input);

  expect(result).toBeInstanceOf(Uint8Array);
  expect(result).toStrictEqual(new Uint8Array(input.buffer, input.byteOffset, input.byteLength));
});

test("ArrayBuffer を Uint8Array に変換する", ({ expect }) => {
  const input: Uint8ArraySource = new ArrayBuffer(3);
  const result = toUint8Array(input);

  expect(result).toBeInstanceOf(Uint8Array);
  expect(result).toStrictEqual(new Uint8Array(input));
});

test("サポートされていない型に対してエラーを投げる", ({ expect }) => {
  const input: any = 123;

  expect(() => toUint8Array(input)).toThrowError("Expected Uint8ArraySource, but got number");
});
