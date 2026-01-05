import { describe, test } from "vitest";
import utf8 from "../../src/shared/_utf8.js";

describe("decode", () => {
  test("有効な UTF-8 バッファーをデコードできる", ({ expect }) => {
    const encoder = new TextEncoder();
    const encoded = encoder.encode("こんにちは、世界！");

    expect(utf8.decode(encoded)).toBe("こんにちは、世界！");
  });

  test("不正な UTF-8 バッファーで TypeError を投げる", ({ expect }) => {
    const invalidUtf8 = new Uint8Array([0xC0, 0x80]);

    expect(() => utf8.decode(invalidUtf8)).toThrow(TypeError);
  });
});

describe("encode", () => {
  test("文字列を UTF-8 Uint8Array にエンコードできる", ({ expect }) => {
    const expected = new TextEncoder().encode("テスト文字列");
    const actual = utf8.encode("テスト文字列");

    expect(actual).toStrictEqual(expected);
    expect(actual instanceof Uint8Array).toBe(true);
  });

  test("空文字列をエンコードできる", ({ expect }) => {
    const expected = new TextEncoder().encode("");

    expect(utf8.encode("")).toStrictEqual(expected);
  });
});

describe("encodeInto", () => {
  test("文字列を指定された Uint8Array にエンコードできる", ({ expect }) => {
    const inputString = "Hello!";
    const destBuffer = new Uint8Array(10);
    const result = utf8.encodeInto(inputString, destBuffer);
    const expectedEncoded = new TextEncoder().encode(inputString);

    expect(destBuffer.slice(0, result.written)).toStrictEqual(expectedEncoded);
    expect(result.read).toBe(inputString.length);
    expect(result.written).toBe(expectedEncoded.length);
  });

  test("バッファーが小さい場合、部分的にエンコードし正しい結果を返す", ({ expect }) => {
    const inputString = "長い文字列をエンコードする。";
    const destBuffer = new Uint8Array(5); // 小さすぎるバッファー
    const result = utf8.encodeInto(inputString, destBuffer);

    // 部分的にエンコードされた結果を検証する。
    const partialExpected = new Uint8Array([233, 149, 183, 0, 0]);
    //                                      ~~~~~~~~~~~~~
    //                                            長

    expect(destBuffer).toStrictEqual(partialExpected);
    expect(result.written).toBe(3); // エンコードできる分だけ書き込まれている。
    expect(result.read).toBeLessThan(inputString.length); // 全ては読み込まれていない。
  });

  test("空文字列をエンコードできる", ({ expect }) => {
    const destBuffer = new Uint8Array(10);
    const result = utf8.encodeInto("", destBuffer);

    expect(result.read).toBe(0);
    expect(result.written).toBe(0);
    expect(destBuffer.every(byte => byte === 0)).toBe(true); // バッファーが変更されていないことを確認する。
  });
});

describe("isValidUtf8", () => {
  test("有効な文字列に対して true を返す", ({ expect }) => {
    expect(utf8.isValidUtf8("有効な文字列")).toBe(true);
    expect(utf8.isValidUtf8("English string")).toBe(true);
    expect(utf8.isValidUtf8("")).toBe(true);
  });

  test("有効な Uint8Array に対して true を返す", ({ expect }) => {
    const encoder = new TextEncoder();

    expect(utf8.isValidUtf8(encoder.encode("有効なバイト列"))).toBe(true);
  });

  test("不正な Uint8Array に対して false を返す", ({ expect }) => {
    const incompleteUtf8 = new Uint8Array([0xE3, 0x81]); // 「あ」は 0xE3 0x81 0x82

    expect(utf8.isValidUtf8(incompleteUtf8)).toBe(false);
  });
});
