import type { Brand } from "valibot";
import { expectTypeOf, test } from "vitest";
import { InvalidBucketNameError } from "../src/errors.js";
import { BucketName } from "../src/index.js";

// 検証に成功すべきバケット名
const VALID_NAMES = [
  "abc123",
  "my-bucket-name",
  "a1-b2-c3",
  "123456781234567812345678123456781234567812345678123456781234567", // 63 文字
];

// ドットを許容するモードでのみ有効なバケット名
const VALID_DOT_NAMES = [
  "my.bucket.name",
  "a.b-c.d-e",
  "bucket.name.with.dot",
];

// 無効なバケット名
const INVALID_NAMES = [
  "",
  "ab", // 3 文字未満
  "1234567812345678123456781234567812345678123456781234567812345678", // 64 文字以上

  "-bucket", // ハイフン始まり
  "bucket-", // ハイフン終わり
  "BucketName", // 大文字を含む
  "bucket_name", // アンダースコアを含む

  // 禁止プレフィックス/サフィックス
  "xn--example",
  "sthree-xyz",
  "amzn-s3-demo-test",
  "test-s3alias",
  "test--ol-s3",
  "test--x-s3",
  "test--table-s3",
];

// IP アドレス形式
const IP_LINK_NAMES = [
  "192.168.0.1",
  "10.0.0.1",
];

// ピリオドが連続するケース
const DOUBLE_DOT_NAMES = [
  "my..bucket",
  "a..b",
];

test.for(VALID_NAMES)("$0 を検証して成功", (name, { expect }) => {
  expect(BucketName.parse(name)).toBe(name);
  expect(BucketName.validate(name)).toBe(true);
});

test.for(VALID_DOT_NAMES)("$0 を検証してエラー", (name, { expect }) => {
  expect(() => BucketName.parse(name)).toThrow(InvalidBucketNameError);
  expect(BucketName.validate(name)).toBe(false);
});

test.for(VALID_DOT_NAMES)("$0 を検証して成功", (name, { expect }) => {
  expect(BucketName.parse(name, { allowDot: true })).toBe(name);
  expect(BucketName.validate(name, { allowDot: true })).toBe(true);
});

test.for(INVALID_NAMES)("$0 を検証してエラー", (name, { expect }) => {
  expect(() => BucketName.parse(name)).toThrow(InvalidBucketNameError);
  expect(BucketName.validate(name)).toBe(false);
});

test.for(IP_LINK_NAMES)("$0 を検証してエラー", (name, { expect }) => {
  expect(() => BucketName.parse(name)).toThrow(InvalidBucketNameError);
  expect(BucketName.validate(name)).toBe(false);
});

test.for(DOUBLE_DOT_NAMES)("$0 を検証してエラー", (name, { expect }) => {
  expect(() => BucketName.parse(name)).toThrow(InvalidBucketNameError);
  expect(BucketName.validate(name)).toBe(false);
});

test("型チェック", () => {
  expectTypeOf(BucketName.parse("test")).toEqualTypeOf<"test" & Brand<"BucketName">>();
  expectTypeOf(BucketName.parse("test" as string)).toEqualTypeOf<string & Brand<"BucketName">>();
});
