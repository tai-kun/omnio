import { test } from "vitest";
import {
  MAX_NUM_PARTS,
  MAX_OBJECT_SIZE,
  MAX_PART_SIZE,
  MIN_NUM_PARTS,
  MIN_PART_SIZE,
} from "../../src/shared/schemas.js";

test("最大オブジェクトサイズは 5 TB", ({ expect }) => {
  expect(MAX_OBJECT_SIZE).toBe(5_000_000_000_000);
});

test("最小パート数は 0", ({ expect }) => {
  expect(MIN_NUM_PARTS).toBe(0);
});

test("最大パート数は 1 万", ({ expect }) => {
  expect(MAX_NUM_PARTS).toBe(10_000);
});

test("最小パートサイズは 5 MB", ({ expect }) => {
  expect(MIN_PART_SIZE).toBe(5_000_000);
});

test("最大パートサイズは 5 GB", ({ expect }) => {
  expect(MAX_PART_SIZE).toBe(5_000_000_000);
});
