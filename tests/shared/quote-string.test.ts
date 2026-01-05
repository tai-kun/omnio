import { test } from "vitest";
import quoteString from "../../src/shared/quote-string.js";

test("文字列を二重引用符で囲う", ({ expect }) => {
  expect(quoteString("あ")).toBe(`"あ"`);
});

test("二重引用符はエスケープされる", ({ expect }) => {
  expect(quoteString(`"`)).toBe(`"\\""`);
});
