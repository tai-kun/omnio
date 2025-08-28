import { test } from "vitest";
import getTypeName from "../src/get-type-name.js";

test.for<[target: unknown, typeName: string]>([
  [null, "null"],
  [undefined, "undefined"],
  [0, "number"],
  [0n, "bigint"],
  ["a", "string"],
  [true, "boolean"],
  [false, "boolean"],
  [Symbol("a"), "symbol"],
  [Symbol.for("a"), "symbol"],
  [() => {}, "Function"],
  [function*() {}, "GeneratorFunction"],
  [async function*() {}, "AsyncGeneratorFunction"],
  [Promise.resolve(), "Promise"],
  [/a/, "RegExp"],
  [new Date(), "Date"],
  [[], "Array"],
  [{}, "Object"],
  [new URL("http://example.com"), "URL"],
])("%o の型名は %s", ([target, typeName], { expect }) => {
  expect(getTypeName(target)).toBe(typeName);
});
