import * as v from "valibot";
import { test } from "vitest";
import getMimeType from "../src/get-mime-type.js";

test("既知の拡張子 .txt に対して適切な MIME タイプを返す", ({ expect }) => {
  expect(getMimeType("example.txt")).toBe("text/plain");
});

test("未知の拡張子に対して defaultType を省略した場合、application/octet-stream を返す", ({ expect }) => {
  expect(getMimeType("example.unknownext")).toBe("application/octet-stream");
});

test("未知の拡張子に対して defaultType を指定した場合、それを返す", ({ expect }) => {
  expect(getMimeType("example.unknownext", "video/mp4")).toBe("video/mp4");
});

test("既知の拡張子に対して defaultType を指定しても、拡張子から得られる MIME タイプを優先して返す", ({ expect }) => {
  const result = getMimeType("example.html", "application/override");
  expect(result).toBe("text/html");
});

test("defaultType に無効な MIME タイプを指定した場合、エラーが投げられる", ({ expect }) => {
  expect(() => {
    getMimeType("example.unknownext", "invalid/type!");
  }).toThrow(v.ValiError);
});
