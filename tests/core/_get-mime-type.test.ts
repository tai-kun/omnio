import { test } from "vitest";
import getMimeType from "../../src/core/_get-mime-type.js";

test("一般的なファイル拡張子から MIME タイプが取得できる。", ({ expect }) => {
  const typeCss = getMimeType("document.css");
  const typeHtml = getMimeType("/path/to/file.html");
  const typeJson = getMimeType("./data/config.json");

  expect(typeCss).toBe("text/css");
  expect(typeHtml).toBe("text/html");
  expect(typeJson).toBe("application/json");
});

test("拡張子がない場合、デフォルト値 'application/octet-stream' が返される。", ({ expect }) => {
  const mimeType = getMimeType("/path/to/file-without-extension");

  expect(mimeType).toBe("application/octet-stream");
});

// 正常系のテスト: defaultType にカスタム値を指定した場合、その値が返されることの確認。
test("カスタムの defaultType が指定された場合、拡張子がないファイルに対してそれが返される。", ({ expect }) => {
  const mimeType = getMimeType("/path/to/another-file", "text/plain");

  expect(mimeType).toBe("text/plain");
});

test("不正な MIME タイプが defaultType として渡された場合、Valibot の検証エラーが発生する", ({ expect }) => {
  expect(() => getMimeType("/path/to/unknown", "invalid mime type")).toThrow();
});
