import singleton from "./_singleton.js";
import utf8 from "./_utf8.js";
import * as v from "./valibot.js";

function utf8EnableCache(x: string): string {
  utf8.enableCache();

  return x;
}

function utf8Encode(x: string): {
  source: string;
  buffer: Uint8Array<ArrayBufferLike>;
} {
  const y = utf8.encode(x); // キャッシュを無効化する前にエンコード処理を実行します。
  utf8.disableCache();

  return {
    source: x,
    buffer: y,
  };
}

export default function StringObjectPathSchema() {
  return singleton("string_object_path_schema", () => (
    v.pipe(
      // オブジェクトパスは文字列である必要があります。
      v.string(),
      // エンコードでオーバーヘッドが発生する前に `.length` で高速に検証します。
      v.minLength(1),
      v.maxLength(1024),
      // 最大 1024 バイトの有効な UTF-8 文字列である必要があります。
      v.transform(utf8EnableCache),
      v.utf8(),
      v.maxBytes(1024),
      // バッファー形式を含めて返します。
      v.transform(utf8Encode),
    )
  ));
}
