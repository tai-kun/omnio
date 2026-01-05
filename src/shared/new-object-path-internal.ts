import objectPathInternalUse from "./_object-path-internal-use.js";
import ObjectPath from "./object-path.js";

/**
 * 検証済みの信頼できる値を使って新しい `ObjectPath` を作成します。
 *
 * @param buffer オブジェクトパスのバッファー表現です。
 * @param string オブジェクトパスの文字列表現です。
 * @returns 新しい `ObjectPath` です。
 */
export default function newObjectPathInternal(
  buffer: Uint8Array<ArrayBufferLike>,
  string: string,
): ObjectPath {
  try {
    objectPathInternalUse.enable = true;
    // 内部利用ではエンコードのオーバーヘッドを減らすために検証済みの値を渡します。
    // @ts-expect-error
    return new ObjectPath(buffer, string);
  } finally {
    objectPathInternalUse.enable = false;
  }
}
