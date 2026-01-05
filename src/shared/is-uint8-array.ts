import isArrayBuffer from "./_is-array-buffer.js";

/**
 * 引数に与えられた値が `buffer` に `ArrayBuffer` を持つ `Uint8Array` オブジェクトかどうか判定します。
 *
 * @param value  `Uint8Array<ArrayBuffer>` オブジェクトであるか検証する値です。
 * @returns `value` が `Uint8Array<ArrayBuffer>` オブジェクトであれば `true`、そうでなければ `false` です。
 */
export default function isUint8Array(value: unknown): value is Uint8Array<ArrayBuffer> {
  return value instanceof Uint8Array && isArrayBuffer(value.buffer);
}
