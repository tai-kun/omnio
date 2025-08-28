import { TypeError } from "./errors.js";
import isArrayBuffer from "./is-array-buffer.js";
import isUint8Array from "./is-uint8-array.js";
import utf8 from "./utf8.js";

/**
 * `Uint8Array` に変換可能な値の型です。
 */
export type Uint8ArraySource = string | ArrayBuffer | ArrayBufferView<ArrayBuffer>;

/**
 * 与えられた値を `Uint8Array` に変換します。
 *
 * @param source `Uint8Array` に変換する値です。
 * `string`、`ArrayBuffer`、または `ArrayBufferView` を受け入れます。
 * @returns 変換された `Uint8Array` です。
 */
export default function toUint8Array(source: Uint8ArraySource): Uint8Array<ArrayBuffer> {
  switch (true) {
    case typeof source === "string":
      return utf8.encode(source);

    case isUint8Array(source):
      return source;

    case ArrayBuffer.isView(source):
      return new Uint8Array(source.buffer, source.byteOffset, source.byteLength);

    case isArrayBuffer(source):
      return new Uint8Array(source);

    default:
      throw new TypeError("Uint8ArraySource", source);
  }
}
