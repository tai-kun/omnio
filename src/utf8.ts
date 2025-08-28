import memo from "./memo.js";

const KiB = 1024;
const BUFFER_SIZE = 6 * KiB;
const SAFE_STRING_LENGTH = 2 * KiB; // BUFFER_SIZE の 3 分の 1

/**
 * `Uint8Array` または Node.js の `Buffer` のどちらかを表す型です。
 */
export type Uint8ArrayLike =
  | Uint8Array
  | (typeof globalThis extends { Buffer: new(...args: any) => infer B } ? B : never);

/**
 * UTF-8 のエンコード・デコードを行うためのユーティリティーオブジェクトです。
 * 頻繁なインスタンスの生成を避けるために、共有の `TextEncoder` と `TextDecoder` を使用します。
 */
export default {
  /**
   * 引数として渡されたバッファーを UTF-8 の形式でデコードした文字列を返します。
   *
   * @param input エンコードされたテキストが入っている `Uint8ArrayLike` です。
   * @returns UTF-8 の形式でデコードされた文字列です。
   */
  decode(input: AllowSharedBufferSource): string {
    return decoder().decode(input);
  },

  /**
   * 引数として渡された文字列をエンコードして `Uint8Array` を返します。
   * 文字列が短い場合は事前に確保された共有バッファーを再利用することで、パフォーマンスを向上させます。
   *
   * @param input エンコードするテキストが入った文字列です。
   * @returns エンコードされた `Uint8Array` です。
   */
  encode(input: string): Uint8Array<ArrayBuffer> {
    if (input.length > SAFE_STRING_LENGTH) {
      // バッファーに収まらない可能性があるので、それを使わずにエンコードします。
      return encoder().encode(input);
    }

    // 高速化のために、文字列が一定の長さ以下であれば、事前に準備したバッファーに書き込むことで、頻繁なアロケーションを防止します。
    // 上の処理で文字列の長さが SAFE_STRING_LENGTH 以下であることは確定しています。
    // また、エンコード後の配列の長さが .length の 3 倍を超えることはないので、BUFFER_SIZE のバッファーに収まります。
    // 参考: https://developer.mozilla.org/docs/Web/API/TextEncoder/encodeInto
    const buf = buffer();
    const res = this.encodeInto(input, buf);
    return buf.slice(0, res.written); // コピー
  },

  /**
   * エンコードする文字列と、UTF-8 エンコード後のテキスト格納先となるバッファーを受け取り、
   * エンコードの進行状況を示すオブジェクトを返します。
   *
   * @param input エンコードするテキストが入った文字列です。
   * @param dest バッファーに収まる範囲で UTF-8 エンコードされたテキストが入ります。
   * @returns エンコード結果です。
   */
  encodeInto(input: string, dest: Uint8ArrayLike): TextEncoderEncodeIntoResult {
    return encoder().encodeInto(input, dest);
  },

  /**
   * 引数として渡された文字列またはバッファーが有効な UTF-8 文字列であるかどうかを返します。
   *
   * @param input 文字列またはバッファーです。
   * @returns `input` が有効な UTF-8 文字列である場合は `true`、それ以外の場合は `false` です。
   */
  isValidUtf8(input: string | Uint8ArrayLike): boolean {
    if (typeof input === "string") {
      input = this.encode(input);
    }

    try {
      this.decode(input); // 不正な UTF-8 でエラーが投げられます。
      return true;
    } catch {
      return false;
    }
  },
};

/**
 * UTF-8 のエンコード時に再利用される共有バッファーを取得します。
 *
 * @returns 共有の `Uint8Array` バッファーです。
 */
function buffer(): Uint8Array<ArrayBuffer> {
  return memo.create("utf8__buffer", () => new Uint8Array(BUFFER_SIZE));
}

/**
 * UTF-8 のデコード時に再利用される共有 `TextDecoder` のインスタンスを取得します。
 *
 * @returns 共有の `TextDecoder` インスタンスです。
 */
function decoder(): TextDecoder {
  return memo.create("utf8__decoder", () => (
    new TextDecoder("utf-8", {
      // 文字列のデコードパフォーマンスは落ちるが、より厳格になる。
      fatal: true,
      ignoreBOM: true,
    })
  ));
}

/**
 * UTF-8 のエンコード時に再利用される共有 `TextEncoder` のインスタンスを取得します。
 *
 * @returns 共有の `TextEncoder` インスタンスです。
 */
function encoder(): TextEncoder {
  return memo.create("utf8__encoder", () => new TextEncoder());
}
