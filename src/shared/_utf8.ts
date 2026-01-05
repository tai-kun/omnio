import isArrayBuffer from "./_is-array-buffer.js";
import singleton from "./_singleton.js";

const B = 1;
const KiB = 1024 * B;
const BUFFER_SIZE = 9 * KiB;
const SAFE_STRING_LENGTH = 3 * KiB; // `BUFFER_SIZE` の 3 分の 1 に設定します。

/**
 * UTF-8 のエンコード時に再利用される共有バッファーを取得します。
 *
 * @returns 共有の `Uint8Array` バッファーです。
 */
function buffer(): Uint8Array<ArrayBuffer> {
  return singleton("utf8__buffer", () => new Uint8Array(BUFFER_SIZE));
}

/**
 * ブラウザと Node.js の両方で利用可能な TextDecoder のインターフェースです。
 */
interface ITextDecoder {
  /**
   * @param input ブラウザでは `ArrayBufferLike | ArrayBufferView<ArrayBufferLike>`、
   * Node.js では `NodeJS.ArrayBufferView | ArrayBuffer` です。
   */
  decode(input: Uint8Array<ArrayBufferLike> | ArrayBuffer): string;
}

/**
 * UTF-8 のデコード時に再利用される共有 `TextDecoder` のインスタンスを取得します。
 *
 * @returns 共有の `TextDecoder` インスタンスです。
 */
function decoder(): ITextDecoder {
  return singleton("utf8__decoder", () => (
    new TextDecoder("utf-8", {
      // 文字列のデコードパフォーマンスは落ちますが、より厳格になります。
      fatal: true,
      ignoreBOM: true,
    })
  ));
}

/**
 * エンコード結果です。
 */
export type EncodeIntoResult = {
  /**
   * 入力で読み取られた Unicode コードの単位です。
   */
  read: number;

  /**
   * 出力に書き込まれた UTF-8 バイト数です。
   */
  written: number;
};

/**
 * ブラウザと Node.js の両方で利用可能な `TextEncoder` のインターフェースです。
 */
interface ITextEncoder {
  /**
   * @returns ブラウザでは `Uint8Array<ArrayBuffer>`、Node.js では `Uint8Array<ArrayBufferLike>` です。
   */
  encode(input?: string): Uint8Array<ArrayBufferLike>;

  /**
   * @param destination ブラウザ、Node.js 共に `Uint8Array<ArrayBufferLike>` です。
   */
  encodeInto(source: string, destination: Uint8Array<ArrayBufferLike>): EncodeIntoResult;
}

/**
 * UTF-8 のエンコード時に再利用される共有 `TextEncoder` のインスタンスを取得します。
 *
 * @returns 共有の `TextEncoder` インスタンスです。
 */
function encoder(): ITextEncoder {
  return singleton("utf8__encoder", () => new TextEncoder());
}

/**
 * キャッシュの状態を返します。
 */
function cacheState() {
  return singleton("utf8__cache_state", () => ({
    enable: false,
    values: null as null | {
      source: string;
      buffer: Uint8Array<ArrayBuffer>;
    },
  }));
}

/**
 * UTF-8 のエンコード・デコードを行うためのユーティリティーオブジェクトです。
 * 頻繁なインスタンスの生成を避けるために、共有の `TextEncoder` と `TextDecoder` を使用します。
 */
const utf8 = {
  /**
   * 引数として渡されたバッファーを UTF-8 の形式でデコードした文字列を返します。
   *
   * @param input エンコードされたテキストが入っているバッファーまたはそのビューです。
   * @returns UTF-8 の形式でデコードされた文字列です。
   */
  decode(input: Uint8Array<ArrayBufferLike> | ArrayBuffer): string {
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
    const state = cacheState();
    if (state.enable && state.values && input === state.values.source) {
      return state.values.buffer.slice(); // コピーします。
    }

    if (input.length > SAFE_STRING_LENGTH) {
      // バッファーに収まらない可能性があるので、それを使わずにエンコードします。
      const encoded = encoder().encode(input);
      if (isArrayBuffer(encoded.buffer)) {
        return encoded as Uint8Array<ArrayBuffer>;
      }

      return encoded.slice();
    }

    // 高速化のために、文字列が一定の長さ以下であれば、事前に準備したバッファーに書き込むことで、頻繁なアロケーションを防止します。
    // 上の処理で文字列の長さが `SAFE_STRING_LENGTH` 以下であることは確定しています。
    // また、エンコード後の配列の長さが `.length` の 3 倍を超えることはないので、`BUFFER_SIZE` のバッファーに収まります。
    // 参考: https://developer.mozilla.org/docs/Web/API/TextEncoder/encodeInto
    const dst = buffer();
    const res = this.encodeInto(input, dst);
    const buf = dst.slice(0, res.written); // コピーします。
    if (state.enable) {
      state.values = {
        source: input,
        buffer: buf.slice(),
      };
    }

    return buf;
  },

  /**
   * エンコードする文字列と、UTF-8 エンコード後のテキスト格納先となるバッファーを受け取り、
   * エンコードの進行状況を示すオブジェクトを返します。
   *
   * @param source エンコードするテキストが入った文字列です。
   * @param destination バッファーに収まる範囲で UTF-8 エンコードされたテキストが入ります。
   * @returns エンコード結果です。
   */
  encodeInto(source: string, destination: Uint8Array<ArrayBufferLike>): EncodeIntoResult {
    const {
      read,
      written,
    } = encoder().encodeInto(source, destination);

    return {
      read,
      written,
    };
  },

  /**
   * 引数として渡された文字列またはバッファーが有効な UTF-8 文字列であるかどうかを返します。
   *
   * @param input 文字列またはバッファーです。
   * @returns `input` が有効な UTF-8 文字列である場合は `true`、それ以外の場合は `false` です。
   */
  isValidUtf8(input: string | Uint8Array<ArrayBufferLike> | ArrayBuffer): boolean {
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

  /**
   * キャッシュを有効にします。
   */
  enableCache(): void {
    cacheState().enable = true;
  },

  /**
   * キャッシュを無効にし、リソースを開放します。
   */
  disableCache(): void {
    const state = cacheState();
    state.enable = false;
    state.values = null;
  },
};

export default utf8;
