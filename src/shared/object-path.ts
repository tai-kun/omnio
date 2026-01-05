import objectPathInternalUse from "./_object-path-internal-use.js";
import utf8 from "./_utf8.js";
import newObjectPathInternal from "./new-object-path-internal.js";
import StringObjectPathSchema from "./string-object-path-schema.js";
import unreachable from "./unreachable.js";
import * as v from "./valibot.js";

const DOT = 46; // "." の UTF-8 コードポイントです。
const SLASH = 47; // "/" の UTF-8 コードポイントです。

/**
 * オブジェクトパスを表すクラスです。
 */
export default class ObjectPath {
  /**
   * オブジェクトパスを解析します。
   *
   * @param objectPath 解析するオブジェクトパスです。
   * @returns 解析された `ObjectPath` オブジェクトです。
   */
  public static parse(objectPath: string): ObjectPath {
    return new ObjectPath(objectPath);
  }

  /**
   * オブジェクトパスが有効かどうか検証します。
   *
   * @param objectPath 検証するオブジェクトパスです。
   * @returns 有効なオブジェクトパスなら `true`、そうでないなら `false` です。
   */
  public static validate(objectPath: string): boolean {
    try {
      // objectPath が解析時に変更されると、元の値と異なる可能性があります。
      return ObjectPath.parse(objectPath).fullpath === objectPath;
    } catch {
      return false;
    }
  }

  /**
   * オブジェクトパスの UTF-8 バイト列です。
   */
  readonly #pathBuff: Uint8Array<ArrayBufferLike>;

  /**
   * オブジェクトパスの完全な文字列表現です。
   */
  readonly #fullpath: string;

  /**
   * パスセグメントの配列です。
   */
  #segments?: readonly string[];

  /**
   * ディレクトリー名です。
   */
  #dirname?: string;

  /**
   * ベースネームの UTF-8 バイト列です。
   */
  #basenameBuff?: Uint8Array<ArrayBufferLike> | null;

  /**
   * ベースネームです。
   */
  #basename?: string;

  /**
   * 拡張子なしのオブジェクト名です。
   */
  #filename?: string;

  /**
   * 拡張子です。
   */
  #extname?: string;

  /**
   * `ObjectPath` の新しいインスタンスを構築します。
   *
   * @param objectPath オブジェクトパスの文字列です。
   */
  public constructor(objectPath: string) {
    if (objectPathInternalUse.enable) {
      // 内部利用ではエンコードのオーバーヘッドを減らすためにバッファーとそれをエンコードした文字列が引数に渡されます。

      const buffer = arguments[0] as Uint8Array<ArrayBufferLike>;
      if (!(buffer instanceof Uint8Array)) {
        unreachable(buffer);
      }

      const source = arguments[1] as string;
      if (typeof source !== "string") {
        unreachable(source);
      }

      this.#pathBuff = buffer;
      this.#fullpath = source;
    } else {
      const {
        buffer,
        source,
      } = v.parse(StringObjectPathSchema(), objectPath);

      this.#pathBuff = buffer;
      this.#fullpath = source;
    }
  }

  /**
   * オブジェクトの完全なパスを取得します。
   */
  public get fullpath(): string {
    return this.#fullpath;
  }

  /**
   * オブジェクトパスのセグメントの配列を取得します。
   * 一番最後のセグメントは `basename` と同じです。
   */
  public get segments(): [...string[], string] {
    if (this.#segments === undefined) {
      const segments: string[] = [];
      let buff = this.#pathBuff;
      for (let i = 0, j = 0; j < this.#pathBuff.length; j++, i++) {
        if (this.#pathBuff[j] === SLASH) {
          segments.push(utf8.decode(buff.slice(0, i)));
          buff = buff.slice(i + 1);
          i = -1; // リセット
        }
      }

      segments.push(utf8.decode(buff));
      this.#basenameBuff = buff;
      this.#segments = segments;
    }

    const cloned = this.#segments.slice() as [...string[], string];
    return cloned;
  }

  /**
   * ディレクトリーのパスを取得します。
   */
  public get dirname(): string {
    return this.#dirname ??= this.segments.slice(0, -1).join("/");
  }

  /**
   * 拡張子付きのオブジェクト名を取得します。
   *
   * @example "file.txt"
   */
  public get basename(): string {
    if (this.#basename === undefined) {
      const segments = this.segments;
      this.#basename = segments[segments.length - 1]!;
    }

    return this.#basename;
  }

  /**
   * 拡張子を除いたオブジェクト名を取得します。
   */
  public get filename(): string {
    if (this.#filename === undefined) {
      this.segments; // ゲッターを呼び出して、this.#basenameBuff を計算します。
      let filename: Uint8Array;
      let extname: Uint8Array;
      const lastDotIndex = this.#basenameBuff!.lastIndexOf(DOT);
      if (lastDotIndex === -1 || lastDotIndex === 0) {
        filename = this.#basenameBuff!; // 例えば拡張子なしの Makefile や隠しファイルの .bashrc など
        extname = new Uint8Array(0);
      } else {
        filename = this.#basenameBuff!.slice(0, lastDotIndex);
        extname = this.#basenameBuff!.slice(lastDotIndex);
      }

      this.#filename = utf8.decode(filename);
      this.#extname = utf8.decode(extname);
      this.#basenameBuff = null; // 使い終わったので破棄
    }

    return this.#filename!;
  }

  /**
   * オブジェクトの拡張子を取得します。ドット (.) から始まります。
   *
   * @example ".txt"
   */
  public get extname(): string {
    if (this.#extname === undefined) {
      this.filename; // this.#extname を計算します。
    }

    return this.#extname!;
  }

  /**
   * `JSON.stringify` で使用される、オブジェクトの文字列表現を返します。
   *
   * @returns パスの文字列表現です。
   */
  public toJSON(): string {
    return this.#fullpath;
  }

  /**
   * オブジェクトの文字列表現を返します。
   *
   * @returns パスの文字列表現です。
   */
  public toString(): string {
    return this.#fullpath;
  }

  /**
   * この `ObjectPath` オブジェクトの複製を作成します。
   *
   * @returns 複製された新しい `ObjectPath` オブジェクトです。
   */
  public clone(): ObjectPath {
    return newObjectPathInternal(this.#pathBuff, this.#fullpath);
  }
}
