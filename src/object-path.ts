import { InvalidObjectPathError, TypeError } from "./errors.js";
import getTypeName from "./get-type-name.js";
import utf8 from "./utf8.js";

const DOT = 46; // "." の UTF-8 コードポイントです。
const SLASH = 47; // "/" の UTF-8 コードポイントです。

/**
 * `ObjectPath` クラスの内部使用を制御するフラグです。`true` の場合、コンストラクターが `Uint8Array` を直接受け取ります。
 */
let internalUse = false;

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
  readonly #pathBuff: Uint8Array;

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
  #basenameBuff?: Uint8Array | null;

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
    if (internalUse) {
      // 内部利用ではエンコードのオーバーヘッドを減らすために `objectPath` にバッファーが渡されます。
      const buff: unknown = objectPath;
      if (!(buff instanceof Uint8Array)) {
        throw new TypeError("Uint8Array", buff);
      }

      this.#pathBuff = buff;
      this.#fullpath = utf8.decode(this.#pathBuff);
    } else {
      if (typeof objectPath !== "string") {
        throw new InvalidObjectPathError(
          `Expected string, but got ${getTypeName(objectPath)}`,
          String(objectPath),
        );
      }

      // エンコードでオーバーヘッドが発生する前に .length で高速に検証します。
      if (objectPath.length > 1024) {
        throw new InvalidObjectPathError("Cannot be longer than 1024 bytes", objectPath);
      }

      const encoded = utf8.encode(objectPath);
      if (encoded.length > 1024) {
        throw new InvalidObjectPathError("Cannot be longer than 1024 bytes", objectPath);
      }
      if (!utf8.isValidUtf8(encoded)) {
        throw new InvalidObjectPathError("Malformed UTF-8", objectPath);
      }

      this.#pathBuff = encoded;
      this.#fullpath = objectPath;
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
    try {
      internalUse = true;
      // 内部利用ではエンコードのオーバーヘッドを減らすために `objectPath` にバッファーを渡します。
      return new ObjectPath(this.#pathBuff as any);
    } finally {
      internalUse = false;
    }
  }
}
