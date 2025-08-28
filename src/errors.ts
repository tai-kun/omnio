import getTypeName, { type TypeName } from "./get-type-name.js";

/**
 * `globalThis.Error` の `options` 引数に `cause` プロパティーが存在するかどうかをチェックする型です。
 * 存在する場合は `globalThis.ErrorOptions` を、存在しない場合は `cause` プロパティーを独自に定義した型を使用します。
 */
export type ErrorOptions = "cause" extends keyof globalThis.Error
  ? Readonly<globalThis.ErrorOptions>
  : { readonly cause?: unknown };

/***************************************************************************************************
 *
 * Omnio 版のエラークラス系
 *
 **************************************************************************************************/

/**
 * Omnio エラーの基底クラスです。
 */
export class Error extends globalThis.Error {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioError" に設定します。
   */
  static {
    this.prototype.name = "OmnioError";
  }

  /**
   * `OmnioError` クラスの新しいインスタンスを初期化します。
   *
   * @param message エラーのメッセージです。
   * @param options `cause` プロパティーを含むオプションです。
   */
  public constructor(message: string, options?: ErrorOptions | undefined) {
    super(message, options);

    if (!("cause" in this) && options && "cause" in options) {
      this.cause = options.cause; // polyfill
    }
  }
}

/**
 * 型が期待値と異なる場合に投げられるエラーです。
 */
export class TypeError extends Error {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioTypeError" に設定します。
   */
  static {
    this.prototype.name = "OmnioTypeError";
  }

  /**
   * 期待される型です。
   */
  public readonly expected: string;

  /**
   * 実際に受け取った値の型です。
   */
  public readonly actual: TypeName;

  /**
   * `OmnioTypeError` クラスの新しいインスタンスを初期化します。
   *
   * @param expectedType 期待される型名、または型名の配列です。
   * @param actualValue 実際に受け取った値です。
   * @param options `cause` プロパティーを含むオプションです。
   */
  public constructor(
    expectedType: TypeName | readonly TypeName[],
    actualValue: unknown,
    options?: ErrorOptions | undefined,
  ) {
    const expectedTypeString = Array.isArray(expectedType)
      ? expectedType.slice().sort().join(" | ")
      : String(expectedType);
    const actualType = getTypeName(actualValue);
    super(`Expected ${expectedTypeString}, but got ${actualType}`, options);
    this.expected = expectedTypeString;
    this.actual = actualType;
  }
}

/**
 * 複数のエラーをまとめるために使用されるエラーです。
 */
export class AggregateError extends Error {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioAggregateError" に設定します。
   */
  static {
    this.prototype.name = "OmnioAggregateError";
  }

  /**
   * 発生したエラーの配列です。
   */
  public override cause: unknown[];

  /**
   * `OmnioAggregateError` クラスの新しいインスタンスを初期化します。
   *
   * @param errors 発生したエラーの配列です。
   * @param options `cause` プロパティーを除くオプションです。
   */
  public constructor(
    errors: readonly unknown[],
    options?: Omit<ErrorOptions, "cause"> | undefined,
  );

  /**
   * `OmnioAggregateError` クラスの新しいインスタンスを初期化します。
   *
   * @param message エラーのメッセージです。
   * @param errors 発生したエラーの配列です。
   * @param options `cause` プロパティーを除くオプションです。
   */
  public constructor(
    message: string,
    errors: readonly unknown[],
    options?: Omit<ErrorOptions, "cause"> | undefined,
  );

  /**
   * `OmnioAggregateError` クラスのオーバーロードされたコンストラクターの実装です。
   */
  public constructor(
    ...args:
      | [
        errors: readonly unknown[],
        options?: Omit<ErrorOptions, "cause"> | undefined,
      ]
      | [
        message: string,
        errors: readonly unknown[],
        options?: Omit<ErrorOptions, "cause"> | undefined,
      ]
  ) {
    const [arg0, arg1, arg2] = args;
    const [errors, options] = Array.isArray(arg1) ? [arg1, undefined] : [[], arg2];
    const message = typeof arg0 === "string" ? arg0 : `${errors.length} error(s)`;
    super(message, options);
    this.cause = errors.slice();
  }
}

/***************************************************************************************************
 *
 * データベース / ファイルシステム
 *
 **************************************************************************************************/

/**
 * データベースに関するエラーです。
 */
export class DbError extends Error {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioDbError" に設定します。
   */
  static {
    this.prototype.name = "OmnioDbError";
  }
}

/**
 * Node.js でのみ利用可能なデータベースに関するエラーです。
 */
export class NodeDbError extends DbError {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioNodeDbError" に設定します。
   */
  static {
    this.prototype.name = "OmnioNodeDbError";
  }
}

/**
 * WASM を利用したデータベースに関するエラーです。
 */
export class WasmDbError extends DbError {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioWasmDbError" に設定します。
   */
  static {
    this.prototype.name = "OmnioWasmDbError";
  }
}

/**
 * ファイルシステムに関するエラーです。
 */
export class FsError extends Error {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioFsError" に設定します。
   */
  static {
    this.prototype.name = "OmnioFsError";
  }
}

/**
 * Node.js のファイルシステムに関するエラーです。
 */
export class NodeFsError extends FsError {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioNodeFsError" に設定します。
   */
  static {
    this.prototype.name = "OmnioNodeFsError";
  }
}

/**
 * OPFS に関するエラーです。
 */
export class OpfsError extends FsError {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioOpfsError" に設定します。
   */
  static {
    this.prototype.name = "OmnioOpfsError";
  }
}

/**
 * ファイルシステムがファイルまたはディレクトーを見つけられない場合に投げられるエラーです。
 */
export class FsPathNotFoundError extends FsError {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioFsPathNotFoundError" に設定します。
   */
  static {
    this.prototype.name = "OmnioFsPathNotFoundError";
  }

  /**
   * ファイルまたはディレクトリーのパスです。
   */
  public readonly path: string;

  /**
   * `OmnioObjectNotFoundError` クラスの新しいインスタンスを初期化します。
   *
   * @param path ファイルまたはディレクトリーのパスです。
   * @param options `cause` プロパティーを含むオプションです。
   */
  public constructor(path: string, options?: ErrorOptions | undefined) {
    super(`Path not found: ${path}`, options);
    this.path = path;
  }
}

/***************************************************************************************************
 *
 * 特定用途
 *
 **************************************************************************************************/

/**
 * オブジェクトのパスになれる値です。
 */
type ObjectPathLike = string | Readonly<{ toString: () => string }>;

/**
 * オブジェクトが見つからない場合に投げられるエラーです。
 */
export class ObjectNotFoundError extends Error {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioObjectNotFoundError" に設定します。
   */
  static {
    this.prototype.name = "OmnioObjectNotFoundError";
  }

  /**
   * オブジェクトが存在するバケットの名前です。
   */
  public readonly bucketName: string;

  /**
   * オブジェクトのパスです。
   */
  public readonly objectPath: string;

  /**
   * `OmnioObjectNotFoundError` クラスの新しいインスタンスを初期化します。
   *
   * @param bucketName オブジェクトが存在するバケットの名前です。
   * @param objectPath オブジェクトのパスです。
   * @param options `cause` プロパティーを含むオプションです。
   */
  public constructor(
    bucketName: string,
    objectPath: ObjectPathLike,
    options?: ErrorOptions | undefined,
  ) {
    const path = String(objectPath);
    super(`Object not found: '${bucketName}:${path}'`, options);
    this.bucketName = bucketName;
    this.objectPath = path;
  }
}

/**
 * オブジェクトがすでに存在する場合に投げられるエラーです。
 */
export class ObjectExistsError extends Error {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioObjectExistsError" に設定します。
   */
  static {
    this.prototype.name = "OmnioObjectExistsError";
  }

  /**
   * オブジェクトが存在するバケットの名前です。
   */
  public readonly bucketName: string;

  /**
   * オブジェクトのパスです。
   */
  public readonly objectPath: string;

  /**
   * `OmnioObjectExistsError` クラスの新しいインスタンスを初期化します。
   *
   * @param bucketName オブジェクトが存在するバケットの名前です。
   * @param objectPath オブジェクトのパスです。
   * @param options `cause` プロパティーを含むオプションです。
   */
  public constructor(
    bucketName: string,
    objectPath: ObjectPathLike,
    options?: ErrorOptions | undefined,
  ) {
    const path = String(objectPath);
    super(`Object already exists: '${bucketName}:${path}'`, options);
    this.bucketName = bucketName;
    this.objectPath = path;
  }
}

/**
 * 実際に保存されるオブジェクトが見つからない場合に投げられるエラーです。
 */
export class EntityNotFoundError extends Error {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioEntityNotFoundError" に設定します。
   */
  static {
    this.prototype.name = "OmnioEntityNotFoundError";
  }

  /**
   * オブジェクトが存在するバケットの名前です。
   */
  public readonly bucketName: string;

  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  public readonly entityId: string;

  /**
   * `OmnioEntityNotFoundError` クラスの新しいインスタンスを初期化します。
   *
   * @param bucketName オブジェクトが存在するバケットの名前です。
   * @param entityId 実際に保存されるオブジェクトの識別子です。
   * @param options `cause` プロパティーを含むオプションです。
   */
  public constructor(
    bucketName: string,
    entityId: string,
    options?: ErrorOptions | undefined,
  ) {
    super(`Entity not found: '${entityId}'`, options);
    this.bucketName = bucketName;
    this.entityId = entityId;
  }
}

/**
 * バケット名の検証エラーです。
 */
export class InvalidBucketNameError extends Error {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioInvalidBucketNameError" に設定します。
   */
  static {
    this.prototype.name = "OmnioInvalidBucketNameError";
  }

  /**
   * 検証したバケット名です。
   */
  public readonly value: string;

  /**
   * `OmnioInvalidBucketNameError` クラスの新しいインスタンスを初期化します。
   *
   * @param message エラーのメッセージです。
   * @param bucketName 検証したバケット名です。
   * @param options `cause` プロパティーを含むオプションです。
   */
  public constructor(message: string, bucketName: string, options?: ErrorOptions | undefined) {
    super("Invalid bucket name: " + message, options);
    this.value = bucketName;
  }
}

/**
 * オブジェクトのパスの検証エラーです。
 */
export class InvalidObjectPathError extends Error {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioInvalidObjectPathError" に設定します。
   */
  static {
    this.prototype.name = "OmnioInvalidObjectPathError";
  }

  /**
   * 検証したオブジェクトのパスです。
   */
  public readonly value: string;

  /**
   * `OmnioInvalidObjectPathError` クラスの新しいインスタンスを初期化します。
   *
   * @param message エラーのメッセージです。
   * @param objectPath 検証したオブジェクトのパスです。
   * @param options `cause` プロパティーを含むオプションです。
   */
  public constructor(
    message: string,
    objectPath: string,
    options?: ErrorOptions | undefined,
  ) {
    super("Invalid object path: " + message, options);
    this.value = objectPath;
  }
}

/**
 * オブジェクトのチェックサムが期待した値と異なる場合に投げられるエラーです。
 */
export class ChecksumMismatchError extends Error {
  /**
   * クラスの静的初期化ブロックです。プロトタイプの `name` プロパティーを "OmnioChecksumMismatchError" に設定します。
   */
  static {
    this.prototype.name = "OmnioChecksumMismatchError";
  }

  /**
   * オブジェクトが存在するバケットの名前です。
   */
  public readonly bucketName: string;

  /**
   * オブジェクトのパスです。
   */
  public readonly objectPath: string;

  /**
   * 期待するオブジェクトのチェックサムです。
   */
  public readonly expected: string;

  /**
   * 実際のオブジェクトのチェックサムです。
   */
  public readonly actual?: string;

  /**
   * `OmnioChecksumMismatchError` クラスの新しいインスタンスを初期化します。
   *
   * @param bucketName オブジェクトが存在するバケットの名前です。
   * @param objectPath オブジェクトのパスです。
   * @param expected 期待するオブジェクトのチェックサムです。
   * @param options `cause` プロパティーを含むオプションです。
   */
  public constructor(
    bucketName: string,
    objectPath: ObjectPathLike,
    expected: string,
    options?:
      | Readonly<
        ErrorOptions & {
          /**
           * 実際のオブジェクトのチェックサムです。
           */
          actual?: string | undefined;
        }
      >
      | undefined,
  ) {
    const path = String(objectPath);
    const actual = options?.actual === undefined ? "" : `, but got '${options.actual}'`;
    super(
      `Object checksum is mismatch: '${bucketName}:${path}': Expected '${expected}'${actual}`,
      options,
    );
    this.bucketName = bucketName;
    this.objectPath = path;
    this.expected = expected;
    if (options?.actual !== undefined) {
      this.actual = options.actual;
    }
  }
}
