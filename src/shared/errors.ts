import { type ErrorMeta, I18nErrorBase, initErrorMessage, setErrorMessage } from "i18n-error-base";
import getTypeName from "type-name";
import { type BaseIssue } from "valibot";
import quoteString from "./quote-string.js";

/***************************************************************************************************
 *
 * ユーティリティー
 *
 **************************************************************************************************/

/**
 * あらゆる値を文字列に整形します。
 *
 * @param value 文字列に整形する値です。
 * @returns 文字列に整形された値です。
 */
export function formatErrorValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }

  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

/***************************************************************************************************
 *
 * エラークラス
 *
 **************************************************************************************************/

/**
 * Omnio エラーの基底クラスです。
 *
 * @template TMeta エラーに紐づくメタデータです。
 */
export class ErrorBase<TMeta extends ErrorMeta | undefined = undefined>
  extends I18nErrorBase<TMeta>
{}

/**************************************************************************************************/

/**
 * 型が期待値と異なる場合に投げられるエラーです。
 */
export class TypeError extends ErrorBase<{
  /**
   * 期待される型です。
   */
  expected: string;

  /**
   * 実際に受け取った値の型です。
   */
  actual: string;
}> {
  static {
    this.prototype.name = "OmnioTypeError";
  }

  /**
   * `OmnioTypeError` クラスの新しいインスタンスを初期化します。
   *
   * @param expectedType 期待される型名、または型名の配列です。
   * @param actualValue 実際に受け取った値です。
   * @param options エラーのオプションです。
   */
  public constructor(
    expectedType: string | readonly string[],
    actualValue: unknown,
    options?: ErrorOptions | undefined,
  ) {
    super(options, {
      actual: getTypeName(actualValue) || `<Anonymous ${typeof actualValue}>`,
      expected: typeof expectedType === "string"
        ? String(expectedType)
        : expectedType.slice().sort().join(" | "),
    });
    initErrorMessage(this, ({ meta }) => `Expected ${meta.expected}, but got ${meta.actual}`);
  }
}

/*#__PURE__*/ setErrorMessage(
  TypeError,
  ({ meta }) => `${meta.expected} を期待しましたが、${meta.actual} を得ました`,
  "ja",
);

/**************************************************************************************************/

/**
 * 到達不能なコードに到達した場合に投げられるエラーです。
 */
export class UnreachableError extends ErrorBase<{
  /**
   * 到達しないはずの値です。
   */
  value?: unknown;
}> {
  static {
    this.prototype.name = "OmnioUnreachableError";
  }

  /**
   * `OmnioUnreachableError` クラスの新しいインスタンスを初期化します。
   *
   * @param args 到達しないはずの値があれば指定します。
   * @param options エラーのオプションです。
   */
  public constructor(args: [never?], options?: ErrorOptions | undefined) {
    super(options, args.length > 0 ? { value: args[0] } : {});
    initErrorMessage(this, ({ meta }) => (
      "value" in meta
        ? "Encountered impossible value: " + formatErrorValue(meta.value)
        : "Unreachable code reached"
    ));
  }
}

/*#__PURE__*/ setErrorMessage(
  UnreachableError,
  ({ meta }) => (
    "value" in meta
      ? "不可能な値に遭遇しました: " + formatErrorValue(meta.value)
      : "到達できないコードに到達しました"
  ),
  "ja",
);

/**************************************************************************************************/

/**
 * 検証エラーの問題点です。
 */
export type Issue = BaseIssue<unknown>;

/**
 * 検証エラーの基底クラスです。
 *
 * @template TMeta エラーに紐づくメタデータです。
 */
export class ValidationErrorBase<TMeta extends ErrorMeta> extends ErrorBase<TMeta> {
  /**
   * @internal
   */
  public constructor(options: ErrorOptions | undefined, meta: TMeta) {
    super(options, meta);
  }
}

/**************************************************************************************************/

/**
 * 入力値検証エラーの基底クラスです。
 *
 * @template TMeta エラーに紐づくメタデータです。
 */
export class InvalidInputErrorBase<TMeta extends ErrorMeta> extends ValidationErrorBase<TMeta> {
  /**
   * @internal
   */
  public constructor(options: ErrorOptions | undefined, meta: TMeta) {
    super(options, meta);
  }
}

/**************************************************************************************************/

/**
 * 入力値の検証に失敗した場合に投げられるエラーです。
 */
export class InvalidInputError extends InvalidInputErrorBase<{
  /**
   * 検証エラーの問題点です。
   */
  issues: [Issue, ...Issue[]];

  /**
   * 検証した入力値です。
   */
  input: unknown;
}> {
  static {
    this.prototype.name = "OmnioInvalidInputError";
  }

  /**
   * `OmnioInvalidInputError` クラスの新しいインスタンスを初期化します。
   *
   * @param issues 検証エラーの問題点です。
   * @param input 検証した入力値です。
   * @param options エラーのオプションです。
   */
  public constructor(
    issues: [Issue, ...Issue[]],
    input: unknown,
    options?: ErrorOptions | undefined,
  ) {
    super(options, { issues, input });
    this.message = issues.map(issue => issue.message).join(": ");
  }
}

/**************************************************************************************************/

/**
 * オブジェクトのサイズがパーツ構成に基づいて予想される最小サイズを下回っている場合に投げられるエラーです。
 */
export class ObjectSizeTooSamllError extends InvalidInputErrorBase<{
  objectSize: number;
  numParts: number;
  partSize: number;
}> {
  static {
    this.prototype.name = "OmnioObjectSizeTooSamllError";
  }

  /**
   * `OmnioObjectSizeTooSamllError` クラスの新しいインスタンスを初期化します。
   *
   * @param objectSize オブジェクトのサイズ (バイト数) です。
   * @param numParts オブジェクトのパートの総数です。
   * @param partSize 各パートのサイズ (バイト数) です。
   * @param options エラーのオプションです。
   */
  public constructor(
    objectSize: number,
    numParts: number,
    partSize: number,
    options?: ErrorOptions | undefined,
  ) {
    super(options, {
      objectSize,
      numParts,
      partSize,
    });
    initErrorMessage(
      this,
      () => "Object size is below the minimum expected size based on the part configuration",
    );
  }
}

/*#__PURE__*/ setErrorMessage(
  ObjectSizeTooSamllError,
  () => "オブジェクトのサイズがパーツ構成に基づいて予想される最小サイズを下回っています",
  "ja",
);

/**************************************************************************************************/

/**
 * オブジェクトのサイズがパーツ構成に基づいて予想される最大サイズを上回っている場合に投げられるエラーです。
 */
export class ObjectSizeTooLargeError extends InvalidInputErrorBase<{
  objectSize: number;
  numParts: number;
  partSize: number;
}> {
  static {
    this.prototype.name = "OmnioObjectSizeTooLargeError";
  }

  /**
   * `OmnioObjectSizeTooLargeError` クラスの新しいインスタンスを初期化します。
   *
   * @param objectSize オブジェクトのサイズ (バイト数) です。
   * @param numParts オブジェクトのパートの総数です。
   * @param partSize 各パートのサイズ (バイト数) です。
   * @param options エラーのオプションです。
   */
  public constructor(
    objectSize: number,
    numParts: number,
    partSize: number,
    options?: ErrorOptions | undefined,
  ) {
    super(options, {
      objectSize,
      numParts,
      partSize,
    });
    initErrorMessage(
      this,
      () => "Object size exceeds the maximum expected size based on the part configuration",
    );
  }
}

/*#__PURE__*/ setErrorMessage(
  ObjectSizeTooLargeError,
  () => "オブジェクトのサイズがパーツ構成に基づいて予想される最大サイズを上回っています",
  "ja",
);

/**************************************************************************************************/

/**
 * 無効な照合順序を得た場合に投げられるエラーです。
 */
export class InvalidCollationError extends InvalidInputErrorBase<{
  collation: string;
}> {
  static {
    this.prototype.name = "OmnioInvalidCollationError";
  }

  /**
   * `OmnioInvalidCollationError` クラスの新しいインスタンスを初期化します。
   *
   * @param collation 成功順序です。
   */
  public constructor(
    collation: string,
    options?: ErrorOptions | undefined,
  ) {
    super(options, { collation });
    initErrorMessage(this, ({ meta }) => "Invalid collation: " + meta.collation);
  }
}

/*#__PURE__*/ setErrorMessage(
  InvalidCollationError,
  ({ meta }) => "無効な照合順序: " + meta.collation,
  "ja",
);

/**************************************************************************************************/

/**
 * 予期しない値に遭遇した場合に投げられるエラーです。
 */
export class UnexpectedValidationError extends ValidationErrorBase<{
  /**
   * 検証エラーの問題点です。
   */
  issues: [Issue, ...Issue[]];

  /**
   * 予期しない値です。
   */
  value: unknown;
}> {
  static {
    this.prototype.name = "OmnioUnexpectedValidationError";
  }

  /**
   * `OmnioUnexpectedValidationError` クラスの新しいインスタンスを初期化します。
   *
   * @param issues 検証エラーの問題点です。
   * @param value 予期しない値です。
   * @param options エラーのオプションです。
   */
  public constructor(
    issues: [Issue, ...Issue[]],
    value: unknown,
    options?: ErrorOptions | undefined,
  ) {
    super(options, { issues, value });
    this.message = issues.map(issue => issue.message).join(": ");
  }
}

/**************************************************************************************************/

/**
 * オブジェクトのパスになれる値です。
 */
type ObjectPathLike = string | { toString(): string };

/**
 * オブジェクトがすでに存在する場合に投げられるエラーです。
 */
export class ObjectExistsError extends ErrorBase<{
  /**
   * オブジェクトが存在するバケットの名前です。
   */
  bucketName: string;

  /**
   * オブジェクトのパスです。
   */
  objectPath: string;
}> {
  static {
    this.prototype.name = "OmnioObjectExistsError";
  }

  /**
   * `OmnioObjectExistsError` クラスの新しいインスタンスを初期化します。
   *
   * @param bucketName オブジェクトが存在するバケットの名前です。
   * @param objectPath オブジェクトのパスです。
   * @param options エラーのオプションです。
   */
  public constructor(
    bucketName: string,
    objectPath: ObjectPathLike,
    options?: ErrorOptions | undefined,
  ) {
    super(options, {
      bucketName,
      objectPath: String(objectPath),
    });
    initErrorMessage(
      this,
      ({ meta }) => `Object exists: ${quoteString(meta.bucketName + ":" + meta.objectPath)}`,
    );
  }
}

/*#__PURE__*/ setErrorMessage(
  ObjectExistsError,
  ({ meta }) =>
    `オブジェクトがすでに存在します: ${quoteString(meta.bucketName + ":" + meta.objectPath)}`,
  "ja",
);

/**************************************************************************************************/

/**
 * オブジェクトが見つからない場合に投げられるエラーです。
 */
export class ObjectNotFoundError extends ErrorBase<{
  /**
   * オブジェクトが存在するバケットの名前です。
   */
  bucketName: string;

  /**
   * オブジェクトのパスです。
   */
  objectPath: string;
}> {
  static {
    this.prototype.name = "OmnioObjectNotFoundError";
  }

  /**
   * `OmnioObjectNotFoundError` クラスの新しいインスタンスを初期化します。
   *
   * @param bucketName オブジェクトが存在するバケットの名前です。
   * @param objectPath オブジェクトのパスです。
   * @param options エラーのオプションです。
   */
  public constructor(
    bucketName: string,
    objectPath: ObjectPathLike,
    options?: ErrorOptions | undefined,
  ) {
    super(options, {
      bucketName,
      objectPath: String(objectPath),
    });
    initErrorMessage(
      this,
      ({ meta }) => `Object not found: ${quoteString(meta.bucketName + ":" + meta.objectPath)}`,
    );
  }
}

/*#__PURE__*/ setErrorMessage(
  ObjectNotFoundError,
  ({ meta }) =>
    `オブジェクトが見つかりません: ${quoteString(meta.bucketName + ":" + meta.objectPath)}`,
  "ja",
);

/**************************************************************************************************/

/**
 * データに矛盾ある場合に投げられるエラーの基底クラスです。
 */
export class DataInconsistencyErrorBase<TMeta extends ErrorMeta | undefined = undefined>
  extends ErrorBase<TMeta>
{
  /**
   * @internal
   */
  public constructor(options: ErrorOptions | undefined, meta: TMeta) {
    super(options, meta);
  }
}

/**************************************************************************************************/

/**
 * オブジェクトのチェックサムが期待した値と異なる場合に投げられるエラーです。
 */
export class ChecksumMismatchError extends DataInconsistencyErrorBase<{
  /**
   * オブジェクトが存在するバケットの名前です。
   */
  readonly bucketName: string;

  /**
   * オブジェクトのパスです。
   */
  readonly objectPath: string;

  /**
   * 期待するオブジェクトのチェックサムです。
   */
  readonly expected: string;

  /**
   * 実際のオブジェクトのチェックサムです。
   */
  readonly actual: string | undefined;
}> {
  static {
    this.prototype.name = "OmnioChecksumMismatchError";
  }

  /**
   * `OmnioChecksumMismatchError` クラスの新しいインスタンスを初期化します。
   *
   * @param bucketName オブジェクトが存在するバケットの名前です。
   * @param objectPath オブジェクトのパスです。
   * @param expected 期待するオブジェクトのチェックサムです。
   * @param options エラーのオプションです。
   */
  public constructor(
    bucketName: string,
    objectPath: ObjectPathLike,
    expected: string,
    options?:
      | ErrorOptions & {
        /**
         * 実際のオブジェクトのチェックサムです。
         */
        readonly actual?: string | undefined;
      }
      | undefined,
  ) {
    super(options, {
      bucketName,
      objectPath: String(objectPath),
      expected,
      actual: options?.actual,
    });
    initErrorMessage(this, ({ meta }) => (
      "Object checksum is mismatch: "
      + quoteString(meta.bucketName + ":" + meta.objectPath)
      + `: Expected ${quoteString(meta.expected)}`
      + (meta.actual === undefined ? "" : `, but got ${quoteString(meta.actual)}`)
    ));
  }
}

/*#__PURE__*/ setErrorMessage(
  ChecksumMismatchError,
  ({ meta }) =>
    "オブジェクトのチェックサムが不一致: "
    + quoteString(meta.bucketName + ":" + meta.objectPath)
    + `: ${quoteString(meta.expected)} を期待しました`
    + (meta.actual === undefined ? "" : `が、実際には ${quoteString(meta.actual)} を得ました`),
  "ja",
);

/**************************************************************************************************/

/**
 * データベースに関連するエラーの基底クラスです。
 */
export class DatabaseErrorBase extends ErrorBase {
  /**
   * @internal
   */
  public constructor(options: ErrorOptions | undefined) {
    super(options, undefined);
  }
}

/**************************************************************************************************/

/**
 * データベースが開いていない状態で操作しようととした場合に投げられるエラーです。
 */
export class DatabaseNotOpenError extends DatabaseErrorBase {
  static {
    this.prototype.name = "OmnioDatabaseNotOpenError";
  }

  /**
   * `OmnioDatabaseNotOpenError` クラスの新しいインスタンスを初期化します。
   *
   * @param options エラーのオプションです。
   */
  public constructor(options?: ErrorOptions | undefined) {
    super(options);
    initErrorMessage(this, () => "Database not open");
  }
}

/*#__PURE__*/ setErrorMessage(DatabaseNotOpenError, () => "データベースが開いていません", "ja");

/**************************************************************************************************/

/**
 * SQL ステートメントがすでに閉じられている状態で操作しようとした場合に投げられるエラーです。
 */
export class SqlStatementClosedError extends DatabaseErrorBase {
  static {
    this.prototype.name = "OmnioSqlStatementClosedError";
  }

  /**
   * `OmnioSqlStatementClosedError` クラスの新しいインスタンスを初期化します。
   *
   * @param options エラーのオプションです。
   */
  public constructor(options?: ErrorOptions | undefined) {
    super(options);
    initErrorMessage(this, () => "SQL statement is closed");
  }
}

/*#__PURE__*/ setErrorMessage(
  SqlStatementClosedError,
  () => "SQL ステートメントは閉じられています",
  "ja",
);

/**************************************************************************************************/

/**
 * ファイルシステムに関連するエラーの基底クラスです。
 */
export class FileSystemErrorBase<TMeta extends ErrorMeta | undefined = undefined>
  extends ErrorBase<TMeta>
{
  /**
   * @internal
   */
  public constructor(options: ErrorOptions | undefined, meta: TMeta) {
    super(options, meta);
  }
}

/**************************************************************************************************/

/**
 * エントリーが見つからない場合に投げられるエラーです。
 */
export class EntryPathNotFoundError extends FileSystemErrorBase<{
  /**
   * エントリーへのパスです。
   */
  path: string;
}> {
  static {
    this.prototype.name = "OmnioEntryPathNotFoundError";
  }

  /**
   * `OmnioEntryPathNotFoundError` クラスの新しいインスタンスを初期化します。
   *
   * @param path エントリーへのパスです。
   * @param options エラーのオプションです。
   */
  public constructor(path: string, options?: ErrorOptions | undefined) {
    super(options, { path });
    initErrorMessage(this, ({ meta }) => `Entry path not found: ${meta.path}`);
  }
}

/*#__PURE__*/ setErrorMessage(
  EntryPathNotFoundError,
  ({ meta }) => `エントリーが見つかりません: ${meta.path}`,
  "ja",
);

/**************************************************************************************************/

/**
 * ファイルシステムが開いていない状態で操作しようととした場合に投げられるエラーです。
 */
export class FileSystemNotOpenError extends FileSystemErrorBase {
  static {
    this.prototype.name = "OmnioFileSystemNotOpenError";
  }

  /**
   * `OmnioFileSystemNotOpenError` クラスの新しいインスタンスを初期化します。
   *
   * @param options エラーのオプションです。
   */
  public constructor(options?: ErrorOptions | undefined) {
    super(options, undefined);
    initErrorMessage(this, () => "File system not open");
  }
}

/*#__PURE__*/ setErrorMessage(
  FileSystemNotOpenError,
  () => "ファイルシステムが開いていません",
  "ja",
);

/**************************************************************************************************/

/**
 * OPFS で "storage-access" に対する権限が与えられていない場合に投げられるエラーです。
 */
export class OpfsPermissionStateError extends FileSystemErrorBase<{
  actual: Exclude<PermissionState, "granted">;
}> {
  static {
    this.prototype.name = "OmnioOpfsPermissionStateError";
  }

  /**
   * `OmnioOpfsPermissionStateError` クラスの新しいインスタンスを初期化します。
   *
   * @param options エラーのオプションです。
   */
  public constructor(
    actual: Exclude<PermissionState, "granted">,
    options?: ErrorOptions | undefined,
  ) {
    super(options, { actual });
    initErrorMessage(
      this,
      ({ meta }) => `The permission given to storage-access was "${meta.actual}"`,
    );
  }
}

/*#__PURE__*/ setErrorMessage(
  OpfsPermissionStateError,
  ({ meta }) => `"storage-access" に与えられた権限は "${meta.actual}" でした`,
  "ja",
);

/**************************************************************************************************/

/**
 * Omnio がすでに閉じられている状態で操作しようとした場合に投げられるエラーです。
 */
export class OmnioClosedError extends DatabaseErrorBase {
  static {
    this.prototype.name = "OmnioOmnioClosedError";
  }

  /**
   * `OmnioOmnioClosedError` クラスの新しいインスタンスを初期化します。
   *
   * @param options エラーのオプションです。
   */
  public constructor(options?: ErrorOptions | undefined) {
    super(options);
    initErrorMessage(this, () => "Omnio is closed");
  }
}

/*#__PURE__*/ setErrorMessage(OmnioClosedError, () => "Omnio は閉じられています", "ja");

/**************************************************************************************************/
