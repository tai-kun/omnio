import sql, { empty, join, raw } from "sql-template-tag";
import { v7 as getObjectId } from "uuid";
import * as v from "valibot";
import type BucketName from "./bucket-name.js";
import type { Db, Row } from "./db/db.types.js";
import {
  ChecksumMismatchError,
  EntityNotFoundError,
  Error,
  ObjectExistsError,
  ObjectNotFoundError,
} from "./errors.js";
import type { Fs } from "./fs/fs.types.js";
import getMimeType from "./get-mime-type.js";
import type { ConsoleLikeLogger } from "./logger/to-console-like-logger.js";
import migrations from "./migrations.js";
import mutex from "./mutex.js";
import ObjectPath from "./object-path.js";
import * as schemas from "./schemas.js";
import type { $Get, $Select, Awaitable } from "./type-utils.js";

/**
 * オブジェクトの説明文を検索用の文字列に変換する関数です。
 */
export interface ToTextSearchQueryString {
  /**
   * オブジェクトの説明文を検索用の文字列に変換します。
   *
   * @param string 任意の文字列です。
   * @returns 検索用の文字列です。
   */
  (string: string): Awaitable<string>;
}

/**
 * オブジェクトの検索用の文字列を説明文に変換する関数です。
 */
export interface FromTextSearchQueryhString {
  /**
   * オブジェクトの検索用の文字列を説明文に変換します。
   *
   * @param string 検索用の文字列です。
   * @returns 検索用の文字列から復元された元の文字列です。
   */
  (string: string): Awaitable<string>;
}

/**
 * オブジェクトの説明文の検索に使用するユーティリティーです。
 */
export interface TextSearch {
  /**
   * オブジェクトの説明文を検索用の文字列に変換する関数です。
   */
  readonly toQueryString: ToTextSearchQueryString;

  /**
   * オブジェクトの検索用の文字列を説明文に変換する関数です。
   */
  readonly fromQueryString: FromTextSearchQueryhString;
}

/**
 * JSON 文字列を JavaScript の値に変換する関数です。
 */
export interface JsonParse {
  /**
   * JSON 文字列を JavaScript の値に変換します。
   *
   * @param text 変換される JSON 文字列です。
   * @returns 変換された JavaScript 値です。
   */
  (text: string): unknown;
}

/**
 * JavaScript の値を JSON 文字列に変換する関数です。
 */
export interface JsonStringify {
  /**
   * JavaScript の値を JSON 文字列に変換します。
   *
   * @param value 変換される JavaScript 値です。
   * @returns 変換された JSON 文字列です。
   */
  (value: unknown): string;
}

/**
 * JavaScript の値と JSON 文字列を相互変換するための関数群です。
 */
export interface Json {
  /**
   * JSON 文字列を JavaScript の値に変換する関数です。
   */
  readonly parse: JsonParse;

  /**
   * JavaScript の値を JSON 文字列に変換する関数です。
   */
  readonly stringify: JsonStringify;
}

/**
 * SQL オブジェクトです。
 */
export interface Sql {
  /**
   * SQL の文字列です。
   */
  readonly text: string;

  /**
   * SQL の実行時に渡すパラメーターです。
   */
  readonly values: readonly unknown[];
}

/**
 * `Metadata` を構築するための入力パラメーターです。
 */
type MetadataInput = Readonly<{
  /**
   * メタデータを記録するためのデータベースです。
   */
  db: Db;

  /**
   * メタデータを永続ストレージに記録するためのファイルシステムです。
   */
  fs: Fs;

  /**
   * ログを記録する関数群です。
   */
  logger: ConsoleLikeLogger;

  /**
   * バケット名です。
   */
  bucketName: BucketName;

  /**
   * オブジェクトの説明文の検索に使用するユーティリティーです。
   */
  textSearch: TextSearch | undefined;

  /**
   * JavaScript の値と JSON 文字列を相互変換するための関数群です。
   *
   * @default JSON
   */
  json: Json | undefined;

  /**
   * オブジェクトの説明文の最大サイズ (バイト数) です。
   *
   * @default 10 KiB
   */
  maxDescriptionTextSize: v.InferOutput<typeof schemas.UnsignedInteger> | undefined;

  /**
   * ユーザー定義のメタデータの最大サイズ (バイト数) です。
   * このサイズは、ユーザー定義のメタデータを `json.stringify` で変換したあとの文字列に対して計算されます。
   *
   * @default 10 KiB
   */
  maxUserMetadataJsonSize: v.InferOutput<typeof schemas.UnsignedInteger> | undefined;
}>;

/**
 * オブジェクトのメタデータを作成するための入力パラメーターです。
 */
type CreateInput = Readonly<{
  /**
   * バケット内のオブジェクトパスです。
   */
  objectPath: ObjectPath;

  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  entityId: v.InferOutput<typeof schemas.EntityId>;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: Readonly<{
    /**
     * 計算されたハッシュ値の 16 進数文字列です。
     */
    value: v.InferOutput<typeof schemas.Checksum>;

    /**
     * ハッシュ関数の内部状態です。
     */
    state: v.InferOutput<typeof schemas.HashState>;
  }>;

  /**
   * オブジェクトのデータ形式です。`undefined` の場合はオブジェクトパスから自動判定されます。
   */
  mimeType: v.InferOutput<typeof schemas.MimeType> | undefined;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  objectSize: v.InferOutput<typeof schemas.UnsignedInteger>;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   *
   * @default []
   */
  objectTags: v.InferOutput<typeof schemas.ObjectTags> | undefined;

  /**
   * オブジェクトの説明文です。
   *
   * @default null
   */
  description: string | null | undefined;

  /**
   * ユーザー定義のメタデータです。
   *
   * @default null
   */
  userMetadata: unknown;

  /**
   * カスタムのタイムスタンプです。
   *
   * @default Date.now()
   */
  timestamp: v.InferOutput<typeof schemas.Timestamp> | undefined;
}>;

/**
 * オブジェクトのメタデータを排他的に作成するための入力パラメーターです。
 */
type CreateExclusiveInput = Readonly<{
  /**
   * バケット内のオブジェクトパスです。
   */
  objectPath: ObjectPath;

  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  entityId: v.InferOutput<typeof schemas.EntityId>;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: Readonly<{
    /**
     * 計算されたハッシュ値の 16 進数文字列です。
     */
    value: v.InferOutput<typeof schemas.Checksum>;

    /**
     * ハッシュ関数の内部状態です。
     */
    state: v.InferOutput<typeof schemas.HashState>;
  }>;

  /**
   * オブジェクトのデータ形式です。`undefined` の場合はオブジェクトパスから自動判定されます。
   */
  mimeType: v.InferOutput<typeof schemas.MimeType> | undefined;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  objectSize: v.InferOutput<typeof schemas.UnsignedInteger>;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   *
   * @default []
   */
  objectTags: v.InferOutput<typeof schemas.ObjectTags> | undefined;

  /**
   * オブジェクトの説明文です。
   *
   * @default null
   */
  description: string | null | undefined;

  /**
   * ユーザー定義のメタデータです。
   *
   * @default null
   */
  userMetadata: unknown;

  /**
   * カスタムのタイムスタンプです。
   *
   * @default Date.now()
   */
  timestamp: v.InferOutput<typeof schemas.Timestamp> | undefined;
}>;

/**
 * 存在するオブジェクトのメタデータを取得するための入力パラメーターです。
 */
type ReadInput = Readonly<{
  /**
   * 結果に含めるカラムを選択します。
   */
  select:
    | Readonly<{
      /**
       * バケット名です。
       *
       * @default false
       */
      bucket?: boolean | undefined;

      /**
       * オブジェクトの識別子です。
       *
       * @default false
       */
      id?: boolean | undefined;

      /**
       * バケット内のオブジェクトパスです。
       *
       * @default false
       */
      path?: boolean | undefined;

      /**
       * オブジェクトのメタデータのレコードタイプです。
       *
       * @default false
       */
      recordType?: boolean | undefined;

      /**
       * `recordType` が更新された時刻 (ミリ秒) です。
       *
       * @default false
       */
      recordTimestamp?: boolean | undefined;

      /**
       * オブジェクトのサイズ (バイト数) です。
       *
       * @default false
       */
      size?: boolean | undefined;

      /**
       * オブジェクト形式です。
       *
       * @default false
       */
      mimeType?: boolean | undefined;

      /**
       * オブジェクトが作成された時刻 (ミリ秒) です。
       *
       * @default false
       */
      createdAt?: boolean | undefined;

      /**
       * オブジェクトが最後に更新された時刻 (ミリ秒) です。
       *
       * @default false
       */
      lastModifiedAt?: boolean | undefined;

      /**
       * オブジェクトのチェックサム (MD5 ハッシュ値) です。
       *
       * @default false
       */
      checksum?: boolean | undefined;

      /**
       * オブジェクトのチェックサムのアルゴリズムです。
       *
       * @default false
       */
      checksumAlgorithm?: boolean | undefined;

      /**
       * オブジェクトに関連付けられたオブジェクトタグです。
       *
       * @default false
       */
      objectTags?: boolean | undefined;

      /**
       * オブジェクトの説明文です。
       *
       * @default false
       */
      description?: boolean | undefined;

      /**
       * ユーザー定義のメタデータです。
       *
       * @default false
       */
      userMetadata?: boolean | undefined;

      /**
       * 実際に保存されるオブジェクトの識別子です。
       *
       * @default false
       */
      entityId?: boolean | undefined;
    }>
    | undefined;

  /**
   * 対象を限定します。
   */
  where: Readonly<{
    /**
     * バケット内のオブジェクトパスです。
     */
    objectPath: ObjectPath;
  }>;
}>;

/**
 * 存在するオブジェクトの内部利用のためのメタデータを取得するための入力パラメーターです。
 */
type ReadInternalInput = Readonly<{
  /**
   * バケット内のオブジェクトパスです。
   */
  objectPath: ObjectPath;
}>;

/**
 * 存在するオブジェクトのメタデータです。
 */
type ObjectMetadata = {
  /**
   * バケット名です。
   */
  bucket: BucketName;

  /**
   * オブジェクトの識別子です。
   */
  id: v.InferOutput<typeof schemas.ObjectId>;

  /**
   * バケット内のオブジェクトパスです。
   */
  path: ObjectPath;

  /**
   * - **`"CREATE"`**: 新しいオブジェクトがバケットに書き込まれたことを示します。
   * - **`"UPDATE_METADATA"`**: オブジェクトの作成後、メタデータが変更されたことを示します。
   */
  recordType: ("CREATE" | "UPDATE_METADATA") & v.Brand<"RecordType">;

  /**
   * `recordType` が更新された時刻 (ミリ秒) です。
   */
  recordTimestamp: v.InferOutput<typeof schemas.Timestamp>;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  size: v.InferOutput<typeof schemas.UnsignedInteger>;

  /**
   * オブジェクト形式です。
   */
  mimeType: v.InferOutput<typeof schemas.MimeType>;

  /**
   * オブジェクトが作成された時刻 (ミリ秒) です。
   */
  createdAt: v.InferOutput<typeof schemas.Timestamp>;

  /**
   * オブジェクトが最後に更新された時刻 (ミリ秒) です。
   */
  lastModifiedAt: v.InferOutput<typeof schemas.Timestamp>;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: v.InferOutput<typeof schemas.Checksum>;

  /**
   * オブジェクトのチェックサムのアルゴリズムです。
   */
  checksumAlgorithm: "MD5";

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  objectTags: schemas.MutableObjectTags;

  /**
   * オブジェクトの説明文です。
   */
  description: string | null;

  /**
   * ユーザー定義のメタデータです。
   */
  userMetadata: unknown;

  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  entityId: v.InferOutput<typeof schemas.EntityId>;
};

/**
 * ゴミ箱に入れられたオブジェクトのメタデータです。
 */
type ObjectMetadataInTrash = {
  /**
   * バケット名です。
   */
  bucket: BucketName;

  /**
   * バケット内のオブジェクトパスです。
   */
  path: ObjectPath;

  /**
   * オブジェクトの識別子です。
   */
  id: v.InferOutput<typeof schemas.ObjectId>;

  /**
   * `recordType` が更新された時刻 (ミリ秒) です。
   */
  recordTimestamp: v.InferOutput<typeof schemas.Checksum>;

  /**
   * オブジェクト形式です。
   */
  mimeType: v.InferOutput<typeof schemas.MimeType>;

  /**
   * オブジェクトが作成された時刻 (ミリ秒) です。
   */
  createdAt: v.InferOutput<typeof schemas.Checksum>;

  /**
   * オブジェクトが最後に更新された時刻 (ミリ秒) です。
   */
  lastModifiedAt: v.InferOutput<typeof schemas.Checksum>;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: v.InferOutput<typeof schemas.Checksum>;

  /**
   * オブジェクトのチェックサムのアルゴリズムです。
   */
  checksumAlgorithm: "MD5";

  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  entityId: string;
};

/**
 * 存在するオブジェクトのメタデータを取得した結果です。
 *
 * @template TSelect SELECT するカラムです。
 */
type ReadOutput<TSelect> = $Select<ObjectMetadata, TSelect>;

/**
 * 存在するオブジェクトの内部利用のためのメタデータを取得した結果です。
 */
type ReadInternalOutput = Readonly<{
  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  entityId: v.InferOutput<typeof schemas.EntityId>;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: Readonly<{
    /**
     * 計算されたハッシュ値の 16 進数文字列です。
     */
    value: v.InferOutput<typeof schemas.Checksum>;

    /**
     * ハッシュ関数の内部状態です。
     */
    state: v.InferOutput<typeof schemas.HashState>;
  }>;
}>;

/**
 * パスが存在するか確認するための入力パラメーターです。
 */
type ExistsInput =
  | Readonly<{
    /**
     * バケット内のディレクトリーパスです。
     */
    dirPath: readonly string[];
  }>
  | Readonly<{
    /**
     * バケット内のオブジェクトパスです。
     */
    objectPath: ObjectPath;
  }>;

/**
 * パスが存在するか確認した結果です。
 */
type ExistsOutput = {
  /**
   * オブジェクトのメタデータが存在すれば `true` です。
   */
  exists: boolean;
};

/**
 * オブジェクトやディレクトリーのステータス情報を取得するための入力パラメーターです。
 */
type StatInput = Readonly<{
  /**
   * バケット内のパスです。
   */
  objectPath: ObjectPath;
}>;

/**
 * オブジェクトやディレクトリーのステータス情報を取得した結果です。
 */
type StatOutput = {
  /**
   * `true` なら指定したパスはオブジェクトです。
   */
  isObject: boolean;

  /**
   * `true` なら指定したパスはディレクトリーです。
   */
  isDirectory: boolean;
};

/**
 * ディレクトリーまたはオブジェクトをリストアップするための入力パラメーターです。
 */
type ListInput = Readonly<{
  /**
   * リストアイテムがオブジェクトのときに結果に含めるカラムを選択します。
   */
  select:
    | Readonly<{
      /**
       * バケット名です。
       *
       * @default false
       */
      bucket?: boolean | undefined;

      /**
       * オブジェクトの識別子です。
       *
       * @default false
       */
      id?: boolean | undefined;

      /**
       * バケット内のオブジェクトパスです。
       *
       * @default false
       */
      path?: boolean | undefined;

      /**
       * オブジェクトのメタデータのレコードタイプです。
       *
       * @default false
       */
      recordType?: boolean | undefined;

      /**
       * `recordType` が更新された時刻 (ミリ秒) です。
       *
       * @default false
       */
      recordTimestamp?: boolean | undefined;

      /**
       * オブジェクトのサイズ (バイト数) です。
       *
       * @default false
       */
      size?: boolean | undefined;

      /**
       * オブジェクト形式です。
       *
       * @default false
       */
      mimeType?: boolean | undefined;

      /**
       * オブジェクトが作成された時刻 (ミリ秒) です。
       *
       * @default false
       */
      createdAt?: boolean | undefined;

      /**
       * オブジェクトが最後に更新された時刻 (ミリ秒) です。
       *
       * @default false
       */
      lastModifiedAt?: boolean | undefined;

      /**
       * オブジェクトのチェックサム (MD5 ハッシュ値) です。
       *
       * @default false
       */
      checksum?: boolean | undefined;

      /**
       * オブジェクトのチェックサムのアルゴリズムです。
       *
       * @default false
       */
      checksumAlgorithm?: boolean | undefined;

      /**
       * オブジェクトに関連付けられたオブジェクトタグです。
       *
       * @default false
       */
      objectTags?: boolean | undefined;

      /**
       * オブジェクトの説明文です。
       *
       * @default false
       */
      description?: boolean | undefined;

      /**
       * ユーザー定義のメタデータです。
       *
       * @default false
       */
      userMetadata?: boolean | undefined;

      /**
       * 実際に保存されるオブジェクトの識別子です。
       *
       * @default false
       */
      entityId?: boolean | undefined;
    }>
    | undefined;

  /**
   * 対象を限定します。
   */
  where: {
    /**
     * ディレクトリーパスです。
     */
    dirPath: readonly string[];

    /**
     * `true` ならオブジェクトのみを、`false` ならディレクトリーのみをリストアップします。
     */
    isObject: boolean | undefined;
  };

  /**
   * スキップするアイテムの数です。
   *
   * @default 0
   */
  skip: v.InferOutput<typeof schemas.UnsignedInteger> | undefined;

  /**
   * 取得するアイテムの最大数です。
   *
   * @default 上限なし
   */
  take: v.InferOutput<typeof schemas.UnsignedInteger> | undefined;

  /**
   * 結果の並び順を指定します。
   */
  orderBy: Readonly<{
    /**
     * オブジェクト名の並び順です。
     *
     * @default "ASC"
     */
    name: v.InferOutput<typeof schemas.OrderType> | undefined;

    /**
     * オブジェクトを先頭にします。
     *
     * @default false
     */
    preferObject: boolean | undefined;
  }>;
}>;

/**
 * ディレクトリーをリストアップした結果です。
 *
 * @template TSelect SELECT するカラムです。
 */
type ListOutputDirectoryItem = {
  /**
   * `false` ならディレクトリーです。
   */
  isObject: false;

  /**
   * リストアイテムの名前です。
   */
  name: string;
};

/**
 * オブジェクトをリストアップした結果です。
 *
 * @template TSelect SELECT するカラムです。
 */
type ListOutputObjectItem<TSelect> = $Select<ObjectMetadata, TSelect> & {
  /**
   * `true` ならオブジェクトです。
   */
  isObject: true;

  /**
   * リストアイテムの名前です。
   */
  name: string;
};

/**
 * ディレクトリーまたはオブジェクトをリストアップした結果です。
 *
 * @template TSelect SELECT するカラムです。
 */
type ListOutput<TSelect, TIsObject> = undefined extends TIsObject
  ? ListOutputDirectoryItem | ListOutputObjectItem<TSelect>
  : (
    | (false extends TIsObject ? ListOutputDirectoryItem : never)
    | (true extends TIsObject ? ListOutputObjectItem<TSelect> : never)
  );

/**
 * ゴミ箱に入れられたオブジェクトのメタデータを取得するための入力パラメーターです。
 */
type ListInTrashInput = Readonly<{
  /**
   * 結果に含めるカラムを選択します。
   */
  select:
    | Readonly<{
      /**
       * バケット名です。
       *
       * @default false
       */
      bucket?: boolean | undefined;

      /**
       * オブジェクトの識別子です。
       *
       * @default false
       */
      id?: boolean | undefined;

      /**
       * バケット内のオブジェクトパスです。
       *
       * @default false
       */
      path?: boolean | undefined;

      /**
       * `recordType` が更新された時刻 (ミリ秒) です。
       *
       * @default false
       */
      recordTimestamp?: boolean | undefined;

      /**
       * オブジェクト形式です。
       *
       * @default false
       */
      mimeType?: boolean | undefined;

      /**
       * オブジェクトが作成された時刻 (ミリ秒) です。
       *
       * @default false
       */
      createdAt?: boolean | undefined;

      /**
       * オブジェクトが最後に更新された時刻 (ミリ秒) です。
       *
       * @default false
       */
      lastModifiedAt?: boolean | undefined;

      /**
       * オブジェクトのチェックサム (MD5 ハッシュ値) です。
       *
       * @default false
       */
      checksum?: boolean | undefined;

      /**
       * オブジェクトのチェックサムのアルゴリズムです。
       *
       * @default false
       */
      checksumAlgorithm?: boolean | undefined;

      /**
       * 実際に保存されるオブジェクトの識別子です。
       *
       * @default false
       */
      entityId?: boolean | undefined;
    }>
    | undefined;

  /**
   * 対象を限定します。
   */
  where:
    | Readonly<{
      /**
       * バケット内のオブジェクトパスです。
       */
      objectPath: ObjectPath;
    }>
    | undefined;

  /**
   * スキップする結果の数です。
   *
   * @default 0
   */
  skip: v.InferOutput<typeof schemas.UnsignedInteger> | undefined;

  /**
   * 取得する結果の最大数です。
   *
   * @default 上限なし
   */
  take: v.InferOutput<typeof schemas.UnsignedInteger> | undefined;
}>;

/**
 * ゴミ箱に入れられたオブジェクトのメタデータを取得した結果です。
 *
 * @template TSelect SELECT するカラムです。
 */
type ListInTrashOutput<TSelect> = $Select<ObjectMetadataInTrash, TSelect>;

/**
 * オブジェクトの説明文を対象に全文検索するための入力パラメーターです。
 */
type SearchInput = Readonly<{
  /**
   * ディレクトリーパスです。
   */
  dirPath: readonly string[];

  /**
   * 検索クエリーです。
   */
  query: string;

  /**
   * スキップする検索結果の数です。
   *
   * @default 0
   */
  skip: v.InferOutput<typeof schemas.UnsignedInteger> | undefined;

  /**
   * 取得する検索結果の最大数です。
   *
   * @default 上限なし
   */
  take: v.InferOutput<typeof schemas.UnsignedInteger> | undefined;

  /**
   * ディレクトリー内のオブジェクトを再帰的に検索するなら `true`、しないなら `false` を指定します。
   *
   * @default false
   */
  recursive: boolean | undefined;

  /**
   * 検索にヒットしたと判断するスコアのしきい値です。
   *
   * @default 0
   */
  scoreThreshold: number | undefined;
}>;

/**
 * オブジェクトの説明文を対象に全文検索した結果です。
 */
type SearchOutput = {
  /**
   * バケット内のオブジェクトパスです。
   */
  objectPath: ObjectPath;

  /**
   * オブジェクトの説明文です。
   */
  description: string;

  /**
   * 検索スコアです。
   */
  searchScore: number;
};

/**
 * オブジェクトのメタデータを移動するための入力パラメーターです。
 */
type MoveInput = Readonly<{
  /**
   * バケット内の移動元のオブジェクトパスです。
   */
  srcObjectPath: ObjectPath;

  /**
   * バケット内の移動先のオブジェクトパスです。
   */
  dstObjectPath: ObjectPath;
}>;

/**
 * オブジェクトのメタデータをコピーするための入力パラメーターです。
 */
type CopyInput = Readonly<{
  /**
   * バケット内のコピー元のオブジェクトパスです。
   */
  srcObjectPath: ObjectPath;

  /**
   * バケット内のコピー先のオブジェクトパスです。
   */
  dstObjectPath: ObjectPath;

  /**
   * 実際に保存されているオブジェクトの識別子です。
   */
  dstEntityId: v.InferOutput<typeof schemas.EntityId>;

  /**
   * カスタムのタイムスタンプです。
   *
   * @default Date.now()
   */
  timestamp: v.InferOutput<typeof schemas.Timestamp> | undefined;
}>;

/**
 * オブジェクトのメタデータを更新するための入力パラメーターです。
 */
type UpdateInput = Readonly<{
  /**
   * バケット内のオブジェクトパスです。
   */
  objectPath: ObjectPath;

  /**
   * オブジェクトのデータ形式です。
   */
  mimeType: v.InferOutput<typeof schemas.MimeType> | undefined;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  objectTags: v.InferOutput<typeof schemas.ObjectTags> | undefined;

  /**
   * オブジェクトの説明文です。
   */
  description: string | null | undefined;

  /**
   * ユーザー定義のメタデータです。
   */
  userMetadata: unknown | undefined;

  /**
   * カスタムのタイムスタンプです。
   *
   * @default Date.now()
   */
  timestamp: v.InferOutput<typeof schemas.Timestamp> | undefined;
}>;

/**
 * オブジェクトのメタデータを排他的に更新するための入力パラメーターです。
 */
type UpdateExclusiveInput = Readonly<{
  /**
   * バケット内のオブジェクトパスです。
   */
  objectPath: ObjectPath;

  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  entityId: v.InferOutput<typeof schemas.EntityId> | undefined;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: Readonly<{
    /**
     * 計算されたハッシュ値の 16 進数文字列です。
     */
    value: v.InferOutput<typeof schemas.Checksum>;

    /**
     * ハッシュ関数の内部状態です。
     */
    state: v.InferOutput<typeof schemas.HashState>;
  }>;

  /**
   * オブジェクトのデータ形式です。
   */
  mimeType: v.InferOutput<typeof schemas.MimeType> | undefined;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  objectSize: v.InferOutput<typeof schemas.UnsignedInteger>;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  objectTags: v.InferOutput<typeof schemas.ObjectTags> | undefined;

  /**
   * オブジェクトの説明文です。
   */
  description: string | null | undefined;

  /**
   * ユーザー定義のメタデータです。
   */
  userMetadata: unknown | undefined;

  /**
   * 既存のメタデータに期待する値です。
   */
  expect: Readonly<{
    /**
     * 既存のチェックサムに期待する値です。
     */
    checksum: v.InferOutput<typeof schemas.Checksum>;
  }>;

  /**
   * カスタムのタイムスタンプです。
   *
   * @default Date.now()
   */
  timestamp: v.InferOutput<typeof schemas.Timestamp> | undefined;
}>;

/**
 * オブジェクトのメタデータに削除フラグをたてるための入力パラメーターです。
 */
type TrashInput = Readonly<{
  /**
   * バケット内のオブジェクトパスです。
   */
  objectPath: ObjectPath;

  /**
   * カスタムのタイムスタンプです。
   *
   * @default Date.now()
   */
  timestamp: v.InferOutput<typeof schemas.Timestamp> | undefined;
}>;

/**
 * オブジェクトのメタデータに削除フラグをたてた結果です。
 */
type TrashOutput = {
  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  entityId: v.InferOutput<typeof schemas.EntityId>;
};

/**
 * オブジェクトのメタデータを削除するための入力パラメーターです。
 */
type DeleteInput = Readonly<{
  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  entityId: v.InferOutput<typeof schemas.EntityId>;
}>;

/**
 * リストアップの結果です。
 *
 * @template T アイテムの型です。
 */
type ListResult<T> = AsyncGenerator<T, void, void>;

/**
 * エラーオブジェクトのようなオブジェクトかどうか判定します。
 *
 * @param e 判定する値です。
 * @returns 判定結果です。
 */
function isErrorObjectLike(e: unknown): e is { readonly message: string } {
  return e instanceof globalThis.Error || (
    typeof e === "object"
    && e !== null
    && "name" in e
    && "message" in e
    && typeof e.name === "string"
    && typeof e.message === "string"
  );
}

/**
 * カラムとそれに対応する値のデータ型などの定義です。
 *
 * @template TKey カラム名の JavaScript 表現です。
 */
type Column<TKey extends string> = Readonly<{
  /**
   * カラム名の JavaScript 表現です。データベースのカラム名はスネークケースですが、
   * JavaScript 表現では、キャメルケースになります。
   */
  key: TKey;

  /**
   * データベースのカラム名です。
   */
  name: string;

  /**
   * `key` の Sql 表現です。
   */
  keySql: Sql;

  /**
   * `SELECT` に埋め込むカラムです。
   */
  selector: Sql;

  /**
   * `SELECT` に埋め込むカラムです。
   */
  toSelector: (table: string) => Sql;

  /**
   * 値の valibot スキーマです。
   */
  valueSchema: v.ObjectEntries[string];
}>;

/**
 * カラムの定義です。
 */
type ColumnDefinition = readonly [
  /**
   * データベースにおけるカラム名です。
   */
  name: string,
  /**
   * カラムの値のデータ型です。
   */
  schema: v.ObjectEntries[string],
];

/**
 * カラムとそれに対応する値のデータ型などの定義を作成します。
 *
 * @template TKey カラム名の JavaScript 表現です。
 * @param columns 行の定義です。
 * @returns カラムとそれに対応する値のデータ型などの定義です。
 */
function createColumns<TKey extends string>(
  columns: Readonly<Record<TKey, ColumnDefinition>>,
): Column<TKey>[] {
  return Object.entries<ColumnDefinition>(columns).map(([key, [name, valueSchema]]) => ({
    key: key as TKey,
    name,
    keySql: raw(key),
    selector: valueSchema !== schemas.Timestamp
      ? raw(name)
      : sql`(EXTRACT(EPOCH FROM ${raw(name)}) * 1000)::BIGINT`,
    toSelector(table) {
      return valueSchema !== schemas.Timestamp
        ? raw(table + "." + name)
        : sql`(EXTRACT(EPOCH FROM ${raw(table)}.${raw(name)}) * 1000)::BIGINT`;
    },
    valueSchema,
  }));
}

export default class Metadata {
  /**
   * メタデータの操作が可能かどうかを示すフラグです。
   */
  #open: boolean;

  /**
   * メタデータを記録するためのデータベースです。
   */
  readonly #db: Db;

  /**
   * メタデータを永続ストレージに記録するためのファイルシステムです。
   */
  readonly #fs: Fs;

  /**
   * ログを記録する関数群です。
   */
  readonly #logger: ConsoleLikeLogger;

  /**
   * バケット名です。
   */
  readonly #bucketName: v.InferOutput<typeof schemas.BucketName>;

  /**
   * オブジェクトの説明文の検索に使用するユーティリティーです。
   */
  readonly #textSearch: Readonly<{
    /**
     * オブジェクトの説明文を検索用の文字列に変換する関数です。
     */
    toQueryString: {
      /**
       * オブジェクトの説明文を検索用の文字列に変換します。
       *
       * @param string 任意の文字列です。
       * @returns 検索用の文字列です。
       */
      (string: string): Promise<schemas.SizeLimitedString>;
    };

    /**
     * オブジェクトの検索用の文字列を説明文に変換する関数です。
     */
    fromQueryString: {
      /**
       * オブジェクトの検索用の文字列を説明文に変換します。
       *
       * @param string 検索用の文字列です。
       * @returns 検索用の文字列から復元された元の文字列です。
       */
      (string: string): Promise<string>;
    };
  }>;

  /**
   * ユーザー定義のメタデータに関して、JavaScript の値と JSON 文字列を相互変換するためのユーティリティーです。
   */
  readonly #userMetaJson: Json;

  /**
   * オブジェクトメタデータのカラムに関する情報です。
   */
  readonly #columns: readonly Column<keyof NonNullable<ReadInput["select"]>>[];

  /**
   * ゴミ箱に入れられたオブジェクトメタデータのカラムに関する情報です。
   */
  readonly #columnsInTrash: readonly Column<keyof NonNullable<ListInTrashInput["select"]>>[];

  /**
   * オブジェクトを検索する際に、インデックスを更新すべきかを示すフラグです。
   * オブジェクトの説明文に変更があったあと、検索前にインデックスを作成/再作成する必要があります。
   */
  #shouldUpdateFtsIndex: boolean;

  /**
   * `Metadata` の新しいインスタンスを構築します。
   *
   * @param inp `Metadata` を構築するための入力パラメーターです。
   */
  public constructor(inp: MetadataInput) {
    const KiB = 1024;
    const {
      db,
      fs,
      json = JSON as Json,
      logger,
      bucketName,
      textSearch = {
        toQueryString: s => s,
        fromQueryString: s => s,
      },
      maxDescriptionTextSize = (10 * KiB) as v.InferOutput<typeof schemas.UnsignedInteger>,
      maxUserMetadataJsonSize = (10 * KiB) as v.InferOutput<typeof schemas.UnsignedInteger>,
    } = inp;
    const descTextSchema = schemas.newSizeLimitedString(maxDescriptionTextSize);
    const userMetaSchema = schemas.newSizeLimitedString(maxUserMetadataJsonSize);
    const parseDescText = v.parser(descTextSchema);
    const parseUserMeta = v.parser(userMetaSchema);

    this.#open = false;
    this.#db = db;
    this.#fs = fs;
    this.#logger = logger;
    this.#bucketName = bucketName;
    this.#textSearch = {
      async toQueryString(s) {
        return parseDescText(await textSearch.toQueryString(s));
      },
      async fromQueryString(s) {
        return await textSearch.fromQueryString(s);
      },
    };
    this.#userMetaJson = {
      parse(s) {
        return json.parse(s);
      },
      stringify(o) {
        return parseUserMeta(json.stringify(o));
      },
    };
    this.#columns = createColumns<keyof NonNullable<ReadInput["select"]>>({
      id: ["id", schemas.ObjectId],
      path: ["path", schemas.ObjectPathLike],
      size: ["size", schemas.UnsignedInteger],
      bucket: ["bucket", schemas.BucketName],
      checksum: ["checksum", schemas.Checksum],
      entityId: ["entity_id", schemas.EntityId],
      mimeType: ["mime_type", schemas.MimeType],
      createdAt: ["created_at", schemas.Timestamp],
      objectTags: ["object_tags", schemas.ObjectTags],
      recordType: ["record_type", schemas.RecordType],
      description: ["description", v.nullable(v.string())],
      userMetadata: [
        "user_metadata",
        v.nullable(v.pipe(
          v.string(),
          v.transform(x => this.#userMetaJson.parse(x)),
        )),
      ],
      lastModifiedAt: ["last_modified_at", schemas.Timestamp],
      recordTimestamp: ["record_timestamp", schemas.Timestamp],
      checksumAlgorithm: ["checksum_algorithm", v.literal("MD5")],
    });
    this.#columnsInTrash = createColumns<keyof NonNullable<ListInTrashInput["select"]>>({
      id: ["id", schemas.ObjectId],
      path: ["path", schemas.ObjectPathLike],
      bucket: ["bucket", schemas.BucketName],
      checksum: ["checksum", schemas.Checksum],
      entityId: ["entityid", schemas.EntityId],
      mimeType: ["mime_type", schemas.MimeType],
      createdAt: ["created_at", schemas.Timestamp],
      lastModifiedAt: ["last_modified_at", schemas.Timestamp],
      recordTimestamp: ["record_timestamp", schemas.Timestamp],
      checksumAlgorithm: ["checksum_algorithm", v.literal("MD5")],
    });
    this.#shouldUpdateFtsIndex = true; // 初回は必ずインデックスを更新します。
  }

  /**
   * SQL クエリーを実行します。
   *
   * @param sql 実行する SQL クエリーです。
   * @returns クエリーの実行結果をイテレーターで返します。
   */
  async #exec(sql: string | Sql): Promise<void> {
    if (!this.#open) {
      throw new Error("Not connected"); // TODO: ちゃんと書く
    }

    if (typeof sql === "string") {
      await this.#db.exec(sql);
      return;
    }

    if (sql.values.length === 0) {
      await this.#db.exec(sql.text);
      return;
    }

    const stmt = await this.#db.prepare(sql.text);
    try {
      await stmt.exec(...sql.values);
    } finally {
      try {
        await stmt.close();
      } catch (ex) {
        this.#logger.error("Metadata.#query: Filed to close async prepared statement", ex);
      }
    }
  }

  /**
   * SQL クエリーを実行し、結果を返します。
   *
   * @param sql 実行する SQL クエリーです。
   * @returns クエリーの実行結果をイテレーターで返します。
   */
  public async *stream(sql: string | Sql): ListResult<Row> {
    if (!this.#open) {
      throw new Error("Not connected"); // TODO: ちゃんと書く
    }

    if (typeof sql === "string") {
      yield* await this.#db.query(sql);
    } else if (sql.values.length === 0) {
      yield* await this.#db.query(sql.text);
    } else {
      const stmt = await this.#db.prepare(sql.text);
      try {
        for await (const row of await stmt.query(...sql.values)) {
          yield row;
        }
      } finally {
        try {
          await stmt.close();
        } catch (ex) {
          this.#logger.error("Metadata.#query: Filed to close async prepared statement", ex);
        }
      }
    }
  }

  /**
   * SQL クエリーを実行し、結果を返します。
   *
   * @param sql 実行する SQL クエリーです。
   * @returns クエリーの実行結果を配列で返します。
   */
  async #query(sql: string | Sql): Promise<Row[]> {
    return await Array.fromAsync(this.stream(sql));
  }

  /**
   * DuckDB の変更内容をファイルに同期します。
   */
  async #flush(): Promise<void> {
    try {
      await this.#exec("CHECKPOINT");
    } catch (ex) {
      this.#logger.error("Metadata.#flush: Failed to synchronize data in WAL to database file", ex);
    }
  }

  /**
   * DuckDB データベースに接続します。
   */
  @mutex
  public async connect(): Promise<void> {
    if (this.#open) {
      return;
    }

    // ディレクトリーを準備します。
    const omnio = await this.#fs.getDirectoryHandle("omnio", { create: true });
    const buckets = await omnio.getDirectoryHandle("buckets", { create: true });
    const bucket = await buckets.getDirectoryHandle(this.#bucketName, { create: true });
    await bucket.getDirectoryHandle("metadata", { create: true });

    // データベースに接続します。
    const path = this.#fs.path.resolve(
      "omnio",
      "buckets",
      this.#bucketName,
      "metadata",
      "duckdb",
    );
    await this.#db.open(path);

    // マイグレーションを行います。
    try {
      this.#open = true; // `.#query` を使うために、フラグを true にします。
      for (const sql of migrations) {
        await this.#exec(sql({
          bucketName: this.#bucketName,
        }));
      }

      await this.#flush();
    } catch (ex) {
      this.#open = false; // フラグを元に戻します。

      // データベースはファイルを利用しているため、データベース -> ファイルシステムの順番で閉じます。
      try {
        await this.#db.close();
      } catch (ex) {
        this.#logger.error("Metadata.connect: Failed to close db", ex);
      }

      throw ex;
    }
  }

  /**
   * データベースを切断します。
   */
  @mutex
  public async disconnect(): Promise<void> {
    if (!this.#open) {
      return;
    }

    // 変更を反映します。
    await this.#db.query("CHECKPOINT");

    // 変更の反映後にデータベースを閉じます。
    await this.#db.close();

    // フラグを更新します。
    this.#open = false;
  }

  /**
   * オブジェクトのメタデータを作成します。
   *
   * @param inp オブジェクトのメタデータを作成するための入力パラメーターです。
   */
  @mutex
  public async create(inp: CreateInput): Promise<void> {
    const {
      checksum,
      entityId,
      mimeType,
      timestamp,
      objectPath,
      objectSize,
      objectTags = [],
      description = null,
      userMetadata = null,
    } = inp;
    const type = mimeType ?? getMimeType(objectPath.basename);
    const desc = description === null ? null : await this.#textSearch.toQueryString(description);
    const meta = userMetadata === null ? null : this.#userMetaJson.stringify(userMetadata);
    const pathSeg = objectPath.segments.length ? join(objectPath.segments, ",") : empty;
    const md5State = checksum.state.length ? join(checksum.state, ",") : empty;
    const objTags = objectTags.length ? join(objectTags, ",") : empty;
    const objId = getObjectId();
    const time = new Date(timestamp ?? Date.now()).toISOString();
    const stmt: Sql[] = [];
    stmt.push(sql`
      INSERT INTO metadata_v1 (
        objectid,
        fullpath,
        path_key,
        path_seg,
        rec_type,
        rec_time,
        obj_size,
        mime_typ,
        new_time,
        mod_time,
        hash_md5,
        md5state,
        obj_tags,
        desc_fts,
        usermeta,
        entityid
      ) VALUES (
        ${objId},
        ${objectPath.fullpath},
        ${objectPath.fullpath},
        ARRAY[${pathSeg}],
        'CREATE',
        ${time},
        ${objectSize},
        ${type},
        ${time},
        ${time},
        ${checksum.value},
        ARRAY[${md5State}],
        ARRAY[${objTags}],
        ${desc},
        ${meta},
        ${entityId}
      )
      ON CONFLICT (path_key)
      DO UPDATE SET
        objectid = ${objId},
        rec_type = 'CREATE',
        rec_time = ${time},
        obj_size = ${objectSize},
        mime_typ = ${type},
        new_time = ${time},
        mod_time = ${time},
        hash_md5 = ${checksum.value},
        md5state = ARRAY[${md5State}],
        obj_tags = ARRAY[${objTags}],
        desc_fts = ${desc},
        usermeta = ${meta},
        entityid = ${entityId}
    `);
    await this.#exec(join(stmt, ""));

    await this.#flush();

    // オブジェクトの説明文が存在するとき、次回の検索前にインデックスを更新する必要があります。
    if (desc) {
      this.#shouldUpdateFtsIndex = true;
    }
  }

  /**
   * オブジェクトのメタデータを排他的に作成します。
   *
   * @param inp オブジェクトのメタデータを排他的に作成するための入力パラメーターです。
   */
  @mutex
  public async createExclusive(inp: CreateExclusiveInput): Promise<void> {
    const {
      checksum,
      entityId,
      mimeType,
      timestamp,
      objectPath,
      objectSize,
      objectTags = [],
      description = null,
      userMetadata = null,
    } = inp;
    const type = mimeType ?? getMimeType(objectPath.basename);
    const desc = description === null ? null : await this.#textSearch.toQueryString(description);
    const meta = userMetadata === null ? null : this.#userMetaJson.stringify(userMetadata);
    const pathSeg = objectPath.segments.length ? join(objectPath.segments, ",") : empty;
    const md5State = checksum.state.length ? join(checksum.state, ",") : empty;
    const objTags = objectTags.length ? join(objectTags, ",") : empty;
    const objId = getObjectId();
    const time = new Date(timestamp ?? Date.now()).toISOString();
    try {
      await this.#exec(sql`
        INSERT INTO metadata_v1 (
          objectid,
          fullpath,
          path_key,
          path_seg,
          rec_type,
          rec_time,
          obj_size,
          mime_typ,
          new_time,
          mod_time,
          hash_md5,
          md5state,
          obj_tags,
          desc_fts,
          usermeta,
          entityid
        ) VALUES (
          ${objId},
          ${objectPath.fullpath},
          ${objectPath.fullpath},
          ARRAY[${pathSeg}],
          'CREATE',
          ${time},
          ${objectSize},
          ${type},
          ${time},
          ${time},
          ${checksum.value},
          ARRAY[${md5State}],
          ARRAY[${objTags}],
          ${desc},
          ${meta},
          ${entityId}
        )
      `);
    } catch (ex) {
      if (
        isErrorObjectLike(ex)
        && ex.message.startsWith(`Constraint Error: Duplicate key "path_key: `)
      ) {
        throw new ObjectExistsError(this.#bucketName, objectPath, { cause: ex });
      }

      throw ex;
    }

    await this.#flush();

    // オブジェクトの説明文が存在するとき、次回の検索前にインデックスを更新する必要があります。
    if (desc) {
      this.#shouldUpdateFtsIndex = true;
    }
  }

  /**
   * 存在するオブジェクトのメタデータを取得します。
   *
   * @param inp オブジェクトのメタデータを取得するための入力パラメーターです。
   * @returns オブジェクトのメタデータを取得した結果です。
   * @throws オブジェクトが見つからない場合は `ObjectNotFoundError` を投げます。
   */
  // @ts-expect-error 方が複雑すぎるのかエラーがでます。
  @mutex.readonly
  public async read<const TInp extends ReadInput>(
    inp: TInp,
  ): Promise<ReadOutput<$Get<TInp, "select">>> {
    const {
      where,
      select,
    } = inp;
    const columns: Sql[] = [];
    const entries: v.ObjectEntries = {};
    const selectAll = select === undefined;
    for (const column of this.#columns) {
      if (!selectAll && select[column.key] !== true) {
        continue;
      }

      columns.push(sql`
        ${column.selector} AS "${column.keySql}"`);
      entries[column.key] = column.valueSchema;
    }

    const [row] = await this.#query(sql`
      SELECT
        ${columns.length ? join(columns, ",") : 1}
      FROM
        metadata
      WHERE
        record_type != 'DELETE'
        AND path = ${where.objectPath.fullpath}
      LIMIT
        1
    `);
    if (!row) {
      throw new ObjectNotFoundError(this.#bucketName, where.objectPath);
    }

    return v.parse(v.object(entries), row) as ReadOutput<$Get<TInp, "select">>;
  }

  /**
   * 存在するオブジェクトの内部利用のためのメタデータを取得します。
   *
   * @param inp 存在するオブジェクトの内部利用のためのメタデータを取得するための入力パラメーターです。
   * @returns 存在するオブジェクトの内部利用のためのメタデータを取得した結果です。
   */
  @mutex.readonly
  public async readInternal(inp: ReadInternalInput): Promise<ReadInternalOutput> {
    const { objectPath } = inp;
    const [row] = await this.#query(sql`
      SELECT
        entityid AS "entityId",
        hash_md5 AS "checksumValue",
        md5state AS "checksumState"
      FROM
        metadata_v1
      WHERE
        path_key = ${objectPath.fullpath}
    `);
    if (!row) {
      throw new ObjectNotFoundError(this.#bucketName, objectPath);
    }

    const {
      entityId,
      checksumValue,
      checksumState,
    } = v.parse(
      v.object({
        entityId: schemas.EntityId,
        checksumValue: schemas.Checksum,
        checksumState: schemas.HashState,
      }),
      row,
    );

    return {
      entityId,
      checksum: {
        value: checksumValue,
        state: checksumState,
      },
    };
  }

  /**
   * パスが存在するか確認します。
   *
   * @param inp パスが存在するか確認するための入力パラメーターです。
   * @returns パスが存在するか確認した結果です。
   */
  @mutex.readonly
  public async exists(inp: ExistsInput): Promise<ExistsOutput> {
    if ("objectPath" in inp) {
      const { objectPath } = inp;
      const [row] = await this.#query(sql`
        SELECT
          1
        FROM
          metadata_v1
        WHERE
          path_key = ${objectPath.fullpath}
      `);

      return {
        exists: !!row,
      };
    }

    const { dirPath } = inp;
    if (dirPath.length === 0) {
      return {
        exists: true,
      };
    }

    const stmt: Sql[] = [];
    stmt.push(sql`
      SELECT
        1
      FROM
        metadata_v1
      WHERE
        array_length(path_seg, 1) > ${dirPath.length}`);
    for (let i = 0; i < dirPath.length; i++) {
      stmt.push(sql`
        AND path_seg[${raw((i + 1).toString(10))}] = ${dirPath[i]}`);
    }
    stmt.push(sql`
      LIMIT
        1
    `);
    const [row] = await this.#query(join(stmt, ""));

    return {
      exists: !!row,
    };
  }

  /**
   * オブジェクトやディレクトリーのステータス情報を取得します。
   *
   * @param inp オブジェクトやディレクトリーのステータス情報を取得するための入力パラメーターです。
   * @returns オブジェクトやディレクトリーのステータス情報を取得した結果です。
   */
  @mutex.readonly
  public async stat(inp: StatInput): Promise<StatOutput> {
    const {
      objectPath: {
        fullpath: pathKey,
        segments: dirPath,
      },
    } = inp;
    const stmt: Sql[] = [];
    stmt.push(sql`
      SELECT
      (
        SELECT
          1
        FROM
          metadata_v1
        WHERE
          path_key = ${pathKey}
      ) AS is_object`);
    stmt.push(sql`,
      (
        SELECT
          1
        FROM
          metadata_v1
        WHERE
          array_length(path_seg, 1) > ${dirPath.length}`);
    for (let i = 0; i < dirPath.length; i++) {
      stmt.push(sql`
          AND path_seg[${raw((i + 1).toString(10))}] = ${dirPath[i]}`);
    }

    stmt.push(sql`
        LIMIT
          1
      ) AS is_directory
    `);
    const [row] = await this.#query(join(stmt, ""));

    return {
      isObject: !!row!["is_object"],
      isDirectory: !!row!["is_directory"],
    };
  }

  async #listObjects(inp: ListInput) {
    const {
      skip = 0,
      take,
      where: {
        dirPath,
      },
      select,
      orderBy: {
        name: nameOrder = "ASC",
      },
    } = inp;

    const columns: Sql[] = [];
    const entries: v.ObjectEntries = {
      name: v.string(),
      isObject: v.literal(true),
    };
    const selectAll = select === undefined;
    for (const column of this.#columns) {
      if (!selectAll && select[column.key] !== true) {
        continue;
      }

      columns.push(sql`
        ${column.toSelector("ref")} AS "${column.keySql}"`);
      entries[column.key] = column.valueSchema;
    }

    const stmt: Sql[] = [];
    const nameIdxSql = raw((dirPath.length + 1).toString(10));
    const pathSegLenSql = raw((dirPath.length + 1).toString(10));
    stmt.push(sql`
      SELECT
        src.path_seg[${nameIdxSql}] AS "name",
        TRUE AS "isObject",
        ${join(columns, ",")}
      FROM
        metadata_v1 AS src
      INNER JOIN
        metadata AS ref
      ON
        src.objectid = ref.id
      WHERE
        array_length(src.path_seg, 1) = ${pathSegLenSql}`);
    for (let i = 0; i < dirPath.length; i++) {
      stmt.push(sql`
        AND src.path_seg[${raw((i + 1).toString(10))}] = ${dirPath[i]}`);
    }

    stmt.push(sql`
      ORDER BY
        "name" ${raw(nameOrder)}`);

    // ページネーションを適用します。
    if (take !== undefined && take > 0) {
      stmt.push(sql`
      LIMIT
        ${take}`);
    }
    if (skip > 0) {
      stmt.push(sql`
      OFFSET
        ${skip}`);
    }

    const rows = await this.#query(join(stmt, ""));
    const schema = v.object(entries);

    return (async function*(rows, schema): any {
      for (const row of rows) {
        yield v.parse(schema, row);
      }
    })(rows, schema);
  }

  async #listDirectories(inp: ListInput) {
    const {
      skip = 0,
      take,
      where: {
        dirPath,
      },
      orderBy: {
        name: nameOrder = "ASC",
      },
    } = inp;

    const stmt: Sql[] = [];
    const nameIdxSql = raw((dirPath.length + 1).toString(10));
    const pathSegLenSql = raw((dirPath.length + 1).toString(10));
    stmt.push(sql`
      SELECT
        DISTINCT ON ("name")
        path_seg[${nameIdxSql}] AS "name",
        FALSE AS "isObject",
      FROM
        metadata_v1
      WHERE
        array_length(path_seg, 1) > ${pathSegLenSql}`);
    for (let i = 0; i < dirPath.length; i++) {
      stmt.push(sql`
        AND path_seg[${raw((i + 1).toString(10))}] = ${dirPath[i]}`);
    }

    stmt.push(sql`
      ORDER BY
        "name" ${raw(nameOrder)}`);

    // ページネーションを適用します。
    if (take !== undefined && take > 0) {
      stmt.push(sql`
      LIMIT
        ${take}`);
    }
    if (skip > 0) {
      stmt.push(sql`
      OFFSET
        ${skip}`);
    }

    const rows = await this.#query(join(stmt, ""));
    const schema = v.object({
      name: v.string(),
      isObject: v.literal(false),
    });

    return (async function*(rows, schema): any {
      for (const row of rows) {
        yield v.parse(schema, row);
      }
    })(rows, schema);
  }

  /**
   * ディレクトリーまたはオブジェクトをリストアップします。
   *
   * @param inp ディレクトリーまたはオブジェクトをリストアップするための入力パラメーターです。
   * @returns リストアップした結果です。
   */
  // @ts-expect-error 方が複雑すぎるのかエラーがでます。
  @mutex.readonly
  public async list<const TInp extends ListInput>(
    inp: TInp,
  ): Promise<ListResult<ListOutput<$Get<TInp, "select">, $Get<$Get<TInp, "where">, "isObject">>>> {
    if (inp.where.isObject === true) {
      return await this.#listObjects(inp);
    }
    if (inp.where.isObject === false) {
      return await this.#listDirectories(inp);
    }

    const {
      skip = 0,
      take,
      where: {
        dirPath,
      },
      select,
      orderBy: {
        name: nameOrder = "ASC",
        preferObject = false,
      },
    } = inp;

    const columns: Sql[] = [];
    const entries: v.ObjectEntries = {
      isObject: v.literal(true),
      name: v.string(),
    };
    const selectAll = select === undefined;
    for (const column of this.#columns) {
      if (!selectAll && select[column.key] !== true) {
        continue;
      }

      columns.push(sql`
        ${column.toSelector("ref")} AS "${column.keySql}"`);
      entries[column.key] = column.valueSchema;
    }

    const stmt: Sql[] = [];
    const nameIdxSql = raw((dirPath.length + 1).toString(10));
    const pathSegLenSql = raw((dirPath.length + 1).toString(10));
    stmt.push(sql`
      SELECT
        DISTINCT ON ("name", "isObject")
        src.path_seg[${nameIdxSql}] AS "name",
        array_length(src.path_seg, 1) = ${pathSegLenSql} AS "isObject",
        ${join(columns, ",")}
      FROM
        metadata_v1 AS src
      LEFT JOIN
        metadata AS ref
      ON
        src.objectid = ref.id
        AND array_length(ref.path_segments, 1) = ${pathSegLenSql}
      WHERE
        array_length(src.path_seg, 1) >= ${pathSegLenSql}`);
    for (let i = 0; i < dirPath.length; i++) {
      stmt.push(sql`
        AND src.path_seg[${raw((i + 1).toString(10))}] = ${dirPath[i]}`);
    }

    stmt.push(sql`
      ORDER BY
        "isObject" ${raw(preferObject ? "DESC" : "ASC")},
        "name" ${raw(nameOrder)}`);

    // ページネーションを適用します。
    if (take !== undefined && take > 0) {
      stmt.push(sql`
      LIMIT
        ${take}`);
    }
    if (skip > 0) {
      stmt.push(sql`
      OFFSET
        ${skip}`);
    }

    const rows = await this.#query(join(stmt, ""));
    const schema = v.union([
      v.object(entries),
      v.object({
        isObject: v.literal(false),
        name: v.string(),
      }),
    ]);

    return (async function*(rows, schema): any {
      for (const row of rows) {
        yield v.parse(schema, row);
      }
    })(rows, schema);
  }

  /**
   * ゴミ箱に入れられたオブジェクトのメタデータを取得します。
   *
   * @param inp オブジェクトのメタデータを取得するための入力パラメーターです。
   * @returns オブジェクトのメタデータを取得した結果です。
   * @throws オブジェクトが見つからない場合は `ObjectNotFoundError` を投げます。
   */
  // @ts-expect-error 方が複雑すぎるのかエラーがでます。
  @mutex.readonly
  public async listInTrash<const TInp extends ListInTrashInput>(
    inp: TInp,
  ): Promise<ListResult<$Select<ObjectMetadataInTrash, $Get<TInp, "select", undefined>>>> {
    const {
      skip = 0,
      take,
      where,
      select,
    } = inp;
    const columns: Sql[] = [];
    const entries: v.ObjectEntries = {};
    const selectAll = select === undefined;
    for (const column of this.#columnsInTrash) {
      if (!selectAll && select[column.key] !== true) {
        continue;
      }

      columns.push(sql`
        ${column.selector} AS "${column.keySql}"`);
      entries[column.key] = column.valueSchema;
    }

    const stmt: Sql[] = [];
    stmt.push(sql`
      SELECT
        ${join(columns, ",")}
      FROM
        metadata_v1
      WHERE
        rec_type = 'DELETE'`);
    if (where?.objectPath !== undefined) {
      stmt.push(sql`
        AND fullpath = ${where.objectPath.fullpath}`);
    }
    // ページネーションを適用します。
    if (take !== undefined && take > 0) {
      stmt.push(sql`
      LIMIT
        ${take}`);
    }
    if (skip > 0) {
      stmt.push(sql`
      OFFSET
        ${skip}`);
    }

    const rows = await this.#query(join(stmt, ""));
    const schema = v.object(entries);

    return (async function*(rows, schema) {
      for (const row of rows) {
        yield v.parse(schema, row) as ListInTrashOutput<$Get<TInp, "select">>;
      }
    })(rows, schema);
  }

  /**
   * オブジェクトの説明文を対象に全文検索します。
   *
   * @param inp オブジェクトの説明文を対象に全文検索するための入力パラメーターです。
   * @returns 検索結果です。
   */
  public async search(inp: SearchInput): Promise<ListResult<SearchOutput>> {
    const {
      skip = 0,
      take,
      query,
      dirPath,
      recursive = false,
      scoreThreshold = 0,
    } = inp;
    if (this.#shouldUpdateFtsIndex) {
      using _ = await mutex.lock(this);
      // 検索前にインデックスを更新します。
      // ドキュメント:
      // https://duckdb.org/docs/stable/core_extensions/full_text_search.html#pragma-create_fts_index
      await this.#exec(sql`
        PRAGMA create_fts_index(
          metadata_v1, objectid, desc_fts,
          stemmer       = 'none',
          stopwords     = 'none',
          ignore        = '',
          strip_accents = false,
          lower         = true,
          overwrite     = true
        );
      `);
      this.#shouldUpdateFtsIndex = false;
    }

    using _ = await mutex.rLock(this);
    const pathSegOp = recursive ? raw(">=") : raw("=");
    const ftsQuery = await this.#textSearch.toQueryString(query);
    const stmt: Sql[] = [];
    stmt.push(sql`
      SELECT
        fullpath,
        desc_fts AS "description",
        fts_main_metadata_v1.match_bm25(objectid, ${ftsQuery}) AS "searchScore"
      FROM
        metadata_v1
      WHERE
        rec_type != 'DELETE'
        AND array_length(path_seg, 1) ${pathSegOp} ${dirPath.length + 1}`);
    for (let i = 0; i < dirPath.length; i++) {
      stmt.push(sql`
        AND path_seg[${raw((i + 1).toString(10))}] = ${dirPath[i]}`);
    }
    stmt.push(sql`
        AND "searchScore" IS NOT NULL
        AND "searchScore" >= ${scoreThreshold}
      ORDER BY
        "searchScore" DESC`);
    // ページネーションを適用します。
    if (take !== undefined) {
      stmt.push(sql`
      LIMIT
        ${take}`);
    }
    if (skip > 0) {
      stmt.push(sql`
      OFFSET
        ${skip}`);
    }

    const rows = await this.#query(join(stmt, ""));
    const schema = v.object({
      fullpath: v.string(),
      description: v.string(),
      searchScore: v.pipe(v.number(), v.finite()),
    });

    return (async function*(rows, schema, ts) {
      for (const row of rows) {
        const {
          fullpath,
          description,
          searchScore,
        } = v.parse(schema, row);

        yield {
          objectPath: new ObjectPath(fullpath),
          description: await ts.fromQueryString(description),
          searchScore,
        };
      }
    })(rows, schema, this.#textSearch);
  }

  /**
   * オブジェクトのメタデータを移動します。
   *
   * @param inp オブジェクトのメタデータを移動するための入力パラメーターです。
   * @throws 移動元のオブジェクトが見つからない場合は `ObjectNotFoundError` を、
   * 移動先のオブジェクトがすでに存在する場合は `ObjectExistsError` を投げます。
   */
  @mutex
  public async move(inp: MoveInput): Promise<void> {
    const {
      srcObjectPath,
      dstObjectPath,
    } = inp;
    let row: Row | undefined;
    try {
      [row] = await this.#query(sql`
        UPDATE
          metadata_v1
        SET
          fullpath = ${dstObjectPath.fullpath},
          path_key = ${dstObjectPath.fullpath},
          path_seg = ARRAY[${join(dstObjectPath.segments, ",")}]
        WHERE
          path_key = ${srcObjectPath.fullpath}
      `);
    } catch (ex) {
      if (
        isErrorObjectLike(ex)
        && ex.message.startsWith(`Constraint Error: Duplicate key "path_key: `)
      ) {
        throw new ObjectExistsError(this.#bucketName, dstObjectPath, { cause: ex });
      }

      throw ex;
    }

    if (row!["Count"] === 0) {
      throw new ObjectNotFoundError(this.#bucketName, srcObjectPath);
    }

    await this.#flush();
  }

  /**
   * オブジェクトのメタデータをコピーします。
   *
   * @param inp オブジェクトのメタデータをコピーするための入力パラメーターです。
   * @throws コピー元のオブジェクトが見つからない場合は `ObjectNotFoundError` を、
   * コピー先のオブジェクトがすでに存在する場合は `ObjectExistsError` を投げます。
   */
  @mutex
  public async copy(inp: CopyInput): Promise<void> {
    const {
      timestamp,
      dstEntityId,
      srcObjectPath,
      dstObjectPath,
    } = inp;
    let row: Row | undefined;
    const time = new Date(timestamp ?? Date.now()).toISOString();
    try {
      [row] = await this.#query(sql`
        INSERT INTO metadata_v1 (
          objectid,
          fullpath,
          path_key,
          path_seg,
          rec_type,
          rec_time,
          obj_size,
          mime_typ,
          new_time,
          mod_time,
          hash_md5,
          md5state,
          obj_tags,
          desc_fts,
          usermeta,
          entityid
        )
        SELECT
          ${getObjectId()},
          ${dstObjectPath.fullpath},
          ${dstObjectPath.fullpath},
          ARRAY[${join(dstObjectPath.segments, ",")}],
          'CREATE',
          ${time},
          obj_size,
          mime_typ,
          ${time},
          ${time},
          hash_md5,
          md5state,
          obj_tags,
          desc_fts,
          usermeta,
          ${dstEntityId}
        FROM
          metadata_v1
        WHERE
          path_key = ${srcObjectPath.fullpath};
      `);
    } catch (ex) {
      if (
        isErrorObjectLike(ex)
        && ex.message.startsWith(`Constraint Error: Duplicate key "path_key: `)
      ) {
        throw new ObjectExistsError(this.#bucketName, dstObjectPath, { cause: ex });
      }

      throw ex;
    }

    if (row!["Count"] === 0) {
      throw new ObjectNotFoundError(this.#bucketName, srcObjectPath);
    }

    await this.#flush();

    // オブジェクトの説明文が存在する可能性があるので、次回の検索前にインデックスを更新する必要があります。
    this.#shouldUpdateFtsIndex = true;
  }

  /**
   * オブジェクトのメタデータを更新します。
   *
   * @param inp オブジェクトのメタデータを更新するための入力パラメーターです。
   * @throws オブジェクトが見つからない場合は `ObjectNotFoundError` を投げます。
   */
  @mutex
  public async update(inp: UpdateInput): Promise<void> {
    const {
      mimeType,
      timestamp,
      objectPath,
      objectTags,
      description,
      userMetadata,
    } = inp;
    if (
      mimeType === undefined
      && timestamp === undefined
      && objectTags === undefined
      && description === undefined
      && userMetadata === undefined
    ) {
      const [row] = await this.#query(sql`
        SELECT
          COUNT(*) AS "Count"
        FROM
          metadata_v1
        WHERE
          path_key = ${objectPath.fullpath}
      `);
      if (row!["Count"] === 0) {
        throw new ObjectNotFoundError(this.#bucketName, objectPath);
      }

      return;
    }

    const stmt: Sql[] = [];
    stmt.push(sql`
      UPDATE
        metadata_v1
      SET
        rec_type = 'UPDATE_METADATA'`);

    if (mimeType !== undefined) {
      stmt.push(sql`,
        mime_typ = ${mimeType}`);
    }

    if (objectTags !== undefined) {
      stmt.push(sql`,
        obj_tags = ARRAY[${objectTags.length ? join(objectTags, ",") : empty}]`);
    }

    if (description !== undefined) {
      const desc = description === null ? null : await this.#textSearch.toQueryString(description);
      stmt.push(sql`,
        desc_fts = ${desc}`);
    }

    if (userMetadata !== undefined) {
      const meta = userMetadata === null ? null : this.#userMetaJson.stringify(userMetadata);
      stmt.push(sql`,
        usermeta = ${meta}`);
    }

    const time = new Date(timestamp ?? Date.now()).toISOString(); // クエリーを実行する直前に時刻を作成します。
    stmt.push(sql`,
        rec_time = ${time},
        mod_time = ${time}
      WHERE
        path_key = ${objectPath.fullpath}
    `);
    const [row] = await this.#query(join(stmt, ""));
    if (row!["Count"] === 0) {
      throw new ObjectNotFoundError(this.#bucketName, objectPath);
    }

    await this.#flush();

    if (description !== undefined) {
      // オブジェクトの説明文が更新された可能性があるので、次回の検索前にインデックスを更新する必要があります。
      this.#shouldUpdateFtsIndex = true;
    }
  }

  /**
   * オブジェクトのメタデータを排他的に更新します。
   *
   * @param inp オブジェクトのメタデータを排他的に更新するための入力パラメーターです。
   */
  @mutex
  public async updateExclusive(inp: UpdateExclusiveInput): Promise<void> {
    const {
      expect,
      checksum,
      entityId,
      mimeType,
      timestamp,
      objectPath,
      objectSize,
      objectTags,
      description,
      userMetadata,
    } = inp;
    const stmt: Sql[] = [];
    stmt.push(sql`
      UPDATE
        metadata_v1
      SET
        rec_type = 'UPDATE_METADATA',
        hash_md5 = ${checksum.value},
        md5state = ARRAY[${checksum.state.length ? join(checksum.state, ",") : empty}],
        obj_size = ${objectSize}`);

    if (entityId !== undefined) {
      stmt.push(sql`,
        entityid = ${entityId}`);
    }

    if (mimeType !== undefined) {
      stmt.push(sql`,
        mime_typ = ${mimeType}`);
    }

    if (objectTags !== undefined) {
      stmt.push(sql`,
        obj_tags = ARRAY[${objectTags.length ? join(objectTags, ",") : empty}]`);
    }

    if (description !== undefined) {
      const desc = description === null ? null : await this.#textSearch.toQueryString(description);
      stmt.push(sql`,
        desc_fts = ${desc}`);
    }

    if (userMetadata !== undefined) {
      const meta = userMetadata === null ? null : this.#userMetaJson.stringify(userMetadata);
      stmt.push(sql`,
        usermeta = ${meta}`);
    }

    try {
      await this.#exec(sql`BEGIN TRANSACTION`);

      const time = new Date(timestamp ?? Date.now()).toISOString(); // クエリーを実行する直前に時刻を作成します。
      stmt.push(sql`,
        rec_time = ${time},
        mod_time = ${time}
      WHERE
        path_key = ${objectPath.fullpath}
        AND hash_md5 = ${expect.checksum}
      `);

      const [row] = await this.#query(join(stmt, ""));
      if (row!["Count"] === 0) {
        // 更新対象がない原因は `path_key` が無いか、`hash_md5` が一致しないかのどちらかです。
        const [row] = await this.#query(sql`
          SELECT
            COUNT(*) AS "Count"
          FROM
            metadata_v1
          WHERE
            path_key = ${objectPath.fullpath}
        `);
        if (row!["Count"] === 0) {
          // `path_key` がないので、オブジェクトが存在しません。
          throw new ObjectNotFoundError(this.#bucketName, objectPath);
        } else {
          // `path_key` はあるので、`hash_md5` が一致しないということになります。
          throw new ChecksumMismatchError(this.#bucketName, objectPath, expect.checksum);
        }
      }

      await this.#exec(sql`COMMIT`);
    } catch (ex) {
      try {
        await this.#exec(sql`ROLLBACK`);
      } catch (ex) {
        this.#logger.error("Metadata.updateExclusive: Failed to rollback", ex);
      }

      throw ex;
    }

    await this.#flush();

    if (description !== undefined) {
      // オブジェクトの説明文が更新された可能性があるので、次回の検索前にインデックスを更新する必要があります。
      this.#shouldUpdateFtsIndex = true;
    }
  }

  /**
   * オブジェクトの削除フラグを立てます。
   *
   * @param inp オブジェクトの削除フラグをたてるための入力パラメーターです。
   * @throws ファイルが見つからない場合は `ObjectNotFoundError` を投げます。
   */
  @mutex
  public async trash(inp: TrashInput): Promise<TrashOutput> {
    const {
      timestamp,
      objectPath,
    } = inp;
    const time = new Date(timestamp ?? Date.now()).toISOString();
    const [row] = await this.#query(sql`
      UPDATE
        metadata_v1
      SET
        path_key = NULL,
        rec_type = 'DELETE',
        rec_time = ${time},
        obj_size = 0,
        md5state = NULL,
        obj_tags = NULL,
        desc_fts = NULL,
        usermeta = NULL
      WHERE
        path_key = ${objectPath.fullpath}
      RETURNING
        entityid AS "entityId"
    `);
    if (!row) {
      throw new ObjectNotFoundError(this.#bucketName, objectPath);
    }

    await this.#flush();

    // オブジェクトの説明文が更新された可能性があるので、次回の検索前にインデックスを更新する必要があります。
    this.#shouldUpdateFtsIndex = true;

    return v.parse(v.object({ entityId: schemas.EntityId }), row);
  }

  /**
   * オブジェクトのメタデータを完全に削除します。
   *
   * @param inp オブジェクトのメタデータを完全に削除するための入力パラメーターです。
   * @throws ファイルが見つからない場合は `ObjectNotFoundError` を投げます。
   */
  @mutex
  public async delete(inp: DeleteInput): Promise<void> {
    const { entityId } = inp;
    const [row] = await this.#query(sql`
      DELETE FROM
        metadata_v1
      WHERE
        entityid = ${entityId}
    `);
    if (row!["Count"] === 0) {
      throw new EntityNotFoundError(this.#bucketName, entityId);
    }

    await this.#flush();
  }
}
