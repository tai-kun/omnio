import sql, { empty, join, raw } from "sql-template-tag";
import { v7 as getObjectId } from "uuid";
import type { IDatabase, Row } from "../shared/database.js";
import {
  ChecksumMismatchError,
  InvalidCollationError,
  ObjectExistsError,
  ObjectNotFoundError,
  ObjectSizeTooLargeError,
  ObjectSizeTooSamllError,
} from "../shared/errors.js";
import isError from "../shared/is-error.js";
import { type ILogger, LogLevel } from "../shared/logger.js";
import quoteString from "../shared/quote-string.js";
import {
  type BucketName,
  BucketNameSchema,
  type Checksum,
  ChecksumSchema,
  type EntityId,
  EntityIdSchema,
  type HashState,
  HashStateSchema,
  type MimeType,
  MimeTypeSchema,
  type NumParts,
  NumPartsSchema,
  type ObjectDirectoryPath,
  type ObjectId,
  ObjectIdSchema,
  type ObjectPath,
  ObjectPathSchema,
  type ObjectSize,
  ObjectSizeSchema,
  type ObjectTags,
  ObjectTagsSchema,
  type OrderType,
  type PartSize,
  PartSizeSchema,
  RecordTypeSchema,
  type SizeLimitedUtf8String,
  SizeLimitedUtf8StringSchema,
  type Timestamp,
  type Uint,
  UintSchema,
} from "../shared/schemas.js";
import type { ITextSearch } from "../shared/text-search.js";
import * as v from "../shared/valibot.js";
import defineColumns, { type Column } from "./_define-columns.js";
import getMimeType from "./_get-mime-type.js";
import migrations from "./_migrations.js";

/***************************************************************************************************
 *
 * 入力パラメーター
 *
 **************************************************************************************************/

/**
 * JSON 文字列を JavaScript の値に変換する関数のインターフェースです。
 */
export interface IJsonParse {
  /**
   * JSON 文字列を JavaScript の値に変換します。
   *
   * @param string 変換される JSON 文字列です。
   * @returns 変換された JavaScript 値です。
   */
  (string: string): unknown;
}

/**
 * JavaScript の値を JSON 文字列に変換する関数のインターフェースです。
 */
export interface IJsonStringify {
  /**
   * JavaScript の値を JSON 文字列に変換します。
   *
   * @param value 変換される JavaScript 値です。
   * @returns 変換された JSON 文字列です。
   */
  (value: unknown): string;
}

/**
 * JavaScript の値と JSON 文字列を相互変換するための関数群のインターフェースです。
 */
export interface IJson {
  /**
   * JSON 文字列を JavaScript の値に変換する関数です。
   */
  readonly parse: IJsonParse;

  /**
   * JavaScript の値を JSON 文字列に変換する関数です。
   */
  readonly stringify: IJsonStringify;
}

/**
 * `Metadata` を構築するための入力パラメーターです。
 */
export type MetadataInput = Readonly<{
  /**
   * バケット名です。
   */
  bucketName: BucketName;

  /**
   * メタデータを記録するためのデータベースです。
   */
  database: IDatabase;

  /**
   * JavaScript の値と JSON 文字列を相互変換するための関数群です。
   */
  json: IJson;

  /**
   * ログを記録する関数群です。
   */
  logger: ILogger;

  /**
   * オブジェクトの説明文の最大サイズ (バイト数) です。
   */
  maxDescriptionTextByteSize: Uint;

  /**
   * ユーザー定義のメタデータの最大サイズ (バイト数) です。
   * このサイズは、ユーザー定義のメタデータを `json.stringify` で変換したあとの文字列に対して計算されます。
   */
  maxUserMetadataJsonByteSize: Uint;

  /**
   * オブジェクトの説明文の検索に使用するユーティリティーです。
   */
  textSearch: ITextSearch;
}>;

/***************************************************************************************************
 *
 * 行データ
 *
 **************************************************************************************************/

/**
 * 存在するオブジェクトのメタデータです。
 */
export type ObjectMetadata = {
  /**
   * バケット名です。
   */
  bucket: BucketName;

  /**
   * オブジェクトの識別子です。
   */
  id: ObjectId;

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
  recordTimestamp: Timestamp;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  size: ObjectSize;

  /**
   * オブジェクトのパートの総数です。
   */
  numParts: NumParts;

  /**
   * 各パートのサイズ (バイト数) です。
   */
  partSize: PartSize;

  /**
   * オブジェクト形式です。
   */
  mimeType: MimeType;

  /**
   * オブジェクトが作成された時刻 (ミリ秒) です。
   */
  createdAt: Timestamp;

  /**
   * オブジェクトが最後に更新された時刻 (ミリ秒) です。
   */
  lastModifiedAt: Timestamp;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: Checksum;

  /**
   * オブジェクトのチェックサムのアルゴリズムです。
   */
  checksumAlgorithm: "MD5";

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  objectTags: ObjectTags;

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
  entityId: EntityId;
};

/**
 * ゴミ箱に入れられたオブジェクトのメタデータです。
 */
export type ObjectInTrashMetadata = {
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
  id: ObjectId;

  /**
   * `recordType` が更新された時刻 (ミリ秒) です。
   */
  recordTimestamp: Timestamp;

  /**
   * オブジェクト形式です。
   */
  mimeType: MimeType;

  /**
   * オブジェクトが作成された時刻 (ミリ秒) です。
   */
  createdAt: Timestamp;

  /**
   * オブジェクトが最後に更新された時刻 (ミリ秒) です。
   */
  lastModifiedAt: Timestamp;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: Checksum;

  /**
   * オブジェクトのチェックサムのアルゴリズムです。
   */
  checksumAlgorithm: "MD5";

  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  entityId: EntityId;
};

/***************************************************************************************************
 *
 * 行データのセレクター
 *
 **************************************************************************************************/

/**
 * 取得のときに結果に含めるカラムを選択するためのクエリーです。
 */
export type ObjectMetadataSelectQuery = Readonly<{
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
   * オブジェクトのパートの総数です。
   *
   * @default false
   */
  numParts?: boolean | undefined;

  /**
   * 各パートのサイズ (バイト数) です。
   *
   * @default false
   */
  partSize?: boolean | undefined;

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
}>;

/**
 * 取得のときに結果に含めるカラムを選択するためのクエリーです。
 */
export type ObjectInTrashMetadataSelectQuery = Pick<
  ObjectMetadataSelectQuery,
  keyof ObjectInTrashMetadata
>;

/***************************************************************************************************
 *
 * 作成
 *
 **************************************************************************************************/

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
  entityId: EntityId;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: Readonly<{
    /**
     * 計算されたハッシュ値の 16 進数文字列です。
     */
    value: Checksum;

    /**
     * ハッシュ関数の内部状態です。
     */
    state: HashState;
  }>;

  /**
   * オブジェクトのデータ形式です。`undefined` の場合はオブジェクトパスから自動判定されます。
   */
  mimeType: MimeType | undefined;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  objectSize: ObjectSize;

  /**
   * オブジェクトのパートの総数です。
   */
  numParts: NumParts;

  /**
   * 各パートのサイズ (バイト数) です。
   */
  partSize: PartSize;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   *
   * @default []
   */
  objectTags: ObjectTags | undefined;

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
  timestamp: Timestamp | undefined;
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
  entityId: EntityId;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: Readonly<{
    /**
     * 計算されたハッシュ値の 16 進数文字列です。
     */
    value: Checksum;

    /**
     * ハッシュ関数の内部状態です。
     */
    state: HashState;
  }>;

  /**
   * オブジェクトのデータ形式です。`undefined` の場合はオブジェクトパスから自動判定されます。
   */
  mimeType: MimeType | undefined;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  objectSize: ObjectSize;

  /**
   * オブジェクトのパートの総数です。
   */
  numParts: NumParts;

  /**
   * 各パートのサイズ (バイト数) です。
   */
  partSize: PartSize;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   *
   * @default []
   */
  objectTags: ObjectTags | undefined;

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
  timestamp: Timestamp | undefined;
}>;

/***************************************************************************************************
 *
 * 読み取り
 *
 **************************************************************************************************/

/**
 * オブジェクトのプロパティーの型を取得します。
 *
 * @template TObject 対象のオブジェクトです。
 * @template TProperty プロパティーのキーです。
 * @template TNotSet プロパティーが存在しない場合の型です。
 * @returns 指定されたプロパティーの型、またはプロパティーが存在しない場合は TNotSet の型です。
 */
export type $Get<
  TObject,
  TProperty extends keyof any,
  TNotSet = undefined,
> = TObject extends { readonly [_ in TProperty]: infer V } ? V : TNotSet;

/**
 * エディターにおけるオブジェクトの型表示をきれいにします。
 *
 * @template T オブジェクトの型です。
 */
type $Simplify<T> = { [P in keyof T]: T[P] } & {};

/**
 * オブジェクトから、指定したプロパティーのみを選択した型を生成します。
 *
 * @template TObject 対象のオブジェクトです。
 * @template TSelect 行データから選択するプロパティーを、真偽値で指定したオブジェクトです。
 * @returns 指定されたプロパティーのみを持つ新しい型、または TSelect が undefined の場合は TObject の型です。
 */
export type $Select<
  TObject,
  TSelect,
> = TSelect extends undefined ? TObject : $Simplify<
  & {
    [
      P in {
        [P in keyof TSelect]: TSelect[P] extends true ? P : never;
      }[keyof TSelect & keyof TObject]
    ]-?: TObject[P];
  }
  & {
    [
      P in {
        [P in keyof TSelect]: (boolean | undefined) extends TSelect[P] ? P : never;
      }[keyof TSelect & keyof TObject]
    ]+?: TObject[P];
  }
>;

/**
 * 存在するオブジェクトのメタデータを取得するための入力パラメーターです。
 */
type ReadInput = Readonly<{
  /**
   * 結果に含めるカラムを選択します。
   */
  select: ObjectMetadataSelectQuery | undefined;

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
 * 存在するオブジェクトのメタデータを取得した結果です。
 *
 * @template TSelect SELECT するカラムです。
 */
type ReadOutput<TSelect> = $Select<ObjectMetadata, TSelect>;

/**
 * 存在するオブジェクトの内部利用のためのメタデータを取得するための入力パラメーターです。
 */
type ReadDetailInput = Readonly<{
  /**
   * バケット内のオブジェクトパスです。
   */
  objectPath: ObjectPath;
}>;

/**
 * 存在するオブジェクトの内部利用のためのメタデータを取得した結果です。
 */
type ReadDetailOutput = Readonly<{
  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  entityId: EntityId;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: Readonly<{
    /**
     * 計算されたハッシュ値の 16 進数文字列です。
     */
    value: Checksum;

    /**
     * ハッシュ関数の内部状態です。
     */
    state: HashState;
  }>;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  size: ObjectSize;

  /**
   * オブジェクトのパートの総数です。
   */
  numParts: NumParts;

  /**
   * 各パートのサイズ (バイト数) です。
   */
  partSize: PartSize;
}>;

/**
 * ゴミ箱に入れられたオブジェクトの内部利用のためのメタデータを取得するための入力パラメーターです。
 */
type ReadInTrashInput = Readonly<{
  /**
   * オブジェクトの識別子です。
   */
  objectId: ObjectId;
}>;

/**
 * ゴミ箱に入れられたオブジェクトの内部利用のためのメタデータを取得した結果です。
 */
type ReadInTrashOutput = Readonly<{
  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  entityId: EntityId;
}>;

/**
 * パスが存在するか確認するための入力パラメーターです。
 */
type ExistsInput =
  | Readonly<{
    /**
     * バケット内のディレクトリーパスです。
     */
    dirPath: ObjectDirectoryPath;
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
  select: ObjectMetadataSelectQuery | undefined;

  /**
   * 対象を限定します。
   */
  where: Readonly<{
    /**
     * ディレクトリーパスです。
     */
    dirPath: ObjectDirectoryPath;

    /**
     * `true` ならオブジェクトのみを、`false` ならディレクトリーのみをリストアップします。
     */
    isObject: boolean | undefined;
  }>;

  /**
   * スキップするアイテムの数です。
   *
   * @default 0
   */
  skip: Uint | undefined;

  /**
   * 取得するアイテムの最大数です。
   *
   * @default 上限なし
   */
  take: Uint | undefined;

  /**
   * 結果の並び順を指定します。
   */
  orderBy: Readonly<{
    /**
     * オブジェクト名の並び順です。
     */
    name: Readonly<{
      /**
       * 並び順です。
       *
       * @default "ASC"
       */
      type: OrderType | undefined;

      /**
       * 照合順序です。
       *
       * @default "nfc"
       */
      collate: string | undefined;
    }>;

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
  select: ObjectInTrashMetadataSelectQuery | undefined;

  /**
   * 対象を限定します。
   */
  where: Readonly<{
    /**
     * ディレクトリーパスです。
     */
    dirPath: ObjectDirectoryPath;
  }>;

  /**
   * スキップする結果の数です。
   *
   * @default 0
   */
  skip: Uint | undefined;

  /**
   * 取得する結果の最大数です。
   *
   * @default 上限なし
   */
  take: Uint | undefined;

  /**
   * 結果の並び順を指定します。
   */
  orderBy: Readonly<{
    /**
     * オブジェクト名の並び順です。
     */
    name: Readonly<{
      /**
       * 並び順です。
       *
       * @default "ASC"
       */
      type: OrderType | undefined;

      /**
       * 照合順序です。
       *
       * @default "nfc"
       */
      collate: string | undefined;
    }>;
  }>;
}>;

/**
 * ゴミ箱に入れられたオブジェクトのメタデータを取得した結果です。
 *
 * @template TSelect SELECT するカラムです。
 */
type ListInTrashOutput<TSelect> = $Select<ObjectInTrashMetadata, TSelect> & {
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
 * オブジェクトの説明文を対象に全文検索するための入力パラメーターです。
 */
type SearchInput = Readonly<{
  /**
   * ディレクトリーパスです。
   */
  dirPath: ObjectDirectoryPath;

  /**
   * 検索クエリーです。
   */
  query: string;

  /**
   * スキップする検索結果の数です。
   *
   * @default 0
   */
  skip: Uint | undefined;

  /**
   * 取得する検索結果の最大数です。
   *
   * @default 上限なし
   */
  take: Uint | undefined;

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
  description: string | null;

  /**
   * 検索スコアです。
   */
  searchScore: number;
};

/***************************************************************************************************
 *
 * 更新
 *
 **************************************************************************************************/

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
  dstEntityId: EntityId;

  /**
   * カスタムのタイムスタンプです。
   *
   * @default Date.now()
   */
  timestamp: Timestamp | undefined;
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
  mimeType: MimeType | undefined;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  objectTags: ObjectTags | undefined;

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
  timestamp: Timestamp | undefined;
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
  entityId: EntityId | undefined;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: Readonly<{
    /**
     * 計算されたハッシュ値の 16 進数文字列です。
     */
    value: Checksum;

    /**
     * ハッシュ関数の内部状態です。
     */
    state: HashState;
  }>;

  /**
   * オブジェクトのデータ形式です。
   */
  mimeType: MimeType | undefined;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  objectSize: ObjectSize;

  /**
   * オブジェクトのパートの総数です。
   */
  numParts: NumParts;

  /**
   * 各パートのサイズ (バイト数) です。
   */
  partSize: PartSize;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  objectTags: ObjectTags | undefined;

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
    checksum: Checksum;
  }>;

  /**
   * カスタムのタイムスタンプです。
   *
   * @default Date.now()
   */
  timestamp: Timestamp | undefined;
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
  timestamp: Timestamp | undefined;
}>;

/**
 * オブジェクトのメタデータに削除フラグをたてた結果です。
 */
type TrashOutput = {
  /**
   * オブジェクトの識別子です。
   */
  objectId: ObjectId;

  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  entityId: EntityId;
};

/***************************************************************************************************
 *
 * 削除
 *
 **************************************************************************************************/

/**
 * オブジェクトのメタデータを削除するための入力パラメーターです。
 */
type DeleteInput = Readonly<{
  /**
   * オブジェクトの識別子です。
   */
  objectId: ObjectId;
}>;

/***************************************************************************************************
 *
 * ユーティリティー
 *
 **************************************************************************************************/

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
 * オブジェクトサイズが仕様を満たしているか検証します。
 *
 * @param objectSize オブジェクトのサイズ (バイト数) です。
 * @param numParts オブジェクトのパートの総数です。
 * @param partSize 各パートのサイズ (バイト数) です。
 */
function assertObjectSize(objectSize: ObjectSize, numParts: NumParts, partSize: PartSize): void {
  if (objectSize <= (partSize * (numParts - 1))) {
    throw new ObjectSizeTooSamllError(objectSize, numParts, partSize);
  }
  if ((partSize * numParts) < objectSize) {
    throw new ObjectSizeTooLargeError(objectSize, numParts, partSize);
  }
}

/***************************************************************************************************
 *
 * `Metadata` クラスの実装
 *
 **************************************************************************************************/

/**
 * オブジェクトのメタデータを管理するクラスです。
 */
export default class Metadata {
  /**
   * メタデータの操作が可能かどうかを示すフラグです。
   */
  #open: boolean;

  /**
   * オブジェクトを検索する際に、インデックスを更新すべきかを示すフラグです。
   * オブジェクトの説明文に変更があったあと、検索前にインデックスを作成/再作成する必要があります。
   */
  #indexFts: boolean;

  /**
   * 照合順序の一覧です。
   */
  #collationSet: ReadonlySet<string>;

  /**
   * メタデータを記録するためのデータベースです。
   */
  readonly #db: IDatabase;

  /**
   * ログを記録する関数群です。
   */
  readonly #logger: ILogger;

  /**
   * バケット名です。
   */
  readonly #bucketName: BucketName;

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
      (string: string): Promise<SizeLimitedUtf8String>;
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
  readonly #userMetaJson: IJson;

  /**
   * オブジェクトメタデータのカラムに関する情報です。
   */
  readonly #columns: readonly Column<keyof NonNullable<ReadInput["select"]>>[];

  /**
   * ゴミ箱に入れられたオブジェクトメタデータのカラムに関する情報です。
   */
  readonly #inTrashColumns: readonly Column<keyof NonNullable<ListInTrashInput["select"]>>[];

  /**
   * `Metadata` の新しいインスタンスを構築します。
   *
   * @param inp `Metadata` を構築するための入力パラメーターです。
   */
  public constructor(inp: MetadataInput) {
    const B = 1;
    const KB = 1_000 * B;

    const {
      json = JSON satisfies IJson,
      logger,
      database,
      bucketName,
      textSearch = {
        toQueryString: s => s,
        fromQueryString: s => s,
      },
      maxDescriptionTextByteSize = 10 * KB,
      maxUserMetadataJsonByteSize = 10 * KB,
    } = inp;
    const DescTextSchema = SizeLimitedUtf8StringSchema(maxDescriptionTextByteSize);
    const UserMetaSchema = SizeLimitedUtf8StringSchema(maxUserMetadataJsonByteSize);

    this.#open = false;
    this.#indexFts = true; // 初回は必ずインデックスを更新します。
    this.#collationSet = new Set();
    this.#db = database;
    this.#logger = logger;
    this.#bucketName = bucketName;
    this.#textSearch = {
      async toQueryString(s) {
        return v.parse(DescTextSchema, await textSearch.toQueryString(s));
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
        return v.parse(UserMetaSchema, json.stringify(o) ?? "null");
      },
    };
    this.#columns = defineColumns<keyof NonNullable<ReadInput["select"]>>({
      id: ["id", ObjectIdSchema()],
      path: ["path", ObjectPathSchema()],
      size: ["size", ObjectSizeSchema()],
      bucket: ["bucket", BucketNameSchema()],
      checksum: ["checksum", ChecksumSchema()],
      entityId: ["entity_id", EntityIdSchema()],
      mimeType: ["mime_type", MimeTypeSchema()],
      numParts: ["num_parts", NumPartsSchema()],
      partSize: ["part_size", PartSizeSchema()],
      createdAt: ["created_at", "Timestamp"],
      objectTags: ["object_tags", ObjectTagsSchema()],
      recordType: ["record_type", RecordTypeSchema()],
      description: ["description", v.nullable(v.string())],
      userMetadata: [
        "user_metadata",
        v.nullable(v.pipe(v.string(), v.transform(x => this.#userMetaJson.parse(x)))),
      ],
      lastModifiedAt: ["last_modified_at", "Timestamp"],
      recordTimestamp: ["record_timestamp", "Timestamp"],
      checksumAlgorithm: ["checksum_algorithm", v.literal("MD5")],
    });
    this.#inTrashColumns = defineColumns<keyof NonNullable<ListInTrashInput["select"]>>({
      id: ["id", ObjectIdSchema()],
      path: ["path", ObjectPathSchema()],
      bucket: ["bucket", BucketNameSchema()],
      checksum: ["checksum", ChecksumSchema()],
      entityId: ["entity_id", EntityIdSchema()],
      mimeType: ["mime_type", MimeTypeSchema()],
      createdAt: ["created_at", "Timestamp"],
      lastModifiedAt: ["last_modified_at", "Timestamp"],
      recordTimestamp: ["record_timestamp", "Timestamp"],
      checksumAlgorithm: ["checksum_algorithm", v.literal("MD5")],
    });
  }

  /**
   * SQL クエリーを実行します。
   *
   * @param sql 実行する SQL クエリーです。
   * @returns クエリーの実行結果をイテレーターで返します。
   */
  async #exec(sql: string | Sql): Promise<void> {
    if (!this.#open) {
      throw new Error("Not connected");
    } else if (typeof sql === "string") {
      this.#logger.log({
        level: LogLevel.DEBUG,
        message: sql,
      });

      await this.#db.exec(sql);
    } else if (sql.values.length === 0) {
      this.#logger.log({
        level: LogLevel.DEBUG,
        message: sql.text,
      });

      await this.#db.exec(sql.text);
    } else {
      this.#logger.log({
        level: LogLevel.DEBUG,
        message: sql.text,
      });

      const stmt = await this.#db.prepare(sql.text);
      try {
        await stmt.exec(...sql.values);
      } finally {
        try {
          await stmt.close();
        } catch (ex) {
          this.#logger.log({
            level: LogLevel.ERROR,
            reason: ex,
            message: "Metadata.#exec: Filed to close prepared statement",
          });
        }
      }
    }
  }

  /**
   * SQL クエリーを実行し、結果を返します。
   *
   * @param sql 実行する SQL クエリーです。
   * @returns クエリーの実行結果をイテレーターで返します。
   */
  public async *stream(sql: string | Sql): AsyncGenerator<Row> {
    if (!this.#open) {
      throw new Error("Not connected");
    } else if (typeof sql === "string") {
      this.#logger.log({
        level: LogLevel.DEBUG,
        message: sql,
      });

      yield* await this.#db.query(sql);
    } else if (sql.values.length === 0) {
      this.#logger.log({
        level: LogLevel.DEBUG,
        message: sql.text,
      });

      yield* await this.#db.query(sql.text);
    } else {
      this.#logger.log({
        level: LogLevel.DEBUG,
        message: sql.text,
      });

      const stmt = await this.#db.prepare(sql.text);
      try {
        for await (const row of await stmt.query(...sql.values)) {
          yield row;
        }
      } finally {
        try {
          await stmt.close();
        } catch (ex) {
          this.#logger.log({
            level: LogLevel.ERROR,
            reason: ex,
            message: "Metadata.stream: Filed to close prepared statement",
          });
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
      this.#logger.log({
        level: LogLevel.ERROR,
        reason: ex,
        message: "Metadata.#flush: Failed to synchronize data in WAL to database file",
      });
    }
  }

  /**
   * DuckDB データベースに接続します。
   */
  public async open(): Promise<void> {
    if (this.#open) {
      return;
    }

    await this.#db.open();

    try {
      // マイグレーションを行います。
      this.#open = true; // `.#exec`, `.#query` を使うために、フラグを true にします。
      for (const sql of migrations) {
        await this.#exec(sql({
          bucketName: this.#bucketName,
        }));
      }

      await this.#exec(sql`PRAGMA collations`);
      const [row] = await this.#query(sql`SELECT list(collname) AS cols FROM pragma_collations()`);
      const schema = v.object({ cols: v.array(v.string()) });
      const { cols } = v.expect(schema, row);
      this.#collationSet = new Set(cols);

      await this.#flush();
    } catch (ex) {
      this.#open = false; // フラグを元に戻します。

      try {
        await this.#db.close();
      } catch (ex) {
        this.#logger.log({
          level: LogLevel.ERROR,
          reason: ex,
          message: "Metadata.connect: Failed to close db",
        });
      }

      throw ex;
    }
  }

  /**
   * データベースを切断します。
   */
  public async close(): Promise<void> {
    if (!this.#open) {
      return;
    }

    // 変更を反映します。
    await this.#db.query("CHECKPOINT");

    // 変更の反映後にデータベースを閉じます。
    await this.#db.close();

    // 内部変数を初期値に戻します。
    this.#open = false;
    this.#indexFts = true;
    this.#collationSet = new Set();
  }

  /*************************************************************************************************
   *
   * 作成
   *
   ************************************************************************************************/

  /**
   * オブジェクトのメタデータを作成します。
   *
   * @param inp オブジェクトのメタデータを作成するための入力パラメーターです。
   */
  public async create(inp: CreateInput): Promise<void> {
    const {
      checksum,
      entityId,
      mimeType,
      numParts,
      partSize,
      timestamp,
      objectPath,
      objectSize,
      objectTags = [],
      description,
      userMetadata,
    } = inp;
    assertObjectSize(objectSize, numParts, partSize);
    const pathSegs = objectPath.segments.length ? join(objectPath.segments) : empty;
    const md5State = checksum.state.length ? join(checksum.state) : empty;
    const objTags = objectTags.length ? join(objectTags) : empty;
    const objId = getObjectId();
    const type = mimeType ?? getMimeType(objectPath.basename);
    const desc = description == null ? null : await this.#textSearch.toQueryString(description);
    const meta = this.#userMetaJson.stringify(userMetadata);
    const time = new Date(timestamp ?? Date.now()).toISOString();
    // await this.#exec(sql`
    //   INSERT INTO metadata_v1 (
    //     objectid,
    //     fullpath,
    //     path_key,
    //     path_seg,
    //     rec_type,
    //     rec_time,
    //     obj_size,
    //     numparts,
    //     partsize,
    //     mime_typ,
    //     new_time,
    //     mod_time,
    //     hash_md5,
    //     md5state,
    //     obj_tags,
    //     desc_fts,
    //     usermeta,
    //     entityid
    //   ) VALUES (
    //     ${objId},
    //     ${objectPath.fullpath},
    //     ${objectPath.fullpath},
    //     ARRAY[${pathSegs}],
    //     'CREATE',
    //     ${time},
    //     ${objectSize},
    //     ${numParts},
    //     ${partSize},
    //     ${type},
    //     ${time},
    //     ${time},
    //     ${checksum.value},
    //     ARRAY[${md5State}],
    //     ARRAY[${objTags}],
    //     ${desc},
    //     ${meta},
    //     ${entityId}
    //   )
    //   ON CONFLICT (path_key)
    //   DO UPDATE SET
    //     rec_type = 'CREATE',
    //     rec_time = ${time},
    //     obj_size = ${objectSize},
    //     numparts = ${numParts},
    //     partsize = ${partSize},
    //     mime_typ = ${type},
    //     new_time = ${time},
    //     mod_time = ${time},
    //     hash_md5 = ${checksum.value},
    //     md5state = ARRAY[${md5State}],
    //     obj_tags = ARRAY[${objTags}],
    //     desc_fts = ${desc},
    //     usermeta = ${meta},
    //     entityid = ${entityId}
    // `);
    await this.#exec(sql`
      INSERT INTO metadata_v1 (
        objectid,
        fullpath,
        path_key,
        path_seg,
        rec_type,
        rec_time,
        obj_size,
        numparts,
        partsize,
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
        ARRAY[${pathSegs}],
        'CREATE',
        ${time},
        ${objectSize},
        ${numParts},
        ${partSize},
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
        rec_type = 'CREATE',
        rec_time = EXCLUDED.rec_time,
        obj_size = EXCLUDED.obj_size,
        numparts = EXCLUDED.numparts,
        partsize = EXCLUDED.partsize,
        mime_typ = EXCLUDED.mime_typ,
        new_time = EXCLUDED.new_time,
        mod_time = EXCLUDED.mod_time,
        hash_md5 = EXCLUDED.hash_md5,
        md5state = EXCLUDED.md5state,
        obj_tags = EXCLUDED.obj_tags,
        desc_fts = EXCLUDED.desc_fts,
        usermeta = EXCLUDED.usermeta,
        entityid = EXCLUDED.entityid
    `);

    // 作成に成功したので、変更内容を反映します。
    await this.#flush();

    // オブジェクトの説明文が存在するとき、次回の検索前にインデックスを更新する必要があります。
    if (desc) {
      this.#indexFts = true;
    }
  }

  /**
   * オブジェクトのメタデータを排他的に作成します。
   *
   * @param inp オブジェクトのメタデータを排他的に作成するための入力パラメーターです。
   */
  public async createExclusive(inp: CreateExclusiveInput): Promise<void> {
    const {
      checksum,
      entityId,
      mimeType,
      numParts,
      partSize,
      timestamp,
      objectPath,
      objectSize,
      objectTags = [],
      description,
      userMetadata,
    } = inp;
    assertObjectSize(objectSize, numParts, partSize);
    const pathSegs = objectPath.segments.length ? join(objectPath.segments) : empty;
    const md5State = checksum.state.length ? join(checksum.state) : empty;
    const objTags = objectTags.length ? join(objectTags) : empty;
    const objId = getObjectId();
    const type = mimeType ?? getMimeType(objectPath.basename);
    const desc = description == null ? null : await this.#textSearch.toQueryString(description);
    const meta = this.#userMetaJson.stringify(userMetadata);
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
          numparts,
          partsize,
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
          ARRAY[${pathSegs}],
          'CREATE',
          ${time},
          ${objectSize},
          ${numParts},
          ${partSize},
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
      if (isError(ex) && ex.message.startsWith(`Constraint Error: Duplicate key "path_key: `)) {
        throw new ObjectExistsError(this.#bucketName, objectPath, { cause: ex });
      }

      throw ex;
    }

    // 作成に成功したので、変更内容を反映します。
    await this.#flush();

    // オブジェクトの説明文が存在するとき、次回の検索前にインデックスを更新する必要があります。
    if (desc) {
      this.#indexFts = true;
    }
  }

  /*************************************************************************************************
   *
   * 読み取り
   *
   ************************************************************************************************/

  /**
   * 存在するオブジェクトのメタデータを取得します。
   *
   * @param inp オブジェクトのメタデータを取得するための入力パラメーターです。
   * @returns オブジェクトのメタデータを取得した結果です。
   * @throws オブジェクトが見つからない場合は `ObjectNotFoundError` を投げます。
   */
  public async read<const TInp extends ReadInput>(inp: TInp): Promise<
    ReadOutput<$Get<TInp, "select">>
  > {
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
        ${column.build()}`);
      entries[column.key] = column.schema;
    }

    const [row] = await this.#query(sql`
      SELECT
        ${columns.length ? join(columns) : 1}
      FROM
        metadata
      WHERE
        record_type != 'DELETE'
        AND path = ${where.objectPath.fullpath}
      LIMIT
        1
    `);
    if (row === undefined) {
      throw new ObjectNotFoundError(this.#bucketName, where.objectPath);
    }
    if (columns.length === 0) {
      const output = {};

      return output as ReadOutput<$Get<TInp, "select">>;
    }

    const schema = v.object(entries);
    const output = v.expect(schema, row);

    return output as ReadOutput<$Get<TInp, "select">>;
  }

  /**
   * 存在するオブジェクトの内部利用のためのメタデータを取得します。
   *
   * @param inp 存在するオブジェクトの内部利用のためのメタデータを取得するための入力パラメーターです。
   * @returns 存在するオブジェクトの内部利用のためのメタデータを取得した結果です。
   */
  public async readDetail(inp: ReadDetailInput): Promise<ReadDetailOutput> {
    const { objectPath } = inp;
    const [row] = await this.#query(sql`
      SELECT
        obj_size AS "size",
        numparts AS "numParts",
        partsize AS "partSize",
        entityid AS "entityId",
        md5state AS "checksumState",
        hash_md5 AS "checksumValue"
      FROM
        metadata_v1
      WHERE
        path_key = ${objectPath.fullpath}
    `);
    if (row === undefined) {
      throw new ObjectNotFoundError(this.#bucketName, objectPath);
    }

    const {
      size,
      entityId,
      numParts,
      partSize,
      checksumValue,
      checksumState,
    } = v.expect(this.#readDetailOutputSchema, row);
    const output = {
      size,
      checksum: {
        value: checksumValue,
        state: checksumState,
      },
      entityId,
      numParts,
      partSize,
    };

    return output;
  }

  readonly #readDetailOutputSchema = v.object({
    entityId: EntityIdSchema(),
    checksumValue: ChecksumSchema(),
    checksumState: HashStateSchema(),
    size: ObjectSizeSchema(),
    numParts: NumPartsSchema(),
    partSize: PartSizeSchema(),
  });

  /**
   * ゴミ箱に入れられたオブジェクトの内部利用のためのメタデータを取得します。
   *
   * @param inp ゴミ箱に入れられたオブジェクトの内部利用のためのメタデータを取得するための入力パラメーターです。
   * @returns ゴミ箱に入れられたオブジェクトの内部利用のためのメタデータを取得した結果です。
   */
  public async readInTrash(inp: ReadInTrashInput): Promise<ReadInTrashOutput> {
    const { objectId } = inp;
    const [row] = await this.#query(sql`
      SELECT
        entityid AS "entityId"
      FROM
        metadata_v1
      WHERE
        rec_type = 'DELETE'
        AND objectid = ${objectId}
    `);
    if (row === undefined) {
      throw new ObjectNotFoundError(this.#bucketName, objectId); // TODO: オブジェクト ID に対応する
    }

    const { entityId } = v.expect(this.#readInTrashOutputSchema, row);
    const output = { entityId };

    return output;
  }

  readonly #readInTrashOutputSchema = v.object({
    entityId: EntityIdSchema(),
  });

  /**
   * パスが存在するか確認します。
   *
   * @param inp パスが存在するか確認するための入力パラメーターです。
   * @returns パスが存在するか確認した結果です。
   */
  public async exists(inp: ExistsInput): Promise<ExistsOutput> {
    if ("objectPath" in inp) {
      const { objectPath } = inp;
      const [row] = await this.#query(sql`
        SELECT
          TRUE AS "isExists"
        FROM
          metadata_v1
        WHERE
          path_key = ${objectPath.fullpath}
      `);
      const schema = v.optional(v.object({ isExists: v.literal(true) }));
      const { isExists } = v.expect(schema, row) || {};
      const output = {
        exists: isExists === true,
      };

      return output;
    }

    const { dirPath } = inp;
    if (dirPath.length === 0) {
      const output = {
        exists: true,
      };

      return output;
    }

    const stmt: Sql[] = [];
    stmt.push(sql`
      SELECT
        TRUE AS "isExists"
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
    const { isExists } = v.expect(this.#existsRowSchema, row) || {};
    const output = {
      exists: isExists === true,
    };

    return output;
  }

  readonly #existsRowSchema = v.optional(v.object({
    isExists: v.literal(true),
  }));

  /**
   * オブジェクトやディレクトリーのステータス情報を取得します。
   *
   * @param inp オブジェクトやディレクトリーのステータス情報を取得するための入力パラメーターです。
   * @returns オブジェクトやディレクトリーのステータス情報を取得した結果です。
   */
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
          TRUE
        FROM
          metadata_v1
        WHERE
          path_key = ${pathKey}
      ) AS "objectExists"`);
    stmt.push(sql`,
      (
        SELECT
          TRUE
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
      ) AS "directoryExists"
    `);
    const [row] = await this.#query(join(stmt, ""));
    const {
      objectExists,
      directoryExists,
    } = v.expect(this.#statRowSchema, row);
    const output = {
      isObject: objectExists === true,
      isDirectory: directoryExists === true,
    };

    return output;
  }

  readonly #statRowSchema = v.object({
    objectExists: v.nullable(v.literal(true)),
    directoryExists: v.nullable(v.literal(true)),
  });

  /**
   * 照合順序を検証します。
   *
   * @param collate 照合順序です。
   */
  #assertCollate(collate: string): void {
    if (this.#collationSet.has(collate) || this.#collationSet.size === 0) {
      return;
    }

    throw new InvalidCollationError(collate);
  }

  /**
   * オブジェクトをリストアップします。
   */
  async #listObjects(inp: ListInput) {
    const {
      skip = 0,
      take,
      where: {
        dirPath,
      },
      select,
      orderBy: {
        name: {
          type: nameOrderType = "ASC",
          collate: nameOrderCollate = "nfc",
        },
      },
    } = inp;
    this.#assertCollate(nameOrderCollate);

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
        ${column.build("ref")}`);
      entries[column.key] = column.schema;
    }

    const stmt: Sql[] = [];
    const nameIdxSql = raw((dirPath.length + 1).toString(10));
    const pathSegLenSql = raw((dirPath.length + 1).toString(10));
    stmt.push(sql`
      SELECT
        src.path_seg[${nameIdxSql}] AS "name",
        TRUE AS "isObject",
        ${join(columns)}
      FROM
        metadata_v1 AS src
      INNER JOIN
        metadata AS ref
      ON
        src.objectid = ref.id
      WHERE
        src.rec_type != 'DELETE'
        AND array_length(src.path_seg, 1) = ${pathSegLenSql}`);
    for (let i = 0; i < dirPath.length; i++) {
      stmt.push(sql`
        AND src.path_seg[${raw((i + 1).toString(10))}] = ${dirPath[i]}`);
    }

    stmt.push(sql`
      ORDER BY
        "name" COLLATE ${raw(quoteString(nameOrderCollate))} ${raw(nameOrderType)}`);

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

    return (async function*() {
      for (const row of rows) {
        yield v.expect(schema, row) as any;
      }
    })();
  }

  /**
   * ディレクトリーをリストアップします。
   */
  async #listDirectories(inp: ListInput) {
    const {
      skip = 0,
      take,
      where: {
        dirPath,
      },
      orderBy: {
        name: {
          type: nameOrderType = "ASC",
          collate: nameOrderCollate = "nfc",
        },
      },
    } = inp;
    this.#assertCollate(nameOrderCollate);

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
        rec_type != 'DELETE'
        AND array_length(path_seg, 1) > ${pathSegLenSql}`);
    for (let i = 0; i < dirPath.length; i++) {
      stmt.push(sql`
        AND path_seg[${raw((i + 1).toString(10))}] = ${dirPath[i]}`);
    }

    stmt.push(sql`
      ORDER BY
        "name" COLLATE ${raw(quoteString(nameOrderCollate))} ${raw(nameOrderType)}`);

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

    return (async function*() {
      for (const row of rows) {
        yield v.expect(schema, row) as any;
      }
    })();
  }

  /**
   * ディレクトリーまたはオブジェクトをリストアップします。
   *
   * @param inp ディレクトリーまたはオブジェクトをリストアップするための入力パラメーターです。
   * @returns リストアップした結果です。
   */
  public async list<const TInp extends ListInput>(inp: TInp): Promise<
    AsyncGenerator<
      ListOutput<
        $Get<TInp, "select">,
        $Get<$Get<TInp, "where">, "isObject">
      >
    >
  > {
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
        name: {
          type: nameOrderType = "ASC",
          collate: nameOrderCollate = "nfc",
        },
        preferObject = false,
      },
    } = inp;
    this.#assertCollate(nameOrderCollate);

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
        ${column.build("ref")}`);
      entries[column.key] = column.schema;
    }

    const stmt: Sql[] = [];
    const nameIdxSql = raw((dirPath.length + 1).toString(10));
    const pathSegLenSql = raw((dirPath.length + 1).toString(10));
    stmt.push(sql`
      SELECT
        DISTINCT ON ("name", "isObject")
        src.path_seg[${nameIdxSql}] AS "name",
        array_length(src.path_seg, 1) = ${pathSegLenSql} AS "isObject",
        ${join(columns)}
      FROM
        metadata_v1 AS src
      LEFT JOIN
        metadata AS ref
      ON
        src.objectid = ref.id
      WHERE
        src.rec_type != 'DELETE'
        AND array_length(src.path_seg, 1) >= ${pathSegLenSql}`);
    for (let i = 0; i < dirPath.length; i++) {
      stmt.push(sql`
        AND src.path_seg[${raw((i + 1).toString(10))}] = ${dirPath[i]}`);
    }

    stmt.push(sql`
      ORDER BY
        "isObject" ${raw(preferObject ? "DESC" : "ASC")},
        "name" COLLATE ${raw(quoteString(nameOrderCollate))} ${raw(nameOrderType)}`);

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

    return (async function*() {
      for (const row of rows) {
        yield v.expect(schema, row) as any;
      }
    })();
  }

  /**
   * ゴミ箱に入れられたオブジェクトのメタデータを取得します。
   *
   * @param inp オブジェクトのメタデータを取得するための入力パラメーターです。
   * @returns オブジェクトのメタデータを取得した結果です。
   * @throws オブジェクトが見つからない場合は `ObjectNotFoundError` を投げます。
   */
  public async listInTrash<const TInp extends ListInTrashInput>(
    inp: TInp,
  ): Promise<AsyncGenerator<ListInTrashOutput<$Get<TInp, "select">>>> {
    const {
      skip = 0,
      take,
      where: {
        dirPath,
      },
      select,
      orderBy: {
        name: {
          type: nameOrderType = "ASC",
          collate: nameOrderCollate = "nfc",
        },
      },
    } = inp;
    this.#assertCollate(nameOrderCollate);

    const columns: Sql[] = [];
    const entries: v.ObjectEntries = {
      name: v.string(),
      isObject: v.literal(true),
    };
    const selectAll = select === undefined;
    for (const column of this.#inTrashColumns) {
      if (!selectAll && select[column.key] !== true) {
        continue;
      }

      columns.push(sql`
        ${column.build("ref")}`);
      entries[column.key] = column.schema;
    }

    const stmt: Sql[] = [];
    const nameIdxSql = raw((dirPath.length + 1).toString(10));
    const pathSegLenSql = raw((dirPath.length + 1).toString(10));
    stmt.push(sql`
      SELECT
        src.path_seg[${nameIdxSql}] AS "name",
        TRUE AS "isObject",
        ${join(columns)}
      FROM
        metadata_v1 AS src
      INNER JOIN
        metadata AS ref
      ON
        src.objectid = ref.id
      WHERE
        src.rec_type = 'DELETE'
        AND array_length(src.path_seg, 1) = ${pathSegLenSql}`);
    for (let i = 0; i < dirPath.length; i++) {
      stmt.push(sql`
        AND src.path_seg[${raw((i + 1).toString(10))}] = ${dirPath[i]}`);
    }

    stmt.push(sql`
      ORDER BY
        "name" COLLATE ${raw(quoteString(nameOrderCollate))} ${raw(nameOrderType)}`);

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

    return (async function*() {
      for (const row of rows) {
        yield v.expect(schema, row) as any;
      }
    })();
  }

  /**
   * オブジェクトの説明文を対象に全文検索します。
   *
   * @param inp オブジェクトの説明文を対象に全文検索するための入力パラメーターです。
   * @returns 検索結果です。
   */
  public async search(inp: SearchInput): Promise<AsyncGenerator<SearchOutput>> {
    const {
      skip = 0,
      take,
      query,
      dirPath,
      recursive = false,
      scoreThreshold = 0,
    } = inp;
    if (this.#indexFts) {
      // 検索前にインデックスを更新します。
      // ドキュメント:
      // https://duckdb.org/docs/stable/core_extensions/full_text_search.html#pragma-create_fts_index
      await this.#exec(sql`
        PRAGMA create_fts_index(
          metadata_v1, objectid, fullpath, desc_fts,
          stemmer       = 'none',
          stopwords     = 'none',
          ignore        = '',
          strip_accents = false,
          lower         = true,
          overwrite     = true
        );
      `);
      this.#indexFts = false;
    }

    const pathSegsOp = recursive ? raw(">=") : raw("=");
    const ftsQuery = await this.#textSearch.toQueryString(query);
    const stmt: Sql[] = [];
    stmt.push(sql`
      SELECT
        fullpath AS "objectPath",
        desc_fts AS "description",
        fts_main_metadata_v1.match_bm25(objectid, ${ftsQuery}) AS "searchScore"
      FROM
        metadata_v1
      WHERE
        rec_type != 'DELETE'
        AND array_length(path_seg, 1) ${pathSegsOp} ${dirPath.length + 1}`);
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

    return (async function*(schema, ts) {
      for (const row of rows) {
        const {
          objectPath,
          description,
          searchScore,
        } = v.expect(schema, row);

        yield {
          objectPath,
          description: description === null ? null : await ts.fromQueryString(description),
          searchScore,
        };
      }
    })(this.#searchRowSchema, this.#textSearch);
  }

  readonly #searchRowSchema = v.object({
    objectPath: ObjectPathSchema(),
    description: v.nullable(v.string()),
    searchScore: v.pipe(v.number(), v.finite()),
  });

  /*************************************************************************************************
   *
   * 更新
   *
   ************************************************************************************************/

  /**
   * オブジェクトのメタデータを移動します。
   *
   * @param inp オブジェクトのメタデータを移動するための入力パラメーターです。
   * @throws 移動元のオブジェクトが見つからない場合は `ObjectNotFoundError` を、
   * 移動先のオブジェクトがすでに存在する場合は `ObjectExistsError` を投げます。
   */
  public async move(inp: MoveInput) {
    const {
      srcObjectPath,
      dstObjectPath,
    } = inp;
    try {
      await this.#exec(sql`BEGIN TRANSACTION`);

      // 移動先のメタデータがあれば予めそれを削除しておきます。
      await this.#query(sql`
        DELETE FROM
          metadata_v1
        WHERE
          path_key = ${dstObjectPath.fullpath}
      `);

      const [row] = await this.#query(sql`
        UPDATE
          metadata_v1
        SET
          fullpath = ${dstObjectPath.fullpath},
          path_key = ${dstObjectPath.fullpath},
          path_seg = ARRAY[${join(dstObjectPath.segments)}]
        WHERE
          path_key = ${srcObjectPath.fullpath}
      `);
      const { Count } = v.expect(this.#countRowSchema, row);
      if (Count === 0) {
        throw new ObjectNotFoundError(this.#bucketName, srcObjectPath);
      }

      await this.#exec(sql`COMMIT`);
    } catch (ex) {
      try {
        await this.#exec(sql`ROLLBACK`);
      } catch (ex) {
        this.#logger.log({
          level: LogLevel.ERROR,
          reason: ex,
          message: "Metadata.move: Failed to rollback",
        });
      }

      throw ex;
    }

    await this.#flush();
  }

  /**
   * オブジェクトのメタデータを移動します。
   *
   * @param inp オブジェクトのメタデータを移動するための入力パラメーターです。
   * @throws 移動元のオブジェクトが見つからない場合は `ObjectNotFoundError` を、
   * 移動先のオブジェクトがすでに存在する場合は `ObjectExistsError` を投げます。
   */
  public async moveExclusive(inp: MoveInput): Promise<void> {
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
          path_seg = ARRAY[${join(dstObjectPath.segments)}]
        WHERE
          path_key = ${srcObjectPath.fullpath}
      `);
    } catch (ex) {
      if (isError(ex) && ex.message.startsWith(`Constraint Error: Duplicate key "path_key: `)) {
        throw new ObjectExistsError(this.#bucketName, dstObjectPath, { cause: ex });
      }

      throw ex;
    }

    const { Count } = v.expect(this.#countRowSchema, row);
    if (Count === 0) {
      throw new ObjectNotFoundError(this.#bucketName, srcObjectPath);
    }

    await this.#flush();
  }

  readonly #countRowSchema = v.object({
    Count: UintSchema(),
  });

  /**
   * オブジェクトのメタデータをコピーします。
   *
   * @param inp オブジェクトのメタデータをコピーするための入力パラメーターです。
   * @throws コピー元のオブジェクトが見つからない場合は `ObjectNotFoundError` を投げます。
   */
  public async copy(inp: CopyInput): Promise<void> {
    const {
      timestamp,
      dstEntityId,
      srcObjectPath,
      dstObjectPath,
    } = inp;
    const objId = getObjectId();
    const time = new Date(timestamp ?? Date.now()).toISOString();
    const [row] = await this.#query(sql`
      INSERT INTO metadata_v1 (
        objectid,
        fullpath,
        path_key,
        path_seg,
        rec_type,
        rec_time,
        obj_size,
        numparts,
        partsize,
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
        ${objId},
        ${dstObjectPath.fullpath},
        ${dstObjectPath.fullpath},
        ARRAY[${join(dstObjectPath.segments)}],
        'CREATE',
        ${time},
        obj_size,
        numparts,
        partsize,
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
        path_key = ${srcObjectPath.fullpath}
      ON CONFLICT (path_key)
      DO UPDATE SET
        rec_type = 'CREATE',
        rec_time = EXCLUDED.rec_time,
        obj_size = EXCLUDED.obj_size,
        numparts = EXCLUDED.numparts,
        partsize = EXCLUDED.partsize,
        mime_typ = EXCLUDED.mime_typ,
        new_time = EXCLUDED.new_time,
        mod_time = EXCLUDED.mod_time,
        hash_md5 = EXCLUDED.hash_md5,
        md5state = EXCLUDED.md5state,
        obj_tags = EXCLUDED.obj_tags,
        desc_fts = EXCLUDED.desc_fts,
        usermeta = EXCLUDED.usermeta,
        entityid = ${dstEntityId}
    `);

    const { Count } = v.expect(this.#countRowSchema, row);
    if (Count === 0) {
      throw new ObjectNotFoundError(this.#bucketName, srcObjectPath);
    }

    await this.#flush();

    // オブジェクトの説明文が存在する可能性があるので、次回の検索前にインデックスを更新する必要があります。
    this.#indexFts = true;
  }

  /**
   * オブジェクトのメタデータをコピーします。
   *
   * @param inp オブジェクトのメタデータをコピーするための入力パラメーターです。
   * @throws コピー元のオブジェクトが見つからない場合は `ObjectNotFoundError` を、
   * コピー先のオブジェクトがすでに存在する場合は `ObjectExistsError` を投げます。
   */
  public async copyExclusive(inp: CopyInput): Promise<void> {
    const {
      timestamp,
      dstEntityId,
      srcObjectPath,
      dstObjectPath,
    } = inp;
    let row: Row | undefined;
    const objId = getObjectId();
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
          numparts,
          partsize,
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
          ${objId},
          ${dstObjectPath.fullpath},
          ${dstObjectPath.fullpath},
          ARRAY[${join(dstObjectPath.segments)}],
          'CREATE',
          ${time},
          obj_size,
          numparts,
          partsize,
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
          path_key = ${srcObjectPath.fullpath}
      `);
    } catch (ex) {
      if (isError(ex) && ex.message.startsWith(`Constraint Error: Duplicate key "path_key: `)) {
        throw new ObjectExistsError(this.#bucketName, dstObjectPath, { cause: ex });
      }

      throw ex;
    }

    const { Count } = v.expect(this.#countRowSchema, row);
    if (Count === 0) {
      throw new ObjectNotFoundError(this.#bucketName, srcObjectPath);
    }

    await this.#flush();

    // オブジェクトの説明文が存在する可能性があるので、次回の検索前にインデックスを更新する必要があります。
    this.#indexFts = true;
  }

  /**
   * オブジェクトのメタデータを更新します。
   *
   * @param inp オブジェクトのメタデータを更新するための入力パラメーターです。
   * @throws オブジェクトが見つからない場合は `ObjectNotFoundError` を投げます。
   */
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
      const { Count } = v.expect(this.#countRowSchema, row);
      if (Count === 0) {
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
        obj_tags = ARRAY[${objectTags.length ? join(objectTags) : empty}]`);
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
    const { Count } = v.expect(this.#countRowSchema, row);
    if (Count === 0) {
      throw new ObjectNotFoundError(this.#bucketName, objectPath);
    }

    await this.#flush();

    if (description !== undefined) {
      // オブジェクトの説明文が更新された可能性があるので、次回の検索前にインデックスを更新する必要があります。
      this.#indexFts = true;
    }
  }

  /**
   * オブジェクトのメタデータを排他的に更新します。
   *
   * @param inp オブジェクトのメタデータを排他的に更新するための入力パラメーターです。
   */
  public async updateExclusive(inp: UpdateExclusiveInput): Promise<void> {
    const {
      expect,
      checksum,
      entityId,
      mimeType,
      numParts,
      partSize,
      timestamp,
      objectPath,
      objectSize,
      objectTags,
      description,
      userMetadata,
    } = inp;
    assertObjectSize(objectSize, numParts, partSize);
    const stmt: Sql[] = [];
    stmt.push(sql`
      UPDATE
        metadata_v1
      SET
        rec_type = 'UPDATE_METADATA',
        hash_md5 = ${checksum.value},
        md5state = ARRAY[${checksum.state.length ? join(checksum.state) : empty}],
        obj_size = ${objectSize},
        numparts = ${numParts},
        partsize = ${partSize}`);

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
        obj_tags = ARRAY[${objectTags.length ? join(objectTags) : empty}]`);
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
      const { Count } = v.expect(this.#countRowSchema, row);
      if (Count === 0) {
        // 更新対象がない原因は `path_key` が無いか、`hash_md5` が一致しないかのどちらかです。
        const [row] = await this.#query(sql`
          SELECT
            COUNT(*) AS "Count"
          FROM
            metadata_v1
          WHERE
            path_key = ${objectPath.fullpath}
        `);
        const { Count } = v.expect(this.#countRowSchema, row);
        if (Count === 0) {
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
        this.#logger.log({
          level: LogLevel.ERROR,
          reason: ex,
          message: "Metadata.updateExclusive: Failed to rollback",
        });
      }

      throw ex;
    }

    await this.#flush();

    if (description !== undefined) {
      // オブジェクトの説明文が更新された可能性があるので、次回の検索前にインデックスを更新する必要があります。
      this.#indexFts = true;
    }
  }

  /**
   * オブジェクトの削除フラグを立てます。
   *
   * @param inp オブジェクトの削除フラグをたてるための入力パラメーターです。
   * @throws ファイルが見つからない場合は `ObjectNotFoundError` を投げます。
   */
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
        numparts = 0,
        md5state = NULL,
        obj_tags = NULL,
        desc_fts = NULL,
        usermeta = NULL
      WHERE
        path_key = ${objectPath.fullpath}
      RETURNING
        objectid AS "objectId",
        entityid AS "entityId"
    `);
    if (!row) {
      throw new ObjectNotFoundError(this.#bucketName, objectPath);
    }

    await this.#flush();

    // オブジェクトの説明文が更新された可能性があるので、次回の検索前にインデックスを更新する必要があります。
    this.#indexFts = true;

    return v.expect(this.#trashRowSchema, row);
  }

  readonly #trashRowSchema = v.object({
    objectId: ObjectIdSchema(),
    entityId: EntityIdSchema(),
  });

  /*************************************************************************************************
   *
   * 削除
   *
   ************************************************************************************************/

  /**
   * オブジェクトのメタデータを完全に削除します。
   *
   * @param inp オブジェクトのメタデータを完全に削除するための入力パラメーターです。
   * @throws ファイルが見つからない場合は `ObjectNotFoundError` を投げます。
   */
  public async delete(inp: DeleteInput): Promise<void> {
    const { objectId } = inp;
    const [row] = await this.#query(sql`
      DELETE FROM
        metadata_v1
      WHERE
        objectid = ${objectId}
    `);
    const { Count } = v.expect(this.#countRowSchema, row);
    if (Count === 0) {
      return; // すでになければそれでヨシ
    }

    await this.#flush();
  }
}
