import { type Asyncmux, asyncmux, type AsyncmuxLock } from "asyncmux";
import VoidLogger from "../envs/shared/logger/void-logger.js";
import PassThroughTextSearch from "../envs/shared/text-search/pass-through-text-search.js";
import type { IDatabase, Row } from "../shared/database.js";
import {
  EntryPathNotFoundError,
  ObjectExistsError,
  ObjectNotFoundError,
  OmnioClosedError,
} from "../shared/errors.js";
import { type ILogger, LogLevel } from "../shared/logger.js";
import {
  type BucketName,
  type BucketNameLike,
  BucketNameSchema,
  type Checksum,
  type EntityId,
  type MimeType,
  type MimeTypeLike,
  MimeTypeSchema,
  MIN_PART_SIZE,
  type NumParts,
  type ObjectDirectoryPath,
  type ObjectDirectoryPathLike,
  ObjectDirectoryPathSchema,
  type ObjectId,
  ObjectIdSchema,
  type ObjectPath,
  type ObjectPathLike,
  ObjectPathSchema,
  type ObjectSize,
  type ObjectTags,
  type ObjectTagsLike,
  ObjectTagsSchema,
  type OpenMode,
  type OpenModeLike,
  OpenModeSchema,
  type OrderTypeLike,
  OrderTypeSchema,
  type PartSize,
  type PartSizeLike,
  PartSizeSchema,
  type Timestamp,
  type TimestampLike,
  TimestampSchema,
  type UintLike,
  UintSchema,
} from "../shared/schemas.js";
import type { IEntityHandle, IStorage } from "../shared/storage.js";
import type { ITextSearch } from "../shared/text-search.js";
import type { Uint8ArraySource } from "../shared/to-uint8-array.js";
import type { Awaitable } from "../shared/type-utils.js";
import unreachable from "../shared/unreachable.js";
import * as v from "../shared/valibot.js";
import getEntityId from "./_get-entity-id.js";
import type { IHash } from "./_hash.js";
import md5 from "./_md5.js";
import type { IJson, Sql } from "./metadata.js";
import type {
  $Get,
  $Select,
  ObjectInTrashMetadata,
  ObjectInTrashMetadataSelectQuery,
  ObjectMetadata,
  ObjectMetadataSelectQuery,
} from "./metadata.js";
import Metadata from "./metadata.js";
import ObjectFileReadStream from "./object-file-read-stream.js";
import ObjectFileWriteStream from "./object-file-write-stream.js";
import ObjectFile from "./object-file.js";

/***************************************************************************************************
 *
 * 入力パラメーター
 *
 **************************************************************************************************/

/**
 * `Omnio` を閉じるときに実行されるクリーンアップ関数のインターフェースです。
 */
export interface ICleanupFunction {
  /**
   * クリーンアップします。
   */
  (): Awaitable<void>;
}

/**
 * セットアップするためのパラメーターです。
 */
export type SetupParams = Readonly<{
  /**
   * バケット名です。
   */
  bucketName: BucketNameLike;

  /**
   * オブジェクトを保存するためのストレージクライアントです。
   */
  storage: IStorage;

  /**
   * メタデータを記録するためのデータベースクライアントです。
   */
  database: IDatabase;

  /**
   * 排他制御のためのキューを管理するオブジェクトです。
   */
  mutex?: Asyncmux | undefined;

  /**
   * JavaScript の値と JSON 文字列を相互変換するための関数群です。
   */
  json?: IJson | undefined;

  /**
   * Omnio で使用されるロガーです。
   * 内部情報や、ただちにアプリケーションを停止する必要はないものの、記録しておくべきメッセージを通知する際に使用されます。
   */
  logger?: ILogger | undefined;

  /**
   * オブジェクトの説明文の検索に使用する関数群です。
   */
  textSearch?: ITextSearch | undefined;

  /**
   * オブジェクトの説明文の最大サイズ (バイト数) です。
   *
   * @default 10 KB
   */
  maxDescriptionTextByteSize?: number | undefined;

  /**
   * ユーザー定義のメタデータの最大サイズ (バイト数) です。
   * このサイズは、ユーザー定義のメタデータを `json.stringify` で変換したあとの文字列に対して計算されます。
   *
   * @default 10 KB
   */
  maxUserMetadataJsonByteSize?: number | undefined;

  /**
   * `Omnio` を閉じるときに実行されるクリーンアップ関数です。
   */
  cleanup?: ICleanupFunction | undefined;
}>;

const B = 1;
const KB = 1000 * B;

const SetupLiteralParamsSchema = () => (v.object({
  bucketName: BucketNameSchema(),
  maxDescriptionTextByteSize: v.optional(UintSchema(), 10 * KB),
  maxUserMetadataJsonByteSize: v.optional(UintSchema(), 10 * KB),
}));

/**
 * `Omnio` の利用を開始する際に実行されるセットアップ関数のインターフェースです。
 */
export interface ISetupFunction {
  /**
   * セットアップします。
   *
   * @returns セットアップするためのパラメーターです。
   */
  (): Awaitable<SetupParams>;
}

/***************************************************************************************************
 *
 * 行データ
 *
 **************************************************************************************************/

export type { ObjectInTrashMetadata, ObjectMetadata };

/***************************************************************************************************
 *
 * 行データのセレクター
 *
 **************************************************************************************************/

export type { ObjectInTrashMetadataSelectQuery, ObjectMetadataSelectQuery };

const ObjectInTrashMetadataSelectQuerySchema = () => (v.object(
  {
    bucket: v.optional(v.boolean()),
    path: v.optional(v.boolean()),
    id: v.optional(v.boolean()),
    recordTimestamp: v.optional(v.boolean()),
    mimeType: v.optional(v.boolean()),
    createdAt: v.optional(v.boolean()),
    lastModifiedAt: v.optional(v.boolean()),
    checksum: v.optional(v.boolean()),
    checksumAlgorithm: v.optional(v.boolean()),
    entityId: v.optional(v.boolean()),
  } satisfies Record<keyof ObjectInTrashMetadataSelectQuery, unknown>,
));

const ObjectMetadataSelectQuerySchema = () => (v.object(
  {
    bucket: v.optional(v.boolean()),
    id: v.optional(v.boolean()),
    path: v.optional(v.boolean()),
    recordType: v.optional(v.boolean()),
    recordTimestamp: v.optional(v.boolean()),
    size: v.optional(v.boolean()),
    numParts: v.optional(v.boolean()),
    partSize: v.optional(v.boolean()),
    mimeType: v.optional(v.boolean()),
    createdAt: v.optional(v.boolean()),
    lastModifiedAt: v.optional(v.boolean()),
    checksum: v.optional(v.boolean()),
    checksumAlgorithm: v.optional(v.boolean()),
    objectTags: v.optional(v.boolean()),
    description: v.optional(v.boolean()),
    userMetadata: v.optional(v.boolean()),
    entityId: v.optional(v.boolean()),
  } satisfies Record<keyof ObjectMetadataSelectQuery, unknown>,
));

/***************************************************************************************************
 *
 * 作成
 *
 **************************************************************************************************/

/**
 * 書き込みストリームのオプションです。
 */
export type CreateWriteStreamOptions = Readonly<{
  /**
   * オブジェクトを開く際のモードです。
   *
   * - **`"w"`**: 書き込みモードで開きます。オブジェクトが存在しない場合は新規作成され、もし存在する場合は上書きします。
   * - **`"wx"`**: 書き込みモードで開きます。オブジェクトが存在する場合はエラーになります。
   * - **`"a"`**: 追加書き込みモードで開きます。オブジェクトが存在しない場合は新規作成されます。
   * - **`"ax"`**: 追加書き込みモードで開きます。オブジェクトが存在する場合はエラーになります
   *
   * @default "w"
   */
  flag?: OpenModeLike | undefined;

  /**
   * 各パートのサイズ (バイト数) です。
   */
  partSize?: PartSizeLike | undefined;

  /**
   * オブジェクトのデータ形式です。`undefined` の場合はオブジェクトパスから自動判定されます。
   * 判定できない場合は "application/octet-stream" になります。
   */
  mimeType?: MimeTypeLike | undefined;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   *
   * @default []
   */
  objectTags?: ObjectTagsLike | undefined;

  /**
   * オブジェクトの説明文です。
   *
   * @default null
   */
  description?: string | null | undefined;

  /**
   * ユーザー定義のメタデータです。
   *
   * @default null
   */
  userMetadata?: unknown;

  /**
   * カスタムのタイムスタンプです。デフォルトで現在時刻です。
   */
  timestamp?: TimestampLike | undefined;

  /**
   * 中止シグナルです。
   */
  abortSignal?: AbortSignal | undefined;
}>;

const CreateWriteStreamOptionsSchema = () => (v.object({
  flag: v.optional(OpenModeSchema(), "w"),
  mimeType: v.optional(MimeTypeSchema()),
  partSize: v.optional(PartSizeSchema()),
  timestamp: v.optional(TimestampSchema()),
  objectTags: v.optional(ObjectTagsSchema()),
  abortSignal: v.optional(v.instance(AbortSignal)),
  description: v.optional(v.nullable(v.string())),
  userMetadata: v.optional(v.unknown()),
}));

/**
 * オプジェクトを書き込む際のオプションです。
 */
export type PutObjectOptions = CreateWriteStreamOptions;

/***************************************************************************************************
 *
 * 読み取り
 *
 **************************************************************************************************/

/**
 * 追加のデータを読み込むためのオプションです。読み込むデータを選択します。
 */
export type ObjectMetadataLoadOptions = Readonly<{
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
}>;

const ObjectMetadataLoadSchema = () => (v.object({
  objectTags: v.optional(v.boolean()),
  description: v.optional(v.boolean()),
  userMetadata: v.optional(v.boolean()),
}));

/**
 * 読み込みストリームのオプションです。
 */
export type CreateReadStreamOptions = Readonly<{
  /**
   * 追加のデータを読み込むためのオプションです。読み込むデータを選択します。`true` の場合は全ての追加のデータを読み込みます。
   *
   * @default false
   */
  load?: boolean | ObjectMetadataLoadOptions | undefined;

  /**
   * 中止シグナルです。
   */
  abortSignal?: AbortSignal | undefined;
}>;

const CreateReadStreamOptionsSchema = () => (v.object({
  load: v.optional(v.pipe(
    v.union([
      v.pipe(
        v.boolean(),
        v.transform((x): Required<ObjectMetadataLoadOptions> => ({
          objectTags: x,
          description: x,
          userMetadata: x,
        })),
      ),
      v.unknown(),
    ]),
    ObjectMetadataLoadSchema(),
  )),
  abortSignal: v.optional(v.instance(AbortSignal)),
}));

/**
 * オブジェクトを取得する際のオプションです。
 */
export type GetObjectOptions = CreateReadStreamOptions;

/**
 * 存在するオブジェクトのメタデータを取得するためのクエリーです。
 */
export type GetObjectMetadataQuery = Readonly<{
  /**
   * 結果に含めるカラムを選択します。
   */
  select?: ObjectMetadataSelectQuery | undefined;

  /**
   * 対象を限定します。
   */
  where: Readonly<{
    /**
     * バケット内のオブジェクトパスです。
     */
    path: ObjectPathLike;
  }>;
}>;

const GetObjectMetadataQuerySchema = () => (v.object({
  select: v.optional(ObjectMetadataSelectQuerySchema()),
  where: v.object({
    path: ObjectPathSchema(),
  }),
}));

/**
 * 存在するオブジェクトのメタデータを取得するためのオプションです。
 */
export type GetObjectMetadataOptions = Readonly<{
  /**
   * 中止シグナルです。
   */
  abortSignal?: AbortSignal | undefined;
}>;

const GetObjectMetadataOptionsSchema = () => (v.object({
  abortSignal: v.optional(v.instance(AbortSignal)),
}));

/**
 * 存在するオブジェクトのメタデータを取得した結果です。
 *
 * @template TSelect SELECT するカラムです。
 */
export type GetObjectMetadataResult<TSelect> = $Select<ObjectMetadata, TSelect>;

/**
 * オブジェクトとディレクトリーのステータス情報です。
 */
export type Stats = {
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
 * ディレクトリーまたはオブジェクトをリストアップするためのクエリーです。
 */
export type ListQuery = Readonly<{
  /**
   * リストアイテムがオブジェクトのときに結果に含めるカラムを選択します。
   */
  select?: ObjectMetadataSelectQuery | undefined;

  /**
   * 対象を限定します。
   */
  where?:
    | Readonly<{
      /**
       * ディレクトリーパスです。
       */
      dirPath?: ObjectDirectoryPathLike | undefined;

      /**
       * `true` ならオブジェクトのみを、`false` ならディレクトリーのみをリストアップします。
       */
      isObject?: boolean | undefined;
    }>
    | undefined;

  /**
   * スキップするアイテムの数です。
   *
   * @default 0
   */
  skip?: UintLike | undefined;

  /**
   * 取得するアイテムの最大数です。
   *
   * @default 上限なし
   */
  take?: UintLike | undefined;

  /**
   * 結果の並び順を指定します。
   */
  orderBy?:
    | Readonly<{
      /**
       * オブジェクト名の並び順です。
       */
      name?:
        | OrderTypeLike
        | Readonly<{
          /**
           * 並び順です。
           *
           * @default "ASC"
           */
          type?: OrderTypeLike | undefined;

          /**
           * 照合順序です。
           *
           * @default "nfc"
           */
          collate?: string | undefined;
        }>
        | undefined;

      /**
       * オブジェクトを先頭にします。
       *
       * @default false
       */
      preferObject?: boolean | undefined;
    }>
    | undefined;
}>;

const ListQuerySchema = () => (v.object({
  select: v.optional(ObjectMetadataSelectQuerySchema()),
  where: v.optional(v.object({
    dirPath: v.optional(ObjectDirectoryPathSchema()),
    isObject: v.optional(v.boolean()),
  })),
  skip: v.optional(UintSchema()),
  take: v.optional(UintSchema()),
  orderBy: v.optional(v.object({
    name: v.optional(v.union([
      v.pipe(
        OrderTypeSchema(),
        v.transform(type => ({
          type,
          collate: undefined,
        })),
      ),
      v.object({
        type: OrderTypeSchema(),
        collate: v.optional(v.string()),
      }),
    ])),
    preferObject: v.optional(v.boolean()),
  })),
}));

/**
 * ディレクトリーをリストアップした結果です。
 *
 * @template TSelect SELECT するカラムです。
 */
export type ListItemDirectory = {
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
export type ListItemObject<TSelect> = $Select<ObjectMetadata, TSelect> & {
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
 * @template TIsObject オブジェクトのみを選択するかどうかです。
 */
export type ListItem<TSelect, TIsObject> = undefined extends TIsObject
  ? ListItemDirectory | ListItemObject<TSelect>
  : (
    | (false extends TIsObject ? ListItemDirectory : never)
    | (true extends TIsObject ? ListItemObject<TSelect> : never)
  );

/**
 * ディレクトリーまたはオブジェクトをリストアップするためのクエリーです。
 */
export type ListInTrashQuery = Readonly<{
  /**
   * リストアイテムがオブジェクトのときに結果に含めるカラムを選択します。
   */
  select?: ObjectInTrashMetadataSelectQuery | undefined;

  /**
   * 対象を限定します。
   */
  where?:
    | Readonly<{
      /**
       * ディレクトリーパスです。
       */
      dirPath?: ObjectDirectoryPathLike | undefined;
    }>
    | undefined;

  /**
   * スキップするアイテムの数です。
   *
   * @default 0
   */
  skip?: UintLike | undefined;

  /**
   * 取得するアイテムの最大数です。
   *
   * @default 上限なし
   */
  take?: UintLike | undefined;

  /**
   * 結果の並び順を指定します。
   */
  orderBy?:
    | Readonly<{
      /**
       * オブジェクト名の並び順です。
       */
      name?:
        | OrderTypeLike
        | Readonly<{
          /**
           * 並び順です。
           *
           * @default "ASC"
           */
          type?: OrderTypeLike | undefined;

          /**
           * 照合順序です。
           *
           * @default "nfc"
           */
          collate?: string | undefined;
        }>
        | undefined;
    }>
    | undefined;
}>;

const ListInTrashQuerySchema = () => (v.object({
  select: v.optional(ObjectInTrashMetadataSelectQuerySchema()),
  where: v.optional(v.object({
    dirPath: v.optional(ObjectDirectoryPathSchema()),
  })),
  skip: v.optional(UintSchema()),
  take: v.optional(UintSchema()),
  orderBy: v.optional(v.object({
    name: v.optional(v.union([
      v.pipe(
        OrderTypeSchema(),
        v.transform(type => ({
          type,
          collate: undefined,
        })),
      ),
      v.object({
        type: OrderTypeSchema(),
        collate: v.optional(v.string()),
      }),
    ])),
  })),
}));

/**
 * ゴミ箱に入れられたオブジェクトのメタデータを取得した結果です。
 *
 * @template TSelect SELECT するカラムです。
 */
export type ListItemInTrash<TSelect> = $Select<ObjectInTrashMetadata, TSelect> & {
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
 * オブジェクトの説明文を対象に全文検索するためのオプションです。
 */
export type SearchObjectsOptions = Readonly<{
  /**
   * 対象を限定します。
   */
  where?:
    | Readonly<{
      /**
       * ディレクトリーパスです。
       */
      dirPath?: ObjectDirectoryPathLike | undefined;
    }>
    | undefined;

  /**
   * スキップする検索結果の数です。
   *
   * @default 0
   */
  skip?: UintLike | undefined;

  /**
   * 取得する検索結果の最大数です。
   *
   * @default 上限なし
   */
  take?: UintLike | undefined;

  /**
   * ディレクトリー内のオブジェクトを再帰的に検索するなら `true`、しないなら `false` を指定します。
   *
   * @default false
   */
  recursive?: boolean | undefined;

  /**
   * 検索にヒットしたと判断するスコアのしきい値です。
   *
   * @default 0
   */
  scoreThreshold?: number | undefined;
}>;

const SearchObjectsOptionsSchema = () => (v.object({
  where: v.optional(v.object({
    dirPath: v.optional(ObjectDirectoryPathSchema()),
  })),
  skip: v.optional(UintSchema()),
  take: v.optional(UintSchema()),
  recursive: v.optional(v.boolean()),
  scoreThreshold: v.optional(v.pipe(v.number(), v.finite())),
}));

/**
 * オブジェクトの説明文を対象に全文検索した結果です。
 */
export type SearchObjectResult = {
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
 * オブジェクトを移動するためのオプションです。
 */
export type MoveObjectOptions = Readonly<{
  /**
   * オブジェクトを開く際のモードです。
   *
   * - **`"w"`**: 書き込みモードで開きます。オブジェクトが存在しない場合は新規作成され、もし存在する場合は上書きします。
   * - **`"wx"`**: 書き込みモードで開きます。オブジェクトが存在する場合はエラーになります。
   * - **`"a"`**: `"w"` と同じです。
   * - **`"ax"`**: `"wx"` と同じです。
   *
   * @default "wx"
   */
  flag?: OpenModeLike | undefined;

  /**
   * 中止シグナルです。
   */
  abortSignal?: AbortSignal | undefined;
}>;

const MoveObjectOptionsSchema = () => (v.object({
  flag: v.optional(OpenModeSchema(), "wx"),
  abortSignal: v.optional(v.instance(AbortSignal)),
}));

/**
 * オブジェクトをコピーするためのオプションです。
 */
export type CopyObjectOptions = Readonly<{
  /**
   * オブジェクトを開く際のモードです。
   *
   * - **`"w"`**: 書き込みモードで開きます。オブジェクトが存在しない場合は新規作成され、もし存在する場合は上書きします。
   * - **`"wx"`**: 書き込みモードで開きます。オブジェクトが存在する場合はエラーになります。
   * - **`"a"`**: `"w"` と同じです。
   * - **`"ax"`**: `"wx"` と同じです。
   *
   * @default "wx"
   */
  flag?: OpenModeLike | undefined;

  /**
   * カスタムのタイムスタンプです。
   *
   * @default Date.now()
   */
  timestamp?: TimestampLike | undefined;

  /**
   * 中止シグナルです。
   */
  abortSignal?: AbortSignal | undefined;
}>;

const CopyObjectOptionsSchema = () => (v.object({
  flag: v.optional(OpenModeSchema(), "wx"),
  timestamp: v.optional(TimestampSchema()),
  abortSignal: v.optional(v.instance(AbortSignal)),
}));

/**
 * オブジェクトのメタデータを更新するためのオプションです。
 */
export type UpdateObjectMetadataOptions = Readonly<{
  /**
   * オブジェクトのデータ形式です。
   */
  mimeType?: MimeTypeLike | undefined;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  objectTags?: ObjectTagsLike | undefined;

  /**
   * オブジェクトの説明文です。
   */
  description?: string | null | undefined;

  /**
   * ユーザー定義のメタデータです。
   */
  userMetadata?: unknown | undefined;

  /**
   * カスタムのタイムスタンプです。
   *
   * @default Date.now()
   */
  timestamp?: TimestampLike | undefined;
}>;

const UpdateObjectMetadataOptionsSchema = () => (v.object({
  mimeType: v.optional(MimeTypeSchema()),
  timestamp: v.optional(TimestampSchema()),
  objectTags: v.optional(ObjectTagsSchema()),
  description: v.optional(v.nullable(v.string())),
  userMetadata: v.optional(v.unknown()),
}));

/**
 * オブジェクトの削除フラグを立てるためのオプションです。
 */
export type TrashObjectOptions = Readonly<{
  /**
   * カスタムのタイムスタンプです。
   *
   * @default Date.now()
   */
  timestamp?: TimestampLike | undefined;
}>;

const TrashObjectOptionsSchema = () => (v.object({
  timestamp: v.optional(TimestampSchema()),
}));

/***************************************************************************************************
 *
 * 削除
 *
 **************************************************************************************************/

/**
 * オブジェクトを削除するためのオプションです。
 */
export type DeleteObjectOptions = Readonly<{
  /**
   * 中止シグナルです。
   */
  abortSignal?: AbortSignal | undefined;
}>;

const DeleteObjectOptionsSchema = () => (v.object({
  abortSignal: v.optional(v.instance(AbortSignal)),
}));

/***************************************************************************************************
 *
 * その他
 *
 **************************************************************************************************/

const SqlSchema = () => (v.union([
  v.string(),
  v.pipe(
    v.object({
      text: v.string(),
      values: v.pipe(v.array(v.unknown()), v.readonly()),
    }),
    v.readonly(),
  ),
]));

/***************************************************************************************************
 *
 * Omnio
 *
 **************************************************************************************************/

export default class Omnio {
  /**
   * `Omnio` の利用を開始する際に実行されるセットアップ関数です。
   */
  readonly #setup: ISetupFunction;

  /**
   * `Omnio` が利用可能なときに設定される値です。
   */
  #props:
    | Readonly<{
      /**
       * バケット名です。
       */
      bucketName: BucketName;

      /**
       * 排他制御のためのキューを管理するオブジェクトです。
       */
      mux: Asyncmux;

      /**
       * Omnio で使用されるロガーです。
       */
      logger: ILogger;

      /**
       * オブジェクトを保存するためのストレージクライアントです。
       */
      storage: IStorage;

      /**
       * オブジェクトのメタデータを管理するオブジェクトです。
       */
      metadata: Metadata;

      /**
       * `Omnio` を閉じるときに実行されるクリーンアップ関数です。
       */
      cleanup: ICleanupFunction | undefined;
    }>
    | null;

  /**
   * `Omnio` の新しいインスタンスを構築します。
   *
   * @param options `Omnio` の利用を開始する際に実行されるセットアップ関数です。
   */
  public constructor(setup: ISetupFunction) {
    this.#setup = setup;
    this.#props = null;
  }

  /**
   * `Omnio` が利用可能かどうかです。
   */
  public get closed(): boolean {
    return this.#props === null;
  }

  /**
   * Omnio の利用を開始します。
   */
  @asyncmux
  public async open(): Promise<void> {
    if (this.#props !== null) {
      return;
    }

    const {
      json = JSON satisfies IJson,
      mutex: mux = asyncmux.create(),
      logger = new VoidLogger(),
      storage,
      cleanup,
      database,
      textSearch = new PassThroughTextSearch(),
      ...otherParams
    } = await this.#setup();
    try {
      const {
        bucketName,
        maxDescriptionTextByteSize,
        maxUserMetadataJsonByteSize,
      } = v.parse(SetupLiteralParamsSchema(), otherParams);
      const metadata = new Metadata({
        json,
        logger,
        database,
        bucketName,
        textSearch,
        maxDescriptionTextByteSize,
        maxUserMetadataJsonByteSize,
      });
      await metadata.open();
      this.#props = {
        mux,
        logger,
        cleanup,
        storage,
        metadata,
        bucketName,
      };
    } catch (ex) {
      try {
        await cleanup?.();
      } catch (ex) {
        logger.log({
          level: LogLevel.ERROR,
          reason: ex,
          message: "Omnio.open: Failed to clean up",
        });
      }

      throw ex;
    }
  }

  /**
   * Omnio の利用を終了します。
   */
  @asyncmux
  public async close(): Promise<void> {
    if (this.#props === null) {
      return;
    }

    const {
      cleanup,
      metadata,
    } = this.#props;
    await metadata.close();
    await cleanup?.();
    this.#props = null;
  }

  /*************************************************************************************************
   *
   * 作成
   *
   ************************************************************************************************/

  async #createWriteModeStream(
    args: Readonly<{
      lock: AsyncmuxLock;
      mimeType: MimeType | undefined;
      partSize: PartSize | undefined;
      timestamp: Timestamp | undefined;
      objectPath: ObjectPath;
      objectTags: ObjectTags | undefined;
      description: string | null | undefined;
      userMetadata: unknown | undefined;
    }>,
  ): Promise<ObjectFileWriteStream> {
    const {
      logger,
      storage,
      metadata,
      bucketName,
    } = this.#props!;
    const {
      lock,
      mimeType,
      partSize = MIN_PART_SIZE,
      timestamp,
      objectPath,
      objectTags,
      description,
      userMetadata,
    } = args;

    // 排他モードではないので、既存のエンティティーを削除するために、古いエンティティー ID を取得します。
    let currentEntityId: EntityId | undefined;
    try {
      const { entityId } = await metadata.read({
        select: {
          entityId: true,
        },
        where: {
          objectPath,
        },
      });
      currentEntityId = entityId;
    } catch (ex) {
      if (ex instanceof ObjectNotFoundError) {
        // オブジェクトが存在しない場合は新規作成するだけです。
      } else {
        throw ex;
      }
    }

    return new ObjectFileWriteStream({
      flag: v.parse(OpenModeSchema(), "w"),
      hash: await md5.create(),
      lock,
      type: mimeType,
      omnio: this,
      expect: undefined,
      logger,
      storage,
      metadata,
      partSize,
      timestamp,
      bucketName,
      objectPath,
      objectTags,
      description,
      newEntityId: getEntityId(),
      oldEntityId: currentEntityId,
      currentSize: undefined,
      userMetadata,
      currentNumParts: undefined,
    });
  }

  async #createAppendModeStream(
    args: Readonly<{
      lock: AsyncmuxLock;
      mimeType: MimeType | undefined;
      partSize: PartSize | undefined;
      timestamp: Timestamp | undefined;
      objectPath: ObjectPath;
      objectTags: ObjectTags | undefined;
      description: string | null | undefined;
      userMetadata: unknown | undefined;
    }>,
  ): Promise<ObjectFileWriteStream> {
    const {
      logger,
      storage,
      metadata,
      bucketName,
    } = this.#props!;
    const {
      lock,
      mimeType,
      partSize = MIN_PART_SIZE,
      timestamp,
      objectPath,
      objectTags,
      description,
      userMetadata,
    } = args;

    // 排他モードではないので、既存のエンティティーを削除するために、古いエンティティー ID を取得します。
    let currentSize: ObjectSize | undefined;
    let currentEntityId: EntityId | undefined;
    let currentChecksum: Checksum | undefined;
    let currentNumParts: NumParts | undefined;
    let currentPartSize: PartSize | undefined;
    let hash: IHash;
    try {
      const {
        size,
        checksum,
        entityId,
        numParts,
        partSize,
      } = await metadata.readDetail({ objectPath });
      currentSize = size;
      currentChecksum = checksum.value;
      currentEntityId = entityId;
      currentNumParts = numParts;
      currentPartSize = partSize;
      hash = await md5.create(checksum.state);
    } catch (ex) {
      if (ex instanceof ObjectNotFoundError) {
        // オブジェクトが存在しない場合は新規作成するだけです。
        hash = await md5.create();
      } else {
        throw ex;
      }
    }

    return new ObjectFileWriteStream({
      flag: v.parse(OpenModeSchema(), "a"),
      hash,
      lock,
      type: mimeType,
      omnio: this,
      expect: currentChecksum && {
        checksum: currentChecksum,
      },
      logger,
      storage,
      metadata,
      partSize: currentPartSize ?? partSize,
      timestamp,
      bucketName,
      objectPath,
      objectTags,
      description,
      newEntityId: getEntityId(),
      oldEntityId: currentEntityId,
      currentSize,
      userMetadata,
      currentNumParts,
    });
  }

  async #createExclusiveModeStream(
    args: Readonly<{
      flag: Extract<OpenModeLike, `${string}x`>;
      lock: AsyncmuxLock;
      mimeType: MimeType | undefined;
      partSize: PartSize | undefined;
      timestamp: Timestamp | undefined;
      objectPath: ObjectPath;
      objectTags: ObjectTags | undefined;
      description: string | null | undefined;
      userMetadata: unknown | undefined;
    }>,
  ): Promise<ObjectFileWriteStream> {
    const {
      logger,
      storage,
      metadata,
      bucketName,
    } = this.#props!;
    const {
      flag,
      lock,
      partSize = MIN_PART_SIZE,
      mimeType,
      timestamp,
      objectPath,
      objectTags,
      description,
      userMetadata,
    } = args;

    return new ObjectFileWriteStream({
      flag: v.parse(OpenModeSchema(), flag),
      hash: await md5.create(),
      lock,
      type: mimeType,
      omnio: this,
      expect: undefined,
      logger,
      storage,
      metadata,
      partSize,
      timestamp,
      bucketName,
      objectPath,
      objectTags,
      description,
      newEntityId: getEntityId(),
      oldEntityId: undefined,
      currentSize: undefined,
      userMetadata,
      currentNumParts: undefined,
    });
  }

  async #createWriteStream(
    args: Readonly<{
      flag: OpenMode;
      lock: AsyncmuxLock;
      mimeType: MimeType | undefined;
      partSize: PartSize | undefined;
      timestamp: Timestamp | undefined;
      objectPath: ObjectPath;
      objectTags: ObjectTags | undefined;
      description: string | null | undefined;
      userMetadata: unknown | undefined;
    }>,
  ): Promise<ObjectFileWriteStream> {
    const {
      flag,
      lock,
      partSize = MIN_PART_SIZE,
      mimeType,
      timestamp,
      objectPath,
      objectTags,
      description,
      userMetadata,
    } = args;

    try {
      switch (flag) {
        case "w":
          return await this.#createWriteModeStream({
            lock,
            mimeType,
            partSize,
            timestamp,
            objectPath,
            objectTags,
            description,
            userMetadata,
          });

        case "a":
          return await this.#createAppendModeStream({
            lock,
            mimeType,
            partSize,
            timestamp,
            objectPath,
            objectTags,
            description,
            userMetadata,
          });

        case "ax":
        case "wx":
          return await this.#createExclusiveModeStream({
            flag,
            lock,
            mimeType,
            partSize,
            timestamp,
            objectPath,
            objectTags,
            description,
            userMetadata,
          });

        default:
          unreachable(flag);
      }
    } catch (ex) {
      lock.unlock();
      throw ex;
    }
  }

  /**
   * オブジェクトを書き込むためのストリームを作成します。
   *
   * @param path 書き込み先のオブジェクトパスです。
   * @param options 書き込みストリームのオプションです。
   * @returns 書き込みストリームです。
   */
  @asyncmux.readonly
  public async createWriteStream(
    path: ObjectPathLike,
    options: CreateWriteStreamOptions | undefined = {},
  ): Promise<ObjectFileWriteStream> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const { mux } = this.#props;
    const objectPath = v.parse(ObjectPathSchema(), path);
    const {
      flag,
      mimeType,
      partSize,
      timestamp,
      objectTags,
      abortSignal,
      description,
      userMetadata,
    } = v.parse(CreateWriteStreamOptionsSchema(), options);

    const lock = await mux.lock({
      key: objectPath.toString(),
      abortSignal,
    });

    return await this.#createWriteStream({
      flag,
      lock,
      mimeType,
      partSize,
      timestamp,
      objectPath,
      objectTags,
      description,
      userMetadata,
    });
  }

  /**
   * 指定したパスにオブジェクトのデータとメタデータを書き込みます。
   *
   * @param path 書き込み先のオブジェクトパスです。
   * @param data `Uint8Array` に変換できる値またはファイルオブジェクトです。
   * @param options オブジェクト書き込み時のオプションです
   */
  @asyncmux.readonly
  public async putObject(
    path: ObjectPathLike,
    data: Uint8ArraySource | Blob,
    options: PutObjectOptions | undefined = {},
  ): Promise<void> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const { logger } = this.#props;

    let source: Uint8ArraySource;
    const opts = { ...options };
    if (data instanceof ObjectFile) {
      source = await data.arrayBuffer();
      opts.mimeType ??= data.type;
      opts.objectTags ??= data.objectTags;
      opts.description ??= data.description;
      if (opts.userMetadata === undefined) {
        opts.userMetadata = data.userMetadata;
      }
    } else if (data instanceof Blob) {
      source = await data.arrayBuffer();
      opts.mimeType ??= data.type;
    } else {
      source = data;
    }

    const w = await this.createWriteStream(path, opts);
    try {
      await w.write(source);
      await w.close();
    } catch (ex) {
      try {
        await w.abort();
      } catch (ex) {
        logger.log({
          level: LogLevel.ERROR,
          reason: ex,
          message: "Omnio.putObject: Failed to abort write-stream",
        });
      }

      throw ex;
    }
  }

  /*************************************************************************************************
   *
   * 読み取り
   *
   ************************************************************************************************/

  async #createReadStream(
    args: Readonly<{
      lock: AsyncmuxLock;
      objectPath: ObjectPath;
      loadSelectQuery: ObjectMetadataLoadOptions | undefined;
    }>,
  ): Promise<[
    reader: ObjectFileReadStream,
    aux: {
      entityId: EntityId;
    },
  ]> {
    const {
      logger,
      storage,
      metadata,
      bucketName,
    } = this.#props!;
    const {
      lock,
      objectPath,
      loadSelectQuery,
    } = args;

    try {
      const {
        id,
        size,
        checksum,
        entityId,
        mimeType,
        numParts,
        lastModifiedAt,
        ...loadResult
      } = await metadata.read({
        select: {
          ...loadSelectQuery,
          id: true,
          size: true,
          checksum: true,
          entityId: true,
          mimeType: true,
          numParts: true,
          lastModifiedAt: true,
        },
        where: {
          objectPath,
        },
      });
      let entityHandle: IEntityHandle;
      try {
        entityHandle = await storage.getDirectoryHandle(entityId, { create: false });
      } catch (ex) {
        if (ex instanceof EntryPathNotFoundError) {
          // 実際に保存されているファイルがないので、メタデータをを削除します。
          try {
            await metadata.delete({ objectId: id });
          } catch (ex) {
            if (ex instanceof ObjectNotFoundError) {
              // あとで `ObjectNotFoundError` を投げるので、ここでは無視します。
            } else {
              logger.log({
                level: LogLevel.ERROR,
                reason: ex,
                message:
                  `Omnio.createReadStream: Failed to delete metadata: '${bucketName}:${objectPath}'`,
              });
            }
          }

          throw new ObjectNotFoundError(bucketName, objectPath, { cause: ex });
        }

        throw ex;
      }

      const reader = new ObjectFileReadStream({
        lock,
        size,
        type: mimeType,
        omnio: this,
        checksum,
        numParts,
        objectId: id,
        bucketName,
        objectPath,
        objectTags: loadResult.objectTags,
        description: loadResult.description,
        entityHandle,
        lastModified: lastModifiedAt,
        userMetadata: loadResult.userMetadata,
      });

      return [reader, {
        entityId,
      }];
    } catch (ex) {
      lock.unlock();
      throw ex;
    }
  }

  /**
   * オブジェクトを読み込むためのストリームを作成します。
   *
   * @param path オブジェクトパスです。
   * @param options 読み込みストリームのオプションです。
   * @returns 読み込みストリームです。
   */
  @asyncmux.readonly
  public async createReadStream(
    path: ObjectPathLike,
    options: CreateReadStreamOptions | undefined = {},
  ): Promise<ObjectFileReadStream> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const { mux } = this.#props!;
    const objectPath = v.parse(ObjectPathSchema(), path);
    const {
      load: loadSelectQuery,
      abortSignal,
    } = v.parse(CreateReadStreamOptionsSchema(), options);

    const lock = await mux.rLock({
      key: objectPath.toString(),
      abortSignal,
    });

    const [r] = await this.#createReadStream({
      lock,
      objectPath,
      loadSelectQuery,
    });
    return r;
  }

  /**
   * オブジェクトを取得します。
   *
   * @param path オブジェクトパスです。
   * @param options オブジェクトを取得する際のオプションです。
   * @returns `File` クラスを継承したオブジェクトの情報です。
   */
  @asyncmux.readonly
  public async getObject(
    path: ObjectPathLike,
    options: GetObjectOptions | undefined = {},
  ): Promise<ObjectFile> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const { mux } = this.#props!;
    const objectPath = v.parse(ObjectPathSchema(), path);
    const {
      load: loadSelectQuery,
      abortSignal,
    } = v.parse(CreateReadStreamOptionsSchema(), options);

    const lock = await mux.rLock({
      key: objectPath.toString(),
      abortSignal,
    });

    const [reader, aux] = await this.#createReadStream({
      lock,
      objectPath,
      loadSelectQuery,
    });
    using r = reader;
    const f = await ObjectFile.create({
      size: r.size,
      type: r.type,
      parts: await Array.fromAsync(r),
      checksum: r.checksum,
      entityId: aux.entityId,
      bucketName: r.bucketName,
      objectPath: r.objectPath,
      objectTags: r.objectTags,
      description: r.description,
      lastModified: r.lastModified,
      userMetadata: r.userMetadata,
    });

    return f;
  }

  /**
   * 存在するオブジェクトのメタデータを取得します。
   *
   * @template TQuery 存在するオブジェクトのメタデータを取得するためのクエリーの型です。
   * @param query 存在するオブジェクトのメタデータを取得するためのクエリーです。
   * @param options 存在するオブジェクトのメタデータを取得するためのオプションです。
   * @returns オブジェクトのメタデータを取得した結果です。
   */
  @asyncmux.readonly
  public async getObjectMetadata<const Tquery extends GetObjectMetadataQuery>(
    query: Tquery,
    options: GetObjectMetadataOptions | undefined = {},
  ): Promise<GetObjectMetadataResult<$Get<Tquery, "select">>> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const {
      mux,
      metadata,
    } = this.#props;
    const {
      where,
      select,
    } = v.parse(GetObjectMetadataQuerySchema(), query);
    const { abortSignal } = v.parse(GetObjectMetadataOptionsSchema(), options);

    using _ = await mux.rLock({
      key: where.path.toString(),
      abortSignal,
    });

    const out = await metadata.read({
      select,
      where: {
        objectPath: where.path,
      },
    });

    return out as any;
  }

  /**
   * オブジェクトが存在するか確認します。
   *
   * @param path オブジェクトパスです。
   * @returns `true` ならオブジェクトが存在します。
   */
  public existsPath(path: ObjectPathLike): Promise<boolean>;

  /**
   * ディレクトリーが存在するか確認します。
   *
   * @param directoryPath ディレクトリーを表すパスセグメントの配列です。
   * @returns `true` ならディレクトリーが存在します。
   */
  public existsPath(directoryPath: readonly string[]): Promise<boolean>;

  /**
   * オブジェクトまたはディレクトリーが存在するか確認します。
   *
   * @param pathOrDirectoryPath オブジェクトパスまたはディレクトリーを表すパスセグメントの配列です。
   * @returns `true` ならオブジェクトまたはディレクトリーが存在します。
   */
  public existsPath(pathOrDirectoryPath: ObjectPathLike | readonly string[]): Promise<boolean>;

  @asyncmux.readonly
  public async existsPath(arg0: ObjectPathLike | readonly string[]): Promise<boolean> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const { metadata } = this.#props;
    const path = v.parse(v.union([ObjectPathSchema(), ObjectDirectoryPathSchema()]), arg0);

    if (Array.isArray(path)) {
      const { exists } = await metadata.exists({ dirPath: path });
      return exists;
    } else {
      const { exists } = await metadata.exists({ objectPath: path });
      return exists;
    }
  }

  /**
   * オブジェクトやディレクトリーのステータス情報を取得します。
   *
   * @returns オブジェクトやディレクトリーのステータス情報です。
   */
  @asyncmux.readonly
  public async statPath(path: ObjectPathLike): Promise<Stats> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const { metadata } = this.#props;
    const objectPath = v.parse(ObjectPathSchema(), path);
    const stats = await metadata.stat({ objectPath });

    return stats;
  }

  /**
   * ディレクトリーまたはオブジェクトをリストアップします。
   *
   * @template TQuery リストアップするためのクエリーの型です。
   * @param query リストアップするためのクエリーです。
   * @returns ディレクトリーまたはオブジェクトをリストアップした結果です。
   */
  @asyncmux.readonly
  public async list<const TQuery extends ListQuery>(query: TQuery): Promise<
    AsyncGenerator<
      ListItem<
        $Get<TQuery, "select">,
        $Get<$Get<TQuery, "where">, "isObject">
      >
    >
  > {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const { metadata } = this.#props;
    const {
      skip,
      take,
      where = {},
      select,
      orderBy = {},
    } = v.parse(ListQuerySchema(), query);

    const list = await metadata.list({
      skip,
      take,
      where: {
        dirPath: where.dirPath || ([] as unknown as ObjectDirectoryPath),
        isObject: where.isObject,
      },
      select,
      orderBy: {
        name: {
          type: orderBy.name?.type,
          collate: orderBy.name?.collate,
        },
        preferObject: orderBy.preferObject,
      },
    });

    return list as any;
  }

  /**
   * ディレクトリーまたはオブジェクトをリストアップします。
   *
   * @template TQuery リストアップするためのクエリーの型です。
   * @param query リストアップするためのクエリーです。
   * @returns ディレクトリーまたはオブジェクトをリストアップした結果です。
   */
  @asyncmux.readonly
  public async listIntrash<const TQuery extends ListInTrashQuery>(query: TQuery): Promise<
    AsyncGenerator<
      ListItemInTrash<
        $Get<TQuery, "select">
      >
    >
  > {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const { metadata } = this.#props;
    const {
      skip,
      take,
      where = {},
      select,
      orderBy = {},
    } = v.parse(ListInTrashQuerySchema(), query);

    const list = await metadata.listInTrash({
      skip,
      take,
      where: {
        dirPath: where.dirPath || ([] as unknown as ObjectDirectoryPath),
      },
      select,
      orderBy: {
        name: {
          type: orderBy.name?.type,
          collate: orderBy.name?.collate,
        },
      },
    });

    return list as any;
  }

  /*************************************************************************************************
   *
   * 更新
   *
   ************************************************************************************************/

  /**
   * オブジェクトの説明文を対象に全文検索します。
   *
   * @param query 検索クエリーです。
   * @param options オブジェクトの説明文を対象に全文検索するためのオプションです。
   * @returns オブジェクトの説明文を対象に全文検索した結果です。
   */
  @asyncmux.readonly
  public async searchObjects(
    query: string,
    options: SearchObjectsOptions = {},
  ): Promise<AsyncGenerator<SearchObjectResult>> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const { metadata } = this.#props;
    const {
      skip,
      take,
      where = {},
      recursive,
      scoreThreshold,
    } = v.parse(SearchObjectsOptionsSchema(), options);
    const list = await metadata.search({
      skip,
      take,
      query: v.parse(v.string(), query),
      dirPath: where.dirPath || ([] as unknown as ObjectDirectoryPath),
      recursive,
      scoreThreshold,
    });

    return list;
  }

  /*************************************************************************************************
   *
   * 更新
   *
   ************************************************************************************************/

  /**
   * オブジェクトを移動します。
   *
   * @param sourcePath 移動元のオブジェクトパスです。
   * @param destinationPath 移動先のオブジェクトパスです。
   * @param options オブジェクトを移動するためのオプションです。
   */
  @asyncmux.readonly
  public async moveObject(
    sourcePath: ObjectPathLike,
    destinationPath: ObjectPathLike,
    options: MoveObjectOptions | undefined = {},
  ): Promise<void> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const {
      mux,
      metadata,
    } = this.#props;
    const srcObjectPath = v.parse(ObjectPathSchema(), sourcePath);
    const dstObjectPath = v.parse(ObjectPathSchema(), destinationPath);
    const {
      flag,
      abortSignal,
    } = v.parse(MoveObjectOptionsSchema(), options);
    if (srcObjectPath.toString() === dstObjectPath.toString()) {
      return;
    }

    using _srcLock = await mux.lock({
      key: srcObjectPath.toString(),
      abortSignal,
    });
    using _dstLock = await mux.lock({
      key: dstObjectPath.toString(),
      abortSignal,
    });

    switch (flag) {
      case "w":
      case "a":
        await metadata.move({
          srcObjectPath,
          dstObjectPath,
        });
        break;

      case "wx":
      case "ax":
        await metadata.moveExclusive({
          srcObjectPath,
          dstObjectPath,
        });
        break;

      default:
        unreachable(flag);
    }
  }

  /**
   * オブジェクトをコピーします。
   *
   * @param sourcePath コピー元のオブジェクトパスです。
   * @param destinationPath コピー先のオブジェクトパスです。
   * @param options オブジェクトをコピーするためのオプションです。
   */
  @asyncmux.readonly
  public async copyObject(
    sourcePath: ObjectPathLike,
    destinationPath: ObjectPathLike,
    options: CopyObjectOptions | undefined = {},
  ): Promise<void> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const {
      mux,
      logger,
      storage,
      metadata,
      bucketName,
    } = this.#props;
    const srcObjectPath = v.parse(ObjectPathSchema(), sourcePath);
    const dstObjectPath = v.parse(ObjectPathSchema(), destinationPath);
    const {
      flag,
      timestamp,
      abortSignal,
    } = v.parse(CopyObjectOptionsSchema(), options);
    if (srcObjectPath.toString() === dstObjectPath.toString()) {
      return;
    }

    using _srcLock = await mux.rLock({
      key: srcObjectPath.toString(),
      abortSignal,
    });
    using _dstLock = await mux.lock({
      key: dstObjectPath.toString(),
      abortSignal,
    });

    const {
      id: srcObjectId,
      entityId: srcEntityId,
      numParts,
    } = await metadata.read({
      select: {
        id: true,
        entityId: true,
        numParts: true,
      },
      where: {
        objectPath: srcObjectPath,
      },
    });
    let oldDstEntityId: EntityId | undefined;
    try {
      const { entityId } = await metadata.read({
        select: {
          entityId: true,
        },
        where: {
          objectPath: srcObjectPath,
        },
      });
      oldDstEntityId = entityId;
    } catch (ex) {
      if (ex instanceof ObjectNotFoundError) {
        // 無視します。
      } else {
        throw ex;
      }
    }

    // 上書き以外でコピー先が存在する場合はエラーを投げます。
    if (oldDstEntityId !== undefined) {
      switch (flag) {
        case "w":
        case "a":
          break;

        case "wx":
        case "ax":
          throw new ObjectExistsError(bucketName, dstObjectPath);

        default:
          unreachable(flag);
      }
    }

    let srcEntityHandle: IEntityHandle;
    try {
      srcEntityHandle = await storage.getDirectoryHandle(srcEntityId, { create: false });
    } catch (ex) {
      if (ex instanceof EntryPathNotFoundError) {
        // 実際に保存されているファイルがないので、メタデータをを削除します。
        try {
          await metadata.delete({ objectId: srcObjectId });
        } catch (ex) {
          if (ex instanceof ObjectNotFoundError) {
            // あとで `ObjectNotFoundError` を投げるので、ここでは無視します。
          } else {
            logger.log({
              level: LogLevel.ERROR,
              reason: ex,
              message:
                `Omnio.copyObject: Failed to delete metadata: '${bucketName}:${srcObjectPath}'`,
            });
          }
        }

        throw new ObjectNotFoundError(bucketName, srcObjectPath, { cause: ex });
      }

      throw ex;
    }

    const newDstEntityId = getEntityId();
    const dstEntityHandle = await storage.getDirectoryHandle(newDstEntityId, { create: true });
    try {
      for (let partNum = 1; partNum <= numParts; partNum++) {
        const partName = partNum.toString(10);
        const dstPartHandle = await dstEntityHandle.getFileHandle(partName, { create: true });
        const srcPartHandle = await srcEntityHandle.getFileHandle(partName, { create: false });
        const partFile = await srcPartHandle.getFile();
        const partData = await partFile.arrayBuffer();
        const w = await dstPartHandle.createWritable({ keepExistingData: false });
        try {
          await w.write(new Uint8Array(partData));
          await w.close();
        } catch (ex) {
          try {
            await w.abort(w);
          } catch (ex) {
            logger.log({
              level: LogLevel.ERROR,
              reason: ex,
              message: "Omnio.copyObject: Failed to abort writable-file-stream",
            });
          }

          throw ex;
        }
      }

      switch (flag) {
        case "w":
        case "a":
          await metadata.copy({
            timestamp,
            dstEntityId: newDstEntityId,
            dstObjectPath,
            srcObjectPath,
          });
          if (oldDstEntityId !== undefined) {
            // 不要になったエンティティーを削除します。
            try {
              await storage.removeEntry(oldDstEntityId, { recursive: true });
            } catch (ex) {
              if (ex instanceof EntryPathNotFoundError) {
                // 無視します。
              } else {
                logger.log({
                  level: LogLevel.ERROR,
                  reason: ex,
                  message: `Omnio.copyObject: Failed to remove entity: ${oldDstEntityId}`,
                });
              }
            }
          }
          break;

        case "wx":
        case "ax":
          // 通常はここに到達しないはずです。
          if (oldDstEntityId !== undefined) {
            throw new ObjectExistsError(bucketName, dstObjectPath);
          }
          await metadata.copyExclusive({
            timestamp,
            dstEntityId: newDstEntityId,
            dstObjectPath,
            srcObjectPath,
          });
          break;

        default:
          unreachable(flag);
      }
    } catch (ex) {
      // コピーに失敗したので、新しいエンティティーを削除します。
      try {
        await storage.removeEntry(newDstEntityId, { recursive: true });
      } catch (ex) {
        if (ex instanceof EntryPathNotFoundError) {
          // 無視します。
        } else {
          logger.log({
            level: LogLevel.ERROR,
            reason: ex,
            message: `Omnio.copyObject: Failed to remove entity: ${newDstEntityId}`,
          });
        }
      }

      throw ex;
    }
  }

  /**
   * オブジェクトのメタデータを更新します。
   *
   * @param path オブジェクトパスです。
   * @param options オブジェクトのメタデータを更新するためのオプションです。
   */
  @asyncmux.readonly
  public async updateObjectMetadata(
    path: ObjectPathLike,
    options: UpdateObjectMetadataOptions | undefined = {},
  ): Promise<void> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const { metadata } = this.#props;
    const objectPath = v.parse(ObjectPathSchema(), path);
    const {
      mimeType,
      timestamp,
      objectTags,
      description,
      userMetadata,
    } = v.parse(UpdateObjectMetadataOptionsSchema(), options);
    await metadata.update({
      mimeType,
      timestamp,
      objectPath,
      objectTags,
      description,
      userMetadata,
    });
  }

  /**
   * オブジェクトの削除フラグを立てます。
   *
   * @param path オブジェクトパスです。
   * @param options オブジェクトの削除フラグを立てるためのオプションです。
   */
  @asyncmux.readonly
  public async trashObject(
    path: ObjectPathLike,
    options: TrashObjectOptions | undefined = {},
  ): Promise<ObjectId> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const { metadata } = this.#props;
    const objectPath = v.parse(ObjectPathSchema(), path);
    const { timestamp } = v.parse(TrashObjectOptionsSchema(), options);
    const { objectId } = await metadata.trash({
      timestamp,
      objectPath,
    });

    return objectId;
  }

  /*************************************************************************************************
   *
   * 削除
   *
   ************************************************************************************************/

  /**
   * 存在するオブジェクトを削除します。
   *
   * @param path オブジェクトパスです。
   * @param options オブジェクトを削除するためのオプションです。
   */
  @asyncmux.readonly
  public async deleteObject(
    path: ObjectPathLike,
    options: DeleteObjectOptions | undefined = {},
  ): Promise<void> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const {
      mux,
      logger,
      storage,
      metadata,
    } = this.#props;
    const objectPath = v.parse(ObjectPathSchema(), path);
    const { abortSignal } = v.parse(DeleteObjectOptionsSchema(), options);

    using _lock = await mux.lock({
      key: objectPath.toString(),
      abortSignal,
    });

    const {
      objectId,
      entityId,
    } = await metadata.trash({
      timestamp: undefined,
      objectPath,
    });
    await metadata.delete({ objectId });
    try {
      await storage.removeEntry(entityId, { recursive: true });
    } catch (ex) {
      if (ex instanceof EntryPathNotFoundError) {
        // 無視します。
      } else {
        logger.log({
          level: LogLevel.ERROR,
          reason: ex,
          message: `Omnio.deleteObject: Failed to remove entity: ${entityId}`,
        });
      }
    }
  }

  /**
   * ゴミ箱に入れられたオブジェクトを削除します。
   *
   * @param objectId オブジェクトの識別子です。
   */
  @asyncmux.readonly
  public async deleteObjectInTrash(objectId: ObjectId): Promise<void> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const {
      logger,
      storage,
      metadata,
    } = this.#props;
    objectId = v.parse(ObjectIdSchema(), objectId);

    const { entityId } = await metadata.readInTrash({ objectId });
    await metadata.delete({ objectId });
    try {
      await storage.removeEntry(entityId, { recursive: true });
    } catch (ex) {
      if (ex instanceof EntryPathNotFoundError) {
        // 無視します。
      } else {
        logger.log({
          level: LogLevel.ERROR,
          reason: ex,
          message: `Omnio.deleteObjectInTrash: Failed to remove entity: ${entityId}`,
        });
      }
    }
  }

  /*************************************************************************************************
   *
   * その他
   *
   ************************************************************************************************/

  /**
   * メタデータを管理するデータベースに対して SQL クエリーを実行し、結果を返します。
   *
   * @param sql 実行する SQL クエリーです。
   * @returns クエリーの実行結果をイテレーターで返します。
   */
  public dangerous_queryObjectMetadata(sql: string | Sql): AsyncGenerator<Row> {
    if (this.#props === null) {
      throw new OmnioClosedError();
    }

    const { metadata } = this.#props;

    return metadata.stream(v.parse(SqlSchema(), sql));
  }
}
