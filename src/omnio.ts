import * as v from "valibot";
import type BucketName from "./bucket-name.js";
import type { Db, Row } from "./db/db.types.js";
import { Error, FsPathNotFoundError, ObjectNotFoundError } from "./errors.js";
import type { DirectoryHandle, FileHandle, Fs } from "./fs/fs.types.js";
import getEntityId from "./get-entity-id.js";
import getMimeType from "./get-mime-type.js";
import type { Logger } from "./logger/logger.types.js";
import toConsoleLikeLogger, { type ConsoleLikeLogger } from "./logger/to-console-like-logger.js";
import VoidLogger from "./logger/void-logger.js";
import md5, { type Hash } from "./md5.js";
import Metadata, { type Json, type Sql, type TextSearch } from "./metadata.js";
import mutex from "./mutex.js";
import ObjectFileWriteStream from "./object-file-write-stream.js";
import ObjectFile, { type ObjectFileLoadOptions } from "./object-file.js";
import type ObjectPath from "./object-path.js";
import * as schemas from "./schemas.js";
import toUint8Array, { type Uint8ArraySource } from "./to-uint8-array.js";
import type { $Get, $Select } from "./type-utils.js";

/**
 * enum オブジェクトを作成します。
 *
 * @template TInput 入力値の型です。
 * @template TOutput 出力値の型です。
 * @template TObject enum オブジェクトの型です。
 * @param schema valibot スキーマです。
 * @param object enum オブジェクトです。
 * @returns valibot スキーマで検証された enum オブジェクトです。
 */
function createEnum<
  const TInput,
  const TOutput,
  const TObject extends { readonly [key: string]: TInput },
>(
  schema: v.BaseSchema<TInput, TOutput, v.BaseIssue<unknown>>,
  object: TObject,
): { readonly [P in keyof TObject]: Extract<TOutput, TObject[P]> } {
  // @ts-expect-error
  return Object.fromEntries(Object.entries(object).map(([k, val]) => [k, v.parse(schema, val)]));
}

/**
 * オブジェクトを開く際のモードです。
 */
const OPEN_MODE = createEnum(schemas.OpenMode, {
  WRITE: "w",
  APPEND: "a",
});

/**
 * 0 です。
 */
const ZERO = v.parse(schemas.UnsignedInteger, 0);

/**
 * アイテムのリストです。
 *
 * @template TItem アイテムの型です。
 */
export type List<TItem> = AsyncGenerator<TItem, void, void>;

/**
 * オブジェクトパスになれるの型です。
 */
export type ObjectPathLike = v.InferInput<typeof schemas.ObjectPathLike>;

/**
 * `Omnio` を構築するためのオプションです。
 */
export type OmnioOptions = Readonly<{
  /**
   * メタデータを記録するためのデータベースです。
   */
  db: Db;

  /**
   * メタデータを永続ストレージに記録するためのファイルシステムです。
   */
  fs: Fs;

  /**
   * バケット名です。
   */
  bucketName: string;

  /**
   * JavaScript の値と JSON 文字列を相互変換するための関数群です。
   *
   * @default JSON
   */
  json?: Json | undefined;

  /**
   * ManagedFs のロガーです。
   */
  logger?: Logger | undefined;

  /**
   * オブジェクトの説明文の検索に使用するユーティリティーです。
   */
  textSearch?: TextSearch | undefined;

  /**
   * オブジェクトの説明文の最大サイズ (バイト数) です。
   *
   * @default 10 KiB
   */
  maxDescriptionTextSize?: number | undefined;

  /**
   * ユーザー定義のメタデータの最大サイズ (バイト数) です。
   * このサイズは、ユーザー定義のメタデータを `json.stringify` で変換したあとの文字列に対して計算されます。
   *
   * @default 10 KiB
   */
  maxUserMetadataJsonSize?: number | undefined;
}>;

/**
 * `Omnio` を構築するためのオプションの valibot スキーマです。
 */
const OmnioOptionsSchema = v.object({
  db: v.object({
    open: v.function(),
    close: v.function(),
    exec: v.function(),
    query: v.function(),
    prepare: v.function(),
  }),
  fs: v.object({
    root: v.string(),
    path: v.object({
      resolve: v.function(),
    }),
    open: v.function(),
    close: v.function(),
    getDirectoryHandle: v.function(),
  }),
  bucketName: schemas.BucketName,
  json: v.optional(v.object({
    parse: v.function(),
    stringify: v.function(),
  })),
  logger: v.optional(v.object({
    log: v.function(),
  })),
  textSearch: v.optional(v.object({
    toQueryString: v.function(),
    fromQueryString: v.function(),
  })),
  maxDescriptionTextSize: v.optional(schemas.UnsignedInteger),
  maxUserMetadataJsonSize: v.optional(schemas.UnsignedInteger),
});

/**
 * オブジェクトを開く際のモードです。
 *
 * - **`"w"`**: 書き込みモードで開きます。オブジェクトが存在しない場合は新規作成され、もし存在する場合は上書きします。
 * - **`"wx"`**: 書き込みモードで開きます。オブジェクトが存在する場合はエラーになります。
 * - **`"a"`**: 追加書き込みモードで開きます。オブジェクトが存在しない場合は新規作成されます。
 * - **`"ax"`**: 追加書き込みモードで開きます。オブジェクトが存在する場合はエラーになります。
 */
export type OpenMode = v.InferInput<typeof schemas.OpenMode>;

/**
 * オプジェクトを書き込む際のオプションです。
 */
export type PutObjectOptions = Readonly<{
  /**
   * オブジェクトを開く際のモードです。
   *
   * - **`"w"`**: 書き込みモードで開きます。オブジェクトが存在しない場合は新規作成され、もし存在する場合は上書きします。
   * - **`"wx"`**: 書き込みモードで開きます。オブジェクトが存在しない場合はエラーになります。
   * - **`"a"`**: 追加書き込みモードで開きます。オブジェクトが存在しない場合は新規作成されます。
   * - **`"ax"`**: 追加書き込みモードで開きます。オブジェクトが存在しない場合はエラーになります
   *
   * @default "w"
   */
  flag?: OpenMode | undefined;

  /**
   * オブジェクトのデータ形式です。`undefined` の場合はオブジェクトパスから自動判定されます。
   * 判定できない場合は "application/octet-stream" になります。
   */
  mimeType?: string | undefined;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   *
   * @default []
   */
  objectTags?: readonly string[] | undefined;

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
}>;

/**
 * オプジェクトを書き込む際のオプションの valibot スキーマです。
 */
const PutOptionsSchema = v.object({
  flag: v.optional(schemas.OpenMode, "w"),
  mimeType: v.optional(schemas.MimeType),
  objectTags: v.optional(schemas.ObjectTags),
  description: v.nullish(v.string()),
  userMetadata: v.optional(v.unknown()),
});

/**
 * 書き込みストリームのオプションです。
 */
export type CreateWriteStreamOptions = Readonly<{
  /**
   * オブジェクトを開く際のモードです。
   *
   * - **`"w"`**: 書き込みモードで開きます。オブジェクトが存在しない場合は新規作成され、もし存在する場合は上書きします。
   * - **`"wx"`**: 書き込みモードで開きます。オブジェクトが存在しない場合はエラーになります。
   * - **`"a"`**: 追加書き込みモードで開きます。オブジェクトが存在しない場合は新規作成されます。
   * - **`"ax"`**: 追加書き込みモードで開きます。オブジェクトが存在しない場合はエラーになります
   *
   * @default "w"
   */
  flag?: OpenMode | undefined;

  /**
   * オブジェクトのデータ形式です。`undefined` の場合はオブジェクトパスから自動判定されます。
   * 判定できない場合は "application/octet-stream" になります。
   */
  mimeType?: string | undefined;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   *
   * @default []
   */
  objectTags?: readonly string[] | undefined;

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
}>;

/**
 * 書き込みストリームのオプションの valibot スキーマです。
 */
const CreateWriteStreamOptionsSchema = v.object({
  flag: v.optional(schemas.OpenMode, "w"),
  mimeType: v.optional(schemas.MimeType),
  objectTags: v.optional(schemas.ObjectTags),
  description: v.nullish(v.string()),
  userMetadata: v.optional(v.unknown()),
});

/**
 * オブジェクトを取得する際のオプションです。
 */
export type GetObjectOptions = Readonly<{
  /**
   * 追加のデータを読み込むためのオプションです。読み込むデータを選択します。`true` の場合は全ての追加のデータを読み込みます。
   *
   * @default false
   */
  load?: boolean | ObjectFileLoadOptions | undefined;
}>;

/**
 * オブジェクトを取得する際のオプションのスキーマです。
 */
const GetObjectOptionsSchema = v.object({
  load: v.optional(v.union([
    v.pipe(
      v.boolean(),
      v.transform(bool => ({
        objectTags: bool,
        description: bool,
        userMetadata: bool,
      })),
    ),
    v.object({
      objectTags: v.optional(v.boolean()),
      description: v.optional(v.boolean()),
      userMetadata: v.optional(v.boolean()),
    }),
  ])),
});

/**
 * 存在するオブジェクトのメタデータを取得するためのクエリーです。
 */
export type GetObjectMetadataQuery = Readonly<{
  /**
   * 結果に含めるカラムを選択します。
   */
  select?:
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
    path: ObjectPathLike;
  }>;
}>;

const GetObjectMetadataQuerySchema = v.object({
  select: v.optional(v.object({
    bucket: v.optional(v.boolean()),
    id: v.optional(v.boolean()),
    path: v.optional(v.boolean()),
    recordType: v.optional(v.boolean()),
    recordTimestamp: v.optional(v.boolean()),
    size: v.optional(v.boolean()),
    mimeType: v.optional(v.boolean()),
    createdAt: v.optional(v.boolean()),
    lastModifiedAt: v.optional(v.boolean()),
    checksum: v.optional(v.boolean()),
    checksumAlgorithm: v.optional(v.boolean()),
    objectTags: v.optional(v.boolean()),
    description: v.optional(v.boolean()),
    userMetadata: v.optional(v.boolean()),
    entityId: v.optional(v.boolean()),
  })),
  where: v.object({
    path: schemas.ObjectPathLike,
  }),
});

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
 * 存在するオブジェクトのメタデータを取得した結果です。
 *
 * @template TSelect SELECT するカラムです。
 */
export type GetObjectMetadataResult<TSelect> = $Select<ObjectMetadata, TSelect>;

/**
 * オブジェクトやディレクトリーのステータス情報です。
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
 * ディレクトリーまたはオブジェクトをリストアップするためのオプションです。
 */
export type ListOptions = Readonly<{
  /**
   * スキップするアイテムの数です。
   *
   * @default 0
   */
  skip?: number | undefined;

  /**
   * 取得するアイテムの最大数です。
   *
   * @default 上限なし
   */
  take?: number | undefined;

  /**
   * 結果の並び順を指定します。
   */
  orderBy?:
    | Readonly<{
      /**
       * オブジェクト名の並び順です。
       *
       * @default "ASC"
       */
      name?: v.InferInput<typeof schemas.OrderType> | undefined;

      /**
       * オブジェクトを先頭にします。
       *
       * @default false
       */
      preferObject?: boolean | undefined;
    }>
    | undefined;
}>;

/**
 * ディレクトリーまたはオブジェクトをリストアップするためのオプションです。
 */
const ListOptionsSchema = v.object({
  skip: v.optional(schemas.UnsignedInteger),
  take: v.optional(schemas.UnsignedInteger),
  orderBy: v.optional(v.object({
    name: v.optional(schemas.OrderType),
    preferObject: v.optional(v.boolean()),
  })),
});

/**
 * ディレクトリーまたはオブジェクトをリストアップした結果です。
 */
export type ListItem = {
  /**
   * `true` ならオブジェクトです。`false` ならディレクトリーです。
   */
  isObject: boolean;

  /**
   * アイテムの名前です。
   */
  name: string;
};

/**
 * オブジェクトの説明文を対象に全文検索するためのオプションです。
 */
export type SearchObjectsOptions = Readonly<{
  /**
   * スキップする検索結果の数です。
   *
   * @default 0
   */
  skip?: number | undefined;

  /**
   * 取得する検索結果の最大数です。
   *
   * @default 上限なし
   */
  take?: number | undefined;

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

/**
 * オブジェクトの説明文を対象に全文検索するためのオプションです。
 */
const SearchObjectsOptionsSchema = v.object({
  skip: v.optional(schemas.UnsignedInteger),
  take: v.optional(schemas.UnsignedInteger),
  recursive: v.optional(v.boolean()),
  scoreThreshold: v.optional(v.pipe(v.number(), v.finite())),
});

/**
 * オブジェクトの説明文を対象に全文検索した結果です。
 */
export type SearchResult = {
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
 * オブジェクトのメタデータを更新するためのオプションです。
 */
type UpdateObjectMetadataOptions = Readonly<{
  /**
   * オブジェクトのデータ形式です。
   */
  mimeType?: string | undefined;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  objectTags?: readonly string[] | undefined;

  /**
   * オブジェクトの説明文です。
   */
  description?: string | null | undefined;

  /**
   * ユーザー定義のメタデータです。
   */
  userMetadata?: unknown | undefined;
}>;

/**
 * オブジェクトのメタデータを更新するためのオプションです。
 */
const UpdateObjectMetadataOptionsSchema = v.object({
  mimeType: v.optional(schemas.MimeType),
  objectTags: v.optional(schemas.ObjectTags),
  description: v.optional(v.nullable(v.string())),
  userMetadata: v.optional(v.unknown()),
});

/**
 * SQL になれる値です。
 */
const SqlSchema = v.union([
  v.string(),
  v.pipe(
    v.object({
      text: v.string(),
      values: v.pipe(v.array(v.unknown()), v.readonly()),
    }),
    v.readonly(),
  ),
]);

export default class Omnio {
  /**
   * バケット内のディレクトリーハンドラーです。
   */
  #bucket: Readonly<Record<"entities", DirectoryHandle>> | null;

  /**
   * ファイルシステムです。
   */
  readonly #fs: Fs;

  /**
   * ログを記録する関数群です。
   */
  readonly #logger: ConsoleLikeLogger;

  /**
   * メタデータを管理するオブジェクトです。
   */
  readonly #metadata: Metadata;

  /**
   * バケット名です。
   */
  public readonly bucketName: BucketName;

  /**
   * `Omnio` の新しいインスタンスを構築します。
   *
   * @param options `Omnio` を構築するためのオプションです。
   */
  public constructor(options: OmnioOptions) {
    const {
      bucketName,
      maxDescriptionTextSize,
      maxUserMetadataJsonSize,
    } = v.parse(OmnioOptionsSchema, options);
    const {
      db,
      fs,
      json,
      logger = new VoidLogger(),
      textSearch,
    } = options;
    this.#bucket = null;
    this.#fs = fs;
    this.#logger = toConsoleLikeLogger(logger);
    this.#metadata = new Metadata({
      db,
      fs,
      json,
      logger: this.#logger,
      bucketName,
      textSearch,
      maxDescriptionTextSize,
      maxUserMetadataJsonSize,
    });
    this.bucketName = bucketName;
  }

  /**
   * `Omnio` が利用可能かどうかを返します。
   */
  public get closed(): boolean {
    return this.#bucket === null;
  }

  /**
   * Omnio の利用を開始します。
   */
  @mutex
  public async open(): Promise<void> {
    if (this.#bucket) {
      return;
    }

    await this.#metadata.connect();
    try {
      const omnio = await this.#fs.getDirectoryHandle("omnio", { create: false });
      const buckets = await omnio.getDirectoryHandle("buckets", { create: false });
      const bucket = await buckets.getDirectoryHandle(this.bucketName, { create: false });
      this.#bucket = {
        entities: await bucket.getDirectoryHandle("entities", { create: true }),
      };
    } catch (ex) {
      this.#bucket = null;
      try {
        await this.#metadata.disconnect();
      } catch (ex) {
        this.#logger.error("Omnio.open: Failed to disconnect metadata", ex);
      }

      throw ex;
    }
  }

  /**
   * Omnio の利用を終了します。
   */
  @mutex
  public async close(): Promise<void> {
    if (!this.#bucket) {
      return;
    }

    await this.#metadata.disconnect();
    this.#bucket = null;
  }

  async #writeObject(
    args: Required<Omit<v.InferOutput<typeof PutOptionsSchema>, "flag">> & {
      data: Uint8Array<ArrayBuffer>;
      objectPath: ObjectPath;
    },
  ): Promise<void> {
    const {
      data,
      mimeType,
      objectPath,
      objectTags,
      description,
      userMetadata,
    } = args;

    // 排他モードではないので、既存のエンティティーを削除するために、古いエンティティー ID を取得します。
    let oldEntityId: v.InferOutput<typeof schemas.EntityId> | undefined;
    try {
      const { entityId } = await this.#metadata.read({
        select: {
          entityId: true,
        },
        where: {
          objectPath,
        },
      });
      oldEntityId = entityId;
    } catch (ex) {
      if (ex instanceof ObjectNotFoundError) {
        // オブジェクトが存在しない場合は新規作成するだけなので、何もしません。
      } else {
        throw ex;
      }
    }

    // データを書き込みます
    const newEntityId = getEntityId();
    const fileHandle = await this.#bucket!.entities.getFileHandle(newEntityId, { create: true });
    const writer = await fileHandle.createWritable();
    const hash = await md5.create();
    try {
      await writer.write(data);
      hash.update(data);
    } catch (ex) {
      // データの書き込みに失敗したので、ストリームを中断します。
      try {
        await writer.abort();
      } catch (ex) {
        this.#logger.error("Omnio.putObject: Failed to abort writer", ex);
      }

      throw ex;
    }

    // エンティティーを保存します。
    await writer.close();

    try {
      const checksum = hash.digest();
      // メタデータを作成します。
      await this.#metadata.create({
        checksum,
        entityId: newEntityId,
        mimeType,
        objectPath,
        objectSize: writer.bytesWritten,
        objectTags,
        description,
        userMetadata,
      });
    } catch (ex) {
      // メタデータの作成に失敗したので、新しいエンティティーを削除します。
      try {
        await this.#bucket!.entities.removeEntry(newEntityId);
      } catch (ex) {
        this.#logger.error("Omnio.putObject: Failed to remove new entity: " + newEntityId, ex);
      }

      throw ex;
    }

    // 排他モードではないので、削除すべき古いエンティティーが存在することがあります。
    if (oldEntityId !== undefined) {
      // 新しいエンティティーの保存とメタデータの作成に成功したので、古いエンティティーを削除します。
      try {
        await this.#bucket!.entities.removeEntry(oldEntityId);
      } catch (ex) {
        this.#logger.error("Omnio.putObject: Failed to remove old entity: " + oldEntityId, ex);
      }
    }
  }

  async #appendObject(
    args: Required<Omit<v.InferOutput<typeof PutOptionsSchema>, "flag">> & {
      data: Uint8Array<ArrayBuffer>;
      objectPath: ObjectPath;
    },
  ): Promise<void> {
    const {
      data,
      mimeType,
      objectPath,
      objectTags,
      description,
      userMetadata,
    } = args;

    // 排他モードではないので、既存のエンティティーに追記するために、現在のエンティティー ID を取得します。
    let oldEntityId: v.InferOutput<typeof schemas.EntityId> | undefined;
    let oldChecksum: v.InferOutput<typeof schemas.Checksum> | undefined;
    let hashState: v.InferOutput<typeof schemas.HashState> | undefined;
    try {
      const {
        entityId,
        checksum,
      } = await this.#metadata.readInternal({ objectPath });
      oldEntityId = entityId;
      oldChecksum = checksum.value;
      // initialSize = objectSize;
      hashState = checksum.state;
    } catch (ex) {
      if (ex instanceof ObjectNotFoundError) {
        // オブジェクトが存在しない場合は新規作成するだけです。
      } else {
        throw ex;
      }
    }

    // データを用意します。
    const newEntityId = getEntityId();
    const fileHandle = await this.#bucket!.entities.getFileHandle(newEntityId, { create: true });
    const writer = await fileHandle.createWritable();
    if (oldEntityId !== undefined) {
      let srcFileHandle: FileHandle;
      try {
        srcFileHandle = await this.#bucket!.entities.getFileHandle(oldEntityId, { create: false });
      } catch (ex) {
        if (ex instanceof FsPathNotFoundError) {
          // エンティティーがないので、メタデータから削除します。
          try {
            await this.#metadata.delete({ entityId: oldEntityId });
          } catch (ex) {
            this.#logger.error(
              `Omnio.putObject: Failed to delete metadata: '${this.bucketName}:${objectPath}'`,
              ex,
            );
          }

          throw new ObjectNotFoundError(this.bucketName, objectPath, { cause: ex });
        }

        throw ex;
      }

      await writer.write(srcFileHandle);
    }

    // データを書き込みます。
    const hash = await md5.create(hashState);
    try {
      await writer.write(data);
      hash.update(data);
    } catch (ex) {
      // データの書き込みに失敗したので、ストリームを中断します。
      try {
        await writer.abort();
      } catch (ex) {
        this.#logger.error("Omnio.putObject: Failed to abort writer", ex);
      }

      throw ex;
    }

    // エンティティーを保存します。
    await writer.close();

    try {
      const checksum = hash.digest();
      const objectSize = v.parse(schemas.UnsignedInteger, writer.bytesWritten);
      if (oldChecksum === undefined) {
        // メタデータを作成します。
        await this.#metadata.create({
          checksum,
          entityId: newEntityId,
          mimeType,
          objectPath,
          objectSize,
          objectTags,
          description,
          userMetadata,
        });
      } else {
        // メタデータを更新します。
        await this.#metadata.updateExclusive({
          expect: {
            checksum: oldChecksum,
          },
          checksum,
          entityId: newEntityId,
          mimeType,
          objectPath,
          objectSize,
          objectTags,
          description,
          userMetadata,
        });
      }
    } catch (ex) {
      // メタデータの作成に失敗したので、新しいエンティティーを削除します。
      try {
        await this.#bucket!.entities.removeEntry(newEntityId);
      } catch (ex) {
        this.#logger.error("Omnio.putObject: Failed to remove new entity: " + newEntityId, ex);
      }

      throw ex;
    }

    // 排他モードではないので、削除すべき古いエンティティーが存在することがあります。
    if (oldEntityId !== undefined) {
      // 新しいエンティティーの保存とメタデータの作成に成功したので、古いエンティティーを削除します。
      try {
        await this.#bucket!.entities.removeEntry(oldEntityId);
      } catch (ex) {
        this.#logger.error("Omnio.putObject: Failed to remove old entity: " + oldEntityId, ex);
      }
    }
  }

  async #putObjectExclusive(
    args: Required<Omit<v.InferOutput<typeof PutOptionsSchema>, "flag">> & {
      data: Uint8Array<ArrayBuffer>;
      objectPath: ObjectPath;
    },
  ): Promise<void> {
    const {
      data,
      mimeType,
      objectPath,
      objectTags,
      description,
      userMetadata,
    } = args;

    // エンティティーを保存します。
    const newEntityId = getEntityId();
    const fileHandle = await this.#bucket!.entities.getFileHandle(newEntityId, { create: true });
    const writer = await fileHandle.createWritable();
    const hash = await md5.create();
    try {
      await writer.write(data);
      hash.update(data);
    } catch (ex) {
      try {
        await writer.abort();
      } catch (ex) {
        this.#logger.error("Omnio.putObject: Failed to abort writer", ex);
      }

      throw ex;
    }

    await writer.close();

    try {
      const checksum = hash.digest();
      // メタデータを作成します。
      await this.#metadata.createExclusive({
        checksum,
        entityId: newEntityId,
        mimeType,
        objectPath,
        objectSize: writer.bytesWritten,
        objectTags,
        description,
        userMetadata,
      });
    } catch (ex) {
      // メタデータの作成に失敗したので、新しいエンティティーを削除します。
      try {
        await this.#bucket!.entities.removeEntry(newEntityId);
      } catch (ex) {
        this.#logger.error("Omnio.putObject: Failed to remove entity: " + newEntityId, ex);
      }

      throw ex;
    }
  }

  /**
   * 指定したパスにオブジェクトのデータとメタデータを書き込みます。
   *
   * @param path 書き込み先のオブジェクトパスです。
   * @param data `Uint8Array` に変換できる値です。
   * @param options オブジェクト書き込み時のオプションです
   */
  @mutex
  public async putObject(
    path: ObjectPathLike,
    data: Uint8ArraySource,
    options: PutObjectOptions | undefined = {},
  ): Promise<void> {
    if (!this.#bucket) {
      throw new Error("Omnio closed");
    }

    const objectPath = v.parse(schemas.ObjectPathLike, path);
    const {
      flag,
      mimeType,
      objectTags,
      description,
      userMetadata,
    } = v.parse(PutOptionsSchema, options);
    const obj = toUint8Array(data);
    const args = {
      data: obj,
      mimeType,
      objectPath,
      objectTags,
      description,
      userMetadata,
    };

    switch (flag) {
      case "w":
        await this.#writeObject(args);
        break;

      case "a":
        await this.#appendObject(args);
        break;

      case "ax":
      case "wx":
        await this.#putObjectExclusive(args);
        break;

      default:
        throw new Error(`Unknown flag: ${String(flag satisfies never)}`);
    }
  }

  async #createWriteStream(
    args: Readonly<{
      mimeType: v.InferOutput<typeof schemas.MimeType>;
      objectPath: ObjectPath;
      objectTags: v.InferOutput<typeof schemas.ObjectTags> | undefined;
      description: string | null | undefined;
      userMetadata: unknown | undefined;
    }>,
  ): Promise<ObjectFileWriteStream> {
    const {
      mimeType,
      objectPath,
      objectTags,
      description,
      userMetadata,
    } = args;

    // 排他モードではないので、既存のエンティティーを削除するために、古いエンティティー ID を取得します。
    let oldEntityId: v.InferOutput<typeof schemas.EntityId> | undefined;
    try {
      const { entityId } = await this.#metadata.read({
        select: {
          entityId: true,
        },
        where: {
          objectPath,
        },
      });
      oldEntityId = entityId;
    } catch (ex) {
      if (ex instanceof ObjectNotFoundError) {
        // オブジェクトが存在しない場合は新規作成するだけです。
      } else {
        throw ex;
      }
    }

    const newEntityId = getEntityId();
    const fileHandle = await this.#bucket!.entities.getFileHandle(newEntityId, { create: true });

    return new ObjectFileWriteStream({
      flag: OPEN_MODE.WRITE,
      hash: await md5.create(),
      type: mimeType,
      omnio: this,
      expect: undefined,
      logger: this.#logger,
      writer: await fileHandle.createWritable(),
      metadata: this.#metadata,
      directory: this.#bucket!.entities,
      entityIds: {
        new: newEntityId,
        old: oldEntityId,
      },
      bucketName: this.bucketName,
      objectPath,
      objectTags,
      offsetSize: ZERO,
      description,
      userMetadata,
    });
  }

  async #createAppendStream(
    args: Readonly<{
      mimeType: v.InferOutput<typeof schemas.MimeType>;
      objectPath: ObjectPath;
      objectTags: v.InferOutput<typeof schemas.ObjectTags> | undefined;
      description: string | null | undefined;
      userMetadata: unknown | undefined;
    }>,
  ): Promise<ObjectFileWriteStream> {
    const {
      mimeType,
      objectPath,
      objectTags,
      description,
      userMetadata,
    } = args;

    // 排他モードではないので、既存のエンティティーを削除するために、古いエンティティー ID を取得します。
    let oldEntityId: v.InferOutput<typeof schemas.EntityId> | undefined;
    let oldChecksum: v.InferOutput<typeof schemas.Checksum> | undefined;
    let hash: Hash;
    try {
      const {
        entityId,
        checksum,
      } = await this.#metadata.readInternal({ objectPath });
      oldEntityId = entityId;
      oldChecksum = checksum.value;
      hash = await md5.create(checksum.state);
    } catch (ex) {
      if (ex instanceof ObjectNotFoundError) {
        // オブジェクトが存在しない場合は新規作成するだけです。
        hash = await md5.create();
      } else {
        throw ex;
      }
    }

    // エンティティーのファイルハンドラーを取得します。
    const newEntityId = getEntityId();
    const fileHandle = await this.#bucket!.entities.getFileHandle(newEntityId, { create: true });

    // 既存のデータをコピーします。
    const writer = await fileHandle.createWritable();
    if (oldEntityId !== undefined) {
      let srcFileHandle: FileHandle;
      try {
        srcFileHandle = await this.#bucket!.entities.getFileHandle(oldEntityId, { create: false });
      } catch (ex) {
        if (ex instanceof FsPathNotFoundError) {
          // エンティティーがないので、メタデータから削除します。
          try {
            await this.#metadata.delete({ entityId: oldEntityId });
          } catch (ex) {
            this.#logger.error(
              `Omnio.putObject: Failed to delete metadata: '${this.bucketName}:${objectPath}'`,
              ex,
            );
          }

          throw new ObjectNotFoundError(this.bucketName, objectPath, { cause: ex });
        }

        throw ex;
      }

      await writer.write(srcFileHandle);
    }

    return new ObjectFileWriteStream({
      flag: OPEN_MODE.APPEND,
      hash,
      type: mimeType,
      omnio: this,
      expect: oldChecksum && {
        checksum: oldChecksum,
      },
      logger: this.#logger,
      writer,
      metadata: this.#metadata,
      directory: this.#bucket!.entities,
      entityIds: {
        new: newEntityId,
        old: oldEntityId,
      },
      bucketName: this.bucketName,
      objectPath,
      objectTags,
      offsetSize: writer.bytesWritten,
      description,
      userMetadata,
    });
  }

  async #createExclusiveStream(
    args: Readonly<{
      flag: Extract<OpenMode, `${string}x`> & v.Brand<"OpenMode">;
      mimeType: v.InferOutput<typeof schemas.MimeType>;
      objectPath: ObjectPath;
      objectTags: v.InferOutput<typeof schemas.ObjectTags> | undefined;
      description: string | null | undefined;
      userMetadata: unknown | undefined;
    }>,
  ): Promise<ObjectFileWriteStream> {
    const {
      flag,
      mimeType,
      objectPath,
      objectTags,
      description,
      userMetadata,
    } = args;

    const newEntityId = getEntityId();
    const fileHandle = await this.#bucket!.entities.getFileHandle(newEntityId, { create: true });

    return new ObjectFileWriteStream({
      flag,
      hash: await md5.create(),
      type: mimeType,
      omnio: this,
      expect: undefined,
      logger: this.#logger,
      writer: await fileHandle.createWritable(),
      metadata: this.#metadata,
      directory: this.#bucket!.entities,
      entityIds: {
        new: newEntityId,
        old: undefined,
      },
      bucketName: this.bucketName,
      objectPath,
      objectTags,
      offsetSize: ZERO,
      description,
      userMetadata,
    });
  }

  /**
   * オブジェクトを書き込むためのストリームを作成します。
   *
   * @param path 書き込み先のオブジェクトパスです。
   * @param options 書き込みストリームのオプションです。
   * @returns 書き込みストリームです。
   */
  @mutex
  public async createWriteStream(
    path: ObjectPathLike,
    options: CreateWriteStreamOptions | undefined = {},
  ): Promise<ObjectFileWriteStream> {
    if (!this.#bucket) {
      throw new Error("Omnio closed");
    }

    const objectPath = v.parse(schemas.ObjectPathLike, path);
    const {
      flag,
      mimeType = getMimeType(objectPath.basename),
      objectTags,
      description,
      userMetadata,
    } = v.parse(CreateWriteStreamOptionsSchema, options);
    switch (flag) {
      case "w":
        return await this.#createWriteStream({
          mimeType,
          objectPath,
          objectTags,
          description,
          userMetadata,
        });

      case "a":
        return await this.#createAppendStream({
          mimeType,
          objectPath,
          objectTags,
          description,
          userMetadata,
        });

      case "ax":
      case "wx":
        return await this.#createExclusiveStream({
          flag,
          mimeType,
          objectPath,
          objectTags,
          description,
          userMetadata,
        });

      default:
        throw new Error(`Unknown flag: ${String(flag satisfies never)}`);
    }
  }

  /**
   * オブジェクトを取得します。
   *
   * @param path オブジェクトパスです。
   * @param options オブジェクトを取得する際のオプションです。
   * @returns `File` クラスを継承したオブジェクトの情報です。
   */
  @mutex.readonly
  public async getObject(
    path: ObjectPathLike,
    options: GetObjectOptions | undefined = {},
  ): Promise<ObjectFile> {
    if (!this.#bucket) {
      throw new Error("Omnio closed");
    }

    const objectPath = v.parse(schemas.ObjectPathLike, path);
    const { load: loadOptions = {} } = v.parse(GetObjectOptionsSchema, options);
    const {
      size,
      checksum,
      entityId,
      mimeType,
      lastModifiedAt,
      ...other
    } = await this.#metadata.read({
      select: {
        ...loadOptions,
        size: true,
        checksum: true,
        entityId: true,
        mimeType: true,
        lastModifiedAt: true,
      },
      where: {
        objectPath,
      },
    });
    let fileHandle: FileHandle;
    try {
      fileHandle = await this.#bucket.entities.getFileHandle(entityId, { create: false });
    } catch (ex) {
      if (ex instanceof FsPathNotFoundError) {
        // 実際に保存されているファイルがないので、メタデータをを削除します。
        try {
          await this.#metadata.delete({ entityId });
        } catch (ex) {
          if (ex instanceof ObjectNotFoundError) {
            // あとで `ObjectNotFoundError` を投げるので、ここでは無視します。
          } else {
            this.#logger.error(
              `Omnio.getObject: Failed to delete metadata: '${this.bucketName}:${objectPath}'`,
              ex,
            );
          }
        }

        throw new ObjectNotFoundError(this.bucketName, objectPath, { cause: ex });
      }

      throw ex;
    }

    return new ObjectFile({
      file: await fileHandle.getFile(),
      size,
      type: mimeType,
      checksum,
      metadata: this.#metadata,
      bucketName: this.bucketName,
      objectPath,
      objectTags: other.objectTags,
      description: other.description,
      lastModified: lastModifiedAt,
      userMetadata: other.userMetadata,
    });
  }

  /**
   * 存在するオブジェクトのメタデータを取得します。
   *
   * @template TQuery 存在するオブジェクトのメタデータを取得するためのクエリーの型です。
   * @param query 存在するオブジェクトのメタデータを取得するためのクエリーです。
   * @returns オブジェクトのメタデータを取得した結果です。
   */
  @mutex.readonly
  public async getObjectMetadata<const Tquery extends GetObjectMetadataQuery>(
    query: Tquery,
  ): Promise<GetObjectMetadataResult<$Get<Tquery, "select">>> {
    const {
      where,
      select,
    } = v.parse(GetObjectMetadataQuerySchema, query);
    const out = await this.#metadata.read({
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
   * @param objectPathOrDirectoryPath オブジェクトパスまたはディレクトリーを表すパスセグメントの配列です。
   * @returns `true` ならオブジェクトまたはディレクトリーが存在します。
   */
  public existsPath(
    objectPathOrDirectoryPath: ObjectPathLike | readonly string[],
  ): Promise<boolean>;

  @mutex.readonly
  public async existsPath(path: ObjectPathLike | readonly string[]): Promise<boolean> {
    if (Array.isArray(path)) {
      return await this.existsDirectory(path);
    } else {
      return await this.existsObject(path);
    }
  }

  /**
   * オブジェクトが存在するか確認します。
   *
   * @param path オブジェクトパスです。
   * @returns `true` ならオブジェクトが存在します。
   */
  @mutex.readonly
  public async existsObject(path: ObjectPathLike): Promise<boolean> {
    const objectPath = v.parse(schemas.ObjectPathLike, path);
    const { exists } = await this.#metadata.exists({ objectPath });

    return exists;
  }

  /**
   * ディレクトリーが存在するか確認します。
   *
   * @param directoryPath ディレクトリーを表すパスセグメントの配列です。
   * @returns `true` ならディレクトリーが存在します。
   */
  @mutex.readonly
  public async existsDirectory(directoryPath: readonly string[]): Promise<boolean> {
    const dirPath = directoryPath; // TODO: 入力値検証
    const { exists } = await this.#metadata.exists({ dirPath });

    return exists;
  }

  /**
   * オブジェクトやディレクトリーのステータス情報を取得します。
   *
   * @returns オブジェクトやディレクトリーのステータス情報です。
   */
  @mutex.readonly
  public async statPath(path: ObjectPathLike): Promise<Stats> {
    const objectPath = v.parse(schemas.ObjectPathLike, path);
    const stats = await this.#metadata.stat({ objectPath });

    return stats;
  }

  /**
   * ディレクトリーまたはオブジェクトをリストアップします。
   *
   * @param directoryPath ディレクトリーを表すパスセグメントの配列です。
   * @param options ディレクトリーまたはオブジェクトをリストアップするためのオプションです。
   * @returns ディレクトリーまたはオブジェクトをリストアップした結果です。
   */
  @mutex.readonly
  public async list(
    directoryPath: readonly string[],
    options: ListOptions = {},
  ): Promise<List<ListItem>> {
    const dirPath = directoryPath; // TODO: 入力値検証
    const {
      skip,
      take,
      orderBy = {},
    } = v.parse(ListOptionsSchema, options);
    const list = await this.#metadata.list({
      skip,
      take,
      dirPath,
      orderBy: {
        name: orderBy.name,
        preferObject: orderBy.preferObject,
      },
    });

    return list;
  }

  /**
   * オブジェクトの説明文を対象に全文検索します。
   *
   * @param directoryPath ディレクトリーを表すパスセグメントの配列です。
   * @param query 検索クエリーです。
   * @param options オブジェクトの説明文を対象に全文検索するためのオプションです。
   * @returns オブジェクトの説明文を対象に全文検索した結果です。
   */
  @mutex.readonly
  public async searchObjects(
    directoryPath: readonly string[],
    query: string,
    options: SearchObjectsOptions = {},
  ): Promise<List<SearchResult>> {
    const dirPath = directoryPath; // TODO: 入力値検証
    const {
      skip,
      take,
      recursive,
      scoreThreshold,
    } = v.parse(SearchObjectsOptionsSchema, options);
    const list = await this.#metadata.search({
      skip,
      take,
      query: v.parse(v.string(), query),
      dirPath,
      recursive,
      scoreThreshold,
    });

    return list;
  }

  /**
   * オブジェクトをコピーします。
   *
   * @param sourcePath コピー元のオブジェクトパスです。
   * @param destinationPath コピー先のオブジェクトパスです。
   */
  @mutex
  public async copyObject(
    sourcePath: ObjectPathLike,
    destinationPath: ObjectPathLike,
  ): Promise<void> {
    if (!this.#bucket) {
      throw new Error("Omnio closed");
    }

    const srcObjectPath = v.parse(schemas.ObjectPathLike, sourcePath);
    const dstObjectPath = v.parse(schemas.ObjectPathLike, destinationPath);

    const { entityId: srcEntityId } = await this.#metadata.read({
      select: {
        entityId: true,
      },
      where: {
        objectPath: srcObjectPath,
      },
    });
    let srcHandle: FileHandle;
    try {
      srcHandle = await this.#bucket.entities.getFileHandle(srcEntityId, { create: false });
    } catch (ex) {
      if (ex instanceof FsPathNotFoundError) {
        // エンティティーがないので、メタデータから削除します。
        try {
          await this.#metadata.delete({ entityId: srcEntityId });
        } catch (ex) {
          this.#logger.error(
            `Omnio.copyObject: Failed to delete metadata: '${this.bucketName}:${srcObjectPath}'`,
            ex,
          );
        }

        throw new ObjectNotFoundError(this.bucketName, srcObjectPath, { cause: ex });
      }

      throw ex;
    }

    const srcFile = await srcHandle.getFile();
    const reader = srcFile.stream();

    const dstEntityId = getEntityId();
    const dstHandle = await this.#bucket.entities.getFileHandle(dstEntityId, { create: true });
    const writer = await dstHandle.createWritable();
    try {
      for await (const chunk of reader) {
        await writer.write(chunk);
      }
    } catch (ex) {
      try {
        await writer.abort();
      } catch (ex) {
        this.#logger.error("Omnio.copyObject: Failed to abort writer", ex);
      }

      throw ex;
    }

    await writer.close();

    try {
      await this.#metadata.copy({
        dstEntityId,
        dstObjectPath,
        srcObjectPath,
      });
    } catch (ex) {
      // メタデータのコピーに失敗したので、エンティティを削除します。
      try {
        await this.#bucket.entities.removeEntry(dstEntityId);
      } catch (ex) {
        this.#logger.error("Omnio.copyObject: Failed to remove entity: " + dstEntityId, ex);
      }

      throw ex;
    }
  }

  /**
   * オブジェクトの名前を変更します。
   *
   * @param sourcePath 変更元のオブジェクトパスです。
   * @param destinationPath 変更先のオブジェクトパスです。
   */
  @mutex
  public async renameObject(
    sourcePath: ObjectPathLike,
    destinationPath: ObjectPathLike,
  ): Promise<void> {
    if (!this.#bucket) {
      throw new Error("Omnio closed");
    }

    const srcObjectPath = v.parse(schemas.ObjectPathLike, sourcePath);
    const dstObjectPath = v.parse(schemas.ObjectPathLike, destinationPath);
    await this.#metadata.move({
      srcObjectPath,
      dstObjectPath,
    });
  }

  /**
   * オブジェクトのメタデータを更新します。
   *
   * @param path オブジェクトパスです。
   * @param options オブジェクトのメタデータを更新するためのオプションです。
   */
  @mutex
  public async updateObjectMetadata(
    path: ObjectPathLike,
    options: UpdateObjectMetadataOptions | undefined = {},
  ): Promise<void> {
    if (!this.#bucket) {
      throw new Error("Omnio closed");
    }

    const objectPath = v.parse(schemas.ObjectPathLike, path);
    const {
      mimeType,
      objectTags,
      description,
      userMetadata,
    } = v.parse(UpdateObjectMetadataOptionsSchema, options);
    await this.#metadata.update({
      mimeType,
      objectPath,
      objectTags,
      description,
      userMetadata,
    });
  }

  /**
   * オブジェクトのメタデータを削除します。
   *
   * @param path オブジェクトパスです。
   */
  @mutex
  public async deleteObject(path: ObjectPathLike): Promise<void> {
    if (!this.#bucket) {
      throw new Error("Omnio closed");
    }

    const objectPath = v.parse(schemas.ObjectPathLike, path);
    const { entityId } = await this.#metadata.trash({ objectPath });
    try {
      await this.#bucket.entities.removeEntry(entityId);
    } catch (ex) {
      if (ex instanceof FsPathNotFoundError) {
        // 何もしません。
      } else {
        throw ex;
      }
    }

    await this.#metadata.delete({ entityId });
  }

  /**
   * メタデータを管理するデータベースに対して SQL クエリーを実行し、結果を返します。
   *
   * @param sql 実行する SQL クエリーです。
   * @returns クエリーの実行結果をイテレーターで返します。
   */
  public dangerous_queryObjectMetadata(sql: string | Sql): List<Row> {
    if (!this.#bucket) {
      throw new Error("Omnio closed");
    }

    return this.#metadata.stream(v.parse(SqlSchema, sql));
  }
}
