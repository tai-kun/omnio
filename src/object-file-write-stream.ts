import * as v from "valibot";
import type BucketName from "./bucket-name.js";
import { Error } from "./errors.js";
import type { DirectoryHandle, WritableFileStream } from "./fs/fs.types.js";
import type { Hash } from "./md5.js";
import mutex from "./mutex.js";
import ObjectIdent from "./object-ident.js";
import type ObjectPath from "./object-path.js";
import * as schemas from "./schemas.js";
import toUint8Array, { type Uint8ArraySource } from "./to-uint8-array.js";
import type { Awaitable } from "./type-utils.js";

/**
 * `Omnio` のインターフェースです。
 */
interface Omnio {
  /**
   * `Omnio` が閉じているかかどうかを示します。
   */
  readonly closed: boolean;
}

/**
 * ログを記録する関数群のインターフェースです。
 */
interface Logger {
  /**
   * エラーメッセージを記録します。
   *
   * @param message エラーメッセージです。
   * @param reason エラーの原因です。
   */
  error(message: string, reason: unknown): void;
}

/**
 * メタデータを管理するオブジェクトのインターフェースです。
 * `ObjectFileWriteStream` クラスが依存するメタデータシステムの抽象化を提供します。
 */
interface Metadata {
  /**
   * オブジェクトのメタデータを作成します。
   *
   * @param inp オブジェクトのメタデータを作成するための入力パラメーターです。
   */
  create(
    inp: Readonly<{
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
    }>,
  ): Awaitable<void>;

  /**
   * オブジェクトのメタデータを排他的に作成します。
   *
   * @param inp オブジェクトのメタデータを排他的に作成するための入力パラメーターです。
   */
  createExclusive(
    inp: Readonly<{
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
    }>,
  ): Awaitable<void>;

  /**
   * 存在するオブジェクトのメタデータを取得します。
   *
   * @param inp オブジェクトのメタデータを取得するための入力パラメーターです。
   * @returns オブジェクトのメタデータを取得した結果です。
   */
  read(
    inp: Readonly<{
      /**
       * 結果に含めるカラムを選択します。
       */
      select: Readonly<{
        /**
         * 実際に保存されるオブジェクトの識別子です。
         */
        entityId: true;
      }>;

      /**
       * 対象を限定します。
       */
      where: Readonly<{
        /**
         * バケット内のオブジェクトパスです。
         */
        objectPath: ObjectPath;
      }>;
    }>,
  ): Awaitable<
    Readonly<{
      /**
       * 実際に保存されるオブジェクトの識別子です。
       */
      entityId: v.InferOutput<typeof schemas.EntityId>;
    }>
  >;

  /**
   * オブジェクトのメタデータを排他的に更新します。
   *
   * @param inp オブジェクトのメタデータを排他的に更新するための入力パラメーターです。
   */
  updateExclusive(
    inp: Readonly<{
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
    }>,
  ): Awaitable<void>;
}

/**
 * `ObjectFileWriteStream` を構築するための入力パラメーターです。
 */
type ObjectFileWriteStreamInput = Readonly<{
  /**
   * ストリームが属するバケット名です。
   */
  bucketName: BucketName;

  /**
   * バケット内のオブジェクトパスです。
   */
  objectPath: ObjectPath;

  /**
   * オブジェクトのデータ形式です。
   */
  type: v.InferOutput<typeof schemas.MimeType> | undefined;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  objectTags: v.InferOutput<typeof schemas.ObjectTags> | undefined;

  /**
   * オブジェクトの説明文です。
   */
  description: string | null | undefined;

  /**
   * オブジェクトのユーザー定義のメタデータです。
   */
  userMetadata: unknown | undefined;

  /**
   * カスタムのタイムスタンプです。
   */
  timestamp: v.InferOutput<typeof schemas.Timestamp> | undefined;

  /**
   * オブジェクトを開く際のモードです。
   */
  flag: v.InferOutput<typeof schemas.OpenMode>;

  /**
   * `Omnio` です。
   */
  omnio: Omnio;

  /**
   * ログを記録する関数群です。
   */
  logger: Logger;

  /**
   * メタデータを管理するオブジェクトです。
   */
  metadata: Metadata;

  /**
   * 書き込み先のディレクトリーハンドラーです。
   */
  directory: Pick<DirectoryHandle, "removeEntry">;

  /**
   * オブジェクトの識別子です。
   */
  entityIds: Readonly<{
    /**
     * 実際に保存されるオブジェクトの識別子です。
     */
    new: v.InferOutput<typeof schemas.EntityId>;

    /**
     * 実際に保存されているオブジェクトの識別子です。
     */
    old: v.InferOutput<typeof schemas.EntityId> | undefined;
  }>;

  /**
   * ハッシュ値を計算するためのストリームです。
   */
  hash: Hash;

  /**
   * 書き込み可能なファイルストリームです。
   */
  writer: WritableFileStream;

  /**
   * オブジェクトの初期サイズサイズ (バイト数) です。
   */
  offsetSize: v.InferOutput<typeof schemas.UnsignedInteger>;

  /**
   * 既存のメタデータに期待する値です。
   */
  expect:
    | Readonly<{
      /**
       * 既存のチェックサムに期待する値です。
       */
      checksum: v.InferOutput<typeof schemas.Checksum>;
    }>
    | undefined;
}>;

/**
 * `ObjectFileWriteStream` の JSON 表現です。
 */
export type ObjectFileWriteStreamJson = {
  /**
   * ストリームが属するバケット名です。
   */
  bucketName: BucketName;

  /**
   * バケット内のオブジェクトパスです。
   */
  objectPath: ObjectPath;

  /**
   * オブジェクトを開く際のモードです。
   */
  flag: v.InferOutput<typeof schemas.OpenMode>;

  /**
   * `true` ならストリームは閉じています。
   */
  closed: boolean;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  size: v.InferOutput<typeof schemas.UnsignedInteger>;

  /**
   * ストリームに書き込まれたバイト数です。
   */
  bytesWritten: v.InferOutput<typeof schemas.UnsignedInteger>;

  /**
   * オブジェクトのデータ形式です。
   */
  type?: v.InferOutput<typeof schemas.MimeType>;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  objectTags?: v.InferOutput<typeof schemas.ObjectTags>;

  /**
   * オブジェクトの説明文です。
   */
  description?: string | null;

  /**
   * オブジェクトのユーザー定義のメタデータです。
   */
  userMetadata?: unknown;

  /**
   * 中断の理由です。
   */
  abortReason?: unknown;

  /**
   * カスタムのタイムスタンプです。
   */
  timestamp?: number;
};

/**
 * オブジェクトを書き込むストリームです。
 */
export default class ObjectFileWriteStream implements AsyncDisposable {
  /**
   * `Omnio` です。
   */
  readonly #omnio: Omnio;

  /**
   * ログを記録する関数群です。
   */
  readonly #logger: Logger;

  /**
   * メタデータを管理するオブジェクトです。
   */
  readonly #metadata: Metadata;

  /**
   * 書き込み先のディレクトリーハンドラーです。
   */
  readonly #directory: Pick<DirectoryHandle, "removeEntry">;

  /**
   * オブジェクトの識別子です。
   */
  readonly #entityIds: Readonly<{
    /**
     * 実際に保存されるオブジェクトの識別子です。
     */
    new: v.InferOutput<typeof schemas.EntityId>;

    /**
     * 実際に保存されているオブジェクトの識別子です。
     */
    old: v.InferOutput<typeof schemas.EntityId> | undefined;
  }>;

  /**
   * ハッシュ値を計算するためのストリームです。
   */
  readonly #hash: Hash;

  /**
   * 書き込み可能なファイルストリームです。
   */
  readonly #writer: WritableFileStream;

  /**
   * オブジェクトの初期サイズサイズ (バイト数) です。
   */
  readonly #offsetSize: v.InferOutput<typeof schemas.UnsignedInteger>;

  /**
   * 既存のメタデータに期待する値です。
   */
  readonly #expect:
    | Readonly<{
      /**
       * 既存のチェックサムに期待する値です。
       */
      checksum: v.InferOutput<typeof schemas.Checksum>;
    }>
    | undefined;

  /**
   * 書き込み中の状態を管理するフラグです。
   */
  #closed: boolean;

  /**
   * 中断の理由です。
   */
  #abortReason?: unknown;

  /**
   * ストリームが属するバケット名です。
   */
  public readonly bucketName: BucketName;

  /**
   * バケット内のオブジェクトパスです。
   */
  public readonly objectPath: ObjectPath;

  /**
   * オブジェクトを開く際のモードです。
   *
   * - **`"w"`**: 書き込みモードで開きます。オブジェクトが存在しない場合は新規作成され、もし存在する場合は上書きします。
   * - **`"wx"`**: 書き込みモードで開きます。オブジェクトが存在する場合はエラーになります。
   * - **`"a"`**: 追加書き込みモードで開きます。オブジェクトが存在しない場合は新規作成されます。
   * - **`"ax"`**: 追加書き込みモードで開きます。オブジェクトが存在する場合はエラーになります
   */
  public readonly flag: v.InferOutput<typeof schemas.OpenMode>;

  /**
   * オブジェクトのデータ形式です。
   */
  public type: string | undefined;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  public objectTags: string[] | undefined;

  /**
   * オブジェクトの説明文です。
   */
  public description: string | null | undefined;

  /**
   * オブジェクトのユーザー定義のメタデータです。
   */
  public userMetadata: unknown | undefined;

  /**
   * カスタムのタイムスタンプです。
   */
  public timestamp: v.InferInput<typeof schemas.Timestamp> | undefined;

  /**
   * `ObjectFileWriteStream` を構築します。
   *
   * @param inp `ObjectFileWriteStream` を構築するための入力パラメーターです。
   */
  public constructor(inp: ObjectFileWriteStreamInput) {
    this.flag = inp.flag;
    this.type = inp.type;
    this.#hash = inp.hash;
    this.#omnio = inp.omnio;
    this.#closed = false;
    this.#expect = inp.expect;
    this.#logger = inp.logger;
    this.#writer = inp.writer;
    this.#metadata = inp.metadata;
    this.timestamp = inp.timestamp;
    this.#directory = inp.directory;
    this.#entityIds = inp.entityIds;
    this.bucketName = inp.bucketName;
    this.objectPath = inp.objectPath;
    this.objectTags = inp.objectTags?.slice();
    this.description = inp.description;
    this.#offsetSize = inp.offsetSize;
    this.userMetadata = inp.userMetadata;

    if (this.flag !== "a" && this.#expect !== undefined) {
      throw new Error(
        "Cannot rely on existing metadata being present except in non-exclusive append mode",
      );
    }
  }

  /**
   * `true` ならストリームは閉じています。
   */
  get closed(): boolean {
    return this.#closed;
  }

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  get size(): v.InferOutput<typeof schemas.UnsignedInteger> {
    // @ts-expect-error
    return this.#writer.bytesWritten - this.#offsetSize;
  }

  /**
   * ストリームに書き込まれたバイト数です。
   */
  get bytesWritten(): v.InferOutput<typeof schemas.UnsignedInteger> {
    return this.#writer.bytesWritten;
  }

  /**
   * ストリームにチャンクデータを書き込みます。
   *
   * @param chunk `Uint8Array` に変換できるチャンクデータです。
   */
  @mutex
  public async write(chunk: Uint8ArraySource): Promise<void> {
    if (this.#closed) {
      throw this.#abortReason;
    }

    if (this.#omnio.closed) {
      this.#closed = true;
      // Omnio が閉じていて諸データを記録できないので、オブジェクトを書き込まずに中断します。
      this.#abortReason = new Error("Failed to write to ObjectFileWriteStream: Omnio closed");
      try {
        await this.#writer.abort(this.#abortReason);
      } catch (ex) {
        this.#logger.error(
          "ObjectFileWriteStream.write: Failed to abort ObjectFileWriteStream",
          ex,
        );
      }

      throw this.#abortReason;
    }

    const data = toUint8Array(chunk);
    if (data.byteLength === 0) {
      return;
    }

    await this.#writer.write(data);
    try {
      this.#hash.update(data);
    } catch (ex) {
      try {
        this.#closed = true;
        this.#abortReason = ex;
        await this.#writer.abort(this.#abortReason);
      } catch (ex) {
        this.#logger.error(
          "ObjectFileWriteStream.write: Failed to abort ObjectFileWriteStream",
          ex,
        );
      }

      throw ex;
    }
  }

  /**
   * ストリームを終了します。これにより、オブジェクトのデータとメタデータが永続化されます。
   */
  @mutex
  public async close(): Promise<void> {
    if (this.#closed) {
      throw this.#abortReason;
    }

    this.#closed = true;

    if (this.#omnio.closed) {
      // Omnio が閉じていて諸データを記録できないので、オブジェクトを書き込まずに中断します。
      this.#abortReason = new Error("Failed to close to ObjectFileWriteStream: Omnio closed");
      try {
        await this.#writer.abort(this.#abortReason);
      } catch (ex) {
        this.#logger.error(
          "ObjectFileWriteStream.close: Failed to abort ObjectFileWriteStream",
          ex,
        );
      }

      throw this.#abortReason;
    }

    // メタデータを作成または更新します。
    const checksum = this.#hash.digest();
    const mimeType = v.parse(v.optional(schemas.MimeType), this.type);
    const timestamp = v.parse(v.optional(schemas.Timestamp), this.timestamp);
    const objectTags = v.parse(v.optional(schemas.ObjectTags), this.objectTags);
    switch (this.flag) {
      case "w":
        // エンティティーを保存します。
        await this.#writer.close();

        try {
          // メタデータを作成または更新します。
          await this.#metadata.create({
            checksum,
            entityId: this.#entityIds.new,
            mimeType,
            timestamp,
            objectPath: this.objectPath,
            objectSize: this.size,
            objectTags,
            description: this.description,
            userMetadata: this.userMetadata,
          });
        } catch (ex) {
          // メタデータの作成または更新に失敗したので、新しいエンティティーを削除します。
          try {
            await this.#directory.removeEntry(this.#entityIds.new, { recursive: false });
          } catch (ex) {
            this.#logger.error(
              "ObjectFileWriteStream.close: Failed to remove new entity: " + this.#entityIds.new,
              ex,
            );
          }

          throw ex;
        }

        // 排他モードではないので、削除すべき古いエンティティーが存在することがあります。
        if (this.#entityIds.old !== undefined && this.#entityIds.old !== this.#entityIds.new) {
          // 新しいエンティティーの保存とメタデータの作成に成功したので、古いエンティティーを削除します。
          try {
            await this.#directory.removeEntry(this.#entityIds.old, { recursive: false });
          } catch (ex) {
            this.#logger.error(
              "ObjectFileWriteStream.close: Failed to remove old entity: " + this.#entityIds.old,
              ex,
            );
          }
        }

        break;

      case "a":
        try {
          // エンティティーを保存します。
          await this.#writer.close();

          if (this.#expect) {
            // 既存のメタデータを更新します。
            await this.#metadata.updateExclusive({
              expect: this.#expect,
              checksum,
              entityId: this.#entityIds.new,
              mimeType,
              timestamp,
              objectPath: this.objectPath,
              objectSize: this.size,
              objectTags,
              description: this.description,
              userMetadata: this.userMetadata,
            });
          } else {
            // メタデータを作成または更新します。
            await this.#metadata.create({
              checksum,
              entityId: this.#entityIds.new,
              mimeType,
              timestamp,
              objectPath: this.objectPath,
              objectSize: this.size,
              objectTags,
              description: this.description,
              userMetadata: this.userMetadata,
            });
          }
        } catch (ex) {
          // メタデータの作成に失敗したので、エンティティーに反映しません。
          try {
            await this.#writer.abort();
          } catch (ex) {
            this.#logger.error("ObjectFileWriteStream.close: Failed to abort writer", ex);
          }

          throw ex;
        }

        // 排他モードではないので、削除すべき古いエンティティーが存在することがあります。
        if (this.#entityIds.old !== undefined && this.#entityIds.old !== this.#entityIds.new) {
          // 新しいエンティティーの保存とメタデータの作成に成功したので、古いエンティティーを削除します。
          try {
            await this.#directory.removeEntry(this.#entityIds.old, { recursive: false });
          } catch (ex) {
            this.#logger.error(
              "ObjectFileWriteStream.close: Failed to remove old entity: " + this.#entityIds.old,
              ex,
            );
          }
        }

        break;

      case "ax":
      case "wx":
        // エンティティーを保存します。
        await this.#writer.close();

        try {
          // メタデータを作成します。
          await this.#metadata.createExclusive({
            checksum,
            entityId: this.#entityIds.new,
            mimeType,
            timestamp,
            objectPath: this.objectPath,
            objectSize: this.size,
            objectTags,
            description: this.description,
            userMetadata: this.userMetadata,
          });
        } catch (ex) {
          // メタデータの作成または更新に失敗したので、新しいエンティティーを削除します。
          try {
            await this.#directory.removeEntry(this.#entityIds.new, { recursive: false });
          } catch (ex) {
            this.#logger.error(
              "ObjectFileWriteStream.close: Failed to remove new entity: " + this.#entityIds.new,
              ex,
            );
          }

          throw ex;
        }

        break;

      default:
        throw new Error(`Unknown flag: ${String(this.flag satisfies never)}`);
    }
  }

  /**
   * ストリームを中断します。
   *
   * @param reason 中断の理由です。
   */
  @mutex
  public async abort(reason?: unknown): Promise<void> {
    if (this.#closed) {
      throw this.#abortReason;
    }

    this.#closed = true;
    this.#abortReason = reason;
    await this.#writer.abort(this.#abortReason);
  }

  /**
   * ストリームの内容を JSON 形式に変換します。これは主にテストやデバッグ目的で使用されます。
   *
   * @returns JSON 形式の値です。
   */
  public toJSON(): ObjectFileWriteStreamJson {
    const json: ObjectFileWriteStreamJson = {
      flag: this.flag,
      size: this.size,
      closed: this.closed,
      bucketName: this.bucketName,
      objectPath: this.objectPath,
      bytesWritten: this.bytesWritten,
    };
    if (this.type !== undefined) {
      json.type = v.parse(schemas.MimeType, this.type);
    }
    if (this.objectTags !== undefined) {
      json.objectTags = v.parse(schemas.ObjectTags, this.objectTags);
    }
    if (this.description !== undefined) {
      json.description = this.description;
    }
    if (this.userMetadata !== undefined) {
      json.userMetadata = this.userMetadata;
    }
    if (this.timestamp !== undefined) {
      json.timestamp = new Date(this.timestamp).getTime();
    }
    if ("abortReason" in this) {
      json.abortReason = this.abortReason;
    }

    return json;
  }

  /**
   * ストリームの内容を `ObjectIdent` に変換します。
   *
   * @returns `ObjectIdent` の新しいインスタンスです。
   */
  public toObjectIdent(): ObjectIdent {
    return new ObjectIdent({
      bucketName: this.bucketName,
      objectPath: this.objectPath,
    });
  }

  /**
   * ストリームを終了します。
   */
  public async [Symbol.asyncDispose](): Promise<void> {
    if (this.#closed) {
      return;
    }

    await this.close();
  }
}
