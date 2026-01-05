import { asyncmux, type AsyncmuxLock } from "asyncmux";
import { OmnioClosedError } from "../shared/errors.js";
import type { IWritableFileStream } from "../shared/file-system.js";
import { type ILogger, LogLevel } from "../shared/logger.js";
import type {
  BucketName,
  Checksum,
  EntityId,
  MimeType,
  MimeTypeLike,
  NumParts,
  ObjectPath,
  ObjectSize,
  ObjectTags,
  OpenMode,
  PartSize,
  Timestamp,
  TimestampLike,
  WritableObjectTagsLike,
} from "../shared/schemas.js";
import {
  MimeTypeSchema,
  NumPartsSchema,
  ObjectSizeSchema,
  ObjectTagsSchema,
  TimestampSchema,
} from "../shared/schemas.js";
import type { IEntityHandle, IStorage } from "../shared/storage.js";
import toUint8Array, { type Uint8ArraySource } from "../shared/to-uint8-array.js";
import unreachable from "../shared/unreachable.js";
import * as v from "../shared/valibot.js";
import type { IHash } from "./_hash.js";
import type Metadata from "./metadata.js";

/**
 * `Omnio` のインターフェースです。
 */
interface IOmnio {
  /**
   * `Omnio` が閉じているかかどうかを示します。
   */
  readonly closed: boolean;
}

/**
 * `ObjectFileWriteStream` を構築するための入力パラメーターです。
 */
type ObjectFileWriteStreamInput = Readonly<{
  /**
   * オブジェクトのデータ形式です。
   */
  type: MimeType | undefined;

  /**
   * バケット名です。
   */
  bucketName: BucketName;

  /**
   * バケット内のオブジェクトパスです。
   */
  objectPath: ObjectPath;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  objectTags: ObjectTags | undefined;

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
  timestamp: Timestamp | undefined;

  /**
   * オブジェクトを開く際のモードです。
   */
  flag: OpenMode;

  /**
   * 実際に保存されるオブジェクトの識別子です。
   */
  newEntityId: EntityId;

  /**
   * 実際に保存されているオブジェクトの識別子です。
   */
  oldEntityId: EntityId | undefined;

  /**
   * 各パートのサイズ (バイト数) です。
   */
  partSize: PartSize;

  /**
   * `Omnio` オブジェクトです。
   */
  omnio: IOmnio;

  /**
   * ログを記録する関数群です。
   */
  logger: ILogger;

  /**
   * メタデータを管理するオブジェクトです。
   */
  metadata: Metadata;

  /**
   * 書き込みロックを獲得した結果です。
   */
  lock: AsyncmuxLock;

  /**
   * オブジェクトを永続化するためのファイルシステムです。
   */
  storage: IStorage;

  /**
   * ハッシュ値を計算するためのストリームです。
   */
  hash: IHash;

  // 以下、上書きに必要になることがあるオプションです。

  /**
   * 既存のメタデータに期待する値です。
   */
  expect:
    | Readonly<{
      /**
       * 既存のチェックサムに期待する値です。
       */
      checksum: Checksum;
    }>
    | undefined;

  /**
   * 現在のオブジェクトのサイズ (バイト数) です。
   */
  currentSize: ObjectSize | undefined;

  /**
   * 現在のオブジェクトのパートの総数です。
   */
  currentNumParts: NumParts | undefined;
}>;

/**
 * オブジェクトを書き込むストリームです。
 */
export default class ObjectFileWriteStream implements AsyncDisposable {
  /**
   * `Omnio` です。
   */
  readonly #omnio: IOmnio;

  /**
   * ログを記録する関数群です。
   */
  readonly #logger: ILogger;

  /**
   * メタデータを管理するオブジェクトです。
   */
  readonly #metadata: Metadata;

  /**
   * オブジェクトを永続化するためのファイルシステムです。
   */
  readonly #storage: IStorage;

  /**
   * ハッシュ値を計算するためのストリームです。
   */
  readonly #hash: IHash;

  /**
   * オブジェクトの識別子です。
   */
  readonly #entityIds: Readonly<{
    /**
     * 実際に保存されるオブジェクトの識別子です。
     */
    new: EntityId;

    /**
     * 実際に保存されているオブジェクトの識別子です。
     */
    old: EntityId | undefined;
  }>;

  /**
   * オブジェクトの初期サイズ (バイト数) です。
   */
  readonly #offsetSize: ObjectSize;

  /**
   * 既存のメタデータに期待する値です。
   */
  readonly #expect:
    | Readonly<{
      /**
       * 既存のチェックサムに期待する値です。
       */
      checksum: Checksum;
    }>
    | undefined;

  /**
   * オブジェクトのパートの総数です。
   */
  readonly #offsetNumParts: NumParts | undefined;

  /**
   * 各パートのサイズ (バイト数) です。
   */
  readonly #partSize: PartSize;

  /**
   * 書き込みロックを獲得した結果です。
   */
  readonly #lock: AsyncmuxLock;

  /**
   * 書き込み中の状態を管理するフラグです。
   */
  #closed: boolean;

  /**
   * 中断の理由です。
   */
  #abortReason: unknown | undefined;

  /**
   * オブジェクトのエンティティーのハンドルです。
   */
  #entityHandle: IEntityHandle | null;

  /**
   * パートデータを書き込むファイルストリームです。
   */
  #writer: (IWritableFileStream & { bytesWritten: number }) | null;

  /**
   * オブジェクトの初期サイズ (バイト数) です。
   */
  #size: ObjectSize;

  /**
   * オブジェクトのパートの総数です。
   */
  #numParts: number;

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
  public readonly flag: OpenMode;

  /**
   * オブジェクトのデータ形式です。
   */
  public type: MimeTypeLike | undefined;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  public objectTags: WritableObjectTagsLike | undefined;

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
  public timestamp: TimestampLike | undefined;

  /**
   * `ObjectFileWriteStream` クラスの新しいインスタンスを初期化します。
   *
   * @param inp `ObjectFileWriteStream` を構築するための入力パラメーターです。
   */
  public constructor(inp: ObjectFileWriteStreamInput) {
    this.#omnio = inp.omnio;
    this.#logger = inp.logger;
    this.#metadata = inp.metadata;
    this.#storage = inp.storage;
    this.#hash = inp.hash;
    this.#entityIds = {
      new: inp.newEntityId,
      old: inp.oldEntityId,
    };
    this.#offsetSize = inp.currentSize ?? (0 as ObjectSize);
    this.#expect = inp.expect;
    this.#offsetNumParts = inp.currentNumParts;
    this.#partSize = inp.partSize;
    this.#lock = inp.lock;
    this.#closed = false;
    this.#abortReason = undefined;
    this.#entityHandle = null;
    this.#writer = null;
    this.#size = this.#offsetSize;
    this.#numParts = inp.currentNumParts ?? 0;
    this.bucketName = inp.bucketName;
    this.objectPath = inp.objectPath;
    this.flag = inp.flag;
    this.type = inp.type;
    this.objectTags = inp.objectTags?.slice();
    this.description = inp.description;
    this.userMetadata = inp.userMetadata;
    this.timestamp = inp.timestamp;

    if (
      this.flag !== "a" && (
        this.#size > 0
        || this.#expect !== undefined
        || this.#offsetNumParts !== undefined
      )
    ) {
      // このクラスをインスタンスを作成する `Omnio` が正しく実装されている限り、エラーが投げられることはありません。
      // したがって、予期しないエラーとして、通常のエラークラスを使ったエラーオブジェクトを投げます。
      throw new Error(`In "${this.flag}" mode, cannot assume that existing object exists`);
    }
  }

  readonly #NumPartsSchema = NumPartsSchema();
  readonly #MimeTypeSchema = v.optional(MimeTypeSchema());
  readonly #TimestampSchema = v.optional(TimestampSchema());
  readonly #ObjectTagsSchema = v.optional(ObjectTagsSchema());
  readonly #ObjectSizeSchema = ObjectSizeSchema();

  /**
   * `true` ならストリームは閉じています。
   */
  get closed(): boolean {
    return this.#closed;
  }

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  get size(): ObjectSize {
    return this.#size;
  }

  /**
   * ストリームに書き込まれたバイト数です。
   */
  get bytesWritten(): ObjectSize {
    return (this.#size - this.#offsetSize) as ObjectSize;
  }

  /**
   * ストリームにチャンクデータを書き込みます。
   *
   * @param chunk `Uint8Array` に変換できるチャンクデータです。
   */
  @asyncmux
  public async write(chunk: Uint8ArraySource): Promise<void> {
    if (this.#closed) {
      throw this.#abortReason;
    }

    if (this.#omnio.closed) {
      this.#closed = true;
      // Omnio が閉じていて諸データを記録できないので、オブジェクトを書き込まずに中断します。
      this.#abortReason = new OmnioClosedError();
      try {
        await this.#writer?.abort(this.#abortReason);
      } catch (ex) {
        this.#logger.log({
          level: LogLevel.ERROR,
          reason: ex,
          message: "ObjectFileWriteStream.write: Failed to abort ObjectFileWriteStream",
        });
      }

      throw this.#abortReason;
    }

    let chunkData: Uint8Array<ArrayBuffer> | null = toUint8Array(chunk);
    if (chunkData.length === 0) {
      return;
    }

    if (this.#entityHandle === null) {
      this.#entityHandle ||= await this.#storage.getDirectoryHandle(this.#entityIds.new, {
        create: true,
      });
    }

    while (chunkData !== null) {
      if (this.#writer === null) {
        if (this.#numParts === this.#offsetNumParts && (this.#size % this.#partSize) > 0) {
          const partName = this.#numParts.toString(10);
          const partHandle = await this.#entityHandle.getFileHandle(partName, { create: false });
          const writer = await partHandle.createWritable({ keepExistingData: true });
          this.#writer = {
            write: (...args) => writer.write(...args),
            abort: (...args) => (this.#writer = null, writer.abort(...args)),
            close: (...args) => (this.#writer = null, writer.close(...args)),
            bytesWritten: this.#size % this.#partSize,
          };
        } else {
          const partName = (this.#numParts + 1).toString(10);
          const partHandle = await this.#entityHandle.getFileHandle(partName, { create: true });
          const writer = await partHandle.createWritable({ keepExistingData: false });
          this.#writer = {
            write: (...args) => writer.write(...args),
            abort: (...args) => (this.#writer = null, writer.abort(...args)),
            close: (...args) => (this.#writer = null, writer.close(...args)),
            bytesWritten: 0,
          };
          this.#numParts += 1;
        }
      }

      let partData: Uint8Array<ArrayBuffer>;
      if ((this.#writer.bytesWritten + chunkData.length) > this.#partSize) {
        // |------|         this.#writer.bytesWritten
        //        |-------| chunkData.length
        // |--------------| this.#writer.bytesWritten + chunkData.length
        // |----------|     this.#partSize
        //        |---|     this.#partSize - this.#writer.bytesWritten
        const p = this.#partSize - this.#writer.bytesWritten;
        partData = chunkData.slice(0, p);
        chunkData = chunkData.slice(p);
      } else {
        partData = chunkData;
        chunkData = null;
      }

      this.#size = v.parse(this.#ObjectSizeSchema, this.#size + partData.length);
      await this.#writer.write(partData);
      this.#writer.bytesWritten += partData.length;
      try {
        this.#hash.update(partData);
      } catch (ex) {
        try {
          this.#closed = true;
          this.#abortReason = ex;
          await this.#writer.abort(this.#abortReason);
        } catch (ex) {
          this.#logger.log({
            level: LogLevel.ERROR,
            reason: ex,
            message: "ObjectFileWriteStream.write: Failed to abort ObjectFileWriteStream",
          });
        }

        throw ex;
      }

      if (this.#writer.bytesWritten >= this.#partSize) {
        await this.#writer.close();
      }
    }
  }

  /**
   * ストリームを終了します。これにより、オブジェクトのデータとメタデータが永続化されます。
   */
  @asyncmux
  public async close(): Promise<void> {
    if (this.#closed) {
      throw this.#abortReason;
    }

    if (this.#omnio.closed) {
      try {
        // Omnio が閉じていて諸データを記録できないので、オブジェクトを書き込まずに中断します。
        await this.abort(new OmnioClosedError());
      } catch (ex) {
        this.#logger.log({
          level: LogLevel.ERROR,
          reason: ex,
          message: "ObjectFileWriteStream.close: Failed to abort ObjectFileWriteStream",
        });
      }

      throw this.#abortReason;
    }

    this.#closed = true;
    try {
      // メタデータを作成または更新します。
      const checksum = this.#hash.digest();
      const numParts = v.parse(this.#NumPartsSchema, this.#numParts);
      const mimeType = v.parse(this.#MimeTypeSchema, this.type);
      const timestamp = v.parse(this.#TimestampSchema, this.timestamp);
      const objectTags = v.parse(this.#ObjectTagsSchema, this.objectTags);
      switch (this.flag) {
        case "w":
          // 最後のパートデータを保存します。
          await this.#writer?.close();

          try {
            // メタデータを作成または更新します。
            await this.#metadata.create({
              checksum,
              entityId: this.#entityIds.new,
              mimeType,
              numParts,
              partSize: this.#partSize,
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
              await this.#storage.removeEntry(this.#entityIds.new, { recursive: true });
            } catch (ex) {
              this.#logger.log({
                level: LogLevel.ERROR,
                reason: ex,
                message: "ObjectFileWriteStream.close: Failed to remove new entity: "
                  + this.#entityIds.new,
              });
            }

            throw ex;
          }

          // 排他モードではないので、削除すべき古いエンティティーが存在することがあります。
          if (this.#entityIds.old !== undefined && this.#entityIds.old !== this.#entityIds.new) {
            // 新しいエンティティーの保存とメタデータの作成に成功したので、古いエンティティーを削除します。
            try {
              await this.#storage.removeEntry(this.#entityIds.old, { recursive: true });
            } catch (ex) {
              this.#logger.log({
                level: LogLevel.ERROR,
                reason: ex,
                message: "ObjectFileWriteStream.close: Failed to remove old entity: "
                  + this.#entityIds.new,
              });
            }
          }

          break;

        case "a":
          try {
            // 最後のパートデータを保存します。
            await this.#writer?.close();

            if (this.#expect) {
              // 既存のメタデータを更新します。
              await this.#metadata.updateExclusive({
                expect: this.#expect,
                checksum,
                entityId: this.#entityIds.new,
                mimeType,
                numParts,
                partSize: this.#partSize,
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
                numParts,
                partSize: this.#partSize,
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
              await this.#writer?.abort();
            } catch (ex) {
              this.#logger.log({
                level: LogLevel.ERROR,
                reason: ex,
                message: "ObjectFileWriteStream.close: Failed to abort writer",
              });
            }

            throw ex;
          }

          // 排他モードではないので、削除すべき古いエンティティーが存在することがあります。
          if (this.#entityIds.old !== undefined && this.#entityIds.old !== this.#entityIds.new) {
            // 新しいエンティティーの保存とメタデータの作成に成功したので、古いエンティティーを削除します。
            try {
              await this.#storage.removeEntry(this.#entityIds.old, { recursive: true });
            } catch (ex) {
              this.#logger.log({
                level: LogLevel.ERROR,
                reason: ex,
                message: "ObjectFileWriteStream.close: Failed to remove old entity: "
                  + this.#entityIds.old,
              });
            }
          }

          break;

        case "ax":
        case "wx":
          // 最後のパートデータを保存します。
          await this.#writer?.close();

          try {
            // メタデータを作成します。
            await this.#metadata.createExclusive({
              checksum,
              entityId: this.#entityIds.new,
              mimeType,
              numParts,
              partSize: this.#partSize,
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
              await this.#storage.removeEntry(this.#entityIds.new, { recursive: true });
            } catch (ex) {
              this.#logger.log({
                level: LogLevel.ERROR,
                reason: ex,
                message: "ObjectFileWriteStream.close: Failed to remove new entity: "
                  + this.#entityIds.new,
              });
            }

            throw ex;
          }

          break;

        default:
          unreachable(this.flag);
      }
    } finally {
      this.#lock.unlock();
    }
  }

  /**
   * ストリームを中断します。
   *
   * @param reason 中断の理由です。
   */
  @asyncmux
  public async abort(reason?: unknown): Promise<void> {
    if (this.#closed) {
      throw this.#abortReason;
    }

    this.#closed = true;
    this.#abortReason = reason;

    // 必ず最初にストリームを中断します。一時ファイルもこのタイミングで削除されるはずです。
    // これより前に上位のディレクトリーを削除しようとすると、ファイルシステムによっては必ずエラーが投げられてしまいます。
    const abortEx: { value?: unknown } = {};
    try {
      await this.#writer?.abort(this.#abortReason);
    } catch (ex) {
      abortEx.value = ex;
    }

    // 一時ファイルが削除されたあと、メタデータが存在していなければ、エンティティーディレクトリーを削除します。
    try {
      const { exists } = await this.#metadata.exists({ objectPath: this.objectPath });
      if (!exists) {
        await this.#storage.removeEntry(this.#entityIds.new, { recursive: true });
      }
    } catch (ex) {
      this.#logger.log({
        level: LogLevel.ERROR,
        reason: ex,
        message: "ObjectFileWriteStream.abort: Failed to purge new entity: "
          + this.#entityIds.new,
      });
    }

    this.#lock.unlock();
    if ("value" in abortEx) {
      throw abortEx.value;
    }
  }

  public async [Symbol.asyncDispose](): Promise<void> {
    this.#lock[Symbol.dispose]();
    if (!this.#closed) {
      await this.close();
    }
  }
}
