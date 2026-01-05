import type { AsyncmuxLock } from "asyncmux";
import { ChecksumMismatchError, OmnioClosedError } from "../shared/errors.js";
import type {
  BucketName,
  Checksum,
  MimeType,
  NumParts,
  ObjectId,
  ObjectPath,
  ObjectSize,
  ObjectTags,
  Timestamp,
} from "../shared/schemas.js";
import type { IEntityHandle } from "../shared/storage.js";
import type { IHash } from "./_hash.js";
import md5 from "./_md5.js";

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
 * `ObjectFileReadStream` を構築するための入力パラメーターです。
 */
type ObjectFileReadStreamInput = Readonly<{
  /**
   * オブジェクトの識別子です。
   */
  objectId: ObjectId;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  size: ObjectSize;

  /**
   * オブジェクトのデータ形式です。
   */
  type: MimeType;

  /**
   * 最終更新日 (ミリ秒) です。
   */
  lastModified: Timestamp;

  /**
   * バケット名です。
   */
  bucketName: BucketName;

  /**
   * バケット内のオブジェクトパスです。
   */
  objectPath: ObjectPath;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: Checksum;

  /**
   * オブジェクトのパートの総数です。
   */
  numParts: NumParts;

  /**
   * `Omnio` オブジェクトです。
   */
  omnio: IOmnio;

  /**
   * 読み取りロックを獲得した結果です。
   */
  lock: AsyncmuxLock;

  /**
   * オブジェクトのエンティティーのハンドルです。
   */
  entityHandle: IEntityHandle;

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
}>;

/**
 * オブジェクトを読み込むストリームです。
 */
export default class ObjectFileReadStream
  implements AsyncIterableIterator<Uint8Array<ArrayBuffer>>, Disposable
{
  /**
   * `Omnio` オブジェクトです。
   */
  readonly #omnio: IOmnio;

  /**
   * 読み取りロックを獲得した結果です。
   */
  readonly #lock: AsyncmuxLock;

  /**
   * オブジェクトのエンティティーのハンドルです。
   */
  readonly #entityHandle: IEntityHandle;

  /**
   * 現在のパートです。
   */
  #partNumber: number;

  /**
   * 最後に整合性を確認するためのハッシュオブジェクトです。
   */
  #hash: IHash | null;

  /**
   * オブジェクトの識別子です。
   */
  public readonly objectId: ObjectId;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  public readonly size: ObjectSize;

  /**
   * オブジェクトのデータ形式です。
   */
  public readonly type: MimeType;

  /**
   * オブジェクトの最終更新日 (ミリ秒) です。
   */
  public readonly lastModified: Timestamp;

  /**
   * オブジェクトが存在するバケットの名前です。
   */
  public readonly bucketName: BucketName;

  /**
   * バケット内のオブジェクトパスです。
   */
  public readonly objectPath: ObjectPath;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  public readonly checksum: Checksum;

  /**
   * オブジェクトのパートの総数です。
   */
  public readonly numParts: NumParts;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  public readonly objectTags: ObjectTags | undefined;

  /**
   * オブジェクトの説明文です。
   */
  public readonly description: string | null | undefined;

  /**
   * ユーザー定義のメタデータです。
   */
  public readonly userMetadata: unknown | undefined;

  /**
   * `ObjectFileReadStream` の新しいインスタンスを構築します。
   *
   * @param inp `ObjectFileReadStream` を構築するための入力パラメーターです。
   */
  public constructor(inp: ObjectFileReadStreamInput) {
    this.#omnio = inp.omnio;
    this.#lock = inp.lock;
    this.#entityHandle = inp.entityHandle;
    this.#partNumber = 1;
    this.#hash = null;
    this.objectId = inp.objectId;
    this.size = inp.size;
    this.type = inp.type;
    this.lastModified = inp.lastModified;
    this.bucketName = inp.bucketName;
    this.objectPath = inp.objectPath;
    this.checksum = inp.checksum;
    this.numParts = inp.numParts;
    this.objectTags = inp.objectTags;
    this.description = inp.description;
    this.userMetadata = inp.userMetadata;
  }

  /**
   * パートデータを取得します。
   *
   * @returns パートデータです。
   */
  public async next(): Promise<
    | { done: true; value: any }
    | { done: false; value: Uint8Array<ArrayBuffer> }
  > {
    if (this.#partNumber > this.numParts) {
      this.#partNumber = 0;
      this.#lock.unlock();
      // 整合性の検証を行います。
      const digest = this.#hash!.digest();
      if (digest.value !== this.checksum) {
        throw new ChecksumMismatchError(this.bucketName, this.objectPath, this.checksum, {
          actual: digest.value,
        });
      }
    }

    if (this.#partNumber === 0) {
      return {
        done: true,
        value: undefined,
      };
    }

    try {
      if (this.#omnio.closed) {
        throw new OmnioClosedError();
      }

      if (this.#partNumber === 1) {
        this.#hash = await md5.create();
      }

      const partName = this.#partNumber.toString(10);
      const partHandle = await this.#entityHandle.getFileHandle(partName, { create: false });
      const partFile = await partHandle.getFile();
      const partBuff = await partFile.arrayBuffer();
      const partData = new Uint8Array(partBuff);
      this.#hash!.update(partData);
      this.#partNumber += 1;

      return {
        done: false,
        value: partData,
      };
    } catch (ex) {
      this.#partNumber = 0;
      this.#lock.unlock();

      throw ex;
    }
  }

  public [Symbol.asyncIterator]() {
    return this;
  }

  public [Symbol.dispose](): void {
    this.#partNumber = 0;
    this.#lock[Symbol.dispose]();
  }
}
