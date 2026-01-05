import { ChecksumMismatchError } from "../shared/errors.js";
import type {
  BucketName,
  Checksum,
  EntityId,
  MimeType,
  ObjectPath,
  ObjectSize,
  ObjectTags,
  Timestamp,
} from "../shared/schemas.js";
import md5 from "./_md5.js";

/**
 * `ObjectFile` を構築するための入力パラメーターです。
 */
type ObjectFileInput = Readonly<{
  /**
   * オブジェクトのパートデータの配列です。
   */
  parts: Uint8Array<ArrayBuffer>[];

  /**
   * オブジェクトが実際に保存されているエンティティーの識別子です。
   */
  entityId: EntityId;

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
   * オブジェクトのデータ形式です。
   */
  type: MimeType;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  size: ObjectSize;

  /**
   * 最終更新日 (ミリ秒) です。
   */
  lastModified: Timestamp;

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
 * オブジェクトの情報を表すクラスです。
 */
export default class ObjectFile extends File {
  /**
   * オブジェクトの名前 (エンティティー ID) です。
   */
  // @ts-expect-error
  public override readonly name: EntityId;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  // @ts-expect-error
  public override readonly size: ObjectSize;

  /**
   * オブジェクトのデータ形式です。
   */
  // @ts-expect-error
  public override readonly type: MimeType;

  /**
   * オブジェクトの最終更新日 (ミリ秒) です。
   */
  // @ts-expect-error
  public override readonly lastModified: Timestamp;

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
   * `ObjectFile` の新しいインスタンスを構築します。
   *
   * @param inp `ObjectFile` を構築するための入力パラメーターです。
   */
  public constructor(inp: ObjectFileInput) {
    super(inp.parts, inp.entityId, { lastModified: inp.lastModified });
    for (
      const [key, value] of [
        ["name", inp.entityId],
        ["size", inp.size],
        ["type", inp.type],
        ["lastModified", inp.lastModified],
      ] as const
    ) {
      Object.defineProperty(this, key, {
        value,
        enumerable: true,
      });
    }

    this.bucketName = inp.bucketName;
    this.objectPath = inp.objectPath;
    this.checksum = inp.checksum;
    this.objectTags = inp.objectTags;
    this.description = inp.description;
    this.userMetadata = inp.userMetadata;
  }

  /**
   * `ObjectFile` の新しいインスタンスを構築します。
   *
   * @param inp `ObjectFile` を構築するための入力パラメーターです。
   */
  public static async create(inp: ObjectFileInput): Promise<ObjectFile> {
    // 整合性の検証を行います。
    {
      const hash = await md5.create();
      for (const part of inp.parts) {
        hash.update(part);
      }

      const digest = hash.digest();
      if (digest.value !== inp.checksum) {
        throw new ChecksumMismatchError(inp.bucketName, inp.objectPath, inp.checksum, {
          actual: digest.value,
        });
      }
    }

    return new ObjectFile(inp);
  }
}
