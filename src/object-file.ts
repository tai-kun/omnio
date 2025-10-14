import type { File as NodeFile } from "node:buffer";
import type * as v from "valibot";
import { Error } from "./errors.js";
import md5 from "./md5.js";
import ObjectIdent, { type ObjectIdentJson } from "./object-ident.js";
import type ObjectPath from "./object-path.js";
import type * as schemas from "./schemas.js";
import type { $Select, Awaitable } from "./type-utils.js";

/**
 * メタデータを管理するオブジェクトのインターフェースです。`ObjectFile` クラスが依存するメタデータシステムの抽象化を提供します。
 */
interface Metadata {
  /**
   * 存在するオブジェクトのメタデータを取得します。
   *
   * @param inp オブジェクトのメタデータを取得するための入力パラメーターです。
   * @returns オブジェクトのメタデータを取得した結果です。
   * @throws オブジェクトが見つからない場合は `ObjectNotFoundError` を投げます。
   */
  read(
    inp: Readonly<{
      /**
       * 結果に含めるカラムを選択します。
       */
      select: {
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
      };
      /**
       * 対象を限定します。
       */
      where: {
        /**
         * バケット内のオブジェクトパスです。
         */
        objectPath: ObjectPath;
      };
    }>,
  ): Awaitable<{
    /**
     * オブジェクトに関連付けられたオブジェクトタグです。
     */
    objectTags?: schemas.MutableObjectTags;

    /**
     * オブジェクトの説明文です。
     */
    description?: string | null;

    /**
     * ユーザー定義のメタデータです。
     */
    userMetadata?: unknown;
  }>;
}

/**
 * `ObjectFile` を構築するための入力パラメーターです。
 */
type ObjectFileInput = Readonly<{
  /**
   * メタデータを管理するオブジェクトです。
   */
  metadata: Metadata;

  /**
   * JavaScript の `File` オブジェクト、または Node.js の `File` オブジェクトです。
   */
  file: globalThis.File;

  /**
   * バケット名です。
   */
  bucketName: v.InferOutput<typeof schemas.BucketName>;

  /**
   * バケット内のオブジェクトパスです。
   */
  objectPath: ObjectPath;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  checksum: v.InferOutput<typeof schemas.Checksum>;

  /**
   * オブジェクトのデータ形式です。
   */
  type: v.InferOutput<typeof schemas.MimeType>;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  size: v.InferOutput<typeof schemas.UnsignedInteger>;

  /**
   * 最終更新日 (ミリ秒) です。
   */
  lastModified: v.InferOutput<typeof schemas.Timestamp>;

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
}>;

/**
 * 追加のデータを読み込むためのオプションです。読み込むデータを選択します。
 */
export type ObjectFileLoadOptions = Readonly<{
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

/**
 * `ObjectFile` の JSON 形式での表現を表す型です。
 */
export type ObjectFileJson =
  & {
    /**
     * オブジェクトが存在するバケットの名前です。
     */
    bucketName: v.InferOutput<typeof schemas.BucketName>;

    /**
     * バケット内のオブジェクトパスです。
     */
    objectPath: ObjectPath;

    /**
     * オブジェクトのサイズ (バイト数) です。
     */
    size: v.InferOutput<typeof schemas.UnsignedInteger>;

    /**
     * オブジェクトのデータ形式です。
     */
    type: v.InferOutput<typeof schemas.MimeType>;

    /**
     * オブジェクトのチェックサム (MD5 ハッシュ値) です。
     */
    checksum: v.InferOutput<typeof schemas.Checksum>;

    /**
     * 最終更新日 (ミリ秒) です。
     */
    lastModified: v.InferOutput<typeof schemas.Timestamp>;

    /**
     * オブジェクトに関連付けられたオブジェクトタグです。
     */
    objectTags?: v.InferOutput<typeof schemas.ObjectTags>;

    /**
     * オブジェクトの説明文です。
     */
    description?: string | null;

    /**
     * ユーザー定義のメタデータです。
     */
    userMetadata?: unknown;
  }
  & (
    "webkitRelativePath" extends keyof globalThis.File ? {
        /**
         * `webkitdirectory` 属性が設定された `input` 要素でユーザーが選択した、
         * 祖先ディレクトリーを基準にしたファイルの相対パスです。
         */
        webkitRelativePath: string;
      }
      : {}
  );

/**
 * オブジェクトの情報を表すクラスです。
 */
export default class ObjectFile extends globalThis.File implements
  Pick<
    ObjectFileJson,
    | "bucketName"
    | "objectPath"
    | "size"
    | "type"
    | "checksum"
    | "lastModified"
  >,
  ObjectIdentJson
{
  /**
   * 元となる JavaScript の `File` オブジェクト、または Node.js の `File` オブジェクトです。
   */
  readonly #file: globalThis.File | NodeFile;

  /**
   * メタデータを管理するオブジェクトです。
   */
  readonly #metadata: Metadata;

  /**
   * オブジェクトが存在するバケットの名前です。
   */
  public readonly bucketName: v.InferOutput<typeof schemas.BucketName>;

  /**
   * バケット内のオブジェクトパスです。
   */
  public readonly objectPath: ObjectPath;

  /**
   * オブジェクトのチェックサム (MD5 ハッシュ値) です。
   */
  public readonly checksum: v.InferOutput<typeof schemas.Checksum>;

  /**
   * オブジェクトのサイズ (バイト数) です。
   */
  public override readonly size: v.InferOutput<typeof schemas.UnsignedInteger>;

  /**
   * オブジェクトのデータ形式です。
   */
  public override readonly type: v.InferOutput<typeof schemas.MimeType>;

  /**
   * オブジェクトの最終更新日 (ミリ秒) です。
   */
  public override readonly lastModified: v.InferOutput<typeof schemas.Timestamp>;

  /**
   * オブジェクトに関連付けられたオブジェクトタグです。
   */
  public objectTags: v.InferOutput<typeof schemas.ObjectTags> | undefined;

  /**
   * オブジェクトの説明文です。
   */
  public description: string | null | undefined;

  /**
   * ユーザー定義のメタデータです。
   */
  public userMetadata: unknown | undefined;

  /**
   * `ObjectFile` の新しいインスタンスを構築します。
   *
   * @param inp `ObjectFile` を構築するための入力パラメーターです。
   */
  public constructor(inp: ObjectFileInput) {
    const {
      file,
      size,
      type,
      checksum,
      metadata,
      bucketName,
      objectPath,
      objectTags,
      description,
      lastModified,
      userMetadata,
    } = inp;
    super([file], file.name, { lastModified });
    this.#file = file;
    this.#metadata = metadata;
    this.bucketName = bucketName;
    this.objectPath = objectPath;
    this.checksum = checksum;
    this.size = size;
    this.type = type;
    this.lastModified = lastModified;
    this.objectTags = objectTags;
    this.description = description;
    this.userMetadata = userMetadata;
  }

  /**
   * 追加のデータを読み込みます。読み込んだデータはこの `ObjectFile` のプロパティーに反映されます。
   *
   * @param options 追加のデータを読み込むためのオプションです。読み込むデータを選択します。
   * @returns 読み込んだ追加のデータです。
   */
  public async load<const TOptions extends ObjectFileLoadOptions | undefined = undefined>(
    options?: TOptions,
  ): Promise<$Select<Required<Awaited<ReturnType<Metadata["read"]>>>, TOptions>> {
    const result = await this.#metadata.read({
      select: {
        description: options?.description,
        userMetadata: options?.userMetadata,
      },
      where: {
        objectPath: this.objectPath,
      },
    });
    if ("objectTags" in result) {
      this.objectTags = result.objectTags;
    }
    if ("description" in result) {
      this.description = result.description;
    }
    if ("userMetadata" in result) {
      this.userMetadata = result.userMetadata;
    }

    return result as any;
  }

  /**
   * オブジェクトのデータとメタデータの整合性を検証します。
   *
   * @experimental
   */
  public async check(): Promise<void> {
    if (this.size !== this.#file.size) {
      throw new Error(
        `Object size should be ${this.size} byte(s) but is actually ${this.#file.size} byte(s)`,
      );
    }

    const hash = await md5.create();
    for await (const chunk of this.#file.slice().stream()) {
      hash.update(chunk);
    }

    const checksum = hash.digest();
    if (this.checksum !== checksum.value) {
      throw new Error(
        `Object checksum should be '${this.checksum}' but is actually '${checksum.value}'`,
      );
    }
  }

  /**
   * `ObjectFile` のプロパティーを JSON 形式に変換します。主にテストやデバッグでの利用を想定しています。
   *
   * @returns JSON 形式の `ObjectFile` です。
   */
  public toJSON(): ObjectFileJson {
    const json: Omit<ObjectFileJson, "webkitRelativePath"> & { webkitRelativePath?: string } = {
      size: this.size,
      type: this.type,
      checksum: this.checksum,
      bucketName: this.bucketName,
      objectPath: this.objectPath,
      lastModified: this.lastModified,
    };
    if (this.objectTags !== undefined) {
      json.objectTags = this.objectTags;
    }
    if (this.description !== undefined) {
      json.description = this.description;
    }
    if (this.userMetadata !== undefined) {
      json.userMetadata = this.userMetadata;
    }
    if ("webkitRelativePath" in this) {
      json.webkitRelativePath = this.webkitRelativePath;
    }

    return json as ObjectFileJson;
  }

  /**
   * `ObjectFile` を `ObjectIdent` に変換します。
   *
   * @returns `ObjectIdent` です。
   */
  public toObjectIdent(): ObjectIdent {
    return new ObjectIdent({
      bucketName: this.bucketName,
      objectPath: this.objectPath,
    });
  }
}
