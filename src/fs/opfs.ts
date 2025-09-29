import normUrl, { type Options as NormUrlOptions } from "normalize-url";
import * as v from "valibot";
import { FsPathNotFoundError, OpfsError, TypeError } from "../errors.js";
import isUint8Array from "../is-uint8-array.js";
import mutex from "../mutex.js";
import * as schemas from "../schemas.js";
import type {
  DirectoryHandle,
  FileHandle,
  Fs,
  GetDirectoryOptions,
  GetFileOptions,
  Path,
  RemoveOptions,
  WritableFileStream,
  WriteChunkType,
} from "./fs.types.js";

export type * from "./fs.types.js";

/**
 * 例外が、エントリーが見つからないことを示すか判定します。
 *
 * @param ex 例外です。
 * @returns 例外が `NotFoundError` であれば `true`、そうでなければ `false` を返します。
 */
function isNotFoundError(ex: unknown): boolean {
  return ex instanceof globalThis.DOMException && ex.name === "NotFoundError";
}

/**
 * ファイルハンドルを取得します。
 *
 * @param fs FileSystemDirectoryHandle のインスタンスです。
 * @param path 操作対象のパスです。
 * @param name 取得するファイルの名前です。
 * @param options ファイル作成のオプションです。
 * @returns ファイルハンドルです。
 */
async function getFileHandle(
  fs: {
    getFileHandle(
      name: string,
      options: Readonly<{ create: boolean }>,
    ): Promise<FileSystemFileHandle>;
  },
  path: string,
  name: string,
  options: Readonly<{ create: boolean }>,
): Promise<FileSystemFileHandle> {
  try {
    return await fs.getFileHandle(name, {
      create: options.create,
    });
  } catch (ex) {
    if (isNotFoundError(ex)) {
      throw new FsPathNotFoundError(path, { cause: ex });
    }

    throw ex;
  }
}

/**
 * ディレクトリーハンドルを取得します。
 *
 * @param fs FileSystemDirectoryHandle のインスタンスです。
 * @param path 操作対象のパスです。
 * @param name 取得するディレクトリーの名前です。
 * @param options ディレクトリー作成のオプションです。
 * @returns ディレクトリーハンドルです。
 */
async function getDirectoryHandle(
  fs: {
    getDirectoryHandle(
      name: string,
      options: Readonly<{ create: boolean }>,
    ): Promise<FileSystemDirectoryHandle>;
  },
  path: string,
  name: string,
  options: Readonly<{ create: boolean }>,
): Promise<FileSystemDirectoryHandle> {
  try {
    return await fs.getDirectoryHandle(name, {
      create: options.create,
    });
  } catch (ex) {
    if (isNotFoundError(ex)) {
      throw new FsPathNotFoundError(path, { cause: ex });
    }

    throw ex;
  }
}

/**
 * ファイルストリームへの書き込みを行うクラスです。OPFS の FileSystemWritableFileStream を使用します。
 */
export class OpfsWritableFileStream implements WritableFileStream {
  /**
   * ネイティブの FileSystemWritableFileStream です。
   */
  readonly #native: FileSystemWritableFileStream;

  /**
   * 書き込み中の状態を管理するフラグです。
   */
  #closed: boolean;

  /**
   * 中断の理由です。
   */
  #abortReason?: unknown;

  /**
   * ストリームに書き込まれたバイト数です。
   */
  #bytesWritten: v.InferOutput<typeof schemas.UnsignedInteger>;

  /**
   * `OpfsWritableFileStream` の新しいインスタンスを構築します。
   *
   * @param native ネイティブの FileSystemWritableFileStream インスタンスです。
   */
  public constructor(native: FileSystemWritableFileStream) {
    this.#native = native;
    this.#closed = false;
    this.#bytesWritten = 0 as v.InferOutput<typeof schemas.UnsignedInteger>;
  }

  /**
   * ストリームに書き込まれたバイト数です。
   */
  public get bytesWritten(): v.InferOutput<typeof schemas.UnsignedInteger> {
    return this.#bytesWritten;
  }

  /**
   * データをファイルに書き込みます。
   *
   * @param data 書き込むデータです。
   */
  @mutex
  public async write(data: WriteChunkType): Promise<void> {
    if (this.#closed) {
      throw this.#abortReason;
    }

    switch (true) {
      case isUint8Array(data): {
        const byteLength = v.parse(schemas.UnsignedInteger, data.byteLength);
        this.#bytesWritten = v.parse(schemas.UnsignedInteger, this.#bytesWritten + byteLength);
        await this.#native.write(data);

        break;
      }

      case data instanceof OpfsFileHandle: {
        const file = await data.getFile();
        const reader = file.stream();
        const writer = this.#native;
        for await (const chunk of reader) {
          const byteLength = v.parse(schemas.UnsignedInteger, chunk.byteLength);
          this.#bytesWritten = v.parse(schemas.UnsignedInteger, this.#bytesWritten + byteLength);
          await writer.write(chunk);
        }

        break;
      }

      default:
        throw new TypeError(["Uint8Array<ArrayBuffer>", "FileHandle"], data);
    }
  }

  /**
   * ストリームを中断し、閉じます。
   *
   * @param reason 中断の理由です。
   */
  @mutex
  public async abort(reason?: unknown): Promise<void> {
    if (this.#closed) {
      throw this.#abortReason;
    }

    try {
      await this.#native.abort(reason);
    } finally {
      this.#closed = true;
      this.#abortReason = reason;
    }
  }

  /**
   * ストリームへの書き込みを完了し、閉じます。
   */
  @mutex
  public async close(): Promise<void> {
    if (this.#closed) {
      throw this.#abortReason;
    }

    try {
      await this.#native.close();
    } finally {
      this.#closed = true;
    }
  }
}

/**
 * ファイルのハンドル（操作を可能にする参照）を行うクラスです。OPFS の FileSystemFileHandle を使用します。
 */
export class OpfsFileHandle implements FileHandle {
  /**
   * ネイティブの FileSystemFileHandle です。
   */
  readonly #native: FileSystemFileHandle;

  /**
   * `OpfsFileHandle` の新しいインスタンスを構築します。
   *
   * @param native ネイティブの FileSystemFileHandle インスタンスです。
   */
  public constructor(native: FileSystemFileHandle) {
    this.#native = native;
  }

  /**
   * ファイルの内容を取得します。
   *
   * @returns ブラウザーの `File` オブジェクトです。
   */
  public async getFile(): Promise<File> {
    return await this.#native.getFile();
  }

  /**
   * ファイルへの書き込みが可能なストリームを作成します。
   *
   * @returns ファイルストリームへの書き込みを行うクラスのインスタンスです。
   */
  public async createWritable(): Promise<OpfsWritableFileStream> {
    const nativeStream = await this.#native.createWritable();

    return new OpfsWritableFileStream(nativeStream);
  }
}

/**
 * ディレクトリーのハンドル（操作を可能にする参照）を行うクラスです。OPFS の FileSystemDirectoryHandle を使用します。
 */
export class OpfsDirectoryHandle implements DirectoryHandle {
  /**
   * ディレクトリーの絶対パスです。
   */
  readonly #dirPath: string;

  /**
   * ネイティブの FileSystemDirectoryHandle です。
   */
  readonly #native: FileSystemDirectoryHandle;

  /**
   * `OpfsDirectoryHandle` の新しいインスタンスを構築します。
   *
   * @param dirPath ディレクトリーの絶対パスです。
   * @param native ネイティブの FileSystemDirectoryHandle インスタンスです。
   */
  public constructor(dirPath: string, native: FileSystemDirectoryHandle) {
    this.#dirPath = dirPath;
    this.#native = native;
  }

  /**
   * ディレクトリー直下から指定のアイテムを削除します。
   *
   * @param name 削除するアイテムの名前です。
   * @param options 削除時のオプションです。
   */
  public async removeEntry(name: string, options: RemoveOptions): Promise<void> {
    try {
      await this.#native.removeEntry(name, options);
    } catch (ex) {
      if (isNotFoundError(ex)) {
        throw new FsPathNotFoundError(this.#dirPath + "/" + name, { cause: ex });
      }

      throw ex;
    }
  }

  /**
   * 指定した名前のファイルハンドルを取得します。
   *
   * @param name 取得するファイルの名前です。
   * @param options ファイル取得時のオプションです。
   * @returns ファイルのハンドル（操作を可能にする参照）を行うクラスのインスタンスです。
   */
  public async getFileHandle(name: string, options: GetFileOptions): Promise<OpfsFileHandle> {
    const filePath = this.#dirPath + "/" + name;
    const nativeHandle = await getFileHandle(this.#native, filePath, name, options);

    return new OpfsFileHandle(nativeHandle);
  }

  /**
   * 指定した名前のディレクトリーハンドルを取得します。
   *
   * @param name 取得するディレクトリーの名前です。
   * @param options ディレクトリー取得時のオプションです。
   * @returns ディレクトリーのハンドル（操作を可能にする参照）を行うクラスのインスタンスです。
   */
  public async getDirectoryHandle(
    name: string,
    options: GetDirectoryOptions,
  ): Promise<OpfsDirectoryHandle> {
    const dirPath = this.#dirPath + "/" + name;
    const nativeHandle = await getDirectoryHandle(this.#native, dirPath, name, options);

    return new OpfsDirectoryHandle(dirPath, nativeHandle);
  }
}

/**
 * ファイルパスを編集するためのユーティリティーです。
 */
export class OpfsPath implements Path {
  /**
   * ファイルシステム操作のルートディレクトリーです。必ずセパレーターで終わります。
   */
  readonly #root: string;

  /**
   * `OpfsPath` の新しいインスタンスを構築します。
   *
   * @param root ファイルシステム操作のルートディレクトリーです。
   */
  public constructor(root: string) {
    this.#root = root;
  }

  /**
   * 絶対パスに解決します。
   *
   * @param paths 結合するパスです。単純にパスセパレーターで結合されます。
   * @returns 絶対パスに解決されたパスです。
   */
  public resolve(...paths: string[]): string {
    const options: Required<NormUrlOptions> = {
      stripWWW: true,
      forceHttp: false,
      stripHash: false,
      forceHttps: false,
      removePath: false,
      stripProtocol: true,
      transformPath: x => x,
      defaultProtocol: "http",
      normalizeProtocol: false,
      stripTextFragment: false,
      removeSingleSlash: true,
      removeExplicitPort: false,
      stripAuthentication: true,
      keepQueryParameters: [/.+/],
      removeTrailingSlash: true,
      sortQueryParameters: true,
      removeDirectoryIndex: false,
      removeQueryParameters: false,
    };
    let resolved = normUrl("example.com/" + paths.join("/"), options)
      .slice("example.com".length);

    if (resolved[0] === "/") {
      resolved = resolved.slice("/".length);
    }

    resolved = this.#root + resolved;

    if (resolved + "/" === this.#root) {
      return this.#root;
    }

    return resolved;
  }
}

/**
 * ファイルシステムを操作するための基本的な機能を提供するクラスです。
 * ファイルシステムの接続、切断、およびディレクトリーへのアクセスを可能にします。
 */
export class Opfs implements Fs {
  /**
   * ファイルシステム操作のルートディレクトリーです。必ずセパレーターで終わります。
   */
  public readonly root: string;

  /**
   * ファイルパスを編集するためのユーティリティーです。
   */
  public readonly path: OpfsPath;

  /**
   * ルートディレクトリーのハンドルです。
   */
  #rootHandle: FileSystemDirectoryHandle | null;

  /**
   * 接続が閉じているか管理するフラグです。
   */
  #closed: boolean;

  /**
   * `Opfs` の新しいインスタンスを構築します。
   *
   * @param rootDir 操作の基準となるルートディレクトリーのパスです。デフォルトはルートディレクトリです。
   */
  public constructor(rootDir: string | undefined = "") {
    let root = (new OpfsPath("opfs://")).resolve(rootDir);
    if (root[root.length - 1] !== "/") {
      root += "/";
    }

    this.root = root;
    this.path = new OpfsPath(this.root);
    this.#rootHandle = null;
    this.#closed = true;
  }

  /**
   * ファイルシステムへの接続を開きます。
   */
  @mutex
  public async open(): Promise<void> {
    if (!this.#closed) {
      return;
    }

    const { state } = await window.navigator.permissions.query({ name: "storage-access" });
    if (state !== "granted") {
      throw new OpfsError(`The permission given to storage-access was "${state}"`);
    }

    await window.navigator.storage.persist();
    this.#rootHandle = await window.navigator.storage.getDirectory();
    const segmentsStr = this.root.slice("opfs://".length, -"/".length);
    if (segmentsStr) {
      for (const name of segmentsStr.split("/")) {
        this.#rootHandle = await this.#rootHandle.getDirectoryHandle(name, { create: true });
      }
    }

    this.#closed = false;
  }

  /**
   * ファイルシステムへの接続を閉じます。
   */
  @mutex
  public async close(): Promise<void> {
    this.#closed = true;
  }

  /**
   * ルートディレクトリーを基準に、指定した名前のディレクトリーハンドルを取得します。
   *
   * @param name 取得するディレクトリーの名前です。
   * @param options ディレクトリー取得時のオプションです。
   * @returns ディレクトリーのハンドル（操作を可能にする参照）を行うクラスのインスタンスです。
   */
  @mutex
  public async getDirectoryHandle(
    name: string,
    options: GetDirectoryOptions,
  ): Promise<OpfsDirectoryHandle> {
    if (this.#closed) {
      throw new OpfsError("Not open");
    }

    const dirPath = this.root + name;
    const nativeHandle = await getDirectoryHandle(this.#rootHandle!, dirPath, name, options);

    return new OpfsDirectoryHandle(dirPath, nativeHandle);
  }
}
