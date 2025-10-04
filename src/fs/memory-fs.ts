import { type IFs, memfs } from "memfs";
import normUrl, { type Options as NormUrlOptions } from "normalize-url";
import * as v from "valibot";
import { FsPathNotFoundError, MemoryFsError, TypeError } from "../errors.js";
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

type IFileHandle = Awaited<ReturnType<IFs["promises"]["open"]>>;

/**
 * 例外が ENOENT エラーかどうかを判定します。
 *
 * @param ex 例外です。
 * @returns ENOENT エラーなら `true`、そうでなければ `false` です。
 */
function isEnoentError(ex: unknown): boolean {
  return (
    (ex instanceof globalThis.Error || (typeof ex === "object" && ex !== null))
    && "code" in ex
    && ex.code === "ENOENT"
  );
}

/**
 * ファイルパスが存在するか検証します。
 *
 * @param fs ファイルシステムです。
 * @param path ファイルパスです。
 */
async function assertFileExists(fs: IFs, path: string): Promise<void> {
  try {
    const stats = await fs.promises.stat(path);
    if (!stats.isFile()) {
      throw new FsPathNotFoundError(path);
    }
  } catch (ex) {
    if (isEnoentError(ex)) {
      throw new FsPathNotFoundError(path, { cause: ex });
    }

    throw ex;
  }
}

/**
 * ディレクトリーパスが存在するか検証します。
 *
 * @param fs ファイルシステムです。
 * @param path ディレクトリーパスです。
 */
async function assertDirectoryExists(fs: IFs, path: string): Promise<void> {
  try {
    const stats = await fs.promises.stat(path);
    if (!stats.isDirectory()) {
      throw new FsPathNotFoundError(path);
    }
  } catch (ex) {
    if (isEnoentError(ex)) {
      throw new FsPathNotFoundError(path, { cause: ex });
    }

    throw ex;
  }
}

/**
 * ファイルストリームへの書き込みを行うクラスです。
 */
export class MemoryFsWritableFileStream implements WritableFileStream {
  /**
   * 書き込み先のファイルパスです。
   */
  readonly #targetPath: string;

  /**
   * 書き込みが完了するまでの一時的なファイルパスです。
   */
  readonly #crswapPath: string;

  /**
   * 書き込みが完了するまでの一時的なファイルハンドルです。
   */
  readonly #fileHandle: IFileHandle;

  /**
   * ファイルシステムです。
   */
  readonly #fs: IFs;

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
   * `MemoryFsWritableFileStream` の新しいインスタンスを構築します。
   *
   * @param targetPath 書き込み先のファイルパスです。
   * @param crswapPath 書き込みが完了するまでの一時的なファイルパスです。
   * @param fileHandle 書き込みが完了するまでの一時的なファイルの書き込みに使用する `IFileHandle` オブジェクトです。
   * @param fs ファイルシステムです。
   */
  public constructor(
    targetPath: string,
    crswapPath: string,
    fileHandle: IFileHandle,
    fs: IFs,
  ) {
    this.#targetPath = targetPath;
    this.#crswapPath = crswapPath;
    this.#fileHandle = fileHandle;
    this.#fs = fs;
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
        await this.#fileHandle.write(data);

        break;
      }

      case data instanceof MemoryFsFileHandle: {
        const reader = this.#fs.createReadStream(data.filePath, { highWaterMark: 4096 });
        const writer = this.#fileHandle;
        try {
          for await (const chunk of reader) {
            if (!isUint8Array(chunk)) {
              throw new TypeError("Buffer", chunk);
            }

            const byteLength = v.parse(schemas.UnsignedInteger, chunk.byteLength);
            this.#bytesWritten = v.parse(schemas.UnsignedInteger, this.#bytesWritten + byteLength);
            await writer.write(chunk);
          }
        } finally {
          try {
            reader.destroy();
          } catch {
            // 無視します。
          }
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
      await this.#fileHandle.close();
    } finally {
      this.#closed = true;
      this.#abortReason = reason;

      try {
        await this.#fs.promises.unlink(this.#crswapPath);
      } catch {
        // 無視します。
      }
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
      await this.#fileHandle.close();
      try {
        await this.#fs.promises.unlink(this.#targetPath);
      } catch {
        // 無視します。
      }

      await this.#fs.promises.rename(this.#crswapPath, this.#targetPath);
    } finally {
      this.#closed = true;

      try {
        await this.#fs.promises.unlink(this.#crswapPath);
      } catch {
        // 無視します。
      }
    }
  }
}

/**
 * ファイルのハンドル（操作を可能にする参照）を行うクラスです。ファイルへのアクセスや、書き込み可能なストリームの作成を可能にします。
 */
export class MemoryFsFileHandle implements FileHandle {
  /**
   * ファイルの絶対パスです。
   */
  public readonly filePath: string;

  /**
   * ファイル名です。
   */
  readonly #name: string;

  /**
   * ファイルシステムです。
   */
  readonly #fs: IFs;

  /**
   * `MemoryFsFileHandle` の新しいインスタンスを構築します。
   *
   * @param filePath ファイルの絶対パスです。
   * @param name ファイル名です。
   * @param fs ファイルシステムです。
   */
  public constructor(filePath: string, name: string, fs: IFs) {
    this.filePath = filePath;
    this.#name = name;
    this.#fs = fs;
  }

  /**
   * ファイルの内容を取得します。
   *
   * @returns `File` オブジェクトです。
   */
  @mutex
  public async getFile(): Promise<File> {
    const stat = await this.#fs.promises.stat(this.filePath);
    const lastMod = stat.mtime.getTime();
    const content = await this.#fs.promises.readFile(this.filePath);
    // ブラウザーの `File` API をエミュレートする簡潔な実装です。
    const file = new File([content as any], this.#name, {
      lastModified: lastMod,
    });

    return file;
  }

  /**
   * ファイルへの書き込みが可能なストリームを作成します。
   *
   * @returns ファイルストリームへの書き込みを行うクラスのインスタンスです。
   */
  @mutex
  public async createWritable(): Promise<MemoryFsWritableFileStream> {
    const targetPath = this.filePath;
    const crswapPath = this.filePath + ".crswap";
    const fileHandle = await this.#fs.promises.open(crswapPath, "w");

    return new MemoryFsWritableFileStream(targetPath, crswapPath, fileHandle, this.#fs);
  }
}

/**
 * ディレクトリーのハンドル（操作を可能にする参照）を行うクラスです。ファイルやサブディレクトリーへのアクセスを可能にします。
 */
export class MemoryFsDirectoryHandle implements DirectoryHandle {
  /**
   * ディレクトリーの絶対パスです。
   */
  readonly #dirPath: string;

  /**
   * ファイルシステムです。
   */
  readonly #fs: IFs;

  /**
   * `MemoryFsDirectoryHandle` の新しいインスタンスを構築します。
   *
   * @param dirPath ディレクトリーの絶対パスです。
   * @param fs ファイルシステムです。
   */
  public constructor(dirPath: string, fs: IFs) {
    this.#dirPath = dirPath;
    this.#fs = fs;
  }

  /**
   * ディレクトリー直下から指定のアイテムを削除します。
   *
   * @param name 削除するアイテムの名前です。
   * @param options 削除時のオプションです。
   */
  @mutex
  public async removeEntry(name: string, options: RemoveOptions): Promise<void> {
    const entryPath = this.#dirPath + "/" + name;
    const { recursive } = options;

    let isFile: boolean;
    let isDirectory: boolean;
    try {
      const stats = await this.#fs.promises.stat(entryPath);
      isFile = stats.isFile();
      isDirectory = stats.isDirectory();
    } catch (ex) {
      if (isEnoentError(ex)) {
        throw new FsPathNotFoundError(entryPath, { cause: ex });
      }

      throw ex;
    }

    switch (true) {
      case isFile:
        await this.#fs.promises.unlink(entryPath);
        break;

      case isDirectory:
        await this.#fs.promises.rm(entryPath, { recursive });
        break;

      default:
        throw new FsPathNotFoundError(entryPath);
    }
  }

  /**
   * 指定した名前のファイルハンドルを取得します。
   *
   * @param name 取得するファイルの名前です。
   * @param options ファイル取得時のオプションです。
   * @returns ファイルのハンドル（操作を可能にする参照）を行うクラスのインスタンスです。
   */
  @mutex
  public async getFileHandle(name: string, options: GetFileOptions): Promise<MemoryFsFileHandle> {
    const filePath = this.#dirPath + "/" + name;
    if (options.create) {
      await this.#fs.promises.writeFile(filePath, "");
    } else {
      await assertFileExists(this.#fs, filePath);
    }

    return new MemoryFsFileHandle(filePath, name, this.#fs);
  }

  /**
   * 指定した名前のディレクトリーハンドルを取得します。
   *
   * @param name 取得するディレクトリーの名前です。
   * @param options ディレクトリー取得時のオプションです。
   * @returns ディレクトリーのハンドル（操作を可能にする参照）を行うクラスのインスタンスです。
   */
  @mutex
  public async getDirectoryHandle(
    name: string,
    options: GetDirectoryOptions,
  ): Promise<MemoryFsDirectoryHandle> {
    const dirPath = this.#dirPath + "/" + name;
    if (options.create) {
      await this.#fs.promises.mkdir(dirPath, {
        recursive: true, // すでに作成済みのときエラーを投げないために再帰的な作成を許可します。
      });
    } else {
      await assertDirectoryExists(this.#fs, dirPath);
    }

    return new MemoryFsDirectoryHandle(dirPath, this.#fs);
  }
}

/**
 * ファイルパスを編集するためのユーティリティーです。
 */
export class MemoryFsPath implements Path {
  /**
   * ファイルシステム操作のルートディレクトリーです。必ずセパレーターで終わります。
   */
  readonly #root: string;

  /**
   * `MemoryFsPath` の新しいインスタンスを構築します。
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
export class MemoryFs implements Fs {
  /**
   * ファイルシステム操作のルートディレクトリーです。必ずセパレーターで終わります。
   */
  readonly root: string;

  /**
   * ファイルパスを編集するためのユーティリティーです。
   */
  readonly path: MemoryFsPath;

  /**
   * ファイルシステムです。
   */
  readonly fs: IFs;

  /**
   * 接続が閉じているか管理するフラグです。
   */
  #closed: boolean;

  /**
   * `MemoryFs` の新しいインスタンスを構築します。
   */
  public constructor() {
    this.root = "memory://";
    this.path = new MemoryFsPath(this.root);
    this.fs = memfs().fs;
    this.#closed = true;
  }

  /**
   * ファイルシステムへの接続を開きます。
   */
  @mutex
  public async open(): Promise<void> {
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
  @mutex.readonly
  public async getDirectoryHandle(
    name: string,
    options: GetDirectoryOptions,
  ): Promise<MemoryFsDirectoryHandle> {
    if (this.#closed) {
      throw new MemoryFsError("Not open");
    }

    const dirPath = this.root + "/" + name;
    if (options.create) {
      await this.fs.promises.mkdir(dirPath, {
        recursive: true, // すでに作成済みのときエラーを投げないために再帰的な作成を許可します。
      });
    } else {
      await assertDirectoryExists(this.fs, dirPath);
    }

    return new MemoryFsDirectoryHandle(dirPath, this.fs);
  }
}
