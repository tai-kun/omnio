import { asyncmux } from "asyncmux";
import * as buffer from "node:buffer";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import {
  EntryPathNotFoundError,
  FileSystemNotOpenError,
  TypeError,
} from "../../../shared/errors.js";
import type {
  CreateWritableOptions,
  GetDirectoryOptions,
  GetFileOptions,
  IDirectoryHandle,
  IFileHandle,
  IFileSystem,
  IWritableFileStream,
  RemoveOptions,
  WriteChunkType,
} from "../../../shared/file-system.js";
import isError from "../../../shared/is-error.js";
import isUint8Array from "../../../shared/is-uint8-array.js";
import {
  FILE_SYSTEM_TEMP_FILE_EXT as TEMP_FILE_EXT,
  type FileSystemDirectoryName as DirectoryName,
  type FileSystemDirectoryNameLike as DirectoryNameLike,
  FileSystemDirectoryNameSchema as DirectoryNameSchema,
  type FileSystemEntryName as EntryName,
  type FileSystemEntryNameLike as EntryNameLike,
  FileSystemEntryNameSchema as EntryNameSchema,
  type FileSystemFileName as FileName,
  type FileSystemFileNameLike as FileNameLike,
  FileSystemFileNameSchema as FileNameSchema,
} from "../../../shared/schemas.js";
import * as v from "../../../shared/valibot.js";

/**
 * 入力値をファイル名またはディレクトリー名として扱います。
 *
 * @param name 入力値です。
 * @returns ファイル名またはディレクトリー名です。
 */
function asEntryName(name: EntryNameLike): EntryName {
  return v.parse(EntryNameSchema(), name);
}

/**
 * 入力値をファイル名として扱います。
 *
 * @param name 入力値です。
 * @returns ファイル名です。
 */
function asFileName(name: FileNameLike): FileName {
  return v.parse(FileNameSchema(), name);
}

/**
 * 入力値をディレクトリー名として扱います。
 *
 * @param name 入力値です。
 * @returns ディレクトリー名です。
 */
function asDirectoryName(name: DirectoryNameLike): DirectoryName {
  return v.parse(DirectoryNameSchema(), name);
}

/**
 * 例外が ENOENT エラーかどうかを判定します。
 *
 * @param ex 例外です。
 * @returns ENOENT エラーなら `true`、そうでなければ `false` です。
 */
function isEnoentError(ex: unknown): boolean {
  return (
    isError(ex)
    && "code" in ex
    && ex.code === "ENOENT"
  );
}

/**
 * ファイルパスが存在するか検証します。
 *
 * @param path ファイルパスです。
 */
async function assertFileExists(path: string): Promise<void> {
  try {
    const stats = await fsp.stat(path);
    if (!stats.isFile()) {
      throw new EntryPathNotFoundError(path);
    }
  } catch (ex) {
    if (isEnoentError(ex)) {
      throw new EntryPathNotFoundError(path, { cause: ex });
    }

    throw ex;
  }
}

/**
 * ディレクトリーパスが存在するか検証します。
 *
 * @param path ディレクトリーパスです。
 */
async function assertDirectoryExists(path: string): Promise<void> {
  try {
    const stats = await fsp.stat(path);
    if (!stats.isDirectory()) {
      throw new EntryPathNotFoundError(path);
    }
  } catch (ex) {
    if (isEnoentError(ex)) {
      throw new EntryPathNotFoundError(path, { cause: ex });
    }

    throw ex;
  }
}

/**
 * ファイルストリームへの書き込みを行うクラスです。Node.js のファイルシステムを使用します。
 */
class WritableFileStream implements IWritableFileStream {
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
  readonly #fileHandle: fsp.FileHandle;

  /**
   * 書き込み中の状態を管理するフラグです。
   */
  #closed: boolean;

  /**
   * 中断の理由です。
   */
  #abortReason?: unknown;

  /**
   * `WritableFileStream` の新しいインスタンスを構築します。
   *
   * @param targetPath 書き込み先のファイルパスです。
   * @param crswapPath 書き込みが完了するまでの一時的なファイルパスです。
   * @param fileHandle 書き込みが完了するまでの一時的なファイルの書き込みに使用する `fsp.FileHandle` オブジェクトです。
   */
  public constructor(
    targetPath: string,
    crswapPath: string,
    fileHandle: fsp.FileHandle,
  ) {
    this.#targetPath = targetPath;
    this.#crswapPath = crswapPath;
    this.#fileHandle = fileHandle;
    this.#closed = false;
  }

  /**
   * データをファイルに書き込みます。
   *
   * @param data 書き込むデータです。
   */
  @asyncmux
  public async write(data: WriteChunkType): Promise<void> {
    if (this.#closed) {
      throw this.#abortReason;
    }

    switch (true) {
      case isUint8Array(data):
        await this.#fileHandle.write(data);
        break;

      default:
        throw new TypeError("Uint8Array<ArrayBuffer>", data);
    }
  }

  /**
   * ストリームを中断し、閉じます。
   *
   * @param reason 中断の理由です。
   */
  @asyncmux
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
        await fsp.unlink(this.#crswapPath);
      } catch {
        // 無視します。
      }
    }
  }

  /**
   * ストリームへの書き込みを完了し、閉じます。
   */
  @asyncmux
  public async close(): Promise<void> {
    if (this.#closed) {
      throw this.#abortReason;
    }

    try {
      await this.#fileHandle.close();
      try {
        await fsp.unlink(this.#targetPath);
      } catch {
        // 無視します。
      }

      await fsp.rename(this.#crswapPath, this.#targetPath);
    } finally {
      this.#closed = true;

      try {
        await fsp.unlink(this.#crswapPath);
      } catch {
        // 無視します。
      }
    }
  }
}

/**
 * ファイルのハンドル（操作を可能にする参照）を行うクラスです。ファイルへのアクセスや、書き込み可能なストリームの作成を可能にします。
 */
class FileHandle implements IFileHandle {
  /**
   * ファイルの絶対パスです。
   */
  public readonly path: string;

  /**
   * `FileHandle` の新しいインスタンスを構築します。
   *
   * @param filePath ファイルの絶対パスです。
   */
  public constructor(filePath: string) {
    this.path = filePath;
  }

  /**
   * ファイルの内容を取得します。
   *
   * @returns Node.js の `buffer.File` オブジェクトです。
   */
  @asyncmux
  public async getFile(): Promise<File> {
    const stat = await fsp.stat(this.path);
    const lastMod = stat.mtime.getTime();
    const content = await fsp.readFile(this.path);
    // ブラウザーの `File` API をエミュレートする簡潔な実装です。
    // `File` クラスはブラウザー環境に固有のため、ここでは Node.js の buffer.File を使用しています。
    const nodeFile = new buffer.File([content], path.basename(this.path), {
      lastModified: lastMod,
    });

    // @ts-ignore
    return nodeFile;
  }

  /**
   * ファイルへの書き込みが可能なストリームを作成します。
   *
   * @param options ファイルへの書き込みが可能なストリームを作成する際のオプションです。
   * @returns ファイルストリームへの書き込みを行うクラスのインスタンスです。
   */
  @asyncmux
  public async createWritable(options: CreateWritableOptions): Promise<WritableFileStream> {
    const targetPath = this.path;
    const crswapPath = this.path + TEMP_FILE_EXT;
    let fileHandle: fsp.FileHandle;
    if (options.keepExistingData) {
      await fsp.copyFile(targetPath, crswapPath);
      fileHandle = await fsp.open(crswapPath, "a");
    } else {
      fileHandle = await fsp.open(crswapPath, "w");
    }

    return new WritableFileStream(targetPath, crswapPath, fileHandle);
  }
}

/**
 * ディレクトリーのハンドル（操作を可能にする参照）を行うクラスです。ファイルやサブディレクトリーへのアクセスを可能にします。
 */
class DirectoryHandle implements IDirectoryHandle {
  /**
   * ディレクトリーの絶対パスです。
   */
  public readonly path: string;

  /**
   * `DirectoryHandle` の新しいインスタンスを構築します。
   *
   * @param dirPath ディレクトリーの絶対パスです。
   */
  public constructor(dirPath: string) {
    this.path = dirPath;
  }

  /**
   * @see {@link removeEntry}
   */
  async #removeEntry(name: string, options: RemoveOptions): Promise<void> {
    const entryPath = path.join(this.path, name);
    const { recursive } = options;

    let isFile: boolean;
    let isDirectory: boolean;
    try {
      const stats = await fsp.stat(entryPath);
      isFile = stats.isFile();
      isDirectory = stats.isDirectory();
    } catch (ex) {
      if (isEnoentError(ex)) {
        throw new EntryPathNotFoundError(entryPath, { cause: ex });
      }

      throw ex;
    }

    switch (true) {
      case isFile:
        await fsp.unlink(entryPath);
        break;

      case isDirectory:
        await fsp.rm(entryPath, { recursive });
        break;

      default:
        throw new EntryPathNotFoundError(entryPath);
    }
  }

  /**
   * ディレクトリー直下から指定のアイテムを削除します。
   *
   * @param name 削除するアイテムの名前です。
   * @param options 削除時のオプションです。
   */
  @asyncmux
  public async removeEntry(name: EntryNameLike, options: RemoveOptions): Promise<void> {
    return await this.#removeEntry(asEntryName(name), options);
  }

  /**
   * @see {@link getFileHandle}
   */
  async #getFileHandle(name: FileName, options: GetFileOptions): Promise<FileHandle> {
    const filePath = path.join(this.path, name);
    if (options.create) {
      await fsp.writeFile(filePath, "");
    } else {
      await assertFileExists(filePath);
    }

    return new FileHandle(filePath);
  }

  /**
   * 指定した名前のファイルハンドルを取得します。
   *
   * @param name 取得するファイルの名前です。
   * @param options ファイル取得時のオプションです。
   * @returns ファイルのハンドル（操作を可能にする参照）を行うクラスのインスタンスです。
   */
  @asyncmux
  public async getFileHandle(name: FileNameLike, options: GetFileOptions): Promise<FileHandle> {
    return await this.#getFileHandle(asFileName(name), options);
  }

  /**
   * @see {@link getDirectoryHandle}
   */
  async #getDirectoryHandle(
    name: DirectoryName,
    options: GetDirectoryOptions,
  ): Promise<DirectoryHandle> {
    const dirPath = path.join(this.path, name);
    if (options.create) {
      await fsp.mkdir(dirPath, {
        recursive: true, // すでに作成済みのときエラーを投げないために再帰的な作成を許可します。
      });
    } else {
      await assertDirectoryExists(dirPath);
    }

    return new DirectoryHandle(dirPath);
  }

  /**
   * 指定した名前のディレクトリーハンドルを取得します。
   *
   * @param name 取得するディレクトリーの名前です。
   * @param options ディレクトリー取得時のオプションです。
   * @returns ディレクトリーのハンドル（操作を可能にする参照）を行うクラスのインスタンスです。
   */
  @asyncmux
  public async getDirectoryHandle(
    name: DirectoryNameLike,
    options: GetDirectoryOptions,
  ): Promise<DirectoryHandle> {
    return await this.#getDirectoryHandle(asDirectoryName(name), options);
  }
}

/**
 * ファイルシステムを操作するための基本的な機能を提供するクラスです。
 * ファイルシステムの接続、切断、およびディレクトリーへのアクセスを可能にします。
 */
export default class LocalFileSystem implements IFileSystem {
  /**
   * ルートディレクトリーです。
   */
  readonly #rootDir: string;

  /**
   * 接続が閉じているか管理するフラグです。
   */
  #closed: boolean;

  /**
   * `NodeFs` の新しいインスタンスを構築します。
   *
   * @param rootDir 操作の基準となるルートディレクトリーのパスです。デフォルトは現在のディレクトリです。
   */
  public constructor(rootDir: string | undefined = "") {
    rootDir = path.normalize(path.resolve(rootDir));
    if (rootDir.endsWith(path.sep)) {
      rootDir = rootDir.slice(0, -path.sep.length);
    }

    this.#rootDir = rootDir;
    this.#closed = true;
  }

  /**
   * ファイルシステムへの接続を開きます。
   */
  @asyncmux
  public async open(): Promise<void> {
    if (!this.#closed) {
      return;
    }

    await fsp.mkdir(this.#rootDir, { recursive: true });
    this.#closed = false;
  }

  /**
   * ファイルシステムへの接続を閉じます。
   */
  @asyncmux
  public async close(): Promise<void> {
    this.#closed = true;
  }

  /**
   * @see {@link getDirectoryHandle}
   */
  async #getDirectoryHandle(
    name: DirectoryName,
    options: GetDirectoryOptions,
  ) {
    const dirPath = path.join(this.#rootDir, name);
    if (options.create) {
      await fsp.mkdir(dirPath, { recursive: true });
    } else {
      await assertDirectoryExists(dirPath);
    }

    return new DirectoryHandle(dirPath);
  }

  /**
   * ルートディレクトリーを基準に、指定した名前のディレクトリーハンドルを取得します。
   *
   * @param name 取得するディレクトリーの名前です。
   * @param options ディレクトリー取得時のオプションです。
   * @returns ディレクトリーのハンドル（操作を可能にする参照）を行うクラスのインスタンスです。
   */
  @asyncmux.readonly
  public async getDirectoryHandle(
    name: DirectoryNameLike,
    options: GetDirectoryOptions,
  ): Promise<DirectoryHandle> {
    if (this.#closed) {
      throw new FileSystemNotOpenError();
    }

    return await this.#getDirectoryHandle(asDirectoryName(name), options);
  }
}
