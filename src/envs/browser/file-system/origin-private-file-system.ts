import { asyncmux } from "asyncmux";
import {
  EntryPathNotFoundError,
  FileSystemNotOpenError,
  OpfsPermissionStateError,
} from "../../../shared/errors.js";
import type {
  GetDirectoryOptions,
  GetFileOptions,
  IDirectoryHandle,
  IFileHandle,
  IFileSystem,
  RemoveOptions,
} from "../../../shared/file-system.js";
import {
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
  fs: Pick<FileSystemDirectoryHandle, "getFileHandle">,
  path: readonly [...DirectoryName[], FileName],
  options: Readonly<{ create: boolean }>,
): Promise<FileSystemFileHandle> {
  try {
    return await fs.getFileHandle(path[path.length - 1]!, { create: options.create });
  } catch (ex) {
    if (isNotFoundError(ex)) {
      throw new EntryPathNotFoundError(path.join("/"), { cause: ex });
    }

    throw ex;
  }
}

/**
 * ディレクトリーハンドルを取得します。
 *
 * @param fs `FileSystemDirectoryHandle` のインスタンスです。
 * @param path 操作対象のパスです。
 * @param name 取得するディレクトリーの名前です。
 * @param options ディレクトリー作成のオプションです。
 * @returns ディレクトリーハンドルです。
 */
async function getDirectoryHandle(
  fs: Pick<FileSystemDirectoryHandle, "getDirectoryHandle">,
  path: readonly DirectoryName[],
  options: Readonly<{ create: boolean }>,
): Promise<FileSystemDirectoryHandle> {
  try {
    return await fs.getDirectoryHandle(path[path.length - 1]!, { create: options.create });
  } catch (ex) {
    if (isNotFoundError(ex)) {
      throw new EntryPathNotFoundError(path.join("/"), { cause: ex });
    }

    throw ex;
  }
}

/**
 * ディレクトリーのハンドル（操作を可能にする参照）を行うクラスです。OPFS の FileSystemDirectoryHandle を使用します。
 */
class DirectoryHandle implements IDirectoryHandle {
  /**
   * ディレクトリーのパスです。
   */
  readonly #dirPath: readonly DirectoryName[];

  /**
   * ネイティブの FileSystemDirectoryHandle です。
   */
  readonly #native: FileSystemDirectoryHandle;

  /**
   * ディレクトリーの絶対パスです。
   */
  public readonly path: string;

  /**
   * `DirectoryHandle` の新しいインスタンスを構築します。
   *
   * @param dirPath ディレクトリーのパスです。
   * @param native ネイティブの FileSystemDirectoryHandle インスタンスです。
   */
  public constructor(dirPath: readonly DirectoryName[], native: FileSystemDirectoryHandle) {
    this.#dirPath = dirPath;
    this.#native = native;
    this.path = dirPath.join("/");
  }

  /**
   * @see {@link removeEntry}
   */
  async #removeEntry(name: EntryName, options: RemoveOptions): Promise<void> {
    try {
      await this.#native.removeEntry(name, options);
    } catch (ex) {
      if (isNotFoundError(ex)) {
        const entryPath = [...this.#dirPath, name];
        throw new EntryPathNotFoundError(entryPath.join("/"), { cause: ex });
      }

      throw ex;
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
  async #getFileHandle(name: FileName, options: GetFileOptions): Promise<FileSystemFileHandle> {
    try {
      return await this.#native.getFileHandle(name, { create: options.create });
    } catch (ex) {
      if (isNotFoundError(ex)) {
        const filePath = [...this.#dirPath, name];
        throw new EntryPathNotFoundError(filePath.join("/"), { cause: ex });
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
  @asyncmux
  public async getFileHandle(
    name: FileNameLike,
    options: GetFileOptions,
  ): Promise<
    IFileHandle & {
      /**
       * ファイルの絶対パスです。
       */
      readonly path: string;
    }
  > {
    const fileName = asFileName(name);
    const handle = await this.#getFileHandle(fileName, options);
    const path = [...this.#dirPath, fileName].join("/");

    return Object.assign(handle, { path });
  }

  /**
   * @see {@link getDirectoryHandle}
   */
  async #getDirectoryHandle(
    name: DirectoryName,
    options: GetDirectoryOptions,
  ): Promise<DirectoryHandle> {
    const dirPath = [...this.#dirPath, name];
    const nativeHandle = await getDirectoryHandle(this.#native, dirPath, options);

    return new DirectoryHandle(dirPath, nativeHandle);
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
export default class OriginPrivateFileSystem implements IFileSystem {
  /**
   * ルートディレクトリーです。
   */
  readonly #rootDir: readonly DirectoryName[];

  /**
   * ルートディレクトリーのハンドルです。
   */
  #rootHandle: FileSystemDirectoryHandle | null;

  /**
   * 接続が閉じているか管理するフラグです。
   */
  #closed: boolean;

  /**
   * `OriginPrivateFileSystem` の新しいインスタンスを構築します。
   *
   * @param rootDir 操作の基準となるルートディレクトリーのパスです。デフォルトはルートディレクトリです。
   */
  public constructor(rootDir: string | undefined = "") {
    if (rootDir === "" || rootDir === "/") {
      this.#rootDir = [];
    } else {
      if (rootDir[0] === "/") {
        rootDir = rootDir.slice(1);
      }
      if (rootDir[rootDir.length - 1] === "/") {
        rootDir = rootDir.slice(0, -1);
      }

      this.#rootDir = v.parse(v.array(DirectoryNameSchema()), rootDir.split("/"));
    }

    this.#rootHandle = null;
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

    const { state } = await window.navigator.permissions.query({ name: "storage-access" });
    if (state !== "granted") {
      throw new OpfsPermissionStateError(state);
    }

    await window.navigator.storage.persist();
    this.#rootHandle = await window.navigator.storage.getDirectory();
    for (const dirName of this.#rootDir) {
      this.#rootHandle = await this.#rootHandle.getDirectoryHandle(dirName, { create: true });
    }

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
  ): Promise<DirectoryHandle> {
    const dirPath = [...this.#rootDir, name];
    const nativeHandle = await getDirectoryHandle(this.#rootHandle!, dirPath, options);

    return new DirectoryHandle(dirPath, nativeHandle);
  }

  /**
   * ルートディレクトリーを基準に、指定した名前のディレクトリーハンドルを取得します。
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
    if (this.#closed) {
      throw new FileSystemNotOpenError();
    }

    return await this.#getDirectoryHandle(asDirectoryName(name), options);
  }
}
