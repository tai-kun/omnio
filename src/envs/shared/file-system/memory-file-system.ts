import { type Asyncmux, asyncmux, type AsyncmuxLock } from "asyncmux";
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
 * ファイルの JSON 表現です。
 */
export type FileJson = {
  /**
   * ファイルデータの全チャンクです。
   */
  chunks: number[][];
};

/**
 * ディレクトリーの JSON 表現です。
 */
export type DirectoryJson = {
  [name: string]: FileJson | DirectoryJson;
};

/**
 * ファイルシステムです。
 */
type Root = Readonly<{
  /**
   * ルートディレクトリーです。
   */
  dir: Directory;

  /**
   * ファイルシステムの接続が閉じているかを示すフラグです。
   */
  closed: boolean;
}>;

/**
 * データを格納しているファイルを表すクラスです。
 */
class File {
  /**
   * ファイル名です。
   */
  readonly #fileName: EntryName;

  /**
   * ファイルを作成した時刻です。
   */
  readonly #timestamp: number;

  /**
   * ファイルデータの全チャンクです。
   */
  readonly chunks: WriteChunkType[];

  /**
   * `File` の新しいインスタンスを構築します。
   *
   * @param chunks ファイルデータの全チャンクです。
   * @param fileName ファイル名です。
   */
  public constructor(chunks: readonly WriteChunkType[], fileName: EntryName) {
    this.#fileName = fileName;
    this.#timestamp = Date.now();
    this.chunks = chunks.slice();
  }

  /**
   * ファイルオブジェクトを複製します。
   *
   * @param fileName 新しいファイル名です。
   * @returns 新しいファイルオブジェクトです。
   */
  public clone(fileName: EntryName): File {
    return new File(this.chunks, fileName);
  }

  /**
   * 新しいファイルオブジェクトを取得します。チャンクデータはすべてコピーされます。
   *
   * @returns ファイルオブジェクトです。
   */
  public getFile(): globalThis.File {
    return new globalThis.File(this.chunks.map(chunk => chunk.slice()), this.#fileName, {
      lastModified: this.#timestamp,
    });
  }

  /**
   * `File` を JSON 形式に変換します。テストで使用されることを想定しています。
   *
   * @return JSON 形式の `File` です。
   */
  public toJSON(): FileJson {
    return {
      chunks: this.chunks.map(chunk => Array.from(chunk)),
    };
  }
}

/**
 * ファイルを保存しておくディレクトリーです。
 */
class Directory extends Map<EntryName, Directory | File> {
  /**
   * `Directory` を JSON 形式に変換します。テストで使用されることを想定しています。
   *
   * @return JSON 形式の `Directory` です。
   */
  public toJSON(): DirectoryJson {
    return Object.fromEntries(this.entries().map(([name, entry]) => [name, entry.toJSON()]));
  }
}

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
 * ファイル名を一時ファイルの名前に変換します。
 *
 * @param name 一時的ファイルの名前です。
 * @returns 一時ファイルの名前です。
 */
function toTempEntryName(name: FileNameLike): EntryName {
  return asEntryName(name + TEMP_FILE_EXT);
}

/**
 * パスを指定してディレクトリーからエントリーを取得します。
 *
 * @param dir ルートディレクトリーです。
 * @param path エントリーへのパスです。
 * @returns エントリーです。
 */
function getEntry(dir: Directory, path: readonly EntryName[]): Directory | File | null {
  let entry: Directory | File | undefined;
  for (let i = 0; i < path.length; i++) {
    const name = path[i]!;
    entry = dir.get(name);
    if (entry instanceof Directory) {
      dir = entry;
    } else if (i < path.length - 1) {
      return null; // 最後まで到達していなければ存在しないとして null を返します。
    } else {
      return entry ?? null;
    }
  }

  return entry ?? dir;
}

/**
 * ファイルストリームへの書き込みを行うクラスです。
 */
class WritableFileStream implements IWritableFileStream {
  /**
   * ファイルシステムです。
   */
  readonly #root: Root;

  /**
   * ディレクトリーへのパスです。
   */
  readonly #dirPath: readonly EntryName[];

  /**
   * 書き込み先のファイル名です。
   */
  readonly #target: EntryName;

  /**
   * 書き込みが完了するまでの一時的なファイル名です。
   */
  readonly #crswap: EntryName;

  /**
   * スタックするチャンクデータです。
   */
  readonly #chunks: WriteChunkType[];

  /**
   * 獲得したロックを解除するためのオブジェクトです。
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
   * `WritableFileStream` の新しいインスタンスを構築します。
   *
   * @param rootDir ルートディレクトリーです。
   * @param dirPath ディレクトリーへのパスです。
   * @param target 書き込み先のファイル名です。
   * @param crswap 書き込みが完了するまでの一時的なファイル名です。
   * @param chunks スタックするチャンクデータです。
   * @param asyncmux 書き込みロックを獲得した結果です。
   */
  public constructor(
    root: Root,
    dirPath: readonly EntryName[],
    target: FileName,
    crswap: EntryName,
    chunks: WriteChunkType[],
    lock: AsyncmuxLock,
  ) {
    this.#root = root;
    this.#dirPath = dirPath;
    this.#target = target;
    this.#crswap = crswap;
    this.#chunks = chunks;
    this.#lock = lock;
    this.#closed = false;
    this.#abortReason = undefined;
  }

  /**
   * データをファイルに書き込みます。
   *
   * @param data 書き込むデータです。
   * @returns ストリームに書き込まれたバイト数です。
   */
  public write(data: WriteChunkType): void {
    if (this.#closed) {
      throw this.#abortReason;
    }
    if (this.#root.closed) {
      throw new FileSystemNotOpenError();
    }

    switch (true) {
      case isUint8Array(data):
        this.#chunks.push(data);
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
  public abort(reason?: unknown): void {
    if (this.#closed) {
      throw this.#abortReason;
    }

    try {
      this.#abortReason = reason;
      const parentPath = this.#dirPath;
      const parent = getEntry(this.#root.dir, parentPath);
      if (!(parent instanceof Directory)) {
        throw new EntryPathNotFoundError(parentPath.join("/"));
      }

      parent.delete(this.#crswap);
    } finally {
      this.#closed = true;
      this.#lock.unlock();
    }
  }

  /**
   * ストリームへの書き込みを完了し、閉じます。
   */
  public close(): void {
    if (this.#closed) {
      throw this.#abortReason;
    }

    try {
      const parentPath = this.#dirPath;
      const parent = getEntry(this.#root.dir, this.#dirPath);
      if (!(parent instanceof Directory)) {
        throw new EntryPathNotFoundError(parentPath.join("/"));
      }

      const crswap = parent.get(this.#crswap);
      if (!(crswap instanceof File)) {
        throw new EntryPathNotFoundError([...parentPath, this.#crswap].join("/"));
      }

      parent.delete(this.#crswap);
      const target = crswap.clone(this.#target);
      parent.set(this.#target, target);
    } finally {
      this.#closed = true;
      this.#lock.unlock();
    }
  }
}

/**
 * ファイルのハンドル（操作を可能にする参照）です。読み書き込み可能なストリームの作成を可能にします。
 */
class FileHandle implements IFileHandle {
  /**
   * ファイルシステムです。
   */
  readonly #root: Root;

  /**
   * ディレクトリーへのパスです。
   */
  readonly #dirPath: readonly EntryName[];

  /**
   * 排他制御のためのキューを管理するオブジェクトです。
   */
  readonly #mux: Asyncmux;

  /**
   * ファイル名です。
   */
  readonly #name: FileName;

  /**
   * `FileHandle` の新しいインスタンスを構築します。
   *
   * @param root ファイルシステムです。
   * @param dirPath ディレクトリーへのパスです。
   * @param mux 排他制御のためのキューを管理するオブジェクトです。
   * @param name ファイル名です。
   */
  public constructor(root: Root, dirPath: readonly EntryName[], mux: Asyncmux, name: FileName) {
    this.#root = root;
    this.#dirPath = dirPath;
    this.#mux = mux;
    this.#name = name;
  }

  /**
   * ファイルの内容を取得します。
   *
   * @returns `File` オブジェクトです。
   */
  @asyncmux
  public async getFile(): Promise<globalThis.File> {
    using _lock = await this.#mux.rLock([...this.#dirPath, this.#name].join("/"));
    if (this.#root.closed) {
      throw new FileSystemNotOpenError();
    }

    const filePath = [...this.#dirPath, this.#name];
    const file = getEntry(this.#root.dir, filePath);
    if (!(file instanceof File)) {
      throw new EntryPathNotFoundError(filePath.join("/"));
    }

    return file.getFile();
  }

  /**
   * ファイルへの書き込みが可能なストリームを作成します。
   *
   * @param options ファイルへの書き込みが可能なストリームを作成する際のオプションです。
   * @returns ファイルストリームへの書き込みを行うクラスのインスタンスです。
   */
  @asyncmux
  public async createWritable(options: CreateWritableOptions): Promise<WritableFileStream> {
    const { keepExistingData } = options;
    const lk = await this.#mux.lock([...this.#dirPath, this.#name].join("/"));
    try {
      if (this.#root.closed) {
        throw new FileSystemNotOpenError();
      }

      const parentPath = this.#dirPath;
      const parent = getEntry(this.#root.dir, parentPath);
      if (!(parent instanceof Directory)) {
        throw new EntryPathNotFoundError(parentPath.join("/"));
      }

      const targetName = this.#name;
      const targetPath = [...this.#dirPath, targetName];
      const targetFile = getEntry(this.#root.dir, targetPath);
      if (targetFile !== null && !(targetFile instanceof File)) {
        throw new EntryPathNotFoundError(targetPath.join("/"));
      }

      const crswapName = toTempEntryName(this.#name);
      const crswapPath = [...this.#dirPath, crswapName];
      if (getEntry(this.#root.dir, crswapPath) !== null) {
        throw new EntryPathNotFoundError(targetPath.join("/"));
      }

      const sourceFile = keepExistingData ? targetFile?.clone(crswapName) : null;
      const crswapFile = sourceFile ?? new File([], crswapName);
      parent.set(crswapName, crswapFile);
      const { chunks } = crswapFile;

      return new WritableFileStream(this.#root, this.#dirPath, targetName, crswapName, chunks, lk);
    } catch (ex) {
      lk.unlock();
      throw ex;
    }
  }
}

/**
 * ディレクトリーのハンドル（操作を可能にする参照）です。 ファイルやサブディレクトリーへのアクセスを可能にします。
 */
class DirectoryHandle implements IDirectoryHandle {
  /**
   * ファイルシステムです。
   */
  readonly #root: Root;

  /**
   * ディレクトリーへのパスです。
   */
  readonly #dirPath: readonly EntryName[];

  /**
   * 排他制御のためのキューを管理するオブジェクトです。
   */
  readonly #mux: Asyncmux;

  /**
   * ディレクトリー名です。
   */
  readonly #name: DirectoryName;

  /**
   * `DirectoryHandle` の新しいインスタンスを構築します。
   *
   * @param root ファイルシステムです。
   * @param dirPath ディレクトリーへのパスです。
   * @param mux 排他制御のためのキューを管理するオブジェクトです。
   * @param name ディレクトリー名です。
   */
  public constructor(
    root: Root,
    dirPath: readonly EntryName[],
    mux: Asyncmux,
    name: DirectoryName,
  ) {
    this.#root = root;
    this.#dirPath = dirPath;
    this.#mux = mux;
    this.#name = name;
  }

  /**
   * @see {@link removeEntry}
   */
  #removeEntry(name: EntryName, { recursive }: RemoveOptions): void {
    const currentPath = [...this.#dirPath, this.#name];
    const current = getEntry(this.#root.dir, currentPath);
    if (!(current instanceof Directory)) {
      throw new EntryPathNotFoundError(currentPath.join("/"));
    }

    const entryPath = [...currentPath, name];
    const entry = getEntry(current, [name]);
    if (entry === null) {
      throw new EntryPathNotFoundError(entryPath.join("/"));
    }
    if (
      entry instanceof File
      // ディレクトリーの場合、再起削除が有効か、空ディレクトリーのときのみ削除できます。
      || recursive || entry.size === 0
    ) {
      current.delete(name);
    } else {
      throw new globalThis.Error("Cannot remove entry: " + entryPath.join("/"));
    }
  }

  /**
   * ディレクトリー直下から指定のアイテムを削除します。
   *
   * @param name 削除するアイテムの名前です。
   * @param options 削除時のオプションです。
   */
  public removeEntry(name: EntryNameLike, options: RemoveOptions): void {
    if (this.#root.closed) {
      throw new FileSystemNotOpenError();
    }

    this.#removeEntry(asEntryName(name), options);
  }

  /**
   * @see {@link getFileHandle}
   */
  #getFileHandle(name: FileName, { create }: GetFileOptions): FileHandle {
    const currentPath = [...this.#dirPath, this.#name];
    const current = getEntry(this.#root.dir, currentPath);
    if (!(current instanceof Directory)) {
      throw new EntryPathNotFoundError(currentPath.join("/"));
    }

    const entryPath = [...currentPath, name];
    const entry = getEntry(current, [name]);
    if (entry === null) {
      if (create) {
        current.set(name, new File([], name));
      } else {
        throw new EntryPathNotFoundError(entryPath.join("/"));
      }
    } else if (entry instanceof Directory) {
      throw new EntryPathNotFoundError(entryPath.join("/"));
    }

    return new FileHandle(this.#root, currentPath, this.#mux, name);
  }

  /**
   * 指定した名前のファイルハンドルを取得します。
   *
   * @param name 取得するファイルの名前です。
   * @param options ファイル取得時のオプションです。
   * @returns ファイルのハンドル（操作を可能にする参照）を行うクラスのインスタンスです。
   */
  public getFileHandle(name: FileNameLike, options: GetFileOptions): FileHandle {
    if (this.#root.closed) {
      throw new FileSystemNotOpenError();
    }

    return this.#getFileHandle(asFileName(name), options);
  }

  /**
   * @see {@link getDirectoryHandle}
   */
  #getDirectoryHandle(name: DirectoryName, { create }: GetDirectoryOptions): DirectoryHandle {
    const currentPath = [...this.#dirPath, this.#name];
    const current = getEntry(this.#root.dir, currentPath);
    if (!(current instanceof Directory)) {
      throw new EntryPathNotFoundError(currentPath.join("/"));
    }

    const entryPath = [...currentPath, name];
    const entry = getEntry(current, [name]);
    if (entry === null) {
      if (create) {
        current.set(name, new Directory());
      } else {
        throw new EntryPathNotFoundError(entryPath.join("/"));
      }
    } else if (entry instanceof File) {
      throw new EntryPathNotFoundError(entryPath.join("/"));
    }

    return new DirectoryHandle(this.#root, currentPath, this.#mux, name);
  }

  /**
   * 指定した名前のディレクトリーハンドルを取得します。
   *
   * @param name 取得するディレクトリーの名前です。
   * @param options ディレクトリー取得時のオプションです。
   * @returns ディレクトリーのハンドル（操作を可能にする参照）を行うクラスのインスタンスです。
   */
  public getDirectoryHandle(
    name: DirectoryNameLike,
    options: GetDirectoryOptions,
  ): DirectoryHandle {
    if (this.#root.closed) {
      throw new FileSystemNotOpenError();
    }

    return this.#getDirectoryHandle(asDirectoryName(name), options);
  }
}

/**
 * ファイルシステムを操作するための基本的な機能を提供するクラスです。
 * ファイルシステムの接続、切断、およびディレクトリーへのアクセスを可能にします。
 */
export default class MemoryFileSystem implements IFileSystem {
  /**
   * ルートディレクトリーです。
   */
  readonly #rootDir: Directory;

  /**
   * ディレクトリーへのパスです。
   */
  readonly #dirPath: readonly EntryName[];

  /**
   * 排他制御のためのキューを管理するオブジェクトです。
   */
  readonly #mux: Asyncmux;

  /**
   * 接続が閉じているか管理するフラグです。
   */
  #closed: boolean;

  /**
   * `MemoryFileSystem` の新しいインスタンスを構築します。
   */
  public constructor() {
    this.#rootDir = new Directory();
    this.#dirPath = [];
    this.#mux = asyncmux.create();
    this.#closed = true;
  }

  public tree(): DirectoryJson {
    return this.#rootDir.toJSON();
  }

  /**
   * ファイルシステムへの接続を開きます。
   */
  public open(): void {
    this.#closed = false;
  }

  /**
   * ファイルシステムへの接続を閉じます。
   */
  public close(): void {
    this.#closed = true;
  }

  /**
   * @see {@link getDirectoryHandle}
   */
  #getDirectoryHandle(name: DirectoryName, { create }: GetDirectoryOptions): DirectoryHandle {
    const currentPath = this.#dirPath;
    const current = getEntry(this.#rootDir, currentPath);
    if (!(current instanceof Directory)) {
      throw new EntryPathNotFoundError(currentPath.join("/"));
    }

    const entryPath = [...currentPath, name];
    const entry = getEntry(current, [name]);
    if (entry === null) {
      if (create) {
        current.set(name, new Directory());
      } else {
        throw new EntryPathNotFoundError(entryPath.join("/"));
      }
    } else if (entry instanceof File) {
      throw new EntryPathNotFoundError(entryPath.join("/"));
    }

    const root = { dir: this.#rootDir };
    Object.defineProperty(root, "closed", { get: () => this.#closed });

    return new DirectoryHandle(root as Root, currentPath, this.#mux, name);
  }

  /**
   * ルートディレクトリーを基準に、指定した名前のディレクトリーハンドルを取得します。
   *
   * @param name 取得するディレクトリーの名前です。
   * @param options ディレクトリー取得時のオプションです。
   * @returns ディレクトリーのハンドル（操作を可能にする参照）を行うクラスのインスタンスです。
   */
  public getDirectoryHandle(
    name: DirectoryNameLike,
    options: GetDirectoryOptions,
  ): DirectoryHandle {
    if (this.#closed) {
      throw new FileSystemNotOpenError();
    }

    return this.#getDirectoryHandle(asDirectoryName(name), options);
  }
}
