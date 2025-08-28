import * as buffer from "node:buffer";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import * as v from "valibot";
import { FsPathNotFoundError, NodeFsError, TypeError } from "../errors.js";
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
  WritableFileStream,
  WriteChunkType,
} from "./fs.types.js";

export type * from "./fs.types.js";

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
 * @param path ファイルパスです。
 */
async function assertFileExists(path: string): Promise<void> {
  try {
    const stats = await fsp.stat(path);
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
 * @param path ディレクトリーパスです。
 */
async function assertDirectoryExists(path: string): Promise<void> {
  try {
    const stats = await fsp.stat(path);
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
 * ファイルストリームへの書き込みを行うクラスです。Node.js のファイルシステムを使用します。
 */
export class NodeFsWritableFileStream implements WritableFileStream {
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
   * ストリームに書き込まれたバイト数です。
   */
  #bytesWritten: v.InferOutput<typeof schemas.UnsignedInteger>;

  /**
   * `NodeFsWritableFileStream` の新しいインスタンスを構築します。
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

      case data instanceof NodeFsFileHandle: {
        const reader = fs.createReadStream(data.filePath, { highWaterMark: 4096 });
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
            reader.close();
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
        await fsp.unlink(this.#crswapPath);
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
export class NodeFsFileHandle implements FileHandle {
  /**
   * ファイルの絶対パスです。
   */
  public readonly filePath: string;

  /**
   * `NodeFsFileHandle` の新しいインスタンスを構築します。
   *
   * @param filePath ファイルの絶対パスです。
   */
  public constructor(filePath: string) {
    this.filePath = filePath;
  }

  /**
   * ファイルの内容を取得します。
   *
   * @returns Node.js の `buffer.File` オブジェクトです。
   */
  @mutex
  public async getFile(): Promise<File> {
    const stat = await fsp.stat(this.filePath);
    const lastMod = stat.mtime.getTime();
    const content = await fsp.readFile(this.filePath);
    // ブラウザーの `File` API をエミュレートする簡潔な実装です。
    // `File` クラスはブラウザー環境に固有のため、ここでは Node.js の buffer.File を使用しています。
    const nodeFile = new buffer.File([content], path.basename(this.filePath), {
      lastModified: lastMod,
    });

    return nodeFile as File;
  }

  /**
   * ファイルへの書き込みが可能なストリームを作成します。
   *
   * @returns ファイルストリームへの書き込みを行うクラスのインスタンスです。
   */
  @mutex
  public async createWritable(): Promise<NodeFsWritableFileStream> {
    const targetPath = this.filePath;
    const crswapPath = this.filePath + ".crswap";
    const fileHandle = await fsp.open(crswapPath, "w");

    return new NodeFsWritableFileStream(targetPath, crswapPath, fileHandle);
  }
}

/**
 * ディレクトリーのハンドル（操作を可能にする参照）を行うクラスです。ファイルやサブディレクトリーへのアクセスを可能にします。
 */
export class NodeFsDirectoryHandle implements DirectoryHandle {
  /**
   * ディレクトリーの絶対パスです。
   */
  readonly #dirPath: string;

  /**
   * `NodeFsDirectoryHandle` の新しいインスタンスを構築します。
   *
   * @param dirPath ディレクトリーの絶対パスです。
   */
  public constructor(dirPath: string) {
    this.#dirPath = dirPath;
  }

  /**
   * ディレクトリー直下から指定のアイテムを削除します。
   *
   * @param name 削除するアイテムの名前です。
   */
  @mutex
  public async removeEntry(name: string): Promise<void> {
    const filePath = path.join(this.#dirPath, name);
    await assertFileExists(filePath);
    await fsp.unlink(filePath);
  }

  /**
   * 指定した名前のファイルハンドルを取得します。
   *
   * @param name 取得するファイルの名前です。
   * @param options ファイル取得時のオプションです。
   * @returns ファイルのハンドル（操作を可能にする参照）を行うクラスのインスタンスです。
   */
  @mutex
  public async getFileHandle(name: string, options: GetFileOptions): Promise<NodeFsFileHandle> {
    const filePath = path.join(this.#dirPath, name);
    if (options.create) {
      await fsp.writeFile(filePath, "");
    } else {
      await assertFileExists(filePath);
    }

    return new NodeFsFileHandle(filePath);
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
  ): Promise<NodeFsDirectoryHandle> {
    const dirPath = path.join(this.#dirPath, name);
    if (options.create) {
      await fsp.mkdir(dirPath, {
        recursive: true, // すでに作成済みのときエラーを投げないために再帰的な作成を許可します。
      });
    } else {
      await assertDirectoryExists(dirPath);
    }

    return new NodeFsDirectoryHandle(dirPath);
  }
}

/**
 * ファイルパスを編集するためのユーティリティーです。
 */
export class NodeFsPath implements Path {
  /**
   * ファイルシステム操作のルートディレクトリーです。必ずセパレーターで終わります。
   */
  readonly #root: string;

  /**
   * `NodeFsPath` の新しいインスタンスを構築します。
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
    let resolved = path.resolve(this.#root, paths.join(path.sep));
    if (resolved.endsWith(path.sep)) {
      resolved = resolved.slice(0, -path.sep.length);
    }
    if (resolved + path.sep === this.#root) {
      return this.#root;
    }
    if (!resolved.startsWith(this.#root)) {
      throw new NodeFsError(
        `Cannot resolve path: Not starts with root (${this.#root}): ${resolved}`,
      );
    }

    return resolved;
  }
}

/**
 * ファイルシステムを操作するための基本的な機能を提供するクラスです。
 * ファイルシステムの接続、切断、およびディレクトリーへのアクセスを可能にします。
 */
export class NodeFs implements Fs {
  /**
   * ファイルシステム操作のルートディレクトリーです。必ずセパレーターで終わります。
   */
  readonly root: string;

  /**
   * ファイルパスを編集するためのユーティリティーです。
   */
  readonly path: NodeFsPath;

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
    let root = path.resolve(rootDir);
    if (!root.endsWith(path.sep)) {
      root += path.sep;
    }

    this.root = root;
    this.path = new NodeFsPath(this.root);
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

    await fsp.mkdir(this.root, { recursive: true });
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
   * ファイルをコピーします。
   *
   * @param sourceFileHandle コピー元のファイルハンドラーです。
   * @param destinationFileHandle コピー先のファイルハンドラーです。
   */
  @mutex
  public async copyFile(
    sourceFileHandle: NodeFsFileHandle,
    destinationFileHandle: NodeFsFileHandle,
  ): Promise<void> {
    await fsp.copyFile(sourceFileHandle.filePath, destinationFileHandle.filePath);
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
  ): Promise<NodeFsDirectoryHandle> {
    if (this.#closed) {
      throw new NodeFsError("Not open");
    }

    const dirPath = path.join(this.root, name);
    if (options.create) {
      await fsp.mkdir(dirPath, { recursive: true });
    } else {
      await assertDirectoryExists(dirPath);
    }

    return new NodeFsDirectoryHandle(dirPath);
  }
}
