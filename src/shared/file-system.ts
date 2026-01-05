import type {
  FileSystemDirectoryNameLike as DirectoryNameLike,
  FileSystemEntryNameLike as EntryNameLike,
  FileSystemFileNameLike as FileNameLike,
} from "./schemas.js";
import type { Awaitable } from "./type-utils.js";

/**
 * ファイルストリームへ書き込むデータの型です。
 */
export type WriteChunkType = Uint8Array<ArrayBuffer>;

/**
 * 書き込み可能なファイルストリームを表すインターフェースです。
 */
export interface IWritableFileStream {
  /**
   * ストリームへデータを非同期で書き込みます。
   *
   * @param data 書き込むデータです。
   * @returns ストリームに書き込まれたバイト数です。
   */
  write(data: WriteChunkType): Awaitable<void>;

  /**
   * ストリームへの書き込みを中断し、閉じます。
   *
   * @param reason 中断の理由です。
   */
  abort(reason?: unknown): Awaitable<void>;

  /**
   * ストリームへの書き込みを完了し、閉じます。
   */
  close(): Awaitable<void>;
}

/**
 * ファイルへの書き込みが可能なストリームを作成する際のオプションです。
 */
export type CreateWritableOptions = Readonly<{
  /**
   * `false` である場合、一時ファイルは空に初期化されます。そうでない場合、既存のファイルがまず一時ファイルにコピーされます。
   */
  keepExistingData: boolean;
}>;

/**
 * ファイルのハンドル（操作を可能にする参照）を表すインターフェースです。読み書き込み可能なストリームの作成を可能にします。
 */
export interface IFileHandle {
  /**
   * ファイルの内容を取得します。
   *
   * @returns ファイルを表す `File` オブジェクトを返します。
   */
  getFile(): Awaitable<File>;

  /**
   * ファイルへの書き込みが可能なストリームを作成します。
   *
   * @param options ファイルへの書き込みが可能なストリームを作成する際のオプションです。
   * @returns 書き込み可能な `IWritableFileStream` オブジェクトを返します。
   */
  createWritable(options: CreateWritableOptions): Awaitable<IWritableFileStream>;
}

/**
 * 削除時のオプションです。
 */
export type RemoveOptions = Readonly<{
  /**
   * エントリーを再帰的に削除するかどうかを指定します。
   */
  recursive: boolean;
}>;

/**
 * ファイル取得時のオプションです。
 */
export type GetFileOptions = Readonly<{
  /**
   * ファイルが存在しない場合に新しく作成するかどうかを指定します。
   * `true` に設定した場合、ファイルが存在しないと作成され、`false` の場合はエラーが投げられます。
   */
  create: boolean;
}>;

/**
 * ディレクトリー取得時のオプションです。
 */
export type GetDirectoryOptions = Readonly<{
  /**
   * ディレクトリーが存在しない場合に新しく作成するかどうかを指定します。
   * `true` に設定した場合、ディレクトリーが存在しないと作成され、`false` の場合はエラーが投げられます。
   */
  create: boolean;
}>;

/**
 * ディレクトリーのハンドル（操作を可能にする参照）を表すインターフェースです。
 * ファイルやサブディレクトリーへのアクセスを可能にします。
 */
export interface IDirectoryHandle {
  /**
   * ディレクトリー直下から指定のアイテムを削除します。
   *
   * @param name 削除するアイテムの名前です。
   * @param options 削除時のオプションです。
   */
  removeEntry(name: EntryNameLike, options: RemoveOptions): Awaitable<void>;

  /**
   * 指定した名前のファイルハンドルを取得します。
   *
   * @param name 取得するファイルの名前です。
   * @param options ファイル取得時のオプションです。
   * @returns ファイルハンドルを表す `IFileHandle` オブジェクトを返します。
   */
  getFileHandle(name: FileNameLike, options: GetFileOptions): Awaitable<IFileHandle>;

  /**
   * 指定した名前のディレクトリーハンドルを取得します。
   *
   * @param name 取得するディレクトリーの名前です。
   * @param options ディレクトリー取得時のオプションです。
   * @returns ディレクトリーハンドルを表す `IDirectoryHandle` オブジェクトを返します。
   */
  getDirectoryHandle(
    name: DirectoryNameLike,
    options: GetDirectoryOptions,
  ): Awaitable<IDirectoryHandle>;
}

/**
 * ファイルシステムを操作するための基本的な機能を提供するインターフェースです。
 * ファイルシステムの接続、切断、およびディレクトリーへのアクセスを可能にします。
 */
export interface IFileSystem {
  /**
   * ファイルシステムへの接続を開きます。
   */
  open(): Awaitable<void>;

  /**
   * ファイルシステムへの接続を閉じます。
   */
  close(): Awaitable<void>;

  /**
   * 指定した名前のディレクトリーハンドルを取得します。
   *
   * @param name 取得するディレクトリーの名前です。
   * @param options ディレクトリー取得時のオプションです。
   * @returns ディレクトリーハンドルを表す `IDirectoryHandle` オブジェクトを返します。
   */
  getDirectoryHandle(
    name: DirectoryNameLike,
    options: GetDirectoryOptions,
  ): Awaitable<IDirectoryHandle>;
}
