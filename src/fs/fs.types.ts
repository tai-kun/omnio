import type * as v from "valibot";
import type * as schemas from "../schemas.js";
import type { Awaitable } from "../type-utils.js";

/**
 * ファイルストリームへ書き込むデータの型です。
 */
export type WriteChunkType = Uint8Array<ArrayBuffer> | FileHandle;

/**
 * 書き込み可能なファイルストリームを表すインターフェースです。
 */
export interface WritableFileStream {
  /**
   * ストリームに書き込まれたバイト数です。
   */
  readonly bytesWritten: v.InferOutput<typeof schemas.UnsignedInteger>;

  /**
   * ストリームへデータを非同期で書き込みます。
   *
   * @param data 書き込むデータです。
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
 * ファイルのハンドル（操作を可能にする参照）を表すインターフェースです。
 * ファイルへのアクセスや、書き込み可能なストリームの作成を可能にします。
 */
export interface FileHandle {
  /**
   * ファイルの内容を取得します。
   *
   * @returns ファイルを表す `File` オブジェクトを返します。
   */
  getFile(): Awaitable<File>;

  /**
   * ファイルへの書き込みが可能なストリームを作成します。
   *
   * @returns 書き込み可能な `WritableFileStream` オブジェクトを返します。
   */
  createWritable(): Awaitable<WritableFileStream>;
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
export interface DirectoryHandle {
  /**
   * ディレクトリー直下から指定のアイテムを削除します。
   *
   * @param name 削除するアイテムの名前です。
   * @param options 削除時のオプションです。
   */
  removeEntry(name: string, options: RemoveOptions): Awaitable<void>;

  /**
   * 指定した名前のファイルハンドルを取得します。
   *
   * @param name 取得するファイルの名前です。
   * @param options ファイル取得時のオプションです。
   * @returns ファイルハンドルを表す `FileHandle` オブジェクトを返します。
   */
  getFileHandle(name: string, options: GetFileOptions): Awaitable<FileHandle>;

  /**
   * 指定した名前のディレクトリーハンドルを取得します。
   *
   * @param name 取得するディレクトリーの名前です。
   * @param options ディレクトリー取得時のオプションです。
   * @returns ディレクトリーハンドルを表す `DirectoryHandle` オブジェクトを返します。
   */
  getDirectoryHandle(name: string, options: GetDirectoryOptions): Awaitable<DirectoryHandle>;
}

/**
 * ファイルパスを編集するためのユーティリティーです。
 */
export interface Path {
  /**
   * 絶対パスに解決します。
   *
   * @param paths 結合するパスです。単純にパスセパレーターで結合されます。
   * @returns 絶対パスに解決されたパスです。
   */
  resolve(...paths: string[]): string;
}

/**
 * ファイルシステムを操作するための基本的な機能を提供するインターフェースです。
 * ファイルシステムの接続、切断、およびディレクトリーへのアクセスを可能にします。
 */
export interface Fs {
  /**
   * ファイルシステム操作のルートディレクトリーです。必ずセパレーターで終わります。
   */
  readonly root: string;

  /**
   * ファイルパスを編集するためのユーティリティーです。
   */
  readonly path: Path;

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
   * @returns ディレクトリーハンドルを表す `DirectoryHandle` オブジェクトを返します。
   */
  getDirectoryHandle(name: string, options: GetDirectoryOptions): Awaitable<DirectoryHandle>;
}
