import type { GetDirectoryOptions, IDirectoryHandle, RemoveOptions } from "./file-system.js";
import type { Awaitable } from "./type-utils.js";

/**
 * オブジェクトのエンティティーのハンドルを表すインターフェースです。
 */
export interface IEntityHandle extends Pick<IDirectoryHandle, "getFileHandle"> {}

/**
 * オブジェクトを永続化するためのファイルシステムのインターフェースです。
 */
export interface IStorage {
  /**
   * ディレクトリー直下から指定のアイテムを削除します。
   *
   * @param name 削除するアイテムの名前です。
   * @param options 削除時のオプションです。
   */
  removeEntry(name: string, options: RemoveOptions): Awaitable<void>;

  /**
   * 指定した名前のディレクトリーハンドルを取得します。
   *
   * @param name 取得するディレクトリーの名前です。
   * @param options ディレクトリー取得時のオプションです。
   * @returns ディレクトリーハンドルを表す `IEntityHandle` オブジェクトを返します。
   */
  getDirectoryHandle(name: string, options: GetDirectoryOptions): Awaitable<IEntityHandle>;
}
