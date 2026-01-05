import type { IJson } from "../../../core/metadata.js";
import type { ISetupFunction } from "../../../core/omnio.js";
import type { ILogger } from "../../../shared/logger.js";
import type { BucketNameLike } from "../../../shared/schemas.js";
import type { ITextSearch } from "../../../shared/text-search.js";
import MemoryFileSystem from "../../shared/file-system/memory-file-system.js";
import DuckdbNodeNeo from "../database/duckdb-node-neo.js";
import getLogger from "./_get-logger.js";

/**
 * `setupOnNodeIoMemoryDbDuckdbNodeNeo` のオプションです。
 */
export type SetupOnNodeIoMemoryDbDuckdbNodeNeoOptions = Readonly<{
  /**
   * バケット名です。
   *
   * @default "omnio"
   */
  bucketName?: BucketNameLike | undefined;

  /**
   * JavaScript の値と JSON 文字列を相互変換するための関数群です。
   */
  json?: IJson | undefined;

  /**
   * Omnio で使用されるロガーです。
   * 内部情報や、ただちにアプリケーションを停止する必要はないものの、記録しておくべきメッセージを通知する際に使用されます。
   */
  logger?: ILogger | undefined;

  /**
   * オブジェクトの説明文の検索に使用する関数群です。
   */
  textSearch?: ITextSearch | undefined;

  /**
   * オブジェクトの説明文の最大サイズ (バイト数) です。
   */
  maxDescriptionTextByteSize?: number | undefined;

  /**
   * ユーザー定義のメタデータの最大サイズ (バイト数) です。
   * このサイズは、ユーザー定義のメタデータを `json.stringify` で変換したあとの文字列に対して計算されます。
   */
  maxUserMetadataJsonByteSize?: number | undefined;
}>;

/**
 * - ランタイム: Node.js
 * - ストレージ: メモリー
 * - データベース: Node (Neo)
 *
 * @param options オプションです。
 * @returns `Omnio` の利用を開始する際に実行されるセットアップ関数です。
 */
export default function setupOnNodeIoMemoryDbDuckdbNodeNeo(
  options: SetupOnNodeIoMemoryDbDuckdbNodeNeoOptions | undefined = {},
): ISetupFunction {
  return async function setupOnNodeIoMemoryDbDuckdbNodeNeoFunction() {
    const {
      json,
      logger,
      bucketName = "omnio",
      textSearch,
      maxDescriptionTextByteSize,
      maxUserMetadataJsonByteSize,
    } = options;
    const fs = new MemoryFileSystem();
    await fs.open();
    const storage = await fs.getDirectoryHandle(bucketName, { create: true });
    const db = new DuckdbNodeNeo(":memory:");
    await db.open();

    async function cleanup() {
      await db.close();
      await fs.close();
    }

    return {
      json,
      logger: getLogger(logger),
      cleanup,
      storage,
      database: db,
      bucketName,
      textSearch,
      maxDescriptionTextByteSize,
      maxUserMetadataJsonByteSize,
    };
  };
}
