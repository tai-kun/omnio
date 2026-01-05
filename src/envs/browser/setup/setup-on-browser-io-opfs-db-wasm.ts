import { VoidLogger } from "@duckdb/duckdb-wasm";
import type { IJson } from "../../../core/metadata.js";
import type { ISetupFunction } from "../../../core/omnio.js";
import type { ILogger } from "../../../shared/logger.js";
import type { BucketNameLike } from "../../../shared/schemas.js";
import type { ITextSearch } from "../../../shared/text-search.js";
import DuckdbWasm, { type DuckdbBundle, type IDuckdbLogger } from "../database/duckdb-wasm.js";
import OriginPrivateFileSystem from "../file-system/origin-private-file-system.js";
import getLogger from "./_get-logger.js";

/**
 * `setupOnBrowserIoOpfsDbDuckdbWasm` のオプションです。
 */
export type SetupOnBrowserIoOpfsDbDuckdbWasmOptions = Readonly<{
  /**
   * DuckDB のロガーです。
   */
  duckdbLogger?: IDuckdbLogger | undefined;

  /**
   * 操作の基準となるルートディレクトリーのパスです。
   *
   * @default "omnio"
   */
  rootDir?: string | undefined;

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
 * - ランタイム: ブラウザー
 * - ストレージ: OPFS
 * - データベース: WASM
 *
 * @param duckdbBundle DuckDB の各種モジュールの情報です。
 * @param options オプションです。
 * @returns `Omnio` の利用を開始する際に実行されるセットアップ関数です。
 */
export default function setupOnBrowserIoOpfsDbDuckdbWasm(
  duckdbBundle: DuckdbBundle,
  options: SetupOnBrowserIoOpfsDbDuckdbWasmOptions | undefined = {},
): ISetupFunction {
  return async function setupOnBrowserIoOpfsDbDuckdbWasmFunction() {
    const {
      json,
      logger,
      rootDir = "omnio",
      bucketName = "omnio",
      textSearch,
      duckdbLogger = new VoidLogger(),
      maxDescriptionTextByteSize,
      maxUserMetadataJsonByteSize,
    } = options;
    const fs = new OriginPrivateFileSystem(rootDir);
    await fs.open();
    const bucket = await fs.getDirectoryHandle(bucketName, { create: true });
    const storage = await bucket.getDirectoryHandle("storage", { create: true });
    const db = new DuckdbWasm(`${bucket.path}/database`, duckdbBundle, duckdbLogger);
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
