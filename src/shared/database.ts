import type { Awaitable } from "./type-utils.js";

/**
 * データベースの行を表す型です。
 */
export type Row = {
  [column: string]: unknown;
};

/**
 * SQL ステートメントのインターフェースです。
 */
export interface IStatement {
  /**
   * ステートメントを閉じます。
   */
  close(): Awaitable<void>;

  /**
   * ステートメントを実行します。
   *
   * @param values SQL ステートメントに渡すパラメーターです。
   */
  exec(...values: unknown[]): Awaitable<void>;

  /**
   * ステートメントを実行し、結果の行を取得します。
   *
   * @param values SQL ステートメントに渡すパラメーターです。
   * @returns 結果の行を含む非同期イテレーブルまたはイテレーブルを返します。
   */
  query(...values: unknown[]): Awaitable<AsyncIterable<Row> | Iterable<Row>>;
}

/**
 * データベース接続のインターフェースです。
 */
export interface IDatabase {
  /**
   * 指定されたパスのデータベースを開きます。
   *
   * @returns データベースが開かれた後に解決される Promise を返します。
   */
  open(): Awaitable<void>;

  /**
   * データベース接続を閉じます。
   *
   * @returns データベース接続が閉じられた後に解決される Promise を返します。
   */
  close(): Awaitable<void>;

  /**
   * SQL クエリーを実行します。
   *
   * @param text 実行する SQL クエリーです。
   */
  exec(text: string): Awaitable<void>;

  /**
   * SQL クエリーを実行し、結果の行を取得します。
   *
   * @param text 実行する SQL クエリーです。
   * @returns 結果の行を含む非同期イテレーブルまたはイテレーブルを返します。
   */
  query(text: string): Awaitable<AsyncIterable<Row> | Iterable<Row>>;

  /**
   * SQL ステートメントを準備します。
   *
   * @param text 準備する SQL ステートメントです。
   * @returns 準備された `Statement` オブジェクトを返します。
   */
  prepare(text: string): Awaitable<IStatement>;
}
