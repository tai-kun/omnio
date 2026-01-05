import type { Awaitable } from "./type-utils.js";

/**
 * オブジェクトの説明文を検索用の文字列に変換する関数のインターフェースです。
 */
export interface IToTextSearchQueryString {
  /**
   * オブジェクトの説明文を検索用の文字列に変換します。
   *
   * @param string 任意の文字列です。
   * @returns 検索用の文字列です。
   */
  (string: string): Awaitable<string>;
}

/**
 * オブジェクトの検索用の文字列を説明文に変換する関数のインターフェースです。
 */
export interface IFromTextSearchQueryhString {
  /**
   * オブジェクトの検索用の文字列を説明文に変換します。
   *
   * @param string 検索用の文字列です。
   * @returns 検索用の文字列から復元された元の文字列です。
   */
  (string: string): Awaitable<string>;
}

/**
 * オブジェクトの説明文の検索に使用する関数群のインターフェースです。
 */
export interface ITextSearch {
  /**
   * オブジェクトの説明文を検索用の文字列に変換する関数です。
   */
  readonly toQueryString: IToTextSearchQueryString;

  /**
   * オブジェクトの検索用の文字列を説明文に変換する関数です。
   */
  readonly fromQueryString: IFromTextSearchQueryhString;
}
