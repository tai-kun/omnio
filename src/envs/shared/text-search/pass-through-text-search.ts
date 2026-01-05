import type { ITextSearch } from "../../../shared/text-search.js";

/**
 * オブジェクトの説明文の検索に使用する関数群です。入力された文字列に対して何もせず通過させます。
 */
export default class PassThroughTextSearch implements ITextSearch {
  /**
   * オブジェクトの説明文を検索用の文字列に変換します。
   *
   * @param string 任意の文字列です。
   * @returns 検索用の文字列です。
   */
  public toQueryString(string: string): string {
    return string;
  }

  /**
   * オブジェクトの検索用の文字列を説明文に変換します。
   *
   * @param string 検索用の文字列です。
   * @returns 検索用の文字列から復元された元の文字列です。
   */
  public fromQueryString(string: string): string {
    return string;
  }
}
