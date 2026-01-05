import type { Checksum, HashState } from "../shared/schemas.js";

/**
 * 計算されたハッシュ値の 16 進数文字列と、内部状態です。
 */
export type Digest = Readonly<{
  /**
   * 計算されたハッシュ値の 16 進数文字列です。
   */
  value: Checksum;

  /**
   * ハッシュ関数の内部状態です。
   */
  state: HashState;
}>;

/**
 * ハッシュ値を計算するためのストリームのインターフェースです。
 */
export interface IHash {
  /**
   * ハッシュ値の計算に必要な内部データを更新します。
   *
   * @param data 追加するバイト列です。
   */
  update(data: Uint8Array): void;

  /**
   * `.update()` で渡されたすべてのデータのダイジェストをハッシュして計算し、16 進数文字列を返します。
   *
   * @returns 計算されたハッシュ値の 16 進数文字列と、内部状態です。
   */
  digest(): Digest;
}
