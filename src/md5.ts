import { createMD5 } from "hash-wasm";
import * as v from "valibot";
import * as schemas from "./schemas.js";

/**
 * 計算されたハッシュ値の 16 進数文字列と、内部状態です。
 */
export type Digest = Readonly<{
  /**
   * 計算されたハッシュ値の 16 進数文字列です。
   */
  value: v.InferOutput<typeof schemas.Checksum>;

  /**
   * ハッシュ関数の内部状態です。
   */
  state: v.InferOutput<typeof schemas.HashState>;
}>;

/**
 * ハッシュ値を計算するためのストリームです。
 */
export interface Hash {
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

/**
 * ハッシュ値の計算に関連するユーティリティーオブジェクトです。
 */
export default {
  /**
   * ハッシュ値を計算するためのストリームを作成します。
   *
   * @param state ハッシュ関数の内部状態です。
   * @returns ハッシュ値を計算するための `Hash` インターフェースを実装したストリームです。
   */
  async create(state?: readonly number[] | undefined): Promise<Hash> {
    const md5 = await createMD5();
    if (state) {
      md5.load(new Uint8Array(state));
    }

    return {
      update(data) {
        md5.update(data);
      },
      digest() {
        const state = md5.save(); // 必ず `.digest()` の前に実行します。
        const value = md5.digest();

        return {
          value: v.parse(schemas.Checksum, value),
          state: v.parse(schemas.HashState, Array.from(state)),
        };
      },
    };
  },

  /**
   * 渡されたデータの MD5 ダイジェストをハッシュして計算します。
   *
   * @param data ハッシュ値を計算する対象のバイト列です。
   * @returns 計算されたハッシュ値の 16 進数文字列と、内部状態です。
   */
  async digest(data: Uint8Array): Promise<Digest> {
    const h = await this.create();
    h.update(data);

    return h.digest();
  },
};
