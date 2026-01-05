import { createMD5 } from "hash-wasm";
import { ChecksumSchema, HashStateSchema } from "../shared/schemas.js";
import * as v from "../shared/valibot.js";
import type { Digest, IHash } from "./_hash.js";

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
  async create(state?: readonly number[] | undefined): Promise<IHash> {
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
          value: v.parse(ChecksumSchema(), value),
          state: v.parse(HashStateSchema(), Array.from(state)),
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
