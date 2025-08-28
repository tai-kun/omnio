declare global {
  /**
   * 一度だけ実行される関数の結果をキャッシュするためのグローバル変数です。
   */
  var omnio__memo__cache: Record<string, any> | undefined;
}

export default {
  /**
   * 一度だけ実行されることを保証する関数です。
   * `id` に紐づく関数 `fn` の実行結果をキャッシュし、同じ `id` で複数回呼び出された場合でも、
   * 関数が再度実行されることなくキャッシュされた結果を返します。
   * 関数 `fn` が Promise を返す場合は、Promise の解決（resolve）または拒否（reject）を待ってからキャッシュします。
   *
   * @template T 実行する関数の型です。
   * @param id キャッシュを一意に識別するための ID です。
   * @param fn 一度だけ実行したい関数です。
   * @returns 関数 `fn` の実行結果、またはそれが Promise を返す場合はその Promise を返します。
   */
  create<T extends (...args: any) => any>(
    id: string,
    fn: T,
  ): ReturnType<T> | Awaited<ReturnType<T>> {
    const cache = globalThis.omnio__memo__cache ||= {};
    if (id in cache) {
      return cache[id];
    }

    let returns = fn();
    if (returns instanceof Promise) {
      cache[id] = returns.then(
        value => {
          cache![id] = value;
          return value;
        },
        reason => {
          delete cache[id];
          throw reason;
        },
      );
    } else {
      cache[id] = returns;
    }

    return cache[id];
  },
  // /**
  //  * `id` に紐づくキャッシュを削除します。
  //  *
  //  * @param id キャッシュを一意に識別するための ID です。
  //  */
  // clear(id: string): void {
  //   const cache = globalThis.omnio__memo__cache ||= {};
  //   delete cache[id];
  // },
};
