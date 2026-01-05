/**
 * T または PromiseLike<T> の型を定義します。これは、非同期処理を扱う場合に便利です。
 *
 * @template T 解決される値の型です。
 */
export type Awaitable<T> = T | PromiseLike<T>;
