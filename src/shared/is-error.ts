/**
 * エラーオブジェクトのようなオブジェクトかどうか判定します。
 *
 * @param e 判定する値です。
 * @returns 判定結果です。
 */
function isError(e: unknown): e is Error {
  return e instanceof Error || (
    e !== null
    && typeof e === "object"
    && ("name" in e && typeof e.name === "string")
    && ("message" in e && typeof e.message === "string")
  );
}

export default globalThis.Error.isError || isError;
