/**
 * 文字列を二重引用符で囲います。
 *
 * @tempalte S 文字列の型です。
 * @param s 文字列です。
 * @returns 二重引用符で囲まれた文字列です。
 */
export default function quoteString<S extends string>(s: S): `"${S}"` {
  return JSON.stringify(s) as `"${S}"`;
}
