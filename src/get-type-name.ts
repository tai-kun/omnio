/**
 * `Function.prototype.toString` メソッドを定数として保持しています。
 * このメソッドは、関数の文字列表現を取得するために使用されます。
 */
const functionToString = Function.prototype.toString;

/**
 * 関数名を取得するための正規表現です。
 * この正規表現は、関数宣言の文字列から関数名を抽出するために使用されます。
 */
const FUNCTION_NAME_REGEX = /^\s*function\s*([^\(\s]+)/;

/**
 * 関数から関数名を取得します。
 *
 * @param func 名前を取得する関数です。
 * @returns 取得した関数名です。
 */
function getFunctionName(func: Function): string {
  return func.name || FUNCTION_NAME_REGEX.exec(functionToString.call(func))?.[1] || "Function";
}

/**
 * `Object.prototype.toString` メソッドを定数として保持しています。
 * このメソッドは、オブジェクトの文字列表現を取得するために使用されます。
 */
const objectToString = Object.prototype.toString;

/**
 * オブジェクトのコンストラクター名を取得します。
 *
 * @param obj コンストラクター名を取得するオブジェクトです。
 * @returns 取得したコンストラクター名です。
 */
function getConstructorName(obj: object): string {
  const name = objectToString.call(obj).slice(8, -1);
  return typeof obj.constructor === "function" && (name === "Object" || name === "Error")
    ? getFunctionName(obj.constructor)
    : name;
}

/**
 * JavaScript 値の型名です。
 */
export type TypeName =
  | "Uint8Array"
  | (string & {});

/**
 * 与えられた値の型名を取得します。
 *
 * @param input 型名を取得する値です。
 * @returns 取得した値の型名です。
 */
export default function getTypeName(input: unknown): TypeName {
  if (input == null) {
    return String(input);
  }

  const t = typeof input;
  if (t === "object") {
    return getConstructorName(input);
  }
  if (t === "function") {
    return getFunctionName(input.constructor);
  }

  return t;
}
