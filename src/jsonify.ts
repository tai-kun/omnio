/**
 * `Number.MAX_SAFE_INTEGER` を `BigInt` に変換した定数です。
 * これを上回る `BigInt` は安全に `Number` に変換できません。
 */
const MAX_SAFE_INTEGER_B = BigInt(Number.MAX_SAFE_INTEGER);

/**
 * `Number.MIN_SAFE_INTEGER` を `BigInt` に変換した定数です。
 * これを下回る `BigInt` は安全に `Number` に変換できません。
 */
const MIN_SAFE_INTEGER_B = BigInt(Number.MIN_SAFE_INTEGER);

/**
 * 任意のコンストラクター関数を表す型です。
 */
interface Constructor {
  new(...args: any): any;
}

/**
 * `registerConstructor` で登録された、カスタムな `toJSON` メソッドを持つマップです。
 * キーにはコンストラクター関数、値には `toJSON` 関数が格納されます。
 */
const customToJsonMap = new Map<Constructor, (this: any) => unknown>();

/**
 * 特定のクラス（値オブジェクト）のカスタムな `toJSON` メソッドを登録します。
 * 登録されたクラスのインスタンスは、`jsonify` 関数でシリアライズされる際に、このカスタムな `toJSON` メソッドが呼ばれます。
 *
 * @template TConstructor 登録するコンストラクターの型です。
 * @param constructor `toJSON` メソッドを登録するクラスのコンストラクターです。
 * @param toJson 登録する `toJSON` メソッドです。`this` は `constructor` のインスタンスとなります。
 */
function registerConstructor<TConstructor extends Constructor>(
  constructor: TConstructor,
  toJson: (this: InstanceType<TConstructor>) => unknown,
): void {
  customToJsonMap.set(constructor, toJson);
}

/**
 * 登録されたカスタムな `toJSON` メソッドを解除します。
 *
 * @template TConstructor 登録を解除するコンストラクターの型です。
 * @param constructor `toJSON` メソッドの登録を解除するクラスのコンストラクターです。
 * @returns 登録が解除された場合は `true`、そうでなければ `false` を返します。
 */
function unregisterConstructor<TConstructor extends Constructor>(
  constructor: TConstructor,
): boolean {
  return customToJsonMap.delete(constructor);
}

/**
 * 登録されたすべてのカスタムな `toJSON` メソッドを解除します。
 */
function clearConstructors(): void {
  customToJsonMap.clear();
}

/**
 * `JSON.stringify` の `replacer` 関数として使用される、値を変換するための関数です。
 * - `BigInt` を `Number` に変換します。
 * - `Map` をオブジェクトに変換します。
 * - `Set` を配列に変換します。
 * - `registerConstructor` で登録されたクラスのインスタンスを、登録された `toJSON` メソッドを使用して変換します。
 *
 * @param _key 変換中のプロパティーのキーです。未使用です。
 * @param value 変換対象の値です。
 * @returns 変換後の値です。
 */
function jsonifyReplacer(_key: string, value: unknown): unknown {
  if (typeof value === "object" && value !== null) {
    // 変換対象外のオブジェクト（配列、プレーンオブジェクト、または `Object.create(null)` によって作成された
    // プロトタイプを持たないオブジェクト）の場合は、そのまま値を返します。
    if (
      Array.isArray(value)
      || value.constructor === Object
      || value.constructor === undefined
    ) {
      return value;
    }

    if (value instanceof Map) {
      return Object.fromEntries(
        // Map の各エントリー（キーと値のペア）を再帰的に `jsonify` します。
        value.entries().map(([key, value]) => [
          jsonify(key),
          jsonify(value),
        ]),
      );
    }

    if (value instanceof Set) {
      return Array.from(value);
    }

    // 登録されたカスタムな `toJSON` メソッドを持つクラスを探索します。
    for (const [Constructor, toJson] of customToJsonMap) {
      if (value instanceof Constructor) {
        // インスタンスが登録されたクラスのものであれば、登録済みの `toJson` メソッドを呼び出して変換します。
        return toJson.call(value);
      }
    }
  } else if (typeof value === "bigint") {
    // BigInt の値が安全な整数の範囲を超える場合に、`RangeError` を投げます。
    if (value > MAX_SAFE_INTEGER_B) {
      throw new RangeError(`BigInt too large to convert to Number: ${value}`);
    }
    if (value < MIN_SAFE_INTEGER_B) {
      throw new RangeError(`BigInt too small to convert to Number: ${value}`);
    }

    return Number(value);
  }

  return value;
}

/**
 * オブジェクトを JSON 形式に変換します。`BigInt` は `Number` に変換します。
 * この関数はデータベースの行データを JSON 形式に変換するために使用されます。
 *
 * @template T 変換後のオブジェクトの型です。
 * @param input 変換するオブジェクトです。
 * @returns JSON 形式に変換されたオブジェクトです。
 */
function jsonify<T = unknown>(input: unknown): T {
  return JSON.parse(JSON.stringify(input, jsonifyReplacer));
}

export default Object.assign(jsonify, {
  /**
   * 登録されたすべてのカスタムな `toJSON` メソッドを解除するための関数です。
   */
  clear: clearConstructors,

  /**
   * カスタムな `toJSON` メソッドを登録するための関数です。
   */
  register: registerConstructor,

  /**
   * カスタムな `toJSON` メソッドの登録を解除するための関数です。
   */
  unregister: unregisterConstructor,
});
