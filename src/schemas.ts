import othMimeTypes from "mime/types/other.js";
import stdMimeTypes from "mime/types/standard.js";
import * as v from "valibot";
import Bucket from "./bucket-name.js";
import ObjectPath from "./object-path.js";
import utf8 from "./utf8.js";

/**
 * 文字列を全て大文字にします。この関数は、入力された文字列の型を保持したまま、全て大文字に変換します。
 *
 * @template T 大文字にする文字列の型です。
 * @param s 大文字にする文字列です。
 * @returns 大文字になった文字列を返します。
 */
function toUpperCase<const T extends string>(s: T): Uppercase<T> {
  return s.toUpperCase() as Uppercase<T>;
}

/**
 * バケット名の valibot スキーマです。
 * このスキーマは、文字列を Bucket オブジェクトに変換し、ブランド型として扱います。
 */
export const BucketName = v.pipe(
  v.string(),
  v.transform(s => Bucket.parse(s) as string),
  v.brand("BucketName"),
);

/**
 * オブジェクトの識別子 (UUID) の valibot スキーマです。
 * このスキーマは、文字列が UUID 形式であることを検証し、ブランド型として扱います。
 */
export const ObjectId = v.pipe(v.string(), v.uuid(), v.brand("ObjectId"));

/**
 * 実際に保存されるファイルの識別子 (UUID) の valibot スキーマです。
 * このスキーマは、文字列が UUID 形式であることを検証し、ブランド型として扱います。
 */
export const EntityId = v.pipe(v.string(), v.uuid(), v.brand("EntityId"));

/**
 * ファイルパスになれる値の valibot スキーマです。
 * このスキーマは、文字列、または Path クラスのインスタンスを検証します。
 */
export const ObjectPathLike = v.union([
  v.pipe(v.string(), v.transform(ObjectPath.parse)),
  v.pipe(v.instance(ObjectPath), v.transform(op => op.clone())),
]);

/**
 * ファイルのチェックサム (MD5 ハッシュ値) の valibot スキーマです。
 * このスキーマは、文字列が32桁の16進数であることを検証し、ブランド型として扱います。
 */
export const Checksum = v.pipe(v.string(), v.regex(/^[0-9a-f]{32}$/), v.brand("Checksum"));

/**
 * MIME タイプの valibot スキーマです。
 * このスキーマは、`mime` ライブラリで定義されている標準およびその他の MIME タイプをリテラル型のユニオンとして検証し、
 * ブランド型として扱います。
 */
export const MimeType = v.pipe(
  v.union([
    ...Object.keys(stdMimeTypes).map(ct => v.literal(ct)),
    ...Object.keys(othMimeTypes).map(ct => v.literal(ct)),
  ]),
  v.brand("MimeType"),
);

/**
 * 0 以上かつ JavaScript における安全な整数の最大値 (9_007_199_254_740_991) 以下の valibot スキーマです。
 * このスキーマは、数値が安全な整数であり、かつ 0 以上であることを検証し、ブランド型として扱います。
 */
export const UnsignedInteger = v.pipe(
  v.number(),
  v.safeInteger(),
  v.minValue(0),
  v.brand("UnsignedInteger"),
);

/**
 * サイズ (バイト数) が制限された文字列の型です。
 */
export type SizeLimitedString = v.InferOutput<ReturnType<typeof newSizeLimitedString>>;

/**
 * サイズ (バイト数) が制限された文字列用の valibot スキーマを作成します。
 * この関数は、指定されたバイト数制限に基づいて、文字列の長さを検証するスキーマを生成します。
 *
 * @param limit 上限 (バイト数)
 * @returns サイズ (バイト数) が制限された文字列用の valibot スキーマを返します。
 */
export function newSizeLimitedString(limit: v.InferOutput<typeof UnsignedInteger>) {
  return v.pipe(
    v.string(),
    v.custom(s => typeof s === "string" && utf8.encode(s).length <= limit),
    v.brand("SizeLimitedString"),
  );
}

/**
 * 並び順を表す文字列の valibot スキーマです。
 * このスキーマは、`asc`、`ASC`、`desc`、`DESC` のいずれかの文字列を検証し、全て大文字に変換して、ブランド型として扱います。
 */
export const OrderType = v.pipe(
  v.union([
    v.literal("asc"),
    v.literal("ASC"),
    v.literal("desc"),
    v.literal("DESC"),
  ]),
  v.transform(toUpperCase),
  v.brand("OrderType"),
);

/**
 * オブジェクトのメタデータのレコードタイプの valibot スキーマです。
 * このスキーマは、`CREATE`、`UPDATE_METADATA`、`DELETE` のいずれかの文字列を検証し、ブランド型として扱います。
 *
 * - **`"CREATE"`**: 新しいオブジェクトがバケットに書き込まれたことを示します。
 * - **`"UPDATE_METADATA"`**: オブジェクトが削除されたことを示します。
 * - **`"DELETE"`**: オブジェクトが削除されたことを示します。
 */
export const RecordType = v.pipe(
  v.union([
    v.literal("CREATE"),
    v.literal("UPDATE_METADATA"),
    v.literal("DELETE"),
  ]),
  v.brand("RecordType"),
);

/**
 * オブジェクトに関連付けられたオブジェクトタグです。
 */
export type MutableObjectTags = SizeLimitedString[] & v.Brand<"ObjectTags">;

/**
 * オブジェクトに関連付けられたオブジェクトタグの valibot スキーマです。
 * オブジェクトタグに最大 128 バイトの文字列を、最大 20 個まで格納することができます。
 */
export const ObjectTags = v.pipe(
  v.array(newSizeLimitedString(128 as v.InferOutput<typeof UnsignedInteger>)),
  v.maxLength(20),
  v.readonly(),
  v.brand("ObjectTags"),
);

/**
 * タイムスタンプの valibot スキーマです。
 * このスキーマは、文字列、数値、`Date` インスタンスを時刻 (ミリ秒) に変換して、ブランド型として扱います。
 * 時刻 (ミリ秒) は安全な整数値であることが保証されています。
 */
export const Timestamp = v.pipe(
  v.union([
    v.string(),
    v.number(),
    v.instance(Date),
  ]),
  v.transform(x => (new Date(x)).getTime()),
  v.safeInteger(),
  v.brand("Timestamp"),
);

/**
 * 符号なし 8 ビット整数の valibot スキーマです。
 */
export const Uint8 = v.pipe(
  v.number(),
  v.finite(),
  v.integer(),
  v.minValue(0),
  v.maxValue(255),
  v.brand("Uint8"),
);

/**
 * ハッシュ関数の内部状態の valibot スキーマです。
 */
export const HashState = v.pipe(
  v.array(Uint8),
  v.readonly(),
  v.brand("HashState"),
);

/**
 * オブジェクトを開く際のモードの valibot です。
 * このスキーマは、`w`、`wx`、`a`、`ax` のいずれかの文字列を検証し、ブランド型として扱います。
 *
 * - **`"w"`**: 書き込みモードで開きます。オブジェクトが存在しない場合は新規作成され、もし存在する場合は上書きします。
 * - **`"wx"`**: 書き込みモードで開きます。オブジェクトが存在する場合はエラーになります。
 * - **`"a"`**: 追加書き込みモードで開きます。オブジェクトが存在しない場合は新規作成されます。
 * - **`"ax"`**: 追加書き込みモードで開きます。オブジェクトが存在する場合はエラーになります。
 */
export const OpenMode = v.pipe(
  v.union([
    v.literal("w"),
    v.literal("wx"),
    v.literal("a"),
    v.literal("ax"),
  ]),
  v.brand("OpenMode"),
);
