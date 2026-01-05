import filenameReservedRegex, { windowsReservedNameRegex } from "filename-reserved-regex";
import othMimeTypes from "mime/types/other.js";
import stdMimeTypes from "mime/types/standard.js";
import singleton from "./_singleton.js";
import newObjectPathInternal from "./new-object-path-internal.js";
import ObjectPath from "./object-path.js";
import StringObjectPathSchema from "./string-object-path-schema.js";
import * as v from "./valibot.js";

const B = 1;
const KB = 1_000 * B;
const MB = 1_000 * KB;
const GB = 1_000 * MB;
const TB = 1_000 * GB;

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

// バケット名は、英数字 (小文字)、ピリオド (.)、およびハイフン (-) のみで構成できます。
// バケット名は、文字または数字で開始および終了する必要があります。
// Amazon S3 Transfer Acceleration で使用されるバケットの名前にピリオド (.) を使用することはできません。
const VALID_BUCKET_NAME_REGEX = /^[a-z0-9][a-z0-9\-]{1,61}[a-z0-9]$/;

/**
 * バケット名の Valibot スキーマです。
 * このスキーマは、文字列を Bucket オブジェクトに変換し、ブランド型として扱います。
 *
 * 参考: https://docs.aws.amazon.com/ja_jp/AmazonS3/latest/userguide/bucketnamingrules.html#general-purpose-bucket-names
 */
export function BucketNameSchema() {
  return singleton("schemas__bucket_name", () => (
    v.pipe(
      // バケット名は文字列である必要があります。
      v.string(),
      // バケット名は 3~63 文字の長さにする必要があります。
      // 正規表現のオーバーヘッドが発生する前に `.length` で高速に検証します。
      v.minLength(3),
      v.maxLength(63),
      // バケット名のプレフィックスは `xn--` で始まってはいけません。
      // バケット名のプレフィックスは `sthree-` で始まってはいけません。
      // バケット名のプレフィックスは `amzn-s3-demo-` で始まってはいけません。
      // バケット名のサフィックスは `-s3alias` で終わってはいけません。
      // バケット名のサフィックスは `--ol-s3` で終わってはいけません。
      // バケット名のサフィックスは `--x-s3` で終わってはいけません。
      // バケット名のサフィックスは `--table-s3` で終わってはいけません。
      v.notStartsWith("xn--"),
      v.notStartsWith("sthree-"),
      v.notStartsWith("amzn-s3-demo-"),
      v.notEndsWith("-s3alias"),
      v.notEndsWith("--ol-s3"),
      v.notEndsWith("--x-s3"),
      v.notEndsWith("--table-s3"),
      // バケット名は IP アドレスの形式 (192.168.5.4 など) にはできません。
      v.regex(VALID_BUCKET_NAME_REGEX),
      // ブランド化します。
      v.brand("BucketName"),
    )
  ));
}

/**
 * バケット名です。
 */
export type BucketNameLike = v.InferInput<ReturnType<typeof BucketNameSchema>>;

/**
 * バケット名です。
 */
export type BucketName = v.InferOutput<ReturnType<typeof BucketNameSchema>>;

/**
 * オブジェクトの識別子 (UUID v7) の Valibot スキーマです。
 * このスキーマは、文字列が UUID v7 形式であることを検証し、ブランド型として扱います。
 */
export function ObjectIdSchema() {
  return singleton("schemas__object_id", () => (
    v.pipe(
      v.string(),
      v.uuidv7(),
      v.brand("ObjectId"),
    )
  ));
}

/**
 * オブジェクトの識別子 (UUID v7) です。
 */
export type ObjectIdLike = v.InferInput<ReturnType<typeof ObjectIdSchema>>;

/**
 * オブジェクトの識別子 (UUID v7) です。
 */
export type ObjectId = v.InferOutput<ReturnType<typeof ObjectIdSchema>>;

/**
 * 実際に保存されるオブジェクトの識別子 (UUID v7) の Valibot スキーマです。
 * このスキーマは、文字列が UUID v7 形式であることを検証し、ブランド型として扱います。
 */
export function EntityIdSchema() {
  return singleton("schemas__entity_id", () => (
    v.pipe(
      v.string(),
      v.uuidv7(),
      v.brand("EntityId"),
    )
  ));
}

/**
 * 実際に保存されるオブジェクトの識別子 (UUID v7) です。
 */
export type EntityIdLike = v.InferInput<ReturnType<typeof EntityIdSchema>>;

/**
 * 実際に保存されるオブジェクトの識別子 (UUID v7) です。
 */
export type EntityId = v.InferOutput<ReturnType<typeof EntityIdSchema>>;

/**
 * オブジェクトパスの Valibot スキーマです。
 * このスキーマは、文字列、または `ObjectPath` クラスのインスタンスを検証します。
 */
export function ObjectPathSchema() {
  return singleton("schemas__object_path", () => (
    v.union([
      v.pipe(
        StringObjectPathSchema(),
        v.transform(({ buffer, source }) => newObjectPathInternal(buffer, source)),
      ),
      v.pipe(
        v.instance(ObjectPath),
        v.transform(x => x.clone()),
      ),
    ])
  ));
}

/**
 * オブジェクトパスになれる値です。
 */
export type ObjectPathLike = v.InferInput<ReturnType<typeof ObjectPathSchema>>;

/**
 * オブジェクトパスです。
 */
export type { ObjectPath };

/**
 * オブジェクトのディレクトリーパスの Valibot スキーマです。
 */
export function ObjectDirectoryPathSchema() {
  return singleton("schemas__object_directory_path", () => (
    v.pipe(
      v.array(v.string()),
      v.readonly(),
      v.maxLength(1025), // 1025 個の空文字を 1024 個のスラッシュ (/) で結合するとちょうどオブジェクトパスの最大バイト数
      v.check(input => {
        if (!Array.isArray(input) || !input.every(s => typeof s === "string")) {
          return false;
        }
        if (input.length === 0) {
          return true;
        }

        const { buffer, source } = v.parse(StringObjectPathSchema(), input.join("/"));
        const { segments } = newObjectPathInternal(buffer, source);

        return input.length === segments.length;
      }),
      v.brand("ObjectDirectoryPath"),
    )
  ));
}

/**
 * オブジェクトのディレクトリーパスです。
 */
export type ObjectDirectoryPathLike = readonly string[];

/**
 * オブジェクトのディレクトリーパスです。
 */
export type ObjectDirectoryPath = v.InferOutput<ReturnType<typeof ObjectDirectoryPathSchema>>;

/**
 * 16 進数表記の MD5 の文字列パターンです。
 */
const MD5_REGEX = /^[0-9a-f]{32}$/;

/**
 * オブジェクトのチェックサム (MD5 ハッシュ値) の Valibot スキーマです。
 * このスキーマは、文字列が 32 桁の 16 進数であることを検証し、ブランド型として扱います。
 */
export function ChecksumSchema() {
  return singleton("schemas__checksum", () => (
    v.pipe(
      v.string(),
      v.regex(MD5_REGEX),
      v.brand("Checksum"),
    )
  ));
}

/**
 * オブジェクトのチェックサムです。
 */
export type ChecksumLike = v.InferInput<ReturnType<typeof ChecksumSchema>>;

/**
 * オブジェクトのチェックサムです。
 */
export type Checksum = v.InferOutput<ReturnType<typeof ChecksumSchema>>;

/**
 * MIME タイプの Valibot スキーマです。
 * このスキーマは、`mime` ライブラリで定義されている標準およびその他の MIME タイプをリテラル型のユニオンとして検証し、
 * ブランド型として扱います。
 */
export function MimeTypeSchema() {
  return singleton("schemas__mime_type", () => (
    v.pipe(
      v.union([
        ...Object.keys(stdMimeTypes).map(ct => v.literal(ct)),
        ...Object.keys(othMimeTypes).map(ct => v.literal(ct)),
      ]),
      v.brand("MimeType"),
    )
  ));
}

/**
 * MIME タイプです。
 */
export type MimeTypeLike = v.InferInput<ReturnType<typeof MimeTypeSchema>>;

/**
 * MIME タイプです。
 */
export type MimeType = v.InferOutput<ReturnType<typeof MimeTypeSchema>>;

/**
 * JavaScript で安全に扱える符号なし整数の Valibot スキーマです。
 * このスキーマは、数値が安全な整数であり、かつ 0 以上であることを検証し、ブランド型として扱います。
 */
export function UintSchema() {
  return singleton("schemas__uint", () => (
    v.pipe(
      v.number(),
      v.safeInteger(),
      v.minValue(0),
      v.brand("Uint"),
    )
  ));
}

/**
 * JavaScript で安全に扱える符号なし整数です。
 */
export type UintLike = v.InferInput<ReturnType<typeof UintSchema>>;

/**
 * JavaScript で安全に扱える符号なし整数です。
 */
export type Uint = v.InferOutput<ReturnType<typeof UintSchema>>;

/**
 * サイズ (バイト数) が制限された文字列用の Valibot スキーマです。
 *
 * @param maxBytes 上限 (バイト数)
 */
export function SizeLimitedUtf8StringSchema(maxBytes: UintLike) {
  return singleton("schemas__size_limited_utf8_string", () => (
    v.pipe(
      v.string(),
      v.utf8(),
      v.maxBytes(v.parse(UintSchema(), maxBytes)),
      v.brand("SizeLimitedUtf8"),
    )
  ));
}

/**
 * サイズ (バイト数) が制限された文字列の型です。
 */
export type SizeLimitedUtf8StringLike = v.InferInput<
  ReturnType<typeof SizeLimitedUtf8StringSchema>
>;

/**
 * サイズ (バイト数) が制限された文字列の型です。
 */
export type SizeLimitedUtf8String = v.InferOutput<ReturnType<typeof SizeLimitedUtf8StringSchema>>;

/**
 * 並び順を表す文字列の Valibot スキーマです。
 * このスキーマは、`asc`、`ASC`、`desc`、`DESC` のいずれかの文字列を検証し、全て大文字に変換して、ブランド型として扱います。
 */
export function OrderTypeSchema() {
  return singleton("schemas__order_type", () => (
    v.pipe(
      v.union([
        v.literal("asc"),
        v.literal("ASC"),
        v.literal("desc"),
        v.literal("DESC"),
      ]),
      v.transform(toUpperCase),
      v.brand("OrderType"),
    )
  ));
}

/**
 * 並び順を表す文字列です。
 */
export type OrderTypeLike = v.InferInput<ReturnType<typeof OrderTypeSchema>>;

/**
 * 並び順を表す文字列です。
 */
export type OrderType = v.InferOutput<ReturnType<typeof OrderTypeSchema>>;

/**
 * オブジェクトのメタデータのレコードタイプの Valibot スキーマです。
 * このスキーマは、`CREATE`、`UPDATE_METADATA`、`DELETE` のいずれかの文字列を検証し、ブランド型として扱います。
 *
 * - **`"CREATE"`**: 新しいオブジェクトがバケットに書き込まれたことを示します。
 * - **`"UPDATE_METADATA"`**: オブジェクトが削除されたことを示します。
 */
export function RecordTypeSchema() {
  return singleton("schemas__record_type", () => (
    v.pipe(
      v.union([
        v.literal("CREATE"),
        v.literal("UPDATE_METADATA"),
      ]),
      v.brand("RecordType"),
    )
  ));
}

/**
 * オブジェクトのメタデータのレコードタイプです。
 */
export type RecordTypeLike = string;

/**
 * オブジェクトのメタデータのレコードタイプです。
 */
export type RecordType = v.InferOutput<ReturnType<typeof RecordTypeSchema>>;

/**
 * オブジェクトに関連付けられたオブジェクトタグの Valibot スキーマです。最大 128 バイトの文字列です。
 */
export function ObjectTagSchema() {
  return singleton("schemas__object_tag", () => (
    v.pipe(
      v.string(),
      v.utf8(),
      v.maxBytes(128),
      v.brand("ObjectTag"),
    )
  ));
}

/**
 * オブジェクトに関連付けられたオブジェクトタグです。
 */
export type ObjectTagLike = v.InferInput<ReturnType<typeof ObjectTagSchema>>;

/**
 * オブジェクトに関連付けられたオブジェクトタグです。
 */
export type ObjectTag = v.InferOutput<ReturnType<typeof ObjectTagSchema>>;

/**
 * オブジェクトに関連付けられたオブジェクトタグの Valibot スキーマです。
 * オブジェクトタグに最大 128 バイトの文字列を、最大 20 個まで格納することができます。
 */
export function ObjectTagsSchema() {
  return singleton("schemas__object_tags", () => (
    v.pipe(
      v.array(ObjectTagSchema()),
      v.transform(x => Array.from(new Set(x))),
      v.maxLength(20),
      v.readonly(),
      v.brand("ObjectTags"),
    )
  ));
}

/**
 * オブジェクトに関連付けられたオブジェクトタグです。
 */
export type WritableObjectTagsLike = string[];

/**
 * オブジェクトに関連付けられたオブジェクトタグです。
 */
export type ObjectTagsLike = readonly string[];

/**
 * オブジェクトに関連付けられたオブジェクトタグです。
 */
export type ObjectTags = v.InferOutput<ReturnType<typeof ObjectTagsSchema>>;

/**
 * タイムスタンプの Valibot スキーマです。
 * このスキーマは、文字列、数値、`Date` インスタンスを時刻 (ミリ秒) に変換して、ブランド型として扱います。
 * 時刻 (ミリ秒) は安全な整数値であることが保証されています。
 */
export function TimestampSchema() {
  return singleton("schemas__timestamp", () => (
    v.pipe(
      v.union([v.string(), v.number(), v.instance(Date)]),
      v.transform(x => new Date(x)),
      v.transform(x => x.getTime()),
      v.safeInteger(),
      v.brand("Timestamp"),
    )
  ));
}

/**
 * タイムスタンプです。
 */
export type TimestampLike = v.InferInput<ReturnType<typeof TimestampSchema>>;

/**
 * タイムスタンプです。
 */
export type Timestamp = v.InferOutput<ReturnType<typeof TimestampSchema>>;

/**
 * 符号なし 8 ビット整数の Valibot スキーマです。
 */
export function Uint8Schema() {
  return singleton("schemas__uint8", () => (
    v.pipe(
      UintSchema(),
      v.maxValue(255 as Uint),
      v.brand("Uint8"),
    )
  ));
}

/**
 * 符号なし 8 ビット整数です。
 */
export type Uint8Like = v.InferInput<ReturnType<typeof Uint8Schema>>;

/**
 * 符号なし 8 ビット整数です。
 */
export type Uint8 = v.InferOutput<ReturnType<typeof Uint8Schema>>;

/**
 * ハッシュ関数の内部状態の Valibot スキーマです。
 */
export function HashStateSchema() {
  return singleton("schemas__hash_state", () => (
    v.pipe(
      v.array(Uint8Schema()),
      v.readonly(),
      v.brand("HashState"),
    )
  ));
}

/**
 * ハッシュ関数の内部状態です。
 */
export type HashStateLike = v.InferInput<ReturnType<typeof HashStateSchema>>;

/**
 * ハッシュ関数の内部状態です。
 */
export type HashState = v.InferOutput<ReturnType<typeof HashStateSchema>>;

/**
 * オブジェクトを開く際のモードの Valibot スキーマです。
 * このスキーマは、`w`、`wx`、`a`、`ax` のいずれかの文字列を検証し、ブランド型として扱います。
 *
 * - **`"w"`**: 書き込みモードで開きます。オブジェクトが存在しない場合は新規作成され、もし存在する場合は上書きします。
 * - **`"wx"`**: 書き込みモードで開きます。オブジェクトが存在する場合はエラーになります。
 * - **`"a"`**: 追加書き込みモードで開きます。オブジェクトが存在しない場合は新規作成されます。
 * - **`"ax"`**: 追加書き込みモードで開きます。オブジェクトが存在する場合はエラーになります。
 */
export function OpenModeSchema() {
  return singleton("schemas__open_mode", () => (
    v.pipe(
      v.union([
        v.literal("w"),
        v.literal("wx"),
        v.literal("a"),
        v.literal("ax"),
      ]),
      v.brand("OpenMode"),
    )
  ));
}

/**
 * オブジェクトを開く際のモードです。
 */
export type OpenModeLike = v.InferInput<ReturnType<typeof OpenModeSchema>>;

/**
 * オブジェクトを開く際のモードです。
 */
export type OpenMode = v.InferOutput<ReturnType<typeof OpenModeSchema>>;

/**
 * オブジェクトサイズ (バイト数) の最大値です。
 */
const _MAX_OBJECT_SIZE = (5 * TB) as Uint;

/**
 * オブジェクトサイズ (バイト数) の最大値の Valibot スキーマです。
 */
export function ObjectSizeSchema() {
  return singleton("schemas__object_size", () => (
    v.pipe(
      UintSchema(),
      v.maxValue(_MAX_OBJECT_SIZE),
      v.brand("ObjectSize"),
    )
  ));
}

/**
 * オブジェクトサイズ (バイト数) の最大値です。
 */
export const MAX_OBJECT_SIZE = _MAX_OBJECT_SIZE as ObjectSize;

/**
 * オブジェクトサイズ (バイト数) の最大値です。
 */
export type ObjectSizeLike = v.InferInput<ReturnType<typeof ObjectSizeSchema>>;

/**
 * オブジェクトサイズ (バイト数) の最大値です。
 */
export type ObjectSize = v.InferOutput<ReturnType<typeof ObjectSizeSchema>>;

/**
 * オブジェクトのパートの総数の最小値です。
 */
const _MIN_NUM_PARTS = 0 as Uint;

/**
 * オブジェクトのパートの総数の最大値です。
 */
const _MAX_NUM_PARTS = 10_000 as Uint;

/**
 * オブジェクトのパートの総数の Valibot スキーマです。
 */
export function NumPartsSchema() {
  return singleton("schemas__num_parts", () => (
    v.pipe(
      UintSchema(),
      v.minValue(_MIN_NUM_PARTS),
      v.maxValue(_MAX_NUM_PARTS),
      v.brand("NumParts"),
    )
  ));
}

/**
 * オブジェクトのパートの総数の最小値です。
 */
export const MIN_NUM_PARTS = _MIN_NUM_PARTS as NumParts;

/**
 * オブジェクトのパートの総数の最大値です。
 */
export const MAX_NUM_PARTS = _MAX_NUM_PARTS as NumParts;

/**
 * オブジェクトのパートの総数です。
 */
export type NumPartsLike = v.InferInput<ReturnType<typeof NumPartsSchema>>;

/**
 * オブジェクトのパートの総数です。
 */
export type NumParts = v.InferOutput<ReturnType<typeof NumPartsSchema>>;

/**
 * 各パートのサイズ (バイト数) の最小値です。
 */
const _MIN_PART_SIZE = (5 * MB) as ObjectSize;

/**
 * 各パートのサイズ (バイト数) の最大値です。
 */
const _MAX_PART_SIZE = (5 * GB) as ObjectSize;

/**
 * 各パートのサイズ (バイト数) の Valibot スキーマです。
 */
export function PartSizeSchema() {
  return singleton("schemas__part_size", () => (
    v.pipe(
      ObjectSizeSchema(),
      v.minValue(_MIN_PART_SIZE),
      v.maxValue(_MAX_PART_SIZE),
      v.brand("PartSize"),
    )
  ));
}

/**
 * 各パートのサイズ (バイト数) の最小値です。
 */
export const MIN_PART_SIZE = _MIN_PART_SIZE as PartSize;

/**
 * 各パートのサイズ (バイト数) の最大値です。
 */
export const MAX_PART_SIZE = _MAX_PART_SIZE as PartSize;

/**
 * 各パートのサイズ (バイト数) です。
 */
export type PartSizeLike = v.InferInput<ReturnType<typeof PartSizeSchema>>;

/**
 * 各パートのサイズ (バイト数) です。
 */
export type PartSize = v.InferOutput<ReturnType<typeof PartSizeSchema>>;

/**
 * ファイル名またはディレクトリー名の Valibot スキーマです。
 *
 * @see https://fs.spec.whatwg.org/#valid-file-name
 * @see https://github.com/sindresorhus/valid-filename
 */
export function FileSystemEntryNameSchema() {
  return singleton("schemas__file_system_entry_name", () => (
    v.pipe(
      v.string(),
      v.notValue("."),
      v.notValue(".."),
      v.minLength(1),
      v.maxLength(255),
      v.notMatch(/[/]/g),
      v.notMatch(filenameReservedRegex()),
      v.notMatch(windowsReservedNameRegex()),
      v.brand("FileSystemEntryName"),
    )
  ));
}

/**
 * ファイル名またはディレクトリー名です。
 */
export type FileSystemEntryNameLike = v.InferInput<ReturnType<typeof FileSystemEntryNameSchema>>;

/**
 * ファイル名またはディレクトリー名です。
 */
export type FileSystemEntryName = v.InferOutput<ReturnType<typeof FileSystemEntryNameSchema>>;

/**
 * 一時ファイルの拡張子です。
 */
export const FILE_SYSTEM_TEMP_FILE_EXT: string = ".crswap";

/**
 * ファイル名の Valibot スキーマです。
 */
export function FileSystemFileNameSchema() {
  return singleton("schemas__file_system_file_name", () => (
    v.pipe(
      FileSystemEntryNameSchema(),
      v.maxLength(255 - FILE_SYSTEM_TEMP_FILE_EXT.length),
      v.brand("FileSystemFileName"),
    )
  ));
}

/**
 * ファイル名です。
 */
export type FileSystemFileNameLike = v.InferInput<ReturnType<typeof FileSystemFileNameSchema>>;

/**
 * ファイル名です。
 */
export type FileSystemFileName = v.InferOutput<ReturnType<typeof FileSystemFileNameSchema>>;

/**
 * ディレクトリー名の Valibot スキーマです。
 */
export function FileSystemDirectoryNameSchema() {
  return singleton("schemas__file_system_directory_name", () => (
    v.pipe(
      FileSystemEntryNameSchema(),
      v.brand("FileSystemDirectoryName"),
    )
  ));
}

/**
 * ディレクトリー名です。
 */
export type FileSystemDirectoryNameLike = v.InferInput<
  ReturnType<typeof FileSystemDirectoryNameSchema>
>;

/**
 * ディレクトリー名です。
 */
export type FileSystemDirectoryName = v.InferOutput<
  ReturnType<typeof FileSystemDirectoryNameSchema>
>;
