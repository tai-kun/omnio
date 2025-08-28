// 参考: https://docs.aws.amazon.com/ja_jp/AmazonS3/latest/userguide/bucketnamingrules.html#general-purpose-bucket-names

import { type Brand, IPV4_REGEX } from "valibot";
import { InvalidBucketNameError } from "./errors.js";
import getTypeName from "./get-type-name.js";

// バケット名は、小文字、数字、ピリオド (.)、およびハイフン (-) のみで構成できます。
// バケット名は、文字または数字で開始および終了する必要があります。
// Amazon S3 Transfer Acceleration で使用されるバケットの名前にピリオド (.) を使用することはできません。
const VALID_BUCKET_NAME_REGEX = /^[a-z0-9][a-z0-9\-]{1,61}[a-z0-9]$/;
const VALID_BUCKET_NAME_REGEX_DOT = /^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$/;

/**
 * バケット名の解析オプションです。
 */
export type BucketNameOptions = Readonly<{
  /**
   * バケット名にドット (.) を利用できるようにするかどうかを指定します。
   *
   * @default false
   * @deprecated
   */
  allowDot?: boolean | undefined; // 現在未使用
}>;

/**
 * 検証されたバケット名の型です。
 *
 * @template TBucketName バケット名の型です。
 */
type BucketName<TBucketName extends string = string> = TBucketName & Brand<"BucketName">;

/**
 * バケット名の検証を行う関数群です。
 */
const BucketName = {
  /**
   * バケット名を解析します。この関数は、検証が失敗した場合にエラーを投げます。
   *
   * @template TBucketName バケット名の型です。
   * @param bucketName 検証するバケット名です。
   * @param options 解析オプションです。
   * @returns 検証されたバケット名です。
   */
  parse<const TBucketName extends string>(
    bucketName: TBucketName,
    options: BucketNameOptions | undefined = {},
  ): BucketName<TBucketName> {
    if (typeof bucketName !== "string") {
      throw new InvalidBucketNameError(
        `Expected string, but got ${getTypeName(bucketName)}`,
        String(bucketName),
      );
    }

    // バケット名は 3~63 文字の長さにする必要があります。
    // 正規表現のオーバーヘッドが発生する前に .length で高速に検証します。
    if (bucketName.length < 3) {
      throw new InvalidBucketNameError("Cannot be shorter than 3 characters", bucketName);
    }
    if (bucketName.length > 63) {
      throw new InvalidBucketNameError("Cannot be longer than 63 characters", bucketName);
    }

    // バケット名には、連続する 2 つのピリオドを含めることはできません。
    // バケット名のプレフィックスは `xn--` で始まってはいけません。
    // バケット名のプレフィックスは `sthree-` で始まってはいけません。
    // バケット名のプレフィックスは `amzn-s3-demo-` で始まってはいけません。
    // バケット名のサフィックスは `-s3alias` で終わってはいけません。
    // バケット名のサフィックスは `--ol-s3` で終わってはいけません。
    // バケット名のサフィックスは `--x-s3` で終わってはいけません。
    // バケット名のサフィックスは `--table-s3` で終わってはいけません。
    if (
      (options.allowDot && bucketName.includes(".."))
      || bucketName.startsWith("xn--")
      || bucketName.startsWith("sthree-")
      || bucketName.startsWith("amzn-s3-demo-")
      || bucketName.endsWith("-s3alias")
      || bucketName.endsWith("--ol-s3")
      || bucketName.endsWith("--x-s3")
      || bucketName.endsWith("--table-s3")
    ) {
      throw new InvalidBucketNameError("Contains invalid characters", bucketName);
    }

    // バケット名は IP アドレスの形式 (192.168.5.4 など) にはできません。
    if (options.allowDot && IPV4_REGEX.test(bucketName)) {
      throw new InvalidBucketNameError("Cannot be an IP address", bucketName);
    }

    const REGEX = options.allowDot ? VALID_BUCKET_NAME_REGEX_DOT : VALID_BUCKET_NAME_REGEX;
    if (!REGEX.test(bucketName)) {
      throw new InvalidBucketNameError("Contains invalid characters", bucketName);
    }

    return bucketName as BucketName<TBucketName>;
  },

  /**
   * バケット名が有効かどうかを検証します。
   *
   * @template TBucketName バケット名の型です。
   * @param bucketName 検証するバケット名です。
   * @param options 解析オプションです。
   * @returns 有効なバケット名なら `true`、そうでないなら `false` です。
   */
  validate<const TBucketName extends string>(
    bucketName: TBucketName,
    options?: BucketNameOptions | undefined,
  ): bucketName is BucketName<TBucketName> {
    try {
      // 将来的に bucketName が解析時に変更されると、元の値と異なる可能性があります。
      return BucketName.parse(bucketName, options) === bucketName;
    } catch {
      return false;
    }
  },
};

export default BucketName;
