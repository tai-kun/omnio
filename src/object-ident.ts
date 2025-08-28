import type BucketName from "./bucket-name.js";
import ObjectPath from "./object-path.js";

/**
 * `ObjectIdent` を構築するための入力パラメーターです。
 */
type ObjectIdentInput = Readonly<{
  /**
   * オブジェクトが存在するバケットの名前です。
   */
  bucketName: BucketName;

  /**
   * オブジェクトへのパスです。
   */
  objectPath: ObjectPath;
}>;

/**
 * `ObjectIdent` の JSON シリアライズされた形式を表す型です。
 */
export type ObjectIdentJson = {
  /**
   * オブジェクトが存在するバケットの名前です。
   */
  bucketName: BucketName;

  /**
   * オブジェクトへのパスです。
   */
  objectPath: ObjectPath;
};

/**
 * `ObjectIdent` の文字列表現を表す型です。`bucketName:objectPath` の形式です。
 */
export type ObjectIdentString = `${BucketName}:${string}`;

/**
 * `ObjectIdent` は、バケット名とオブジェクトパスの組み合わせでオブジェクトの場所を特定するクラスです。
 */
export default class ObjectIdent implements ObjectIdentJson {
  /**
   * オブジェクトが存在するバケットの名前です。
   */
  public bucketName: BucketName;

  /**
   * オブジェクトへのパスです。
   */
  public objectPath: ObjectPath;

  /**
   * `ObjectIdent` の新しいインスタンスを構築します。
   *
   * @param inp `ObjectIdent` を構築するための入力パラメーターです。
   */
  public constructor(inp: ObjectIdentInput) {
    this.bucketName = inp.bucketName;
    this.objectPath = inp.objectPath;
  }

  /**
   * `JSON.stringify` で使用される、オブジェクトの文字列表現を返します。
   *
   * @returns JSON 形式の `ObjectIdent` です。
   */
  public toJSON(): ObjectIdentJson {
    return {
      objectPath: this.objectPath,
      bucketName: this.bucketName,
    };
  }

  /**
   * オブジェクトの文字列表現を返します。
   *
   * @returns `bucketName:objectPath` の形式で、`ObjectIdent` の文字列表現を返します。
   */
  public toString(): ObjectIdentString {
    return `${this.bucketName}:${this.objectPath.fullpath}`;
  }
}
