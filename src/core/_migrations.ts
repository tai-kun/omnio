// dprint-ignore-file

import sql, { raw, type Sql } from "sql-template-tag";
import type { BucketName } from "../shared/schemas.js";

/**
 * マイグレーションに必要なパラメーターです。
 */
type Params = Readonly<{
  /**
   * バケット名です。
   */
  bucketName: BucketName;
}>;

const migrations: ((params: Params) => Sql)[] = [];

// メタデータ (v1) 用のテーブルがなければ作成します。このテーブルは Omnio が内部的に使用するテーブルであり、ユーザーがこの
// テーブルを直接変更することは想定されていません。
// カラム名    説明
// objectid  メタデータテーブル内で一意のキーです。作成されてから削除されるまで変わりません。
// fullpath  オブジェクトのパスです。バケット内で一意です。ビューでパスからメタデータを検索するときに使用します。
// path_key  オブジェクトのパスです。ただし削除フラグが立った場合、NULL になります。削除されていないメタデータを高速に
//           検索するときに使用します。
// path_seg  オブジェクトのパスの各セグメントです。これは、ディレクトリー判定やパスの階層の特定に使用されます。
// rec_type  メタデータに対して行われた操作の種別を記録します。これは S3 メタデータの `record_type` に似ています。
//           - "CREATE":          新しいオブジェクトがバケットに書き込まれたことを示します。
//           - "UPDATE_METADATA": オブジェクトの作成後、メタデータが変更されたことを示します。
//           - "DELETE":          オブジェクトが削除されたことを示します。
// rec_time  最後にメタデータに対して行われた操作の時刻を記録します。これは　S3 メタデータの `record_timestamp`
//           に似ています。
// obj_size  オブジェクトのサイズ (バイト数) です。これは `File` オブジェクトの構築に必要です。
// numparts  オブジェクトのパートの総数です。
// partsize  各パートのバイト数です。最後のパートはこの値以下になります。
// mime_typ  オブジェクトの MIME タイプです。これは `File` オブジェクトの構築に必要です。
// new_time  オブジェクトが作成された時刻です。
// mod_time  オブジェクトが最後に更新された時刻です。これは `File` オブジェクトの構築に必要です。
// hash_md5  オブジェクトの MD5 ハッシュ値です。整合性を確認するために使用します。
// md5state  MD5 ハッシュ関数の内部状態です。オブジェクトにデータを追記するとき、ハッシュ値を更新するために使用します。
//           元のオブジェクトを使って最初からハッシュ値を計算しなくて済むため、効率的にハッシュ値を更新できます。
// obj_tags  オブジェクトに関連付けられたオブジェクトタグです。オブジェクトをパス以外でグループ化するために使用します。
// desc_fts  全文検索用の文字列です。オブジェクトをキーワードで検索するために使用します。
// usermeta  ユーザー定義のメタデータです。任意の JSON 値を使用できます。
// entityid  実際に保存されているオブジェクトの実体の ID です。
migrations.push(() => sql`

CREATE TABLE IF NOT EXISTS metadata_v1 (
  objectid UUID       PRIMARY KEY,
  fullpath TEXT       NOT NULL,
  path_key TEXT,
  path_seg TEXT[]     NOT NULL,
  rec_type TEXT       NOT NULL,
  rec_time TIMESTAMP  NOT NULL,
  obj_size BIGINT     NOT NULL,
  numparts SMALLINT   NOT NULL,
  partsize BIGINT     NOT NULL,
  mime_typ TEXT       NOT NULL,
  new_time TIMESTAMP  NOT NULL,
  mod_time TIMESTAMP  NOT NULL,
  hash_md5 TEXT       NOT NULL,
  md5state SMALLINT[],
  obj_tags TEXT[],
  desc_fts TEXT,
  usermeta JSON,
  entityid UUID       NOT NULL
)

`);

// path_key 一意制約を適用します。これはレコードタイプが DELETE ではないときのみ、つまりオブジェクトが存在するときのみ
// 1 つの fullpath に対して 1 つだけの実体を紐付けることを強制するためです。削除済みのオブジェクトは重複する fullpath
// が存在する可能性があります。
migrations.push(() => sql`

CREATE UNIQUE INDEX IF NOT EXISTS
  metadata_v1__path_key__unq
ON
  metadata_v1 (path_key)

`);

// 複数のパスが 1 つの実体を共有しないように、entityid に一意制約を適用します。
migrations.push(() => sql`

CREATE UNIQUE INDEX IF NOT EXISTS
  metadata_v1__entityid__unq
ON
  metadata_v1 (entityid)

`);

// メタデータ (v1) を参照するためのテーブルを作成します。読み取り専用であり、ユーザーが独自のクエリーでメタデータを取得する
// ときに参照されるテーブルです。
migrations.push(({ bucketName }) => sql`

CREATE OR REPLACE VIEW
  metadata
AS
SELECT
  '${raw(bucketName)}' AS "bucket",
  objectid AS "id",
  fullpath AS "path",
  path_seg AS "path_segments",
  rec_type AS "record_type",
  rec_time AS "record_timestamp",
  obj_size AS "size",
  numparts AS "num_parts",
  partsize AS "part_size",
  mime_typ AS "mime_type",
  new_time AS "created_at",
  mod_time AS "last_modified_at",
  hash_md5 AS "checksum",
  'MD5'    AS "checksum_algorithm",
  obj_tags AS "object_tags",
  desc_fts AS "description",
  usermeta AS "user_metadata",
  entityid AS "entity_id"
FROM
  metadata_v1

`);

export default migrations;
