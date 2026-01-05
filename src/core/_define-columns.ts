import sql, { join, raw } from "sql-template-tag";
import * as v from "valibot";
import { TimestampSchema } from "../shared/schemas.js";

/**
 * SQL オブジェクトです。
 */
interface Sql {
  /**
   * SQL の文字列です。
   */
  readonly text: string;

  /**
   * SQL の実行時に渡すパラメーターです。
   */
  readonly values: readonly unknown[];
}

/**
 * カラムとそれに対応する値のデータ型などの定義です。
 *
 * @template TKey カラム名の JavaScript 表現です。
 */
export type Column<TKey extends string> = Readonly<{
  /**
   * カラム名の JavaScript 表現です。データベースのカラム名はスネークケースですが、
   * JavaScript 表現では、キャメルケースになります。
   */
  key: TKey;

  /**
   * `SELECT` に埋め込むカラムを構築します。
   */
  build: (table?: string) => Sql;

  /**
   * 値の Valibot スキーマです。
   */
  schema: v.ObjectEntries[string];
}>;

/**
 * カラムの定義です。
 */
type ColumnDefinition = readonly [
  /**
   * データベースにおけるカラム名です。
   */
  name: string,

  /**
   * カラムの値のデータ型です。
   */
  schema: v.ObjectEntries[string] | "Timestamp",
];

/**
 * 行データを取得するために、各列を定義します。
 *
 * @param columnMap 列のマップです。
 * @returns 各列の定義です。
 */
export default function defineColumns<TKey extends string>(
  columnMap: {
    readonly [P in TKey]: ColumnDefinition;
  },
): Column<TKey>[] {
  const columns = Object.entries<ColumnDefinition>(columnMap).map(([key, [name, schema]]) => ({
    key: key as TKey,
    build(table?: string) {
      const selector = table === undefined
        ? this._selector
        : this._toSelector(table);

      return join([selector, this._asKey], " ");
    },
    schema: schema === "Timestamp"
      ? TimestampSchema()
      : schema,
    _asKey: sql`AS "${raw(key)}"`,
    _selector: schema === "Timestamp"
      ? sql`(EXTRACT(EPOCH FROM ${raw(name)}) * 1000)::BIGINT`
      : raw(name),
    _toSelector(table: string) {
      return schema === "Timestamp"
        ? sql`(EXTRACT(EPOCH FROM ${raw(table)}.${raw(name)}) * 1000)::BIGINT`
        : raw(table + "." + name);
    },
  }));

  return columns;
}
