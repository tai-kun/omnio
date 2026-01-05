import {
  DuckDBArrayValue,
  DuckDBBitValue,
  DuckDBBlobValue,
  type DuckDBConnection,
  DuckDBDateValue,
  DuckDBDecimalValue,
  DuckDBInstance,
  DuckDBIntervalValue,
  DuckDBListValue,
  DuckDBMapValue,
  type DuckDBResult,
  DuckDBStructValue,
  DuckDBTimestampMillisecondsValue,
  DuckDBTimestampNanosecondsValue,
  DuckDBTimestampSecondsValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampValue,
  DuckDBTimeTZValue,
  DuckDBTimeValue,
  DuckDBUnionValue,
  DuckDBUUIDValue,
} from "@duckdb/node-api";
import { asyncmux } from "asyncmux";
import type { IDatabase, IStatement, Row } from "../../../shared/database.js";
import { DatabaseNotOpenError, SqlStatementClosedError } from "../../../shared/errors.js";
import jsonify, { type IJsonify } from "../../../shared/jsonify.js";

/**
 * `DuckDBResult` からすべての行を読み取り、行のジェネレーターを生成します。
 *
 * @param jsonify オブジェクトを JSON 形式に変換する関数です。
 * @param sam 読み取る `DuckDBResult` オブジェクトです。
 * @returns 行のジェネレーターを返します。
 */
async function* readAllRows(jsonify: IJsonify, sam: DuckDBResult): AsyncGenerator<Row, void, void> {
  const cols = sam.columnNames();
  while (true) {
    const chunk = await sam.fetchChunk();
    if (!chunk || chunk.rowCount === 0) {
      break;
    }

    const rows = chunk.getRowObjects(cols);
    for (const row of rows) {
      yield jsonify(row) as Row;
    }
  }
}

/**
 * `DuckDBResult` からすべての行を読み取ります。各行に対して何もしません。
 *
 * @param sam 読み取る `DuckDBResult` オブジェクトです。
 */
async function consumeAllRows(sam: DuckDBResult): Promise<void> {
  const cols = sam.columnNames();
  while (true) {
    const chunk = await sam.fetchChunk();
    if (!chunk || chunk.rowCount === 0) {
      break;
    }

    const rows = chunk.getRowObjects(cols);
    for (const _ of rows) {
    }
  }
}

/**
 * SQL ステートメントを扱うためのクラスです。
 */
class Statement implements IStatement {
  /**
   * オブジェクトを JSON 形式に変換する関数です。
   */
  readonly #jsonify: IJsonify;

  /**
   * 実行する SQL クエリーです。
   */
  readonly #text: string;

  /**
   * `DuckDBConnection` オブジェクトです。
   */
  readonly #conn: DuckDBConnection;

  /**
   * ステートメントが開いているかどうかを示すフラグです。
   */
  #open: boolean;

  /**
   * `Statement` クラスの新しいインスタンスを生成します。
   *
   * @param jsonify オブジェクトを JSON 形式に変換する関数です。
   * @param text 実行する SQL クエリーです。
   * @param conn `DuckDBConnection` オブジェクトです。
   */
  public constructor(jsonify: IJsonify, text: string, conn: DuckDBConnection) {
    this.#jsonify = jsonify;
    this.#text = text;
    this.#conn = conn;
    this.#open = true;
  }

  /**
   * ステートメントを閉じます。
   */
  public close(): void {
    this.#open = false;
  }

  /**
   * ステートメントを実行します。
   *
   * @param values SQL ステートメントに渡すパラメーターです。
   */
  public async exec(...values: unknown[]): Promise<void> {
    if (!this.#open) {
      throw new SqlStatementClosedError();
    }

    const sam = await this.#conn.stream(this.#text, values as any[]);
    await consumeAllRows(sam);
  }

  /**
   * ステートメントを実行し、結果の行を非同期ジェネレーターとして取得します。
   *
   * @param values SQL ステートメントに渡すパラメーターです。
   * @returns 行の非同期ジェネレーターを返します。
   */
  public async *query(...values: unknown[]): AsyncGenerator<Row, void, void> {
    if (!this.#open) {
      throw new SqlStatementClosedError();
    }

    const sam = await this.#conn.stream(this.#text, values as any[]);
    yield* readAllRows(this.#jsonify, sam);
  }
}

/**
 * Node.js 環境でデータベース接続を管理するためのクラスです。
 * 内部的に DuckDB の Node.js Client (Neo) を使用しています。
 */
export default class DuckdbNodeNeo implements IDatabase {
  /**
   * オブジェクトを JSON 形式に変換する関数です。
   */
  readonly #jsonify: IJsonify | undefined;

  /**
   * データベースファイルのパスです。
   */
  readonly #path: string;

  /**
   * `DuckDBInstance` を作成する際のオプションです。
   */
  readonly #options: Record<string, string> | undefined;

  /**
   * DuckDB インスタンスと接続のプライベートプロパティーです。
   */
  #duckdb: { ins: DuckDBInstance; con: DuckDBConnection } | null;

  /**
   * `DuckdbNodeNeo` クラスの新しいインスタンスを生成します。
   *
   * @param path データベースファイルのパスです。
   * @param jsonify オブジェクトを JSON 形式に変換する関数です。
   * @param options `DuckDBInstance` を作成する際のオプションです。
   * @see https://duckdb.org/docs/stable/configuration/overview.html
   */
  public constructor(
    path: string,
    jsonify?: IJsonify | undefined,
    options?: Record<string, string> | undefined,
  ) {
    this.#path = path;
    this.#duckdb = null;
    this.#options = options;
    this.#jsonify = jsonify;
  }

  /**
   * 指定されたパスのデータベースを開きます。
   */
  @asyncmux
  public async open(): Promise<void> {
    if (this.#duckdb) {
      return;
    }

    const ins = await DuckDBInstance.create(this.#path, this.#options);
    const con = await ins.connect();
    this.#duckdb = {
      ins,
      con,
    };
  }

  /**
   * データベース接続を閉じます。
   */
  @asyncmux
  public async close(): Promise<void> {
    if (!this.#duckdb) {
      return;
    }

    this.#duckdb.con.closeSync();
    this.#duckdb.ins.closeSync();
    this.#duckdb = null;
  }

  /**
   * SQL クエリーを実行します。
   *
   * @param text 実行する SQL クエリーです。
   */
  @asyncmux.readonly
  public async exec(text: string): Promise<void> {
    if (!this.#duckdb) {
      throw new DatabaseNotOpenError();
    }

    const sam = await this.#duckdb.con.stream(text);
    await consumeAllRows(sam);
  }

  /**
   * SQL クエリーを実行し、結果の行を非同期ジェネレーターとして取得します。
   *
   * @param text 実行する SQL クエリーです。
   * @returns 行の非同期ジェネレーターを返します。
   */
  public async *query(text: string): AsyncGenerator<Row, void, void> {
    if (!this.#duckdb) {
      throw new DatabaseNotOpenError();
    }

    using _lock = await asyncmux.readonly(this);
    const sam = await this.#duckdb.con.stream(text);
    yield* readAllRows(this.#jsonify || jsonify, sam);
  }

  /**
   * SQL ステートメントを準備します。
   *
   * @param text 準備する SQL ステートメントです。
   * @returns 準備された `IStatement` オブジェクトを返します。
   */
  @asyncmux.readonly
  public async prepare(text: string): Promise<IStatement> {
    if (!this.#duckdb) {
      throw new DatabaseNotOpenError();
    }

    // this.#duckdb.con.prepare メソッドがありますが、なぜかパラメーターのバインディングに失敗するので、コネクション
    // を直接使用します。
    return new Statement(this.#jsonify || jsonify, text, this.#duckdb.con);
  }
}

/***************************************************************************************************
 *
 * toJSON
 *
 **************************************************************************************************/

if (!("toJSON" in DuckDBArrayValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBArrayValue, function() {
    return this.items;
  });
}

if (!("toJSON" in DuckDBBitValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBBitValue, function() {
    return this.toBits();
  });
}

if (!("toJSON" in DuckDBBlobValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBBlobValue, function() {
    return Array.from(this.bytes);
  });
}

if (!("toJSON" in DuckDBDateValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBDateValue, function() {
    return this.toString();
  });
}

if (!("toJSON" in DuckDBDecimalValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBDecimalValue, function() {
    return this.toString();
  });
}

if (!("toJSON" in DuckDBIntervalValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBIntervalValue, function() {
    return this.toString();
  });
}

if (!("toJSON" in DuckDBListValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBListValue, function() {
    return this.items;
  });
}

if (!("toJSON" in DuckDBMapValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBMapValue, function() {
    return new Map(this.entries.map(entry => [
      entry.key,
      entry.value,
    ]));
  });
}

if (!("toJSON" in DuckDBStructValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBStructValue, function() {
    return this.entries;
  });
}

if (!("toJSON" in DuckDBTimestampMillisecondsValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBTimestampMillisecondsValue, function() {
    return (new Date(Number(this.millis))).toISOString();
  });
}

if (!("toJSON" in DuckDBTimestampNanosecondsValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBTimestampNanosecondsValue, function() {
    return (new Date(Number(this.nanos / 1_000_000n))).toISOString();
  });
}

if (!("toJSON" in DuckDBTimestampSecondsValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBTimestampSecondsValue, function() {
    return (new Date(Number(this.seconds * 1_000n))).toISOString();
  });
}

if (!("toJSON" in DuckDBTimestampTZValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBTimestampTZValue, function() {
    return (new Date(Number(this.micros / 1_000n))).toISOString();
  });
}

if (!("toJSON" in DuckDBTimestampValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBTimestampValue, function() {
    return (new Date(Number(this.micros / 1_000n))).toISOString();
  });
}

if (!("toJSON" in DuckDBTimeTZValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBTimeTZValue, function() {
    return this.toString();
  });
}

if (!("toJSON" in DuckDBTimeValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBTimeValue, function() {
    return this.toString();
  });
}

if (!("toJSON" in DuckDBUnionValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBUnionValue, function() {
    return this.toString();
  });
}

if (!("toJSON" in DuckDBUUIDValue.prototype)) {
  /*#__PURE__*/ jsonify.register(DuckDBUUIDValue as any, function() {
    return this.toString();
  });
}
