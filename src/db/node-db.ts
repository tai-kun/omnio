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
import { NodeDbError } from "../errors.js";
import jsonify from "../jsonify.js";
import type { Db, Row, Statement } from "./db.types.js";

export type * from "./db.types.js";

/**
 * `DuckDBResult` からすべての行を読み取り、行のジェネレーターを生成します。
 *
 * @param sam 読み取る `DuckDBResult` オブジェクトです。
 * @returns 行のジェネレーターを返します。
 */
async function* readAllRows(sam: DuckDBResult): AsyncGenerator<Row, void, void> {
  const cols = sam.columnNames();
  while (true) {
    const chunk = await sam.fetchChunk();
    if (!chunk || chunk.rowCount === 0) {
      break;
    }

    const rows = chunk.getRowObjects(cols);
    for (const row of rows) {
      yield jsonify<Row>(row);
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
export class NodeDbStatement implements Statement {
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
   * `NodeDbStatement` クラスの新しいインスタンスを生成します。
   *
   * @param text 実行する SQL クエリーです。
   * @param conn `DuckDBConnection` オブジェクトです。
   */
  public constructor(text: string, conn: DuckDBConnection) {
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
      throw new NodeDbError("closed");
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
      throw new NodeDbError("closed");
    }

    const sam = await this.#conn.stream(this.#text, values as any[]);
    yield* readAllRows(sam);
  }
}

/**
 * Node.js 環境でデータベース接続を管理するためのクラスです。
 * 内部的に DuckDB の Node.js Client (Neo) を使用しています。
 */
export class NodeDb implements Db {
  /**
   * DuckDB インスタンスと接続のプライベートプロパティーです。
   */
  #duckdb: { ins: DuckDBInstance; con: DuckDBConnection } | null;

  #options: Record<string, string> | undefined;

  /**
   * `NodeDb` クラスの新しいインスタンスを生成します。
   *
   * @param options `DuckDBInstance` を作成する際のオプションです。
   * @see https://duckdb.org/docs/stable/configuration/overview.html
   */
  public constructor(options?: Record<string, string> | undefined) {
    this.#duckdb = null;
    this.#options = options;
  }

  /**
   * 指定されたパスのデータベースを開きます。
   *
   * @param path データベースファイルのパスです。
   */
  public async open(path: string): Promise<void> {
    if (path.startsWith("memory://")) {
      path = ":memory:";
    }

    const ins = await DuckDBInstance.create(path, this.#options);
    const con = await ins.connect();
    this.#duckdb = {
      ins,
      con,
    };
  }

  /**
   * データベース接続を閉じます。
   */
  public close(): void {
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
  public async exec(text: string): Promise<void> {
    if (!this.#duckdb) {
      throw new NodeDbError("Not open");
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
      throw new NodeDbError("Not open");
    }

    const sam = await this.#duckdb.con.stream(text);
    yield* readAllRows(sam);
  }

  /**
   * SQL ステートメントを準備します。
   *
   * @param text 準備する SQL ステートメントです。
   * @returns 準備された `Statement` オブジェクトを返します。
   */
  public async prepare(text: string): Promise<Statement> {
    if (!this.#duckdb) {
      throw new NodeDbError("Not open");
    }

    // this.#duckdb.con.prepare メソッドがありますが、なぜかパラメーターのバインディングに失敗するので、コネクション
    // を直接使用します。
    return new NodeDbStatement(text, this.#duckdb.con);
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// toJSON
//
////////////////////////////////////////////////////////////////////////////////////////////////////

if (!("toJSON" in DuckDBArrayValue.prototype)) {
  jsonify.register(DuckDBArrayValue, function() {
    return this.items;
  });
}

if (!("toJSON" in DuckDBBitValue.prototype)) {
  jsonify.register(DuckDBBitValue, function() {
    return this.toBits();
  });
}

if (!("toJSON" in DuckDBBlobValue.prototype)) {
  jsonify.register(DuckDBBlobValue, function() {
    return Array.from(this.bytes);
  });
}

if (!("toJSON" in DuckDBDateValue.prototype)) {
  jsonify.register(DuckDBDateValue, function() {
    return this.toString();
  });
}

if (!("toJSON" in DuckDBDecimalValue.prototype)) {
  jsonify.register(DuckDBDecimalValue, function() {
    return this.toString();
  });
}

if (!("toJSON" in DuckDBIntervalValue.prototype)) {
  jsonify.register(DuckDBIntervalValue, function() {
    return this.toString();
  });
}

if (!("toJSON" in DuckDBListValue.prototype)) {
  jsonify.register(DuckDBListValue, function() {
    return this.items;
  });
}

if (!("toJSON" in DuckDBMapValue.prototype)) {
  jsonify.register(DuckDBMapValue, function() {
    return new Map(this.entries.map(entry => [
      entry.key,
      entry.value,
    ]));
  });
}

if (!("toJSON" in DuckDBStructValue.prototype)) {
  jsonify.register(DuckDBStructValue, function() {
    return this.entries;
  });
}

if (!("toJSON" in DuckDBTimestampMillisecondsValue.prototype)) {
  jsonify.register(DuckDBTimestampMillisecondsValue, function() {
    return (new Date(Number(this.millis))).toISOString();
  });
}

if (!("toJSON" in DuckDBTimestampNanosecondsValue.prototype)) {
  jsonify.register(DuckDBTimestampNanosecondsValue, function() {
    return (new Date(Number(this.nanos / 1_000_000n))).toISOString();
  });
}

if (!("toJSON" in DuckDBTimestampSecondsValue.prototype)) {
  jsonify.register(DuckDBTimestampSecondsValue, function() {
    return (new Date(Number(this.seconds * 1_000n))).toISOString();
  });
}

if (!("toJSON" in DuckDBTimestampTZValue.prototype)) {
  jsonify.register(DuckDBTimestampTZValue, function() {
    return (new Date(Number(this.micros / 1_000n))).toISOString();
  });
}

if (!("toJSON" in DuckDBTimestampValue.prototype)) {
  jsonify.register(DuckDBTimestampValue, function() {
    return (new Date(Number(this.micros / 1_000n))).toISOString();
  });
}

if (!("toJSON" in DuckDBTimeTZValue.prototype)) {
  jsonify.register(DuckDBTimeTZValue, function() {
    return this.toString();
  });
}

if (!("toJSON" in DuckDBTimeValue.prototype)) {
  jsonify.register(DuckDBTimeValue, function() {
    return this.toString();
  });
}

if (!("toJSON" in DuckDBUnionValue.prototype)) {
  jsonify.register(DuckDBUnionValue, function() {
    return this.toString();
  });
}

if (!("toJSON" in DuckDBUUIDValue.prototype)) {
  jsonify.register(DuckDBUUIDValue as any, function() {
    return this.toString();
  });
}
