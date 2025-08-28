import {
  AsyncDuckDB,
  type AsyncDuckDBConnection,
  AsyncPreparedStatement,
  DuckDBAccessMode,
  type DuckDBBundle,
  type Logger as DuclDBLogger,
} from "@duckdb/duckdb-wasm";
import { WasmDbError } from "../errors.js";
import jsonify from "../jsonify.js";
import type { Db, Row, Statement } from "./db.types.js";

export type * from "./db.types.js";

/**
 * `AsyncDuckDBConnection` の `send` メソッドの戻り値の型を定義します。
 */
type AsyncRecordBatchStreamReader = Awaited<ReturnType<AsyncDuckDBConnection["send"]>>;

/**
 * `AsyncRecordBatchStreamReader` からすべての行を読み取り、行のジェネレーターを生成します。
 *
 * @param sam 読み取る `AsyncRecordBatchStreamReader` オブジェクトです。
 * @returns 行の非同期ジェネレーターを返します。
 */
async function* readAllRows(sam: AsyncRecordBatchStreamReader): AsyncGenerator<Row, void, void> {
  for await (const rows of sam) {
    for (const row of rows) {
      yield jsonify<Row>(row);
    }
  }
}

/**
 * SQL ステートメントを扱うためのクラスです。
 */
export class WasmDbStatement implements Statement {
  /**
   * DuckDB の非同期プリペアドステートメントのプライベートプロパティーです。
   */
  readonly #stmt: AsyncPreparedStatement;

  /**
   * ステートメントが開いているかどうかを示すフラグです。
   */
  #open: boolean;

  /**
   * `WasmDbStatement` クラスの新しいインスタンスを生成します。
   *
   * @param stmt `AsyncPreparedStatement` オブジェクトです。
   */
  public constructor(stmt: AsyncPreparedStatement) {
    this.#stmt = stmt;
    this.#open = true;
  }

  /**
   * ステートメントを閉じます。
   */
  public async close(): Promise<void> {
    if (!this.#open) {
      return;
    }

    await this.#stmt.close();
    this.#open = false;
  }

  /**
   * ステートメントを実行します。
   *
   * @param values SQL ステートメントに渡すパラメーターです。
   */
  public async exec(...values: unknown[]): Promise<void> {
    if (!this.#open) {
      throw new WasmDbError("closed");
    }

    // .send だとなぜか「Cannot prepare multiple statements at once!」というエラーが出るため、ストリームを消費
    // するのではなく、.query の結果を破棄するようにします。
    await this.#stmt.query(...values);
  }

  /**
   * ステートメントを実行し、結果の行を非同期ジェネレーターとして取得します。
   *
   * @param values SQL ステートメントに渡すパラメーターです。
   * @returns 行の非同期ジェネレーターを返します。
   */
  public async *query(...values: unknown[]): AsyncGenerator<Row, void, void> {
    if (!this.#open) {
      throw new WasmDbError("closed");
    }

    const sam = await this.#stmt.send(...values);
    yield* readAllRows(sam);
  }
}

/**
 * DuckDB の各種モジュールの情報です。
 */
export type DuckdbBundle = DuckDBBundle;

/**
 * DuckDB のロガーのインターフェースです。
 */
export interface DuckdbLogger extends DuclDBLogger {}

/**
 * データベース接続を管理するためのクラスです。内部的に WebAssembly 版の DuckDB を使用しています。
 */
export class WasmDb implements Db {
  /**
   * DuckDB インスタンスと接続のプライベートプロパティーです。
   */
  #duckdb: { ins: AsyncDuckDB; con: AsyncDuckDBConnection } | null;

  /**
   * 読み込み済みの DuckDB バンドルのプライベートプロパティーです。
   */
  readonly #bundle: DuckdbBundle;

  /**
   * データベース操作を記録するロガーのプライベートプロパティーです。
   */
  readonly #logger: DuckdbLogger;

  /**
   * WasmDb クラスの新しいインスタンスを生成します。
   * @param bundle DuckDB の各種モジュール情報を含むバンドルです。
   * @param logger ログを記録するためのロガーです。
   */
  public constructor(bundle: DuckdbBundle, logger: DuckdbLogger) {
    this.#duckdb = null;
    this.#bundle = bundle;
    this.#logger = logger;
  }

  /**
   * 指定されたパスのデータベースを開きます。
   *
   * @param path データベースファイルのパスです。
   * @returns データベースが開かれた後に解決される Promise を返します。
   */
  public async open(path: string): Promise<void> {
    const {
      mainModule,
      mainWorker,
      pthreadWorker,
    } = this.#bundle;
    const ins = new AsyncDuckDB(this.#logger, new Worker(mainWorker!));
    await ins.instantiate(mainModule, pthreadWorker);
    await ins.open({
      path,
      accessMode: DuckDBAccessMode.READ_WRITE,
    });
    const con = await ins.connect();
    this.#duckdb = {
      ins,
      con,
    };
  }

  /**
   * データベース接続を閉じます。
   */
  public async close(): Promise<void> {
    if (!this.#duckdb) {
      return;
    }

    await this.#duckdb.con.close();
    await this.#duckdb.ins.terminate();
    this.#duckdb = null;
  }

  /**
   * SQL クエリーを実行します。
   *
   * @param text 実行する SQL クエリーです。
   */
  public async exec(text: string): Promise<void> {
    if (!this.#duckdb) {
      throw new WasmDbError("Not open");
    }

    // .send だとなぜか「Cannot prepare multiple statements at once!」というエラーが出るため、ストリームを消費
    // するのではなく、.query の結果を破棄するようにします。
    await this.#duckdb.con.query(text);
  }

  /**
   * SQL クエリーを実行し、結果の行を非同期ジェネレーターとして取得します。
   *
   * @param text 実行する SQL クエリーです。
   * @returns 行の非同期ジェネレーターを返します。
   */
  public async *query(text: string): AsyncGenerator<Row, void, void> {
    if (!this.#duckdb) {
      throw new WasmDbError("Not open");
    }

    const sam = await this.#duckdb.con.send(text);
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
      throw new WasmDbError("Not open");
    }

    const stmt = await this.#duckdb.con.prepare(text);
    return new WasmDbStatement(stmt);
  }
}
