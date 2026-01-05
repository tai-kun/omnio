import {
  AsyncDuckDB,
  type AsyncDuckDBConnection,
  AsyncPreparedStatement,
  DuckDBAccessMode,
  type DuckDBBundle,
  type Logger as DuclDBLogger,
} from "@duckdb/duckdb-wasm";
import { type Asyncmux, asyncmux, type AsyncmuxLock } from "asyncmux";
import type { IDatabase, IStatement, Row } from "../../../shared/database.js";
import { DatabaseNotOpenError, SqlStatementClosedError } from "../../../shared/errors.js";
import defaultJsonify, { type IJsonify } from "../../../shared/jsonify.js";

/**
 * `AsyncDuckDBConnection` の `send` メソッドの戻り値の型を定義します。
 */
type AsyncRecordBatchStreamReader = Awaited<ReturnType<AsyncDuckDBConnection["send"]>>;

/**
 * `AsyncRecordBatchStreamReader` からすべての行を読み取り、行のジェネレーターを生成します。
 *
 * @param jsonify オブジェクトを JSON 形式に変換する関数です。
 * @param sam 読み取る `AsyncRecordBatchStreamReader` オブジェクトです。
 * @returns 行の非同期ジェネレーターを返します。
 */
async function* readAllRows(
  jsonify: IJsonify,
  sam: AsyncRecordBatchStreamReader,
): AsyncGenerator<Row, void, void> {
  for await (const rows of sam) {
    for (const row of rows) {
      yield jsonify(row) as Row;
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
   * DuckDB の非同期プリペアドステートメントのプライベートプロパティーです。
   */
  readonly #stmt: AsyncPreparedStatement;

  /**
   * 獲得した書き込みロックです。
   */
  readonly #lock: AsyncmuxLock;

  /**
   * ステートメントが開いているかどうかを示すフラグです。
   */
  #open: boolean;

  /**
   * `Statement` クラスの新しいインスタンスを生成します。
   *
   * @param jsonify オブジェクトを JSON 形式に変換する関数です。
   * @param stmt `AsyncPreparedStatement` オブジェクトです。
   * @param lock 獲得した書き込みロックです。
   */
  public constructor(jsonify: IJsonify, stmt: AsyncPreparedStatement, lock: AsyncmuxLock) {
    this.#jsonify = jsonify;
    this.#stmt = stmt;
    this.#open = true;
    this.#lock = lock;
  }

  /**
   * ステートメントを閉じます。
   */
  public async close(): Promise<void> {
    if (!this.#open) {
      return;
    }

    try {
      await this.#stmt.close();
    } finally {
      this.#open = false;
      this.#lock.unlock();
    }
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

    // `.send` だとなぜか「Cannot prepare multiple statements at once!」というエラーが出るため、ストリームを消費
    // するのではなく、`.query` の結果を破棄するようにします。
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
      throw new SqlStatementClosedError();
    }

    const sam = await this.#stmt.send(...values);
    yield* readAllRows(this.#jsonify, sam);
  }
}

/**
 * DuckDB の各種モジュールの情報です。
 */
export type DuckdbBundle = DuckDBBundle;

/**
 * DuckDB のロガーのインターフェースです。
 */
export interface IDuckdbLogger extends DuclDBLogger {}

/**
 * データベース接続を管理するためのクラスです。内部的に WebAssembly 版の DuckDB を使用しています。
 */
export default class DuckdbWasm implements IDatabase {
  /**
   * オブジェクトを JSON 形式に変換する関数です。
   */
  readonly #jsonify: IJsonify;

  /**
   * データベースファイルのパスです。
   */
  readonly #path: string;

  /**
   * 読み込み済みの DuckDB バンドルのプライベートプロパティーです。
   */
  readonly #bundle: DuckdbBundle;

  /**
   * データベース操作を記録するロガーのプライベートプロパティーです。
   */
  readonly #logger: IDuckdbLogger;

  /**
   * 排他制御のためのキューを管理するオブジェクトです。
   */
  readonly #mux: Asyncmux;

  /**
   * DuckDB インスタンスと接続のプライベートプロパティーです。
   */
  #duckdb: Readonly<{ ins: AsyncDuckDB; con: AsyncDuckDBConnection }> | null;

  /**
   * `DuckdbWasm` クラスの新しいインスタンスを生成します。
   *
   * @param path データベースファイルのパスです。
   * @param bundle DuckDB の各種モジュール情報を含むバンドルです。
   * @param logger ログを記録するためのロガーです。
   * @param jsonify オブジェクトを JSON 形式に変換する関数です。
   */
  public constructor(
    path: string,
    bundle: DuckdbBundle,
    logger: IDuckdbLogger,
    jsonify: IJsonify | undefined = defaultJsonify,
  ) {
    this.#jsonify = jsonify;
    this.#path = path;
    this.#bundle = bundle;
    this.#logger = logger;
    this.#mux = asyncmux.create();
    this.#duckdb = null;
  }

  /**
   * 指定されたパスのデータベースを開きます。
   */
  @asyncmux
  public async open(): Promise<void> {
    const {
      mainModule,
      mainWorker,
      pthreadWorker,
    } = this.#bundle;
    // @ts-ignore
    const worker = new Worker(mainWorker!);
    const ins = new AsyncDuckDB(this.#logger, worker);
    await ins.instantiate(mainModule, pthreadWorker);
    await ins.open({
      path: this.#path,
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
  @asyncmux
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
  @asyncmux.readonly
  public async exec(text: string): Promise<void> {
    if (!this.#duckdb) {
      throw new DatabaseNotOpenError();
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
      throw new DatabaseNotOpenError();
    }

    using _lock = await asyncmux.readonly(this);
    const sam = await this.#duckdb.con.send(text);
    yield* readAllRows(this.#jsonify, sam);
  }

  /**
   * SQL ステートメントを準備します。
   *
   * @param text 準備する SQL ステートメントです。
   * @returns 準備された `Statement` オブジェクトを返します。
   */
  public async prepare(text: string): Promise<Statement> {
    if (!this.#duckdb) {
      throw new DatabaseNotOpenError();
    }

    const lock = await this.#mux.lock(); // 複数のステートメントを作成しないように書き込みロックします。
    try {
      const stmt = await this.#duckdb.con.prepare(text);
      return new Statement(this.#jsonify, stmt, lock);
    } catch (ex) {
      lock.unlock();
      throw ex;
    }
  }
}
