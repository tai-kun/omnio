import type { LogEntry, Logger } from "./logger.types.js";

/**
 * ログをどこにも記録せず、単に破棄するだけのロガーです。
 * `Logger` インターフェースを実装しており、ログ処理が不要な場合に使用されます。
 */
export default class VoidLogger implements Logger {
  /**
   * ログを記録するメソッドですが、何も処理を行いません。
   *
   * @param _entry ログの内容です。
   */
  log(_entry: LogEntry): void {}
}
