import type { ILogger, LogEntry } from "../../../shared/logger.js";

/**
 * ログをどこにも記録せず、単に破棄するだけのロガーです。
 * `ILogger` インターフェースを実装しており、ログ処理が不要な場合に使用されます。
 */
export default class VoidLogger implements ILogger {
  /**
   * ログを記録するメソッドですが、何も処理を行いません。
   *
   * @param _entry ログの内容です。
   */
  log(_entry: LogEntry): void {}
}
