import { type ILogger, type LogEntry, LogLevel } from "../../../shared/logger.js";

/**
 * ログを標準出力や標準エラーに記録するロガーです。`ILogger` インターフェースを実装しています。
 */
export default class ConsoleLogger implements ILogger {
  /**
   * このロガーが記録するログレベルのしきい値です。指定されたレベル以上のログのみが記録されます。
   */
  public readonly level: LogLevel;

  /**
   * `ConsoleLogger` の新しいインスタンスを構築します。
   *
   * @param level 記録するログレベルのしきい値です。指定されない場合は `LogLevel.DEBUG` が使用されます。
   */
  public constructor(level: LogLevel | undefined = LogLevel.ERROR) {
    this.level = level;
  }

  /**
   * ログを記録します。`entry.level` がこのロガーの `level` 以上の場合にのみ、メッセージを `console` に出力します。
   *
   * @param entry ログの内容です。
   */
  public log(entry: LogEntry): void {
    if (entry.level < this.level) {
      return;
    }

    switch (entry.level) {
      case LogLevel.ERROR:
        if ("reason" in entry) {
          console.error(entry.message, entry.reason);
        } else {
          console.error(entry.message);
        }
        break;

      case LogLevel.WARN:
        if ("reason" in entry) {
          console.warn(entry.message, entry.reason);
        } else {
          console.warn(entry.message);
        }
        break;

      case LogLevel.INFO:
        console.info(entry.message);
        break;

      case LogLevel.DEBUG:
        console.debug(entry.message);
        break;

      default:
        entry satisfies never;
    }
  }
}
