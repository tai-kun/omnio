/**
 * ログレベルの型定義です。
 */
export type LogLevel = (typeof LogLevel)[keyof typeof LogLevel];

/**
 * ログレベルを定義する定数です。
 */
export const LogLevel = {
  DEBUG: 1,
  INFO: 2,
  WARN: 3,
  ERROR: 4,
  QUIET: 5,
} as const;

/**
 * ログの内容を定義する型です。
 */
export type LogEntry =
  | {
    /**
     * ログレベルです。
     */
    level: typeof LogLevel.DEBUG;
    /**
     * デバッグメッセージです。
     */
    message: string;
  }
  | {
    /**
     * ログレベルです。
     */
    level: typeof LogLevel.INFO;
    /**
     * 諸情報です。
     */
    message: string;
  }
  | {
    /**
     * ログレベルです。
     */
    level: typeof LogLevel.WARN;
    /**
     * 警告メッセージです。
     */
    message: string;
    /**
     * 警告の原因です。
     */
    reason?: unknown;
  }
  | {
    /**
     * ログレベルです。
     */
    level: typeof LogLevel.ERROR;
    /**
     * エラーメッセージです。
     */
    message: string;
    /**
     * エラーの原因です。
     */
    reason?: unknown;
  };

/**
 * Omnio で使用されるロガーのインターフェースです。
 * 内部情報や、ただちにアプリケーションを停止する必要はないものの、記録しておくべきメッセージを通知する際に使用されます。
 */
export interface ILogger {
  /**
   * 指定されたログレベルとメッセージでログを記録します。
   *
   * @param entry ログの内容です。
   */
  log(entry: LogEntry): void;
}
