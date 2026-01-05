import { type ILogger, LogLevel } from "../../../shared/logger.js";
import ConsoleLogger from "../../shared/logger/console-logger.js";

function isDebugMode(): boolean {
  return ["DEBUG", "RUNNER_DEBUG", "ACTIONS_RUNNER_DEBUG", "ACTIONS_STEP_DEBUG"]
    .some(k => ["1", "true"].includes(process.env[k]?.toLowerCase()!));
}

export default function getLogger(logger: ILogger | undefined): ILogger {
  if (logger !== undefined) {
    return logger;
  } else if (isDebugMode()) {
    return new ConsoleLogger(LogLevel.DEBUG);
  } else {
    return new ConsoleLogger(LogLevel.WARN);
  }
}
