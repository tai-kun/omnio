import { type ILogger, LogLevel } from "../../../shared/logger.js";
import ConsoleLogger from "../../shared/logger/console-logger.js";

function isDebugMode(): boolean {
  try {
    return (
      // @ts-expect-error
      process.env.DEBUG === "1" || process.env.DEBUG === "true"
      // @ts-expect-error
      || process.env.RUNNER_DEBUG === "1" || process.env.RUNNER_DEBUG === "true"
      // @ts-expect-error
      || process.env.ACTIONS_RUNNER_DEBUG === "1" || process.env.ACTIONS_RUNNER_DEBUG === "true"
      // @ts-expect-error
      || process.env.ACTIONS_STEP_DEBUG === "1" || process.env.ACTIONS_STEP_DEBUG === "true"
    );
  } catch {
    return false;
  }
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
