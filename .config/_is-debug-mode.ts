/**
 * デバッグモードで実行されているか確認します。
 *
 * @returns デバッグモードなら `true` です。
 */
export default function isDebugMode(): boolean {
  return ["DEBUG", "RUNNER_DEBUG", "ACTIONS_RUNNER_DEBUG", "ACTIONS_STEP_DEBUG"]
    .some(k => ["1", "true"].includes(process.env[k]?.toLowerCase()!));
}
