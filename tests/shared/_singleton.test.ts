import { beforeEach, test } from "vitest";
import singleton from "../../src/shared/_singleton.js";

// 各テスト前にキャッシュを初期化する。
beforeEach(() => {
  globalThis.omnio__singleton = new Map();
});

test("同期関数の結果をキャッシュする", ({ expect }) => {
  let count = 0;
  const fn = () => {
    count++;
    return "結果";
  };

  const result1 = singleton("sync1", fn);
  const result2 = singleton("sync1", fn);

  expect(result1).toBe("結果");
  expect(result2).toBe("結果");
  expect(count).toBe(1); // 1 度しか実行されていない
});

test("非同期関数の結果をキャッシュする", async ({ expect }) => {
  let count = 0;
  const fn = async () => {
    count++;
    return "非同期結果";
  };

  const result1 = await singleton("async1", fn);
  const result2 = await singleton("async1", fn);

  expect(result1).toBe("非同期結果");
  expect(result2).toBe("非同期結果");
  expect(count).toBe(1); // 1 度しか実行されていない
});

test("Promise が reject された場合はキャッシュしない", async ({ expect }) => {
  let count = 0;
  const fn = async () => {
    count++;
    throw new Error("失敗");
  };

  await expect(singleton("reject1", fn)).rejects.toThrow("失敗");
  await expect(singleton("reject1", fn)).rejects.toThrow("失敗");
  expect(count).toBe(2); // 失敗時はキャッシュされないため 2 度実行される
});

test("同期関数と非同期関数が別々にキャッシュされる", async ({ expect }) => {
  let syncCount = 0;
  let asyncCount = 0;
  const syncFn = () => {
    syncCount++;
    return "sync";
  };
  const asyncFn = async () => {
    asyncCount++;
    return "async";
  };

  singleton("id-sync", syncFn);
  await singleton("id-async", asyncFn);

  singleton("id-sync", syncFn);
  await singleton("id-async", asyncFn);

  expect(syncCount).toBe(1);
  expect(asyncCount).toBe(1);
});
