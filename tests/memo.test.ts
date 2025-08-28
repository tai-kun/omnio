import { beforeEach, test } from "vitest";
import memo from "../src/memo.js";

// 各テスト前にキャッシュを初期化する。
beforeEach(() => {
  globalThis.omnio__memo__cache = {};
});

test("同期関数の結果をキャッシュすること", ({ expect }) => {
  let count = 0;
  const fn = () => {
    count++;
    return "結果";
  };

  const result1 = memo.create("sync1", fn);
  const result2 = memo.create("sync1", fn);

  expect(result1).toBe("結果");
  expect(result2).toBe("結果");
  expect(count).toBe(1); // 1 度しか実行されていないこと。
});

test("非同期関数の結果をキャッシュすること", async ({ expect }) => {
  let count = 0;
  const fn = async () => {
    count++;
    return "非同期結果";
  };

  const result1 = await memo.create("async1", fn);
  const result2 = await memo.create("async1", fn);

  expect(result1).toBe("非同期結果");
  expect(result2).toBe("非同期結果");
  expect(count).toBe(1); // 1 度しか実行されていないこと。
});

test("Promise が reject された場合はキャッシュしないこと", async ({ expect }) => {
  let count = 0;
  const fn = async () => {
    count++;
    throw new Error("失敗");
  };

  await expect(memo.create("reject1", fn)).rejects.toThrow("失敗");
  await expect(memo.create("reject1", fn)).rejects.toThrow("失敗");
  expect(count).toBe(2); // 失敗時はキャッシュされないため 2 度実行されること。
});

// test("clear によりキャッシュが削除されること", ({ expect }) => {
//   let count = 0;
//   const fn = () => {
//     count++;
//     return "削除テスト";
//   };

//   memo.create("clear1", fn);
//   memo.clear("clear1");
//   memo.create("clear1", fn);

//   expect(count).toBe(2); // 削除後に再度実行されること。
// });

test("同期関数と非同期関数が別々にキャッシュされること", async ({ expect }) => {
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

  memo.create("id-sync", syncFn);
  await memo.create("id-async", asyncFn);

  memo.create("id-sync", syncFn);
  await memo.create("id-async", asyncFn);

  expect(syncCount).toBe(1);
  expect(asyncCount).toBe(1);
});
