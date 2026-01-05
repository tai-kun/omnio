import { describe, test } from "vitest";
import MemoryFileSystem from "../../../../src/envs/shared/file-system/memory-file-system.js";
import utf8 from "../../../../src/shared/_utf8.js";
import {
  EntryPathNotFoundError,
  FileSystemNotOpenError,
  InvalidInputError,
  TypeError,
} from "../../../../src/shared/errors.js";

/**
 * テスト用のファイルシステムインスタンスを作成し、開きます。
 *
 * @returns 開かれた `MemoryFileSystem` のインスタンス。
 */
function createOpenedFileSystem(): MemoryFileSystem {
  const fs = new MemoryFileSystem();
  fs.open();

  return fs;
}

describe("機能テスト", () => {
  test("open/close: 接続が閉じていると getDirectoryHandle は失敗する", ({ expect }) => {
    const fs = new MemoryFileSystem();

    expect(() => fs.getDirectoryHandle("dir1", { create: false }))
      .toThrow(FileSystemNotOpenError);
  });

  test("open/close: open すると getDirectoryHandle が機能し、close すると再び失敗する", ({ expect }) => {
    const fs = createOpenedFileSystem();

    expect(() => fs.getDirectoryHandle("dir1", { create: false }))
      .toThrow(EntryPathNotFoundError);

    fs.close();

    expect(() => fs.getDirectoryHandle("dir1", { create: false }))
      .toThrow(FileSystemNotOpenError);
  });

  test("getDirectoryHandle (Root): create: true でディレクトリーを作成", ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir1", { create: true });

    expect(dirHandle).toBeDefined();
    expect(fs.tree()).toStrictEqual({
      "dir1": {},
    });
  });

  test("getDirectoryHandle (Root): create: false で存在しないディレクトリーはエラー", ({ expect }) => {
    const fs = createOpenedFileSystem();

    expect(() => fs.getDirectoryHandle("dir1", { create: false }))
      .toThrow(EntryPathNotFoundError);
  });

  test("getDirectoryHandle (Root): 不正なディレクトリー名はエラー", ({ expect }) => {
    const fs = createOpenedFileSystem();

    expect.soft(() => fs.getDirectoryHandle(".", { create: true }))
      .toThrow(InvalidInputError);
    expect.soft(() => fs.getDirectoryHandle("..", { create: true }))
      .toThrow(InvalidInputError);
    expect.soft(() => fs.getDirectoryHandle("dir\n", { create: true }))
      .toThrow(InvalidInputError);
    expect.soft(() => fs.getDirectoryHandle("dir/name", { create: true }))
      .toThrow(InvalidInputError);
    expect.soft(() => fs.getDirectoryHandle("a".repeat(255), { create: true }))
      .not
      .toThrow();
    expect.soft(() => fs.getDirectoryHandle("a".repeat(256), { create: true }))
      .toThrow(InvalidInputError);
  });

  test("FileHandle (Root): 不正なファイル名はエラー", ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dir = fs.getDirectoryHandle("a", { create: true });

    expect.soft(() => dir.getFileHandle(".", { create: true }))
      .toThrow(InvalidInputError);
    expect.soft(() => dir.getFileHandle("..", { create: true }))
      .toThrow(InvalidInputError);
    expect.soft(() => dir.getFileHandle("dir\n", { create: true }))
      .toThrow(InvalidInputError);
    expect.soft(() => dir.getFileHandle("dir/name", { create: true }))
      .toThrow(InvalidInputError);
    expect.soft(() => dir.getFileHandle("a".repeat(248), { create: true }))
      .not
      .toThrow();
    expect.soft(() => dir.getFileHandle("a".repeat(249), { create: true }))
      .toThrow(InvalidInputError);
  });

  test("DirectoryHandle.getDirectoryHandle (Nested): ネストしたディレクトリーを作成", ({ expect }) => {
    const fs = createOpenedFileSystem();
    const rootDir = fs.getDirectoryHandle("dir1", { create: true });
    const nestedDir = rootDir.getDirectoryHandle("dir2", { create: true });

    expect(nestedDir).toBeDefined();
    expect(fs.tree()).toEqual({
      "dir1": {
        "dir2": {},
      },
    });
  });

  test("DirectoryHandle.getFileHandle: create: true でファイルを作成", ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir1", { create: true });
    const fileHandle = dirHandle.getFileHandle("file.txt", { create: true });

    expect(fileHandle).toBeDefined();
    expect(fs.tree()).toEqual({
      "dir1": {
        "file.txt": {
          chunks: [],
        },
      },
    });
  });

  test("DirectoryHandle.getFileHandle: create: false で存在しないファイルはエラー", ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir1", { create: true });

    expect(() => dirHandle.getFileHandle("file.txt", { create: false }))
      .toThrow(EntryPathNotFoundError);
  });

  test("DirectoryHandle.getFileHandle: 既存のディレクトリーと同じ名前のファイルを作成しようとするとエラー", ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir1", { create: true });
    dirHandle.getDirectoryHandle("entry", { create: true });

    expect(() => dirHandle.getFileHandle("entry", { create: true }))
      .toThrow(EntryPathNotFoundError);
  });

  test("DirectoryHandle.getDirectoryHandle: 既存のファイルと同じ名前のディレクトリーを作成しようとするとエラー", ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir1", { create: true });
    dirHandle.getFileHandle("entry", { create: true });

    expect(() => dirHandle.getDirectoryHandle("entry", { create: true }))
      .toThrow(EntryPathNotFoundError);
  });

  test("FileHandle.createWritable (write/close): ファイルへの書き込みが正常に完了する", async ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir", { create: true });
    const fileHandle = dirHandle.getFileHandle("test.txt", { create: true });
    const writable = await fileHandle.createWritable({ keepExistingData: false });
    const data1 = utf8.encode("こんにちは");
    const data2 = utf8.encode("世界");
    writable.write(data1);
    writable.write(data2);
    writable.close();
    const file = await fileHandle.getFile();
    const content = await file.text();

    expect(content).toBe("こんにちは世界");
    expect(fs.tree()).toEqual({
      "dir": {
        "test.txt": {
          chunks: [
            Array.from(data1),
            Array.from(data2),
          ],
        },
      },
    });
  });

  test("FileHandle.createWritable (write/abort): ファイル書き込みを中断すると一時ファイルが削除される", async ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir", { create: true });
    const fileHandle = dirHandle.getFileHandle("test.txt", { create: true });
    const initialFile = await fileHandle.getFile();

    expect(await initialFile.text()).toBe("");

    const writable = await fileHandle.createWritable({ keepExistingData: false });

    expect(fs.tree()).toEqual({
      "dir": {
        "test.txt": {
          chunks: [],
        },
        "test.txt.crswap": {
          chunks: [],
        },
      },
    });

    const data1 = utf8.encode("データ");
    writable.write(data1);

    expect(fs.tree()).toEqual({
      "dir": {
        "test.txt": {
          chunks: [],
        },
        "test.txt.crswap": {
          chunks: [
            Array.from(data1),
          ],
        },
      },
    });

    writable.abort("テスト中断");

    expect(fs.tree()).toEqual({
      "dir": {
        "test.txt": {
          chunks: [],
        },
      },
    });

    const finalFile = await fileHandle.getFile();

    expect(await finalFile.text()).toBe("");
  });

  test("WritableFileStream: close または abort 後の write/close/abort はエラー", async ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir", { create: true });
    const fileHandle = dirHandle.getFileHandle("test.txt", { create: true });
    const writableClose = await fileHandle.createWritable({ keepExistingData: false });
    writableClose.close();

    expect(() => writableClose.write(new Uint8Array())).toThrow(undefined);
    expect(() => writableClose.close()).toThrow(undefined);
    expect(() => writableClose.abort("any reason")).toThrow(undefined);

    const writableAbort = await fileHandle.createWritable({ keepExistingData: false });
    const reasonAbort = "Aborted";
    writableAbort.abort(reasonAbort);

    expect(() => writableAbort.write(new Uint8Array())).toThrow(reasonAbort);
    expect(() => writableAbort.close()).toThrow(reasonAbort);
    expect(() => writableAbort.abort("other reason")).toThrow(reasonAbort);
  });

  test("WritableFileStream: 不正なデータ型 (Uint8Array 以外) で write すると TypeError", async ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir", { create: true });
    const fileHandle = dirHandle.getFileHandle("test.txt", { create: true });
    const writable = await fileHandle.createWritable({ keepExistingData: false });

    // @ts-expect-error テストのために不正な型を渡します。
    expect(() => writable.write("string data")).toThrow(TypeError);
    // @ts-expect-error テストのために不正な型を渡します。
    expect(() => writable.write(new ArrayBuffer(8))).toThrow(TypeError);

    writable.close();
  });

  test("WritableFileStream: keepExistingData=false で上書き", async ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir", { create: true });
    const fileHandle = dirHandle.getFileHandle("test.txt", { create: true });
    {
      const writable = await fileHandle.createWritable({ keepExistingData: false });
      writable.write(new Uint8Array([0, 1, 2]));
      writable.close();
    }
    {
      const writable = await fileHandle.createWritable({ keepExistingData: false });
      writable.write(new Uint8Array([3, 4, 5]));
      writable.close();
    }

    expect(fs.tree()).toEqual({
      "dir": {
        "test.txt": {
          chunks: [
            [3, 4, 5],
          ],
        },
      },
    });
  });

  test("WritableFileStream: keepExistingData=true で追記", async ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir", { create: true });
    const fileHandle = dirHandle.getFileHandle("test.txt", { create: true });
    {
      const writable = await fileHandle.createWritable({ keepExistingData: true });
      writable.write(new Uint8Array([0, 1, 2]));
      writable.close();
    }
    {
      const writable = await fileHandle.createWritable({ keepExistingData: true });
      writable.write(new Uint8Array([3, 4, 5]));
      writable.close();
    }

    expect(fs.tree()).toEqual({
      "dir": {
        "test.txt": {
          chunks: [
            [0, 1, 2],
            [3, 4, 5],
          ],
        },
      },
    });
  });

  test("DirectoryHandle.removeEntry: ファイルを削除", ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir", { create: true });
    dirHandle.getFileHandle("file.txt", { create: true });
    dirHandle.removeEntry("file.txt", { recursive: false });

    expect(fs.tree()).toEqual({
      "dir": {},
    });
  });

  test("DirectoryHandle.removeEntry: 空のディレクトリーを削除 (recursive: false)", ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir", { create: true });
    dirHandle.getDirectoryHandle("emptyDir", { create: true });
    dirHandle.removeEntry("emptyDir", { recursive: false });

    expect(fs.tree()).toEqual({
      "dir": {},
    });
  });

  test("DirectoryHandle.removeEntry: 空でないディレクトリーの削除 (recursive: false はエラー)", async ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir", { create: true });
    const nestedDir = dirHandle.getDirectoryHandle("nested", { create: true });
    nestedDir.getFileHandle("file.txt", { create: true });

    // recursive: false で削除しようとするとエラーになります。
    expect(() => dirHandle.removeEntry("nested", { recursive: false })).toThrow();
  });

  test("DirectoryHandle.removeEntry: 空でないディレクトリーの削除 (recursive: true は成功)", async ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir", { create: true });
    const nestedDir = dirHandle.getDirectoryHandle("nested", { create: true });
    nestedDir.getFileHandle("file.txt", { create: true });
    dirHandle.removeEntry("nested", { recursive: true });

    expect(fs.tree()).toEqual({
      "dir": {},
    });
  });

  test("DirectoryHandle.removeEntry: 存在しないエントリーの削除はエラー", ({ expect }) => {
    const fs = createOpenedFileSystem();
    const dirHandle = fs.getDirectoryHandle("dir", { create: true });

    expect(() => dirHandle.removeEntry("nothing", { recursive: false }))
      .toThrow(EntryPathNotFoundError);
  });
});

describe("E2E シナリオ", () => {
  const TEST_DATA_1 = new Uint8Array([0x01, 0x02, 0x03]);
  const TEST_DATA_2 = new Uint8Array([0x04, 0x05]);
  const TEST_DATA_COMBINED = new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05]);

  test("ファイルシステム全体（作成、書き込み、読み取り、削除）の基本的なライフサイクル", async ({ expect }) => {
    const fs = createOpenedFileSystem();

    // ディレクトリーの作成 (create: true)

    const dirHandle = fs.getDirectoryHandle("testDir", { create: true });

    expect(dirHandle).toBeDefined();
    expect(fs.tree()).toStrictEqual({
      "testDir": {},
    });

    // ファイルの作成 (create: true)

    const fileHandle = dirHandle.getFileHandle("testFile.txt", { create: true });

    expect(fileHandle).toBeDefined();
    expect(fs.tree()).toStrictEqual({
      "testDir": {
        "testFile.txt": {
          chunks: [],
        },
      },
    });

    // ファイルへの書き込み

    const writable = await fileHandle.createWritable({ keepExistingData: false });

    expect(writable).toBeDefined();
    expect(fs.tree()).toStrictEqual({
      "testDir": {
        "testFile.txt": {
          chunks: [],
        },
        "testFile.txt.crswap": {
          chunks: [],
        },
      },
    });

    writable.write(TEST_DATA_1);
    writable.write(TEST_DATA_2);
    writable.close();

    expect(fs.tree()).toStrictEqual({
      "testDir": {
        "testFile.txt": {
          chunks: [
            Array.from(TEST_DATA_1),
            Array.from(TEST_DATA_2),
          ],
        },
      },
    });

    // ファイルの読み取り

    const file = await fileHandle.getFile();

    expect(file.name).toBe("testFile.txt");
    expect(file.size).toBe(TEST_DATA_COMBINED.byteLength);

    const content = new Uint8Array(await file.arrayBuffer());

    expect(content).toEqual(TEST_DATA_COMBINED);

    // ファイルの削除 (recursive: false)

    dirHandle.removeEntry("testFile.txt", { recursive: false });

    // ファイルが存在しないことの確認 (create: false)

    expect(fs.tree()).toStrictEqual({
      "testDir": {},
    });
    expect(() => dirHandle.getFileHandle("testFile.txt", { create: false }))
      .toThrow(EntryPathNotFoundError);

    // ディレクトリーの削除 (recursive: true)

    const subDirHandle = dirHandle.getDirectoryHandle("subDir", { create: true });
    const subFileHandle = subDirHandle.getFileHandle("subFile.txt", { create: true });
    const subWritable = await subFileHandle.createWritable({ keepExistingData: false });
    subWritable.write(TEST_DATA_1);
    subWritable.close();

    expect(fs.tree()).toStrictEqual({
      "testDir": {
        "subDir": {
          "subFile.txt": {
            chunks: [
              Array.from(TEST_DATA_1),
            ],
          },
        },
      },
    });

    // ルートから testDir を再帰的に削除します。

    dirHandle.removeEntry("subDir", { recursive: true });

    // サブディレクトリーが存在しないことの確認

    expect(fs.tree()).toStrictEqual({
      "testDir": {},
    });
    expect(() => dirHandle.getDirectoryHandle("subDir", { create: false }))
      .toThrow(EntryPathNotFoundError);
  });
});
