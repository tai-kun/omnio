import * as fs from "node:fs/promises";
import * as path from "node:path";
import { beforeEach } from "node:test";
import { afterAll, beforeAll, describe, test } from "vitest";
import { NodeFsError } from "../../src/errors.js";
import {
  NodeFs,
  NodeFsDirectoryHandle,
  NodeFsFileHandle,
  NodeFsWritableFileStream,
} from "../../src/fs/node-fs.js";

const testDir = path.join(
  "tests",
  "fs",
  ".temp",
  "nodefs.server.test.ts",
  (new Date()).toISOString(),
);

function defer(fn: () => void | PromiseLike<void>): AsyncDisposable {
  return {
    async [Symbol.asyncDispose]() {
      await fn();
    },
  };
}

function encode(s: string) {
  return new TextEncoder().encode(s);
}

describe("NodeFsPath", () => {
  test("単純なパス結合が正しく解決される", ({ expect }) => {
    const nodefsPath = (new NodeFs(testDir)).path;
    const result = nodefsPath.resolve("dir", "file.txt");

    expect(result).toBe(path.resolve(testDir, "dir", "file.txt"));
  });

  test("ルートディレクトリーと同一パスの場合はルートディレクトリーを返す", ({ expect }) => {
    const nodefsPath = (new NodeFs(testDir)).path;
    const result = nodefsPath.resolve("");

    expect(result).toBe(path.resolve(testDir) + path.sep);
  });

  test("パスがルートディレクトリー外を指す場合は NodeFsError を投げる", ({ expect }) => {
    const nodefsPath = (new NodeFs(testDir)).path;

    expect(() => nodefsPath.resolve("..", "..", "etc", "passwd")).toThrow(NodeFsError);
  });

  test("解決後のパスが末尾にセパレーターを持つ場合は除去される", ({ expect }) => {
    const nodefsPath = (new NodeFs(testDir)).path;
    const result = nodefsPath.resolve("dir/");

    expect(result.endsWith(path.sep)).toBe(false);
    expect(result).toBe(path.resolve(testDir, "dir"));
  });
});

describe("NodeFs", () => {
  test("constructor() は root プロパティを正しく設定する", ({ expect }) => {
    const nodefs = new NodeFs(testDir);

    expect(nodefs.root).toBe(path.resolve(testDir) + path.sep);
  });

  test("open() はルートディレクトリを作成し、接続状態を変更する", async ({ expect }) => {
    const nodefs = new NodeFs(path.join(testDir, "open_test"));
    await nodefs.open();
    const stats = await fs.stat(nodefs.root);

    expect(stats.isDirectory()).toBe(true);
  });

  test("getDirectoryHandle() は閉じた状態で呼び出すとエラーを投げる", async ({ expect }) => {
    const nodefs = new NodeFs(testDir);

    await expect(nodefs.getDirectoryHandle("test_dir", { create: false }))
      .rejects
      .toThrow(NodeFsError);
  });

  test("getDirectoryHandle() はオープンな状態でディレクトリハンドルを返す", async ({ expect }) => {
    const nodefs = new NodeFs(testDir);
    await nodefs.open();
    await using _ = defer(async () => await nodefs.close());

    await expect(nodefs.getDirectoryHandle("new_dir", { create: true }))
      .resolves
      .toBeInstanceOf(NodeFsDirectoryHandle);
  });
});

describe("NodeFsDirectoryHandle", () => {
  const dirPath = path.join(testDir, "dir_handle_test");
  let dirHandle: NodeFsDirectoryHandle;

  beforeAll(async () => {
    await fs.mkdir(dirPath, { recursive: true });
    dirHandle = new NodeFsDirectoryHandle(dirPath);
  });

  test("getFileHandle() はファイルを作成し、ファイルハンドルを返す", async ({ expect }) => {
    const fileName = "test_file.txt";

    await expect(dirHandle.getFileHandle(fileName, { create: true }))
      .resolves
      .toBeInstanceOf(NodeFsFileHandle);

    const filePath = path.join(dirPath, fileName);
    const stats = await fs.stat(filePath);

    expect(stats.isFile()).toBe(true);
  });

  test("getDirectoryHandle() はディレクトリを作成し、ディレクトリハンドルを返す", async ({ expect }) => {
    const dirName = "subdir";
    await dirHandle.getDirectoryHandle(dirName, { create: true });
    const newDirPath = path.join(dirPath, dirName);
    const stats = await fs.stat(newDirPath);

    expect(stats.isDirectory()).toBe(true);
  });
});

describe("NodeFsFileHandle", () => {
  const filePath = path.join(testDir, "file_handle_test.txt");
  const fileContent = "テストコンテンツです。";
  let fileHandle: NodeFsFileHandle;

  beforeAll(async () => {
    await fs.writeFile(filePath, fileContent);
    fileHandle = new NodeFsFileHandle(filePath);
  });

  test("getFile() はファイルの内容を含む File オブジェクトを返す", async ({ expect }) => {
    const file = await fileHandle.getFile();

    expect(file).toBeInstanceOf(File);
    expect(file.name).toBe("file_handle_test.txt");
    await expect(file.text())
      .resolves
      .toBe(fileContent);
  });

  test("createWritable() は `NodeFsWritableFileStream` を返す", async ({ expect }) => {
    const writableStream = await fileHandle.createWritable();
    await using _ = defer(async () => await writableStream.close());

    expect(writableStream).toBeInstanceOf(NodeFsWritableFileStream);
  });
});

describe("NodeFsWritableFileStream", () => {
  const targetPath = path.join(testDir, "writable_file_test.txt");
  const crswapPath = path.join(testDir, "writable_file_test.txt.crswap");

  beforeEach(async () => {
    await fs.rm(targetPath, { force: true });
    await fs.rm(crswapPath, { force: true });
  });

  test("write() と close() はファイルを正常に書き換える", async ({ expect }) => {
    await fs.writeFile(targetPath, "既存のデータです。");

    const fileHandle = await fs.open(crswapPath, "w");
    const writableStream = new NodeFsWritableFileStream(
      targetPath,
      crswapPath,
      fileHandle,
    );
    await writableStream.write(encode("新しいコンテンツです。"));
    await writableStream.close();

    await expect(fs.readFile(targetPath, "utf-8"))
      .resolves
      .toBe("新しいコンテンツです。");
    await expect(fs.stat(crswapPath))
      .rejects
      .toThrow("no such file or directory");
  });

  test("abort() は書き込みを中止し、一時ファイルを削除する", async ({ expect }) => {
    const originalContent = "元のデータです。";
    await fs.writeFile(targetPath, originalContent);

    const fileHandle = await fs.open(crswapPath, "w");
    const writableStream = new NodeFsWritableFileStream(
      targetPath,
      crswapPath,
      fileHandle,
    );
    await writableStream.write(encode("中止するデータです。"));
    await writableStream.abort();

    await expect(fs.readFile(targetPath, "utf-8"))
      .resolves
      .toBe(originalContent);
    await expect(fs.stat(crswapPath))
      .rejects
      .toThrow("no such file or directory");
  });

  test("write() は close() 後に呼び出すとエラーを投げる", async ({ expect }) => {
    const fileHandle = await fs.open(crswapPath, "w");
    const writableStream = new NodeFsWritableFileStream(
      targetPath,
      crswapPath,
      fileHandle,
    );
    await writableStream.close();

    await expect(writableStream.write(encode("無効な書き込み")))
      .rejects
      .toThrow(undefined);
  });

  test("write() は abort() 後に呼び出すとエラーを投げる", async ({ expect }) => {
    const fileHandle = await fs.open(crswapPath, "w");
    const writableStream = new NodeFsWritableFileStream(
      targetPath,
      crswapPath,
      fileHandle,
    );
    await writableStream.abort("中止理由です。");

    await expect(writableStream.write(encode("無効な書き込み")))
      .rejects
      .toThrow("中止理由です。");
  });

  // test("close() で keepExistingData: true の場合、ストリームを使用してデータを追加する", async ({ expect }) => {
  //   const originalContent = "既存のデータです。";
  //   await fs.writeFile(targetPath, originalContent);
  //   const fileHandle = await fs.open(crswapPath, "w");
  //   const writableStream = new NodeFsWritableFileStream(
  //     targetPath,
  //     crswapPath,
  //     fileHandle,
  //     { keepExistingData: true },
  //   );
  //   await writableStream.write(encode("追加データです。"));
  //   await writableStream.close();

  //   await expect(fs.readFile(targetPath, "utf-8"))
  //     .resolves
  //     .toBe("既存のデータです。追加データです。");
  // });
});

describe("e2e", () => {
  const directoryName = "flow_test_dir";
  const fileName = "flow_test_file.txt";
  const fileContent = "This is a test content for full flow.";
  let nodefs: NodeFs;

  beforeAll(async () => {
    nodefs = new NodeFs(testDir);
    await nodefs.open();
  });

  afterAll(async () => {
    await nodefs.close();
  });

  test("NodeFs からディレクトリ、ファイル、ストリームを介した一連の操作が正常に完了する", async ({ expect }) => {
    // 1. NodeFs からディレクトリハンドルを取得する

    const dirHandle = await nodefs.getDirectoryHandle(directoryName, { create: true });

    expect(dirHandle).toBeInstanceOf(NodeFsDirectoryHandle);

    const dirPath = path.join(nodefs.root, directoryName);
    const dirStats = await fs.stat(dirPath);

    expect(dirStats.isDirectory()).toBe(true);

    // 2. ディレクトリハンドルからファイルハンドルを取得する

    const fileHandle = await dirHandle.getFileHandle(fileName, { create: true });

    expect(fileHandle).toBeInstanceOf(NodeFsFileHandle);

    const filePath = path.join(dirPath, fileName);
    const fileStats = await fs.stat(filePath);

    expect(fileStats.isFile()).toBe(true);

    // 3. ファイルハンドルから書き込みストリームを取得し、ファイルに書き込む

    const writableStream = await fileHandle.createWritable();

    expect(writableStream).toBeInstanceOf(NodeFsWritableFileStream);

    await writableStream.write(encode(fileContent));
    await writableStream.close();

    // 4. 書き込み後のファイル内容を検証する

    await expect(fs.readFile(filePath, "utf-8"))
      .resolves
      .toBe(fileContent);

    // 5. 書き込み後のファイルハンドルから getFile() で内容を検証する

    const file = await fileHandle.getFile();

    await expect(file.text())
      .resolves
      .toBe(fileContent);
  });
});
