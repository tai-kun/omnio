import { afterAll, beforeAll, describe, test } from "vitest";
import { Opfs } from "../../src/fs/opfs.js";

function encode(s: string) {
  return new TextEncoder().encode(s);
}

describe("OpfsPath", () => {
  const ROOT = "root";

  test("単純なパス結合が正しく解決される", ({ expect }) => {
    const path = (new Opfs(ROOT)).path;
    const result = path.resolve("dir", "file.txt");

    expect(result).toBe("opfs://root/dir/file.txt");
  });

  test("ルートディレクトリーと同一パスの場合はルートディレクトリーを返す", ({ expect }) => {
    const path = (new Opfs(ROOT)).path;
    const result = path.resolve("");

    expect(result).toBe("opfs://root/");
  });

  test("ルートディレクトリーより上の改装には行けない", ({ expect }) => {
    const path = (new Opfs(ROOT)).path;
    const result = path.resolve("../../../other");

    expect(result).toBe("opfs://root/other");
  });

  test("パスが '://' で始まる場合でもルートディレクトリー配下になる", ({ expect }) => {
    const path = (new Opfs(ROOT)).path;
    const result = path.resolve("://absolute/path");

    expect(result).toBe("opfs://root/:/absolute/path");
  });
});

describe("e2e", () => {
  const directoryName = "flow_test_dir";
  const fileName = "flow_test_file.txt";
  const fileContent = "This is a test content for full flow.";
  let opfs: Opfs;

  beforeAll(async () => {
    opfs = new Opfs();
    await opfs.open();
  });

  afterAll(async () => {
    await opfs.close();
  });

  test("Opfs からディレクトリ、ファイル、ストリームを介した一連の操作が正常に完了する", async ({ expect }) => {
    // 1. Opfs からディレクトリハンドルを取得する

    const dirHandle = await opfs.getDirectoryHandle(directoryName, { create: true });

    await expect(
      window.navigator.storage.getDirectory()
        .then(h => h.getDirectoryHandle(directoryName)),
    )
      .resolves
      .toBeInstanceOf(FileSystemDirectoryHandle);

    // 2. ディレクトリハンドルからファイルハンドルを取得する

    const fileHandle = await dirHandle.getFileHandle(fileName, { create: true });

    await expect(
      window.navigator.storage.getDirectory()
        .then(h => h.getDirectoryHandle(directoryName))
        .then(h => h.getFileHandle(fileName)),
    )
      .resolves
      .toBeInstanceOf(FileSystemFileHandle);

    // 3. ファイルハンドルから書き込みストリームを取得し、ファイルに書き込む

    const writableStream = await fileHandle.createWritable();
    await writableStream.write(encode(fileContent));
    await writableStream.close();

    // 4. 書き込み後のファイル内容を検証する

    await expect(
      window.navigator.storage.getDirectory()
        .then(h => h.getDirectoryHandle(directoryName))
        .then(h => h.getFileHandle(fileName))
        .then(h => h.getFile())
        .then(f => f.text()),
    )
      .resolves
      .toBe(fileContent);

    // 5. 書き込み後のファイルハンドルから getFile() で内容を検証する

    const file = await fileHandle.getFile();

    await expect(file.text())
      .resolves
      .toBe(fileContent);
  });
});
