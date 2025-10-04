import { afterAll, beforeAll, describe, test } from "vitest";
import { MemoryFs } from "../../src/fs/memory-fs.js";

function encode(s: string) {
  return new TextEncoder().encode(s);
}

describe("MemoryFsPath", () => {
  test("単純なパス結合が正しく解決される", ({ expect }) => {
    const memoryFsPath = (new MemoryFs()).path;
    const result = memoryFsPath.resolve("dir", "file.txt");

    expect(result).toBe("memory://dir/file.txt");
  });

  test("ルートディレクトリーと同一パスの場合はルートディレクトリーを返す", ({ expect }) => {
    const memoryFsPath = (new MemoryFs()).path;
    const result = memoryFsPath.resolve("");

    expect(result).toBe("memory://");
  });

  test("ルートディレクトリーより上の改装には行けない", ({ expect }) => {
    const path = (new MemoryFs()).path;
    const result = path.resolve("../../../other");

    expect(result).toBe("memory://other");
  });

  test("解決後のパスが末尾にセパレーターを持つ場合は除去される", ({ expect }) => {
    const memoryFsPath = (new MemoryFs()).path;
    const result = memoryFsPath.resolve("dir/");

    expect(result.endsWith("/")).toBe(false);
    expect(result).toBe("memory://dir");
  });
});

describe("e2e", () => {
  const directoryName = "flow_test_dir";
  const fileName = "flow_test_file.txt";
  const fileContent = "This is a test content for full flow.";
  let memoryFs: MemoryFs;

  beforeAll(async () => {
    memoryFs = new MemoryFs();
    await memoryFs.open();
  });

  afterAll(async () => {
    await memoryFs.close();
  });

  test("ディレクトリ、ファイル、ストリームを介した一連の操作が正常に完了する", async ({ expect }) => {
    // 1. ディレクトリハンドルを取得する

    const dirHandle = await memoryFs.getDirectoryHandle(directoryName, { create: true });

    // 2. ディレクトリハンドルからファイルハンドルを取得する

    const fileHandle = await dirHandle.getFileHandle(fileName, { create: true });

    // 3. ファイルハンドルから書き込みストリームを取得し、ファイルに書き込む

    const writableStream = await fileHandle.createWritable();
    await writableStream.write(encode(fileContent));
    await writableStream.close();

    // 4. 書き込み後のファイル内容を検証する

    await expect(
      memoryFs.getDirectoryHandle(directoryName, { create: false })
        .then(h => h.getFileHandle(fileName, { create: false }))
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
