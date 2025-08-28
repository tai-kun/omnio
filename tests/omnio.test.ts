import * as duckdb from "@duckdb/duckdb-wasm";
import eHworker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import mvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdbWasmEh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import duckdbWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import * as superjson from "superjson";
import * as v from "valibot";
import { describe, test as vitest } from "vitest";
import { WasmDb } from "../src/db/wasm-db.js";
import { ObjectExistsError, ObjectNotFoundError } from "../src/errors.js";
import { Opfs } from "../src/fs/opfs.js";
import { Omnio } from "../src/index.js";
import * as omnioLogger from "../src/logger/index.js";

let counter = 0;
let nowDatetimeString: string | undefined;
const test = vitest.extend<{
  omnio: Omnio;
}>({
  async omnio({ task }, use) {
    if (typeof document !== "undefined") {
      // https://duckdb.org/docs/stable/clients/wasm/instantiation#vite
      const duckdbBundle = await duckdb.selectBundle({
        mvp: {
          mainModule: duckdbWasm,
          mainWorker: mvpWorker,
        },
        eh: {
          mainModule: duckdbWasmEh,
          mainWorker: eHworker,
        },
      });
      const duckdbLogger = __DEBUG__
        ? new duckdb.ConsoleLogger()
        : new duckdb.VoidLogger();
      const omnio = new Omnio({
        db: new WasmDb(duckdbBundle, duckdbLogger),
        fs: new Opfs(`${counter++}`),
        logger: __DEBUG__
          ? new omnioLogger.ConsoleLogger()
          : new omnioLogger.VoidLogger(),
        bucketName: "test",
        textSearch: undefined,
        json: superjson,
        maxDescriptionTextSize: 128,
        maxUserMetadataJsonSize: 128,
      });
      await omnio.open();
      await use(omnio);
      await omnio.close();
    } else {
      const path = await import("node:path");
      const { NodeDb } = await import("../src/db/node-db.js");
      const { NodeFs } = await import("../src/fs/node-fs.js");
      const testDir = path.join(
        "tests",
        ".temp",
        "omnio.server.test.ts",
        nowDatetimeString ??= (new Date()).toISOString(),
        task.name,
      );
      const omnio = new Omnio({
        db: new NodeDb(),
        fs: new NodeFs(testDir),
        logger: __DEBUG__
          ? new omnioLogger.ConsoleLogger()
          : new omnioLogger.VoidLogger(),
        bucketName: "test",
        textSearch: undefined,
        json: superjson,
        maxDescriptionTextSize: 128,
        maxUserMetadataJsonSize: 128,
      });
      await omnio.open();
      await use(omnio);
      await omnio.close();
    }
  },
});

describe("putObject", () => {
  test("オブジェクトを作成できる", async ({ omnio, expect }) => {
    await expect(omnio.putObject("file.txt", ""))
      .resolves
      .not
      .toThrow();
  });

  test("重複するパスで作成すると上書きになる", async ({ omnio, expect }) => {
    await expect(omnio.putObject("path/to/file.txt", "foo"))
      .resolves
      .not
      .toThrow();
    await expect(omnio.putObject("path/to/file.txt", "barbaz"))
      .resolves
      .not
      .toThrow();

    const obj = await omnio.getObject("path/to/file.txt");

    expect(obj).toBeInstanceOf(globalThis.File);
    expect.soft(obj.type).toBe("text/plain");
    expect.soft(obj.bucketName).toBe("test");
    expect.soft(obj.objectPath.toString()).toBe("path/to/file.txt");
    expect.soft(obj.checksum).toMatch(/^[0-9a-f]{32}$/);
    expect.soft(obj.name).toMatch(v.UUID_REGEX);
    expect.soft(obj.size).toBe(6);
    expect.soft(obj.objectTags).toBeUndefined();
    expect.soft(obj.description).toBeUndefined();
    expect.soft(obj.userMetadata).toBeUndefined();
    await expect.soft(obj.text())
      .resolves
      .toBe("barbaz");
  });

  test("wx フラグを使って重複するパスで作成するとエラー", async ({ omnio, expect }) => {
    await expect(omnio.putObject("path/to/file.txt", "foo"))
      .resolves
      .not
      .toThrow();
    await expect(omnio.putObject("path/to/file.txt", "barbaz", { flag: "wx" }))
      .rejects
      .toThrow(ObjectExistsError);
  });

  test("a フラグを使うと追記できる", async ({ omnio, expect }) => {
    await expect(omnio.putObject("path/to/file.txt", "foo"))
      .resolves
      .not
      .toThrow();
    await expect(omnio.putObject("path/to/file.txt", "barbaz", { flag: "a" }))
      .resolves
      .not
      .toThrow();

    const obj = await omnio.getObject("path/to/file.txt");

    expect.soft(obj.size).toBe(9);
    await expect.soft(obj.text())
      .resolves
      .toBe("foobarbaz");
  });

  test("追記モードでチェックサムが正しく計算される", async ({ omnio, expect }) => {
    let checksum1: string;
    let checksum2: string;
    {
      await omnio.putObject("path/to/file1.txt", "foobarbaz");
      const { checksum } = await omnio.getObjectMetadata({
        select: {
          checksum: true,
        },
        where: {
          path: "path/to/file1.txt",
        },
      });
      checksum1 = checksum;
    }
    {
      await omnio.putObject("path/to/file2.txt", "foo");
      await omnio.putObject("path/to/file2.txt", "barbaz", { flag: "a" });
      const { checksum } = await omnio.getObjectMetadata({
        select: {
          checksum: true,
        },
        where: {
          path: "path/to/file2.txt",
        },
      });
      checksum2 = checksum;
    }

    expect(checksum1).toBe(checksum2);
  });
});

describe("createWriteStream", () => {
  test("ストリームでオブジェクトを作成できる", async ({ omnio, expect }) => {
    const w = await omnio.createWriteStream("path/to/file.txt");

    expect(w.bytesWritten).toBe(0);
    expect(w.size).toBe(0);
    expect(w.closed).toBe(false);

    await w.write("foo");

    expect(w.bytesWritten).toBe(3);
    expect(w.size).toBe(3);

    await w.write("barbaz");

    expect(w.bytesWritten).toBe(9);
    expect(w.size).toBe(9);

    await w.close();

    expect(w.closed).toBe(true);

    const obj = await omnio.getObject("path/to/file.txt");

    expect.soft(obj.size).toBe(9);
    await expect.soft(obj.text())
      .resolves
      .toBe("foobarbaz");
  });

  test("ストリームを中断できる", async ({ omnio, expect }) => {
    const w = await omnio.createWriteStream("path/to/file.txt");
    await w.write("foo");
    await w.abort();

    await expect(omnio.getObject("path/to/file.txt"))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("重複するパスでストリームで作成すると上書きになる", async ({ omnio, expect }) => {
    {
      await using w = await omnio.createWriteStream("path/to/file.txt");
      await w.write("foo");
    }
    {
      await using w = await omnio.createWriteStream("path/to/file.txt");
      await w.write("barbaz");
    }

    const obj = await omnio.getObject("path/to/file.txt");

    await expect.soft(obj.text())
      .resolves
      .toBe("barbaz");
  });

  test("ストリームで wx フラグを使って重複するパスで作成すると閉じるときにエラー", async ({ omnio, expect }) => {
    await expect(omnio.putObject("path/to/file.txt", "foo"))
      .resolves
      .not
      .toThrow();

    await using w = await omnio.createWriteStream("path/to/file.txt", { flag: "wx" });
    await w.write("foo");
    await expect(w.close())
      .rejects
      .toThrow(ObjectExistsError);
  });

  test("ストリームで a フラグを使うと追記できる", async ({ omnio, expect }) => {
    await expect(omnio.putObject("path/to/file.txt", "foo"))
      .resolves
      .not
      .toThrow();
    {
      await using w = await omnio.createWriteStream("path/to/file.txt", { flag: "a" });
      await w.write("bar");
      await w.write("baz");
    }

    const obj = await omnio.getObject("path/to/file.txt");

    await expect.soft(obj.text())
      .resolves
      .toBe("foobarbaz");
  });
});

describe("getObjectMetadata", () => {
  test("オブジェクトのメタデータを取得できる", async ({ omnio, expect }) => {
    await expect(omnio.putObject("path/to/file.txt", "foo"))
      .resolves
      .not
      .toThrow();
    await expect(omnio.getObjectMetadata({
      select: {
        mimeType: true,
      },
      where: {
        path: "path/to/file.txt",
      },
    }))
      .resolves
      .toStrictEqual({
        mimeType: "text/plain",
      });
  });
});

describe("existsPath", () => {
  test("パスが存在するか確認できる", async ({ omnio, expect }) => {
    await expect(omnio.putObject("path/to/file.txt", "foo"))
      .resolves
      .not
      .toThrow();
    await expect(omnio.existsPath("path/to/file.txt"))
      .resolves
      .toBe(true);
    await expect(omnio.existsPath(["path", "to"]))
      .resolves
      .toBe(true);
    await expect(omnio.existsPath(["path", "to", "file.txt"]))
      .resolves
      .toBe(false);
  });
});

// metadata.test.ts でテスト済み
// describe("statPath", () => {});

// metadata.test.ts でテスト済み
// describe("list", () => {});

// metadata.test.ts でテスト済み
// describe("searchObjects", () => {});

describe.todo("copyObject", () => {
});

// metadata.test.ts でテスト済み
// describe("renameObject", () => {});

// metadata.test.ts でテスト済み
// describe("updateObjectMetadata", () => {});

describe.todo("deleteObject", () => {
});

// metadata.test.ts でテスト済み
// describe("queryObjectMetadata", () => {});
