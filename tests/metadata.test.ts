import * as duckdb from "@duckdb/duckdb-wasm";
import eHworker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import mvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdbWasmEh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import duckdbWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import * as superjson from "superjson";
import * as v from "valibot";
import { describe, expectTypeOf, test as vitest } from "vitest";
import { WasmDb } from "../src/db/wasm-db.js";
import {
  ChecksumMismatchError,
  EntityNotFoundError,
  ObjectExistsError,
  ObjectNotFoundError,
} from "../src/errors.js";
import { Opfs } from "../src/fs/opfs.js";
import getEntityId from "../src/get-entity-id.js";
import { BucketName, ObjectPath } from "../src/index.js";
import * as omnioLogger from "../src/logger/index.js";
import toConsoleLikeLogger from "../src/logger/to-console-like-logger.js";
import Metadata from "../src/metadata.js";
import * as schemas from "../src/schemas.js";

let counter = 0;
let nowDatetimeString: string | undefined;
const test = vitest.extend<{
  metadata: Metadata;
}>({
  async metadata({ task }, use) {
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
      const metadata = new Metadata({
        db: new WasmDb(duckdbBundle, duckdbLogger),
        fs: new Opfs(`${counter++}`),
        logger: toConsoleLikeLogger(
          __DEBUG__
            ? new omnioLogger.ConsoleLogger()
            : new omnioLogger.VoidLogger(),
        ),
        bucketName: asBucketName("test"),
        textSearch: undefined,
        json: superjson,
        maxDescriptionTextSize: asUint(128),
        maxUserMetadataJsonSize: asUint(128),
      });
      await metadata.connect();
      await use(metadata);
      await metadata.disconnect();
    } else {
      const path = await import("node:path");
      const { NodeDb } = await import("../src/db/node-db.js");
      const { NodeFs } = await import("../src/fs/node-fs.js");
      const testDir = path.join(
        "tests",
        ".temp",
        "metadata.server.test.ts",
        nowDatetimeString ??= (new Date()).toISOString(),
        task.name,
      );
      const metadata = new Metadata({
        db: new NodeDb(),
        fs: new NodeFs(testDir),
        logger: toConsoleLikeLogger(
          __DEBUG__
            ? new omnioLogger.ConsoleLogger()
            : new omnioLogger.VoidLogger(),
        ),
        bucketName: asBucketName("test"),
        textSearch: undefined,
        json: superjson,
        maxDescriptionTextSize: asUint(128),
        maxUserMetadataJsonSize: asUint(128),
      });
      await metadata.connect();
      await use(metadata);
      await metadata.disconnect();
    }
  },
});

describe("Metadata", () => {
  test("インスタンスを構築できる", async ({ metadata, expect }) => {
    expect(metadata).toBeInstanceOf(Metadata);
  });
});

describe("create", () => {
  test("メタデータを作成できる", async ({ metadata, expect }) => {
    await expect(metadata.create({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
  });

  test("重複するパスで作成すると上書きになる", async ({ metadata, expect }) => {
    await expect(metadata.create({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("path/to/file.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.create({
      checksum: {
        value: asChecksum("11111111111111111111111111111111"),
        state: asHashState([]),
      },
      entityId: asEntityId("0198a275-bad6-7e39-8541-ed79afda8c84"),
      mimeType: asMimeType("application/octet-stream"),
      objectPath: ObjectPath.parse("path/to/file.txt"),
      objectSize: asUint(128),
      objectTags: asObjectTags(["foo"]),
      description: "new description",
      userMetadata: {
        foo: "bar",
      },
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.read({
      select: {
        checksum: true,
        entityId: true,
        mimeType: true,
        size: true,
        objectTags: true,
        description: true,
        userMetadata: true,
      },
      where: {
        objectPath: ObjectPath.parse("path/to/file.txt"),
      },
    }))
      .resolves
      .toStrictEqual({
        checksum: "11111111111111111111111111111111",
        entityId: "0198a275-bad6-7e39-8541-ed79afda8c84",
        mimeType: "application/octet-stream",
        size: 128,
        objectTags: ["foo"],
        description: "new description",
        userMetadata: {
          foo: "bar",
        },
      });
  });

  test("重複したエンティティで作成しようとしてエラー", async ({ metadata, expect }) => {
    await expect(metadata.create({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("file-1.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.create({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("file-2.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .rejects
      .toThrow(
        "Duplicate key \"entityid: 01989d2b-9d77-7988-ac2d-23659f27b88f\" violates unique constraint.",
      );
  });
});

describe("createExclusive", () => {
  test("メタデータを排他的に作成できる", async ({ metadata, expect }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
  });

  test("重複するパスで排他的に作成しようとしてエラー", async ({ metadata, expect }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("path/to/file.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("0198a275-bad6-7e39-8541-ed79afda8c84"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("path/to/file.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .rejects
      .toThrow(ObjectExistsError);
  });

  test("サイズ上限を超える説明文で排他的に作成しようとしてエラー", async ({ metadata, expect }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("path/to/file.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: "0".repeat(128 + 1),
      userMetadata: null,
    }))
      .rejects
      .toThrow(v.ValiError);
  });

  test("サイズ上限を超えるユーザー定義メタデータで排他的に作成しようとしてエラー", async ({ metadata, expect }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("path/to/file.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: ["0".repeat(128 - 2 + 1)],
    }))
      .rejects
      .toThrow(v.ValiError);
  });

  test("重複したエンティティで排他的に作成しようとしてエラー", async ({ metadata, expect }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("file-1.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("file-2.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .rejects
      .toThrow(
        "Duplicate key \"entityid: 01989d2b-9d77-7988-ac2d-23659f27b88f\" violates unique constraint.",
      );
  });
});

describe("read", () => {
  test("すべてのメタデータを取得できる", async ({ metadata, expect }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags(["foo", "bar"]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();

    const out = await metadata.read({
      select: undefined,
      where: {
        objectPath: ObjectPath.parse("file.txt"),
      },
    });

    expectTypeOf(out).toEqualTypeOf<{
      id: string & v.Brand<"ObjectId">;
      bucket: BucketName;
      path: ObjectPath;
      recordType: ("CREATE" | "UPDATE_METADATA") & v.Brand<"RecordType">;
      recordTimestamp: number & v.Brand<"Timestamp">;
      size: number & v.Brand<"UnsignedInteger">;
      mimeType: string & v.Brand<"MimeType">;
      createdAt: number & v.Brand<"Timestamp">;
      lastModifiedAt: number & v.Brand<"Timestamp">;
      checksum: string & v.Brand<"Checksum">;
      checksumAlgorithm: "MD5";
      objectTags: (schemas.SizeLimitedString[]) & v.Brand<"ObjectTags">;
      description: string | null;
      userMetadata: unknown;
      entityId: string & v.Brand<"EntityId">;
    }>();
    expect(out).toStrictEqual({
      id: expect.stringMatching(v.UUID_REGEX),
      bucket: "test",
      path: expect.any(ObjectPath),
      recordType: "CREATE",
      recordTimestamp: expect.any(Number),
      size: 0,
      mimeType: "text/plain",
      createdAt: expect.any(Number),
      lastModifiedAt: expect.any(Number),
      checksum: expect.stringMatching(/^[0-9a-f]{32}$/),
      checksumAlgorithm: "MD5",
      objectTags: ["foo", "bar"],
      description: null,
      userMetadata: null,
      entityId: "01989d2b-9d77-7988-ac2d-23659f27b88f",
    });
  });

  test("特定のメタデータを取得できる", async ({ metadata, expect }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();

    const out = await metadata.read({
      select: {
        createdAt: true,
        recordType: true,
      },
      where: {
        objectPath: ObjectPath.parse("file.txt"),
      },
    });

    expectTypeOf(out).toEqualTypeOf<{
      createdAt: number & v.Brand<"Timestamp">;
      recordType: ("CREATE" | "UPDATE_METADATA") & v.Brand<"RecordType">;
    }>();
    expect(out).toStrictEqual({
      createdAt: expect.any(Number),
      recordType: "CREATE",
    });
  });

  test("存在しないメタデータを取得しようとしてエラー", async ({ metadata, expect }) => {
    await expect(metadata.read({
      select: undefined,
      where: {
        objectPath: ObjectPath.parse("file.txt"),
      },
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("ユーザー定義のメタデータを取得できる", async ({ metadata, expect }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: new Map([["foo", "bar"]]),
    }))
      .resolves
      .not
      .toThrow();

    const out = await metadata.read({
      select: {
        userMetadata: true,
      },
      where: {
        objectPath: ObjectPath.parse("file.txt"),
      },
    });

    expectTypeOf(out).toEqualTypeOf<{
      userMetadata: unknown;
    }>();
    expect(out).toStrictEqual({
      userMetadata: new Map([["foo", "bar"]]),
    });
  });
});

describe("readInternal", () => {
  test("内部利用のためのメタデータを取得できる", async ({ metadata, expect }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([1, 2, 3]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags(["foo", "bar"]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();

    await expect(metadata.readInternal({ objectPath: ObjectPath.parse("file.txt") }))
      .resolves
      .toStrictEqual({
        entityId: "01989d2b-9d77-7988-ac2d-23659f27b88f",
        checksum: {
          value: "00000000000000000000000000000000",
          state: [1, 2, 3],
        },
      });
  });

  test("存在しない内部利用のためのメタデータを取得しようとしてエラー", async ({ metadata, expect }) => {
    await expect(metadata.readInternal({ objectPath: ObjectPath.parse("file.txt") }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });
});

describe("exists", () => {
  test("メタデータが存在する場合は true", async ({ metadata, expect }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asUint(0),
      objectTags: asObjectTags(["foo", "bar"]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.exists({ objectPath: ObjectPath.parse("file.txt") }))
      .resolves
      .toStrictEqual({
        exists: true,
      });
  });

  test("メタデータが存在しない場合は false", async ({ metadata, expect }) => {
    await expect(metadata.exists({ objectPath: ObjectPath.parse("file.txt") }))
      .resolves
      .toStrictEqual({
        exists: false,
      });
  });

  test("ディレクトリが存在する場合は true", async ({ metadata, expect }) => {
    const objectPath = ObjectPath.parse("path/to/file.txt");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags(["foo", "bar"]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect.soft(metadata.exists({ dirPath: [] }))
      .resolves
      .toStrictEqual({
        exists: true,
      });
    await expect.soft(metadata.exists({ dirPath: ["path"] }))
      .resolves
      .toStrictEqual({
        exists: true,
      });
    await expect.soft(metadata.exists({ dirPath: ["path", "to"] }))
      .resolves
      .toStrictEqual({
        exists: true,
      });
  });

  test("ディレクトリが存在しない場合は false", async ({ metadata, expect }) => {
    await expect.soft(metadata.exists({ dirPath: [] }))
      .resolves
      .toStrictEqual({
        exists: true,
      });
    await expect.soft(metadata.exists({ dirPath: ["path"] }))
      .resolves
      .toStrictEqual({
        exists: false,
      });
    await expect.soft(metadata.exists({ dirPath: ["path", "to"] }))
      .resolves
      .toStrictEqual({
        exists: false,
      });
  });
});

describe("stat", () => {
  test("オブジェクトパスのステータス情報を取得できる", async ({ metadata, expect }) => {
    const PATHS = [
      "file1.txt",
      "file1.txt/file2.txt",
      "a/file1.txt",
      "a/b/file1.txt",
    ];
    for (const path of PATHS) {
      await expect(metadata.createExclusive({
        checksum: {
          value: asChecksum("00000000000000000000000000000000"),
          state: asHashState([]),
        },
        entityId: getEntityId(),
        mimeType: asMimeType("text/plain"),
        objectPath: ObjectPath.parse(path),
        objectSize: asUint(0),
        objectTags: asObjectTags([]),
        description: null,
        userMetadata: null,
      }))
        .resolves
        .not
        .toThrow();
    }

    await expect.soft(metadata.stat({ objectPath: ObjectPath.parse("file1.txt") }))
      .resolves
      .toStrictEqual({
        isObject: true,
        isDirectory: true,
      });
    await expect.soft(metadata.stat({ objectPath: ObjectPath.parse("a") }))
      .resolves
      .toStrictEqual({
        isObject: false,
        isDirectory: true,
      });
    await expect.soft(metadata.stat({ objectPath: ObjectPath.parse("a/file1.txt") }))
      .resolves
      .toStrictEqual({
        isObject: true,
        isDirectory: false,
      });
    await expect.soft(metadata.stat({ objectPath: ObjectPath.parse("a/b") }))
      .resolves
      .toStrictEqual({
        isObject: false,
        isDirectory: true,
      });
    await expect.soft(metadata.stat({ objectPath: ObjectPath.parse("a/b/file1.txt") }))
      .resolves
      .toStrictEqual({
        isObject: true,
        isDirectory: false,
      });
  });

  test("存在しないオブジェクトパスのステータス情報を取得できる", async ({ metadata, expect }) => {
    await expect.soft(metadata.stat({ objectPath: ObjectPath.parse("none.txt") }))
      .resolves
      .toStrictEqual({
        isObject: false,
        isDirectory: false,
      });
  });
});

describe("search", () => {
  test("指定したディレクトリ直下の説明文を対象に全文検索できる", async ({ metadata, expect }) => {
    const DOCS = {
      "path/file1.txt": "foo shallow",
      "path/to/file1.txt": "foo foo foo bar baz",
      "path/to/file2.txt": "foo foo bar bar",
      "path/to/file3.txt": "foo",
      "path/to/file4.txt": "qux",
      "path/to/file5.txt": undefined,
      "path/to/dir/file1.txt": "foo deep",
    };
    for (const [path, description] of Object.entries(DOCS)) {
      await expect(metadata.createExclusive({
        checksum: {
          value: asChecksum("00000000000000000000000000000000"),
          state: asHashState([]),
        },
        entityId: getEntityId(),
        mimeType: asMimeType("text/plain"),
        objectPath: ObjectPath.parse(path),
        objectSize: asUint(0),
        objectTags: asObjectTags([]),
        description,
        userMetadata: null,
      }))
        .resolves
        .not
        .toThrow();
    }

    await expect(Array.fromAsync(
      await metadata.search({
        dirPath: ["path", "to"],
        query: "foo",
        take: undefined,
        skip: undefined,
        recursive: undefined,
        scoreThreshold: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          objectPath: expect.any(ObjectPath),
          description: "foo", // 完全一致
          searchScore: expect.any(Number),
        },
        {
          objectPath: expect.any(ObjectPath),
          description: "foo foo foo bar baz", // 一致回数が多い
          searchScore: expect.any(Number),
        },
        {
          objectPath: expect.any(ObjectPath),
          description: "foo foo bar bar", // 一致回数が少ない
          searchScore: expect.any(Number),
        },
      ]);
    await expect(Array.fromAsync(
      await metadata.search({
        dirPath: ["path", "to"],
        query: "foo",
        take: asUint(1),
        skip: undefined,
        recursive: undefined,
        scoreThreshold: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          objectPath: expect.any(ObjectPath),
          description: "foo", // 完全一致
          searchScore: expect.any(Number),
        },
      ]);
    await expect(Array.fromAsync(
      await metadata.search({
        dirPath: ["path", "to"],
        query: "bar",
        take: undefined,
        skip: undefined,
        recursive: undefined,
        scoreThreshold: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          objectPath: expect.any(ObjectPath),
          description: "foo foo bar bar", // 一致回数が多い
          searchScore: expect.any(Number),
        },
        {
          objectPath: expect.any(ObjectPath),
          description: "foo foo foo bar baz", // 一致回数が少ない
          searchScore: expect.any(Number),
        },
      ]);
    await expect(Array.fromAsync(
      await metadata.search({
        dirPath: ["path"],
        query: "foo",
        take: undefined,
        skip: undefined,
        recursive: undefined,
        scoreThreshold: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          objectPath: expect.any(ObjectPath),
          description: "foo shallow",
          searchScore: expect.any(Number),
        },
      ]);
    await expect(Array.fromAsync(
      await metadata.search({
        dirPath: ["path", "to", "dir"],
        query: "foo",
        take: undefined,
        skip: undefined,
        recursive: undefined,
        scoreThreshold: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          objectPath: expect.any(ObjectPath),
          description: "foo deep",
          searchScore: expect.any(Number),
        },
      ]);
  });

  test("日本語で全文検索できる", async ({ metadata, expect }) => {
    const DOCS = {
      "path/to/file1.txt": "これは日本語で書かれた説明文です",
      "path/to/file2.txt": "これは日本語で書かれた文字列です",
    };
    for (const [path, description] of Object.entries(DOCS)) {
      await expect(metadata.createExclusive({
        checksum: {
          value: asChecksum("00000000000000000000000000000000"),
          state: asHashState([]),
        },
        entityId: getEntityId(),
        mimeType: asMimeType("text/plain"),
        objectPath: ObjectPath.parse(path),
        objectSize: asUint(0),
        objectTags: asObjectTags([]),
        description,
        userMetadata: null,
      }))
        .resolves
        .not
        .toThrow();
    }

    await expect.soft(Array.fromAsync(
      await metadata.search({
        dirPath: ["path", "to"],
        query: "説明",
        take: undefined,
        skip: undefined,
        recursive: undefined,
        scoreThreshold: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          objectPath: expect.any(ObjectPath),
          description: "これは日本語で書かれた説明文です",
          searchScore: expect.any(Number),
        },
      ]);
    await expect.soft(Array.fromAsync(
      await metadata.search({
        dirPath: ["path", "to"],
        query: "字 説明",
        take: undefined,
        skip: undefined,
        recursive: undefined,
        scoreThreshold: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          objectPath: expect.any(ObjectPath),
          description: "これは日本語で書かれた説明文です",
          searchScore: expect.any(Number),
        },
        {
          objectPath: expect.any(ObjectPath),
          description: "これは日本語で書かれた文字列です",
          searchScore: expect.any(Number),
        },
      ]);
    await expect.soft(Array.fromAsync(
      await metadata.search({
        dirPath: ["path", "to"],
        query: "文字",
        take: undefined,
        skip: undefined,
        recursive: undefined,
        scoreThreshold: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          objectPath: expect.any(ObjectPath),
          description: "これは日本語で書かれた文字列です",
          searchScore: expect.any(Number),
        },
        {
          objectPath: expect.any(ObjectPath),
          description: "これは日本語で書かれた説明文です",
          searchScore: expect.any(Number),
        },
      ]);
  });
});

describe("list", () => {
  test("オブジェクトをリストアップできる", async ({ metadata, expect }) => {
    const PATHS = [
      "file1.txt",
      "file1.txt/file2.txt",
      "a",
      "a/file1.txt",
      "a/b/file1.txt",
      "a/b/file2.txt",
      "a/d/file1.txt",
      "b/c/d/file1.txt",
    ];
    for (const path of PATHS) {
      await expect(metadata.createExclusive({
        checksum: {
          value: asChecksum("00000000000000000000000000000000"),
          state: asHashState([]),
        },
        entityId: getEntityId(),
        mimeType: asMimeType("text/plain"),
        objectPath: ObjectPath.parse(path),
        objectSize: asUint(0),
        objectTags: asObjectTags([]),
        description: undefined,
        userMetadata: null,
      }))
        .resolves
        .not
        .toThrow();
    }

    await expect.soft(Array.fromAsync(
      await metadata.list({
        select: {
          size: true,
          mimeType: true,
          lastModifiedAt: true,
        },
        where: {
          dirPath: [],
          isObject: true,
        },
        take: undefined,
        skip: undefined,
        orderBy: {
          name: undefined,
          preferObject: undefined,
        },
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          isObject: true,
          name: "a",
          size: 0,
          mimeType: "text/plain",
          lastModifiedAt: expect.any(Number),
        },
        {
          isObject: true,
          name: "file1.txt",
          size: 0,
          mimeType: "text/plain",
          lastModifiedAt: expect.any(Number),
        },
      ]);
  });

  test("ディレクトリーをリストアップできる", async ({ metadata, expect }) => {
    const PATHS = [
      "file1.txt",
      "file1.txt/file2.txt",
      "a",
      "a/file1.txt",
      "a/b/file1.txt",
      "a/b/file2.txt",
      "a/d/file1.txt",
      "b/c/d/file1.txt",
    ];
    for (const path of PATHS) {
      await expect(metadata.createExclusive({
        checksum: {
          value: asChecksum("00000000000000000000000000000000"),
          state: asHashState([]),
        },
        entityId: getEntityId(),
        mimeType: asMimeType("text/plain"),
        objectPath: ObjectPath.parse(path),
        objectSize: asUint(0),
        objectTags: asObjectTags([]),
        description: undefined,
        userMetadata: null,
      }))
        .resolves
        .not
        .toThrow();
    }

    await expect.soft(Array.fromAsync(
      await metadata.list({
        select: undefined,
        where: {
          dirPath: [],
          isObject: false,
        },
        take: undefined,
        skip: undefined,
        orderBy: {
          name: undefined,
          preferObject: undefined,
        },
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          isObject: false,
          name: "a",
        },
        {
          isObject: false,
          name: "b",
        },
        {
          isObject: false,
          name: "file1.txt",
        },
      ]);
  });

  test("ディレクトリとオブジェクトをリストアップできる", async ({ metadata, expect }) => {
    const PATHS = [
      "file1.txt",
      "file1.txt/file2.txt",
      "a",
      "a/file1.txt",
      "a/b/file1.txt",
      "a/b/file2.txt",
      "a/d/file1.txt",
      "b/c/d/file1.txt",
    ];
    for (const path of PATHS) {
      await expect(metadata.createExclusive({
        checksum: {
          value: asChecksum("00000000000000000000000000000000"),
          state: asHashState([]),
        },
        entityId: getEntityId(),
        mimeType: asMimeType("text/plain"),
        objectPath: ObjectPath.parse(path),
        objectSize: asUint(0),
        objectTags: asObjectTags([]),
        description: undefined,
        userMetadata: null,
      }))
        .resolves
        .not
        .toThrow();
    }

    await expect.soft(Array.fromAsync(
      await metadata.list({
        select: {
          size: true,
          mimeType: true,
          lastModifiedAt: true,
        },
        where: {
          dirPath: [],
          isObject: undefined,
        },
        take: undefined,
        skip: undefined,
        orderBy: {
          name: undefined,
          preferObject: undefined,
        },
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          isObject: false,
          name: "a",
        },
        {
          isObject: false,
          name: "b",
        },
        {
          isObject: false,
          name: "file1.txt",
        },
        {
          isObject: true,
          name: "a",
          size: 0,
          mimeType: "text/plain",
          lastModifiedAt: expect.any(Number),
        },
        {
          isObject: true,
          name: "file1.txt",
          size: 0,
          mimeType: "text/plain",
          lastModifiedAt: expect.any(Number),
        },
      ]);
    await expect.soft(Array.fromAsync(
      await metadata.list({
        select: {
          size: true,
          mimeType: true,
          lastModifiedAt: true,
        },
        where: {
          dirPath: ["a"],
          isObject: undefined,
        },
        take: undefined,
        skip: undefined,
        orderBy: {
          name: undefined,
          preferObject: undefined,
        },
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          isObject: false,
          name: "b",
        },
        {
          isObject: false,
          name: "d",
        },
        {
          isObject: true,
          name: "file1.txt",
          size: 0,
          mimeType: "text/plain",
          lastModifiedAt: expect.any(Number),
        },
      ]);
    await expect.soft(Array.fromAsync(
      await metadata.list({
        select: {
          size: true,
          mimeType: true,
          lastModifiedAt: true,
        },
        where: {
          dirPath: ["a", "b"],
          isObject: undefined,
        },
        take: undefined,
        skip: undefined,
        orderBy: {
          name: undefined,
          preferObject: undefined,
        },
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          isObject: true,
          name: "file1.txt",
          size: 0,
          mimeType: "text/plain",
          lastModifiedAt: expect.any(Number),
        },
        {
          isObject: true,
          name: "file2.txt",
          size: 0,
          mimeType: "text/plain",
          lastModifiedAt: expect.any(Number),
        },
      ]);
    await expect.soft(Array.fromAsync(
      await metadata.list({
        select: {
          size: true,
          mimeType: true,
          lastModifiedAt: true,
        },
        where: {
          dirPath: ["b", "c"],
          isObject: undefined,
        },
        take: undefined,
        skip: undefined,
        orderBy: {
          name: undefined,
          preferObject: undefined,
        },
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          isObject: false,
          name: "d",
        },
      ]);
  });
});

describe("move", () => {
  test("メタデータを移動できる", async ({ metadata, expect }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/moved.txt");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: srcObjectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.move({
      srcObjectPath,
      dstObjectPath,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.read({
      select: undefined,
      where: {
        objectPath: srcObjectPath,
      },
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
    await expect(metadata.read({
      select: {
        entityId: true,
      },
      where: {
        objectPath: dstObjectPath,
      },
    }))
      .resolves
      .toStrictEqual({
        entityId: "01989d2b-9d77-7988-ac2d-23659f27b88f",
      });
  });

  test("存在しないメタデータを移動しようとしてエラー", async ({ metadata, expect }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/moved.txt");

    await expect(metadata.move({
      srcObjectPath,
      dstObjectPath,
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("すでに存在するメタデータへ移動しようとしてエラー", async ({ metadata, expect }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/moved.txt");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: srcObjectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("0198a275-bad6-7e39-8541-ed79afda8c84"),
      mimeType: asMimeType("text/plain"),
      objectPath: dstObjectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.move({
      srcObjectPath,
      dstObjectPath,
    }))
      .rejects
      .toThrow(ObjectExistsError);
  });
});

describe("copy", () => {
  test("メタデータをコピーできる", async ({ metadata, expect }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/copied.txt");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: srcObjectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.copy({
      srcObjectPath,
      dstObjectPath,
      dstEntityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
    }))
      .rejects
      .toThrowError(
        "Duplicate key \"entityid: 01989d2b-9d77-7988-ac2d-23659f27b88f\" violates unique constraint.",
      );
    await expect(metadata.copy({
      srcObjectPath,
      dstObjectPath,
      dstEntityId: asEntityId("0198a275-bad6-7e39-8541-ed79afda8c84"),
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.read({
      select: {},
      where: {
        objectPath: srcObjectPath,
      },
    }))
      .resolves
      .toStrictEqual({});
    await expect(metadata.read({
      select: {},
      where: {
        objectPath: srcObjectPath,
      },
    }))
      .resolves
      .toStrictEqual({});
  });

  test("存在しないメタデータをコピーしようとしてエラー", async ({ metadata, expect }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/copied.txt");

    await expect(metadata.copy({
      srcObjectPath,
      dstObjectPath,
      dstEntityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("すでに存在するメタデータへコピーしようとしてエラー", async ({ metadata, expect }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/copied.txt");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath: srcObjectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("0198a275-bad6-7e39-8541-ed79afda8c84"),
      mimeType: asMimeType("text/plain"),
      objectPath: dstObjectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.copy({
      srcObjectPath,
      dstObjectPath,
      dstEntityId: asEntityId("2583336f-678e-4ca8-81a3-8c71f922c047"),
    }))
      .rejects
      .toThrow(ObjectExistsError);
  });
});

describe("update", () => {
  test("メタデータを更新できる", async ({ metadata, expect }) => {
    const objectPath = ObjectPath.parse("path/to/file.txt");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([1, 2, 3]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: "old description",
      userMetadata: {
        old: "metadata",
      },
    }))
      .resolves
      .not
      .toThrow();

    // mimeType を更新
    await expect(metadata.update({
      objectPath,
      mimeType: asMimeType("application/json"),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.read({
      select: {
        mimeType: true,
      },
      where: {
        objectPath,
      },
    }))
      .resolves
      .toStrictEqual({
        mimeType: "application/json",
      });

    // description を更新
    await expect(metadata.update({
      objectPath,
      mimeType: undefined,
      objectTags: undefined,
      description: "new description",
      userMetadata: undefined,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.read({
      select: {
        description: true,
      },
      where: {
        objectPath,
      },
    }))
      .resolves
      .toStrictEqual({
        description: "new description",
      });

    // metadata を更新
    await expect(metadata.update({
      objectPath,
      mimeType: undefined,
      objectTags: undefined,
      description: undefined,
      userMetadata: {
        old: null,
        new: "metadata",
      },
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.read({
      select: {
        userMetadata: true,
      },
      where: {
        objectPath,
      },
    }))
      .resolves
      .toStrictEqual({
        userMetadata: {
          old: null,
          new: "metadata",
        },
      });

    // description を null に更新
    await expect(metadata.update({
      objectPath,
      mimeType: undefined,
      objectTags: undefined,
      description: null,
      userMetadata: undefined,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.read({
      select: {
        description: true,
      },
      where: {
        objectPath,
      },
    }))
      .resolves
      .toStrictEqual({
        description: null,
      });

    // metadata を null に更新
    await expect(metadata.update({
      objectPath,
      mimeType: undefined,
      objectTags: undefined,
      description: undefined,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.read({
      select: {
        userMetadata: true,
      },
      where: {
        objectPath,
      },
    }))
      .resolves
      .toStrictEqual({
        userMetadata: null,
      });
  });

  test("存在しないメタデータを排他的に更新しようとしてエラー", async ({ metadata, expect }) => {
    const objectPath = ObjectPath.parse("path/to/file.txt");

    await expect(metadata.update({
      objectPath,
      mimeType: undefined,
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
    await expect(metadata.update({
      objectPath,
      mimeType: undefined,
      objectTags: undefined,
      description: undefined,
      userMetadata: null,
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("説明文を更新したあと検索に反映される", async ({ metadata, expect }) => {
    const objectPath = ObjectPath.parse("path/to/file.txt");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([1, 2, 3]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: "old",
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(Array.fromAsync(
      await metadata.search({
        dirPath: ["path", "to"],
        query: "old",
        take: undefined,
        skip: undefined,
        recursive: undefined,
        scoreThreshold: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          objectPath: expect.any(ObjectPath),
          description: "old",
          searchScore: expect.any(Number),
        },
      ]);
    await expect(metadata.update({
      objectPath,
      mimeType: undefined,
      objectTags: undefined,
      description: "new",
      userMetadata: undefined,
    }))
      .resolves
      .not
      .toThrow();
    await expect.soft(Array.fromAsync(
      await metadata.search({
        dirPath: ["path", "to"],
        query: "old",
        take: undefined,
        skip: undefined,
        recursive: undefined,
        scoreThreshold: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([]);
    await expect.soft(Array.fromAsync(
      await metadata.search({
        dirPath: ["path", "to"],
        query: "new",
        take: undefined,
        skip: undefined,
        recursive: undefined,
        scoreThreshold: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          objectPath: expect.any(ObjectPath),
          description: "new",
          searchScore: expect.any(Number),
        },
      ]);
  });
});

describe("updateExclusive", () => {
  test("メタデータを排他的に更新できる", async ({ metadata, expect }) => {
    const objectPath = ObjectPath.parse("path/to/file.txt");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([1, 2, 3]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: "old description",
      userMetadata: {
        old: "metadata",
      },
    }))
      .resolves
      .not
      .toThrow();

    // mimeType を更新
    await expect(metadata.updateExclusive({
      objectPath,
      expect: {
        checksum: asChecksum("00000000000000000000000000000000"),
      },
      checksum: {
        value: asChecksum("22222222222222222222222222222222"),
        state: asHashState([4, 5, 6]),
      },
      entityId: undefined,
      mimeType: asMimeType("application/json"),
      objectSize: asUint(128),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.read({
      select: {
        mimeType: true,
      },
      where: {
        objectPath,
      },
    }))
      .resolves
      .toStrictEqual({
        mimeType: "application/json",
      });
  });

  test("存在しないメタデータを排他的に更新しようとしてエラー", async ({ metadata, expect }) => {
    const objectPath = ObjectPath.parse("path/to/file.txt");

    await expect(metadata.updateExclusive({
      objectPath,
      expect: {
        checksum: asChecksum("00000000000000000000000000000000"),
      },
      checksum: {
        value: asChecksum("22222222222222222222222222222222"),
        state: asHashState([4, 5, 6]),
      },
      entityId: undefined,
      mimeType: asMimeType("application/json"),
      objectSize: asUint(128),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("期待するチェックサムと異なるメタデータを排他的に更新しようとしてエラー", async ({ metadata, expect }) => {
    const objectPath = ObjectPath.parse("path/to/file.txt");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([1, 2, 3]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: "old description",
      userMetadata: {
        old: "metadata",
      },
    }))
      .resolves
      .not
      .toThrow();

    await expect(metadata.updateExclusive({
      objectPath,
      expect: {
        checksum: asChecksum("11111111111111111111111111111111"),
      },
      checksum: {
        value: asChecksum("22222222222222222222222222222222"),
        state: asHashState([4, 5, 6]),
      },
      entityId: undefined,
      mimeType: undefined,
      objectSize: asUint(128),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
    }))
      .rejects
      .toThrow(ChecksumMismatchError);
  });

  // test("説明文を排他的に更新したあと検索に反映される", async ({ metadata, expect }) => {
  // });
});

describe("trash, listInTrash", () => {
  test("削除フラグを立てられる", async ({ metadata, expect }) => {
    const objectPath = ObjectPath.parse("file.txt");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.trash({ objectPath }))
      .resolves
      .toStrictEqual({
        entityId: "01989d2b-9d77-7988-ac2d-23659f27b88f",
      });
    await expect(metadata.read({
      select: {},
      where: {
        objectPath,
      },
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
    await expect(Array.fromAsync(
      await metadata.listInTrash({
        select: {
          entityId: true,
        },
        where: {
          objectPath,
        },
        skip: undefined,
        take: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          entityId: "01989d2b-9d77-7988-ac2d-23659f27b88f",
        },
      ]);
  });

  test("存在しないメタデータに削除フラグを立てようとしてエラー", async ({ metadata, expect }) => {
    const objectPath = ObjectPath.parse("file.txt");

    await expect(metadata.trash({ objectPath }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("すでに削除フラグが立てられたメタデータに削除フラグを立てようとしてエラー", async ({ metadata, expect }) => {
    const objectPath = ObjectPath.parse("file.txt");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.trash({ objectPath }))
      .resolves
      .toStrictEqual({
        entityId: "01989d2b-9d77-7988-ac2d-23659f27b88f",
      });
    await expect(metadata.trash({ objectPath }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("削除フラグを立てたあと検索に反映される", async ({ metadata, expect }) => {
    const objectPath = ObjectPath.parse("file.txt");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      objectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: "foo",
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(Array.fromAsync(
      await metadata.search({
        dirPath: [],
        query: "foo",
        take: asUint(1),
        skip: undefined,
        recursive: undefined,
        scoreThreshold: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          objectPath: expect.any(ObjectPath),
          description: "foo",
          searchScore: expect.any(Number),
        },
      ]);
    await expect(metadata.trash({ objectPath }))
      .resolves
      .toStrictEqual({
        entityId: "01989d2b-9d77-7988-ac2d-23659f27b88f",
      });
    await expect(Array.fromAsync(
      await metadata.search({
        dirPath: [],
        query: "foo",
        take: asUint(1),
        skip: undefined,
        recursive: undefined,
        scoreThreshold: undefined,
      }),
    ))
      .resolves
      .toStrictEqual([]);
  });
});

describe("delete", () => {
  test("メタデータを削除できる", async ({ metadata, expect }) => {
    const objectPath = ObjectPath.parse("file.txt");
    const entityId = asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId,
      mimeType: asMimeType("text/plain"),
      objectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: "foo",
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.read({
      select: {},
      where: {
        objectPath,
      },
    }))
      .resolves
      .toBeDefined();
    await expect(metadata.delete({ entityId }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.read({
      select: {},
      where: {
        objectPath,
      },
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("存在しないメタデータを削除しようとしてエラー", async ({ metadata, expect }) => {
    const entityId = asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f");

    await expect(metadata.delete({ entityId }))
      .rejects
      .toThrow(EntityNotFoundError);
  });

  test("削除フラグが経っているメタデータを削除できる", async ({ metadata, expect }) => {
    const objectPath = ObjectPath.parse("file.txt");
    const entityId = asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f");

    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId,
      mimeType: asMimeType("text/plain"),
      objectPath,
      objectSize: asUint(0),
      objectTags: asObjectTags([]),
      description: "foo",
      userMetadata: null,
    }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.trash({ objectPath }))
      .resolves
      .toStrictEqual({
        entityId,
      });
    await expect(metadata.delete({ entityId }))
      .resolves
      .not
      .toThrow();
    await expect(metadata.read({
      select: {},
      where: {
        objectPath,
      },
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });
});

function asBucketName(name: string) {
  return v.parse(schemas.BucketName, name);
}

function asEntityId(str: string) {
  return v.parse(schemas.EntityId, str);
}

function asChecksum(str: string) {
  return v.parse(schemas.Checksum, str);
}

function asMimeType(str: string) {
  return v.parse(schemas.MimeType, str);
}

function asUint(num: number) {
  return v.parse(schemas.UnsignedInteger, num);
}

function asObjectTags(tags: readonly string[]) {
  return v.parse(schemas.ObjectTags, tags);
}

function asHashState(state: readonly number[]) {
  return v.parse(schemas.HashState, state);
}
