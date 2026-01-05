import * as duckdb from "@duckdb/duckdb-wasm";
import ehWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import mvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdbWasmEh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import duckdbWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import * as superjson from "superjson";
import { describe, expectTypeOf, test as vitest } from "vitest";
import getEntityId from "../../src/core/_get-entity-id.js";
import Metadata from "../../src/core/metadata.js";
import DuckdbWasm from "../../src/envs/browser/database/duckdb-wasm.js";
import ConsoleLogger from "../../src/envs/shared/logger/console-logger.js";
import VoidLogger from "../../src/envs/shared/logger/void-logger.js";
import PassThroughTextSearch from "../../src/envs/shared/text-search/pass-through-text-search.js";
import type { IDatabase } from "../../src/shared/database.js";
import {
  ChecksumMismatchError,
  InvalidInputError,
  ObjectExistsError,
  ObjectNotFoundError,
  ObjectSizeTooLargeError,
  ObjectSizeTooSamllError,
} from "../../src/shared/errors.js";
import { LogLevel } from "../../src/shared/logger.js";
import ObjectPath from "../../src/shared/object-path.js";
import {
  type BucketName,
  BucketNameSchema,
  type Checksum,
  ChecksumSchema,
  type EntityId,
  EntityIdSchema,
  HashStateSchema,
  type MimeType,
  MimeTypeSchema,
  type NumParts,
  NumPartsSchema,
  ObjectDirectoryPathSchema,
  type ObjectId,
  ObjectIdSchema,
  type ObjectSize,
  ObjectSizeSchema,
  type ObjectTags,
  ObjectTagsSchema,
  type PartSize,
  PartSizeSchema,
  type RecordType,
  type Timestamp,
  TimestampSchema,
  UintSchema,
} from "../../src/shared/schemas.js";
import * as v from "../../src/shared/valibot.js";

const test = vitest.extend<{
  metadata: Metadata;
}>({
  async metadata({}, use) {
    let database: IDatabase;
    if (__CLIENT__) {
      // https://duckdb.org/docs/stable/clients/wasm/instantiation#vite
      const duckdbBundle = await duckdb.selectBundle({
        mvp: {
          mainModule: duckdbWasm,
          mainWorker: mvpWorker,
        },
        eh: {
          mainModule: duckdbWasmEh,
          mainWorker: ehWorker,
        },
      });
      database = new DuckdbWasm(":memory:", duckdbBundle, new duckdb.VoidLogger());
    } else {
      const {
        default: DuckdbNodeNeo,
      } = await import("../../src/envs/node/database/duckdb-node-neo.js");
      database = new DuckdbNodeNeo(":memory:");
    }

    const metadata = new Metadata({
      json: superjson,
      logger: __DEBUG__
        ? new ConsoleLogger(LogLevel.DEBUG)
        : new VoidLogger(),
      database,
      bucketName: asBucketName("test"),
      textSearch: new PassThroughTextSearch(),
      maxDescriptionTextByteSize: asUint(50),
      maxUserMetadataJsonByteSize: asUint(50),
    });
    await database.open();
    await metadata.open();
    await use(metadata);
    await metadata.close();
    database.close();
  },
});

/***************************************************************************************************
 *
 * 入力パラメーター
 *
 **************************************************************************************************/

describe("constructor", () => {
  test("インスタンスを構築できる", async ({ expect, metadata }) => {
    expect(metadata).toBeInstanceOf(Metadata);
  });
});

/***************************************************************************************************
 *
 * 作成
 *
 **************************************************************************************************/

describe("create", () => {
  test("メタデータを作成できる", async ({ expect, metadata }) => {
    await expect(metadata.create({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .toBeUndefined();
  });

  test("オブジェクトサイズがパート構成以下だとエラー", async ({ expect, metadata }) => {
    await expect(metadata.create({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(1),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .rejects
      .toThrow(ObjectSizeTooSamllError);
  });

  test("オブジェクトサイズがパート構成より大さいとエラー", async ({ expect, metadata }) => {
    await expect(metadata.create({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(1),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(5e6 + 1),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .rejects
      .toThrow(ObjectSizeTooLargeError);
  });

  test("サイズ上限を超える説明文で作成しようとしてエラー", async ({ expect, metadata }) => {
    await expect(metadata.create({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: "0".repeat(50 + 1),
      userMetadata: null,
    }))
      .rejects
      .toThrow(InvalidInputError);
  });

  test("サイズ上限を超えるユーザー定義メタデータで作成しようとしてエラー", async ({ expect, metadata }) => {
    await expect(metadata.create({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: ["0".repeat(50)],
    }))
      .rejects
      .toThrow(InvalidInputError);
  });

  test("重複するパスで作成すると上書きになる", async ({ expect, metadata }) => {
    await expect(metadata.create({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("path/to/file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .toBeUndefined();
    await expect(metadata.create({
      checksum: {
        value: asChecksum("11111111111111111111111111111111"),
        state: asHashState([]),
      },
      entityId: asEntityId("0198a275-bad6-7e39-8541-ed79afda8c84"),
      mimeType: asMimeType("application/octet-stream"),
      numParts: asNumParts(1),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("path/to/file.txt"),
      objectSize: asObjectSize(128),
      objectTags: asObjectTags(["foo"]),
      description: "new description",
      userMetadata: {
        foo: "bar",
      },
    }))
      .resolves
      .toBeUndefined();
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

  test("重複したエンティティで作成しようとしてエラー", async ({ expect, metadata }) => {
    await expect(metadata.create({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file-1.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .toBeUndefined();
    await expect(metadata.create({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file-2.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .rejects
      .toThrow(
        "Duplicate key \"entityid: 01989d2b-9d77-7988-ac2d-23659f27b88f\" violates unique constraint.",
      );
  });

  test("タイムスタンプを指定して作成できる", async ({ expect, metadata }) => {
    await expect(metadata.create({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: asTimestamp("2025-09-20T00:06:39.623Z"),
      objectPath: ObjectPath.parse("path/to/file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .toBeUndefined();
    await expect(metadata.read({
      select: {
        lastModifiedAt: true,
        recordTimestamp: true,
      },
      where: {
        objectPath: ObjectPath.parse("path/to/file.txt"),
      },
    }))
      .resolves
      .toStrictEqual({
        lastModifiedAt: Date.parse("2025-09-20T00:06:39.623Z"),
        recordTimestamp: Date.parse("2025-09-20T00:06:39.623Z"),
      });
  });
});

describe("createExclusive", () => {
  test("メタデータを作成できる", async ({ expect, metadata }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .toBeUndefined();
  });

  test("オブジェクトサイズがパート構成以下だとエラー", async ({ expect, metadata }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(1),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .rejects
      .toThrow(ObjectSizeTooSamllError);
  });

  test("オブジェクトサイズがパート構成より大さいとエラー", async ({ expect, metadata }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(1),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(5e6 + 1),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .rejects
      .toThrow(ObjectSizeTooLargeError);
  });

  test("サイズ上限を超える説明文で作成しようとしてエラー", async ({ expect, metadata }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: "0".repeat(50 + 1),
      userMetadata: null,
    }))
      .rejects
      .toThrow(InvalidInputError);
  });

  test("サイズ上限を超えるユーザー定義メタデータで作成しようとしてエラー", async ({ expect, metadata }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: ["0".repeat(50)],
    }))
      .rejects
      .toThrow(InvalidInputError);
  });

  test("重複するパスで作成しようとしてエラー", async ({ expect, metadata }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("path/to/file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .toBeUndefined();
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("11111111111111111111111111111111"),
        state: asHashState([]),
      },
      entityId: asEntityId("0198a275-bad6-7e39-8541-ed79afda8c84"),
      mimeType: asMimeType("application/octet-stream"),
      numParts: asNumParts(1),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("path/to/file.txt"),
      objectSize: asObjectSize(128),
      objectTags: asObjectTags(["foo"]),
      description: "new description",
      userMetadata: {
        foo: "bar",
      },
    }))
      .rejects
      .toThrow(ObjectExistsError);
  });

  test("重複したエンティティで作成しようとしてエラー", async ({ expect, metadata }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file-1.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .toBeUndefined();
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file-2.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .rejects
      .toThrow(
        "Duplicate key \"entityid: 01989d2b-9d77-7988-ac2d-23659f27b88f\" violates unique constraint.",
      );
  });

  test("タイムスタンプを指定して作成できる", async ({ expect, metadata }) => {
    await expect(metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: asTimestamp("2025-09-20T00:06:39.623Z"),
      objectPath: ObjectPath.parse("path/to/file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    }))
      .resolves
      .toBeUndefined();
    await expect(metadata.read({
      select: {
        lastModifiedAt: true,
        recordTimestamp: true,
      },
      where: {
        objectPath: ObjectPath.parse("path/to/file.txt"),
      },
    }))
      .resolves
      .toStrictEqual({
        lastModifiedAt: Date.parse("2025-09-20T00:06:39.623Z"),
        recordTimestamp: Date.parse("2025-09-20T00:06:39.623Z"),
      });
  });
});

/***************************************************************************************************
 *
 * 読み取り
 *
 **************************************************************************************************/

describe("read", () => {
  test("すべてのメタデータを取得できる", async ({ expect, metadata }) => {
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: asTimestamp("2025-09-20T00:06:39.623Z"),
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags(["foo", "bar"]),
      description: null,
      userMetadata: null,
    });
    const out = await metadata.read({
      select: undefined,
      where: {
        objectPath: ObjectPath.parse("file.txt"),
      },
    });

    expectTypeOf(out).toEqualTypeOf<{
      bucket: BucketName;
      id: ObjectId;
      path: ObjectPath;
      recordType: RecordType;
      recordTimestamp: Timestamp;
      size: ObjectSize;
      numParts: NumParts;
      partSize: PartSize;
      mimeType: MimeType;
      createdAt: Timestamp;
      lastModifiedAt: Timestamp;
      checksum: Checksum;
      checksumAlgorithm: "MD5";
      objectTags: ObjectTags;
      description: string | null;
      userMetadata: unknown;
      entityId: EntityId;
    }>();
    expect(out).toStrictEqual({
      bucket: "test",
      id: expect.stringMatching(v.UUIDv7_REGEX),
      path: expect.any(ObjectPath),
      recordType: "CREATE",
      recordTimestamp: expect.any(Number),
      size: 0,
      numParts: 0,
      partSize: 5e6,
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

  test("特定のメタデータを取得できる", async ({ expect, metadata }) => {
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: asTimestamp("2025-09-20T00:06:39.623Z"),
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags(["foo", "bar"]),
      description: null,
      userMetadata: null,
    });
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
      createdAt: Timestamp;
      recordType: RecordType;
    }>();
    expect(out).toStrictEqual({
      createdAt: expect.any(Number),
      recordType: "CREATE",
    });
  });

  test("存在しないメタデータを取得しようとしてエラー", async ({ expect, metadata }) => {
    await expect(metadata.read({
      select: undefined,
      where: {
        objectPath: ObjectPath.parse("file.txt"),
      },
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("ユーザー定義のメタデータを取得できる", async ({ expect, metadata }) => {
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: asTimestamp("2025-09-20T00:06:39.623Z"),
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags(["foo", "bar"]),
      description: null,
      userMetadata: new Set(["foo"]),
    });
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
      userMetadata: new Set(["foo"]),
    });
  });
});

describe("readDetail", () => {
  test("内部利用のためのメタデータを取得できる", async ({ expect, metadata }) => {
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([1, 2, 3]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: asTimestamp("2025-09-20T00:06:39.623Z"),
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags(["foo", "bar"]),
      description: null,
      userMetadata: null,
    });

    await expect(metadata.readDetail({ objectPath: ObjectPath.parse("file.txt") }))
      .resolves
      .toStrictEqual({
        size: 0,
        checksum: {
          value: "00000000000000000000000000000000",
          state: [1, 2, 3],
        },
        entityId: "01989d2b-9d77-7988-ac2d-23659f27b88f",
        numParts: 0,
        partSize: 5e6,
      });
  });

  test("存在しない内部利用のためのメタデータを取得しようとしてエラー", async ({ expect, metadata }) => {
    await expect(metadata.readDetail({ objectPath: ObjectPath.parse("file.txt") }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });
});

describe("exists", () => {
  test("メタデータが存在する場合は true", async ({ expect, metadata }) => {
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([1, 2, 3]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: asTimestamp("2025-09-20T00:06:39.623Z"),
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags(["foo", "bar"]),
      description: null,
      userMetadata: null,
    });

    await expect(metadata.exists({ objectPath: ObjectPath.parse("file.txt") }))
      .resolves
      .toStrictEqual({
        exists: true,
      });
  });

  test("メタデータが存在しない場合は false", async ({ expect, metadata }) => {
    await expect(metadata.exists({ objectPath: ObjectPath.parse("file.txt") }))
      .resolves
      .toStrictEqual({
        exists: false,
      });
  });

  test("ディレクトリが存在する場合は true", async ({ expect, metadata }) => {
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([1, 2, 3]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: asTimestamp("2025-09-20T00:06:39.623Z"),
      objectPath: ObjectPath.parse("path/to/file.txt"),
      objectSize: asObjectSize(0),
      objectTags: asObjectTags(["foo", "bar"]),
      description: null,
      userMetadata: null,
    });

    await expect.soft(metadata.exists({ dirPath: asObjectDirectoryPath([]) }))
      .resolves
      .toStrictEqual({
        exists: true,
      });
    await expect.soft(metadata.exists({ dirPath: asObjectDirectoryPath(["path"]) }))
      .resolves
      .toStrictEqual({
        exists: true,
      });
    await expect.soft(metadata.exists({
      dirPath: asObjectDirectoryPath(["path", "to"]),
    }))
      .resolves
      .toStrictEqual({
        exists: true,
      });
  });

  test("ディレクトリが存在しない場合は false", async ({ expect, metadata }) => {
    await expect.soft(metadata.exists({ dirPath: asObjectDirectoryPath([]) }))
      .resolves
      .toStrictEqual({
        exists: true,
      });
    await expect.soft(metadata.exists({ dirPath: asObjectDirectoryPath(["path"]) }))
      .resolves
      .toStrictEqual({
        exists: false,
      });
    await expect.soft(metadata.exists({
      dirPath: asObjectDirectoryPath(["path", "to"]),
    }))
      .resolves
      .toStrictEqual({
        exists: false,
      });
  });
});

describe("stat", () => {
  test("オブジェクトパスのステータス情報を取得できる", async ({ expect, metadata }) => {
    const PATHS = [
      "file1.txt",
      "file1.txt/file2.txt",
      "a/file1.txt",
      "a/b/file1.txt",
    ];
    for (const path of PATHS) {
      await metadata.createExclusive({
        checksum: {
          value: asChecksum("00000000000000000000000000000000"),
          state: asHashState([1, 2, 3]),
        },
        entityId: getEntityId(),
        mimeType: asMimeType("text/plain"),
        numParts: asNumParts(0),
        partSize: asPartSize(5e6),
        timestamp: asTimestamp("2025-09-20T00:06:39.623Z"),
        objectPath: ObjectPath.parse(path),
        objectSize: asObjectSize(0),
        objectTags: asObjectTags(["foo", "bar"]),
        description: null,
        userMetadata: null,
      });
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

  test("存在しないオブジェクトパスのステータス情報を取得できる", async ({ expect, metadata }) => {
    await expect.soft(metadata.stat({ objectPath: ObjectPath.parse("none.txt") }))
      .resolves
      .toStrictEqual({
        isObject: false,
        isDirectory: false,
      });
  });
});

describe("search", () => {
  test("指定したディレクトリ直下の説明文を対象に全文検索できる", async ({ expect, metadata }) => {
    const DOCS = {
      "i/x1.txt": "foo shallow",
      "i/j/x1.txt": "foo foo foo bar baz",
      "i/j/x2.txt": "foo foo bar bar",
      "i/j/x3.txt": "foo",
      "i/j/x4.txt": "qux",
      "i/j/x5.txt": undefined,
      "i/j/k/x1.txt": "foo deep",
    };
    for (const [path, description] of Object.entries(DOCS)) {
      await metadata.createExclusive({
        checksum: {
          value: asChecksum("00000000000000000000000000000000"),
          state: asHashState([]),
        },
        entityId: getEntityId(),
        mimeType: asMimeType("text/plain"),
        numParts: asNumParts(0),
        partSize: asPartSize(5e6),
        timestamp: undefined,
        objectPath: ObjectPath.parse(path),
        objectSize: asObjectSize(0),
        objectTags: asObjectTags([]),
        description,
        userMetadata: null,
      });
    }

    await expect.soft(Array.fromAsync(
      await metadata.search({
        dirPath: asObjectDirectoryPath(["i", "j"]),
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
          description: "foo foo foo bar baz", // 一致回数が多い
          searchScore: expect.any(Number),
        },
        {
          objectPath: expect.any(ObjectPath),
          description: "foo foo bar bar",
          searchScore: expect.any(Number),
        },
        {
          objectPath: expect.any(ObjectPath),
          description: "foo", // 一致回数が少ない
          searchScore: expect.any(Number),
        },
      ]);
    await expect.soft(Array.fromAsync(
      await metadata.search({
        dirPath: asObjectDirectoryPath(["i", "j"]),
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
          description: "foo foo foo bar baz", // 一致回数が多い
          searchScore: expect.any(Number),
        },
      ]);
    await expect.soft(Array.fromAsync(
      await metadata.search({
        dirPath: asObjectDirectoryPath(["i", "j"]),
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
    await expect.soft(Array.fromAsync(
      await metadata.search({
        dirPath: asObjectDirectoryPath(["i"]),
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
    await expect.soft(Array.fromAsync(
      await metadata.search({
        dirPath: asObjectDirectoryPath(["i", "j", "k"]),
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

  test("パスを対象に全文検索できる", async ({ expect, metadata }) => {
    const DOCS = {
      "path/to/ファイル.txt": null,
      "path/to/説明書.txt": null,
    };
    for (const [path, description] of Object.entries(DOCS)) {
      await metadata.createExclusive({
        checksum: {
          value: asChecksum("00000000000000000000000000000000"),
          state: asHashState([]),
        },
        entityId: getEntityId(),
        mimeType: asMimeType("text/plain"),
        numParts: asNumParts(0),
        partSize: asPartSize(5e6),
        timestamp: undefined,
        objectPath: ObjectPath.parse(path),
        objectSize: asObjectSize(0),
        objectTags: asObjectTags([]),
        description,
        userMetadata: null,
      });
    }

    const list = JSON.parse(JSON.stringify(
      await Array.fromAsync(
        await metadata.search({
          dirPath: asObjectDirectoryPath([]),
          query: "ファイル",
          take: undefined,
          skip: undefined,
          recursive: true,
          scoreThreshold: undefined,
        }),
      ),
    ));

    expect(list).toStrictEqual([
      {
        objectPath: "path/to/ファイル.txt",
        description: null,
        searchScore: expect.any(Number),
      },
    ]);
  });

  test("日本語で全文検索できる", async ({ expect, metadata }) => {
    const DOCS = {
      "path/to/file1.txt": "これは日本語で書かれた説明文です",
      "path/to/file2.txt": "これは日本語で書かれた文字列です",
    };
    for (const [path, description] of Object.entries(DOCS)) {
      await metadata.createExclusive({
        checksum: {
          value: asChecksum("00000000000000000000000000000000"),
          state: asHashState([]),
        },
        entityId: getEntityId(),
        mimeType: asMimeType("text/plain"),
        numParts: asNumParts(0),
        partSize: asPartSize(5e6),
        timestamp: undefined,
        objectPath: ObjectPath.parse(path),
        objectSize: asObjectSize(0),
        objectTags: asObjectTags([]),
        description,
        userMetadata: null,
      });
    }

    await expect.soft(Array.fromAsync(
      await metadata.search({
        dirPath: asObjectDirectoryPath(["path", "to"]),
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
        dirPath: asObjectDirectoryPath(["path", "to"]),
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
        dirPath: asObjectDirectoryPath(["path", "to"]),
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
  test("オブジェクトをリストアップできる", async ({ expect, metadata }) => {
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
      await metadata.createExclusive({
        checksum: {
          value: asChecksum("00000000000000000000000000000000"),
          state: asHashState([]),
        },
        entityId: getEntityId(),
        mimeType: asMimeType("text/plain"),
        numParts: asNumParts(0),
        partSize: asPartSize(5e6),
        timestamp: undefined,
        objectPath: ObjectPath.parse(path),
        objectSize: asObjectSize(0),
        objectTags: asObjectTags([]),
        description: null,
        userMetadata: null,
      });
    }

    await expect.soft(Array.fromAsync(
      await metadata.list({
        select: {
          size: true,
          mimeType: true,
          lastModifiedAt: true,
        },
        where: {
          dirPath: asObjectDirectoryPath([]),
          isObject: true,
        },
        take: undefined,
        skip: undefined,
        orderBy: {
          name: {
            type: undefined,
            collate: undefined,
          },
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

  test("ディレクトリーをリストアップできる", async ({ expect, metadata }) => {
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
      await metadata.createExclusive({
        checksum: {
          value: asChecksum("00000000000000000000000000000000"),
          state: asHashState([]),
        },
        entityId: getEntityId(),
        mimeType: asMimeType("text/plain"),
        numParts: asNumParts(0),
        partSize: asPartSize(5e6),
        timestamp: undefined,
        objectPath: ObjectPath.parse(path),
        objectSize: asObjectSize(0),
        objectTags: asObjectTags([]),
        description: null,
        userMetadata: null,
      });
    }

    await expect.soft(Array.fromAsync(
      await metadata.list({
        select: undefined,
        where: {
          dirPath: asObjectDirectoryPath([]),
          isObject: false,
        },
        take: undefined,
        skip: undefined,
        orderBy: {
          name: {
            type: undefined,
            collate: undefined,
          },
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

  test("ディレクトリとオブジェクトをリストアップできる", async ({ expect, metadata }) => {
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
      await metadata.createExclusive({
        checksum: {
          value: asChecksum("00000000000000000000000000000000"),
          state: asHashState([]),
        },
        entityId: getEntityId(),
        mimeType: asMimeType("text/plain"),
        numParts: asNumParts(0),
        partSize: asPartSize(5e6),
        timestamp: undefined,
        objectPath: ObjectPath.parse(path),
        objectSize: asObjectSize(0),
        objectTags: asObjectTags([]),
        description: null,
        userMetadata: null,
      });
    }

    await expect.soft(Array.fromAsync(
      await metadata.list({
        select: {
          size: true,
          mimeType: true,
          lastModifiedAt: true,
        },
        where: {
          dirPath: asObjectDirectoryPath([]),
          isObject: undefined,
        },
        take: undefined,
        skip: undefined,
        orderBy: {
          name: {
            type: undefined,
            collate: undefined,
          },
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
          dirPath: asObjectDirectoryPath(["a"]),
          isObject: undefined,
        },
        take: undefined,
        skip: undefined,
        orderBy: {
          name: {
            type: undefined,
            collate: undefined,
          },
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
          dirPath: asObjectDirectoryPath(["a", "b"]),
          isObject: undefined,
        },
        take: undefined,
        skip: undefined,
        orderBy: {
          name: {
            type: undefined,
            collate: undefined,
          },
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
          dirPath: asObjectDirectoryPath(["b", "c"]),
          isObject: undefined,
        },
        take: undefined,
        skip: undefined,
        orderBy: {
          name: {
            type: undefined,
            collate: undefined,
          },
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

/***************************************************************************************************
 *
 * 更新
 *
 **************************************************************************************************/

describe("move", () => {
  test("メタデータを移動できる", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/moved.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: srcObjectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    });

    await expect(metadata.move({
      srcObjectPath,
      dstObjectPath,
    }))
      .resolves
      .toBeUndefined();
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

  test("存在しないメタデータを移動しようとしてエラー", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/moved.txt");

    await expect(metadata.move({
      srcObjectPath,
      dstObjectPath,
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("すでに存在するメタデータへ移動すると上書き", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/moved.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: srcObjectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags(["src"]),
      description: "src",
      userMetadata: ["src"],
    });
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("11111111111111111111111111111111"),
        state: asHashState([]),
      },
      entityId: asEntityId("0198a275-bad6-7e39-8541-ed79afda8c84"),
      mimeType: asMimeType("audio/mp3"),
      numParts: asNumParts(2),
      partSize: asPartSize(10e6),
      timestamp: undefined,
      objectPath: dstObjectPath,
      objectSize: asObjectSize(19e6),
      objectTags: asObjectTags(["dst"]),
      description: "dst",
      userMetadata: ["dst"],
    });

    await expect(metadata.move({
      srcObjectPath,
      dstObjectPath,
    }))
      .resolves
      .toBeUndefined();
    await expect(metadata.read({
      select: {
        checksum: true,
        entityId: true,
        mimeType: true,
        numParts: true,
        partSize: true,
        objectTags: true,
        description: true,
        userMetadata: true,
      },
      where: {
        objectPath: dstObjectPath,
      },
    }))
      .resolves
      .toStrictEqual({
        checksum: "00000000000000000000000000000000",
        entityId: "01989d2b-9d77-7988-ac2d-23659f27b88f",
        mimeType: "text/plain",
        numParts: 0,
        partSize: 5e6,
        objectTags: ["src"],
        description: "src",
        userMetadata: ["src"],
      });
  });
});

describe("moveExclusive", () => {
  test("メタデータを移動できる", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/moved.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: srcObjectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    });

    await expect(metadata.moveExclusive({
      srcObjectPath,
      dstObjectPath,
    }))
      .resolves
      .toBeUndefined();
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

  test("存在しないメタデータを移動しようとしてエラー", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/moved.txt");

    await expect(metadata.moveExclusive({
      srcObjectPath,
      dstObjectPath,
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("すでに存在するメタデータへ移動しようとしてエラー", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/moved.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: srcObjectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    });
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("0198a275-bad6-7e39-8541-ed79afda8c84"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: dstObjectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    });

    await expect(metadata.moveExclusive({
      srcObjectPath,
      dstObjectPath,
    }))
      .rejects
      .toThrow(ObjectExistsError);
  });
});

describe("copy", () => {
  test("メタデータをコピーできる", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/copied.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: srcObjectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    });

    await expect(metadata.copy({
      srcObjectPath,
      dstObjectPath,
      dstEntityId: asEntityId("0198a275-bad6-7e39-8541-ed79afda8c84"),
      timestamp: undefined,
    }))
      .resolves
      .toBeUndefined();
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

  test("存在しないメタデータをコピーしようとしてエラー", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/copied.txt");

    await expect(metadata.copy({
      srcObjectPath,
      dstObjectPath,
      dstEntityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      timestamp: undefined,
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("同じパスへメタデータをコピーできる", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = srcObjectPath;
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: srcObjectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    });

    await expect(metadata.copy({
      srcObjectPath,
      dstObjectPath,
      dstEntityId: asEntityId("019a0e6e-2053-7f24-8c06-05ccd32ec7e6"),
      timestamp: undefined,
    }))
      .resolves
      .toBeUndefined();
  });

  test("すでに存在するメタデータへコピーすると上書き", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/copied.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: srcObjectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags(["src"]),
      description: "src",
      userMetadata: ["src"],
    });
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("11111111111111111111111111111111"),
        state: asHashState([]),
      },
      entityId: asEntityId("0198a275-bad6-7e39-8541-ed79afda8c84"),
      mimeType: asMimeType("audio/mp3"),
      numParts: asNumParts(2),
      partSize: asPartSize(10e6),
      timestamp: undefined,
      objectPath: dstObjectPath,
      objectSize: asObjectSize(19e6),
      objectTags: asObjectTags(["dst"]),
      description: "dst",
      userMetadata: ["dst"],
    });

    await expect(metadata.copy({
      srcObjectPath,
      dstObjectPath,
      dstEntityId: asEntityId("019a0e6e-2053-7f24-8c06-05ccd32ec7e6"),
      timestamp: undefined,
    }))
      .resolves
      .toBeUndefined();
    await expect(metadata.read({
      select: {
        checksum: true,
        entityId: true,
        mimeType: true,
        numParts: true,
        partSize: true,
        objectTags: true,
        description: true,
        userMetadata: true,
      },
      where: {
        objectPath: dstObjectPath,
      },
    }))
      .resolves
      .toStrictEqual({
        checksum: "00000000000000000000000000000000",
        entityId: "019a0e6e-2053-7f24-8c06-05ccd32ec7e6",
        mimeType: "text/plain",
        numParts: 0,
        partSize: 5e6,
        objectTags: ["src"],
        description: "src",
        userMetadata: ["src"],
      });
  });
});

describe("copyExclusive", () => {
  test("メタデータをコピーできる", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/copied.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: srcObjectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    });

    await expect(metadata.copyExclusive({
      srcObjectPath,
      dstObjectPath,
      dstEntityId: asEntityId("0198a275-bad6-7e39-8541-ed79afda8c84"),
      timestamp: undefined,
    }))
      .resolves
      .toBeUndefined();
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
        entityId: "0198a275-bad6-7e39-8541-ed79afda8c84",
      });
  });

  test("存在しないメタデータをコピーしようとしてエラー", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/copied.txt");

    await expect(metadata.copyExclusive({
      srcObjectPath,
      dstObjectPath,
      dstEntityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      timestamp: undefined,
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("同じパスへメタデータをコピーしようとしてエラー", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = srcObjectPath;
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: srcObjectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    });

    await expect(metadata.copyExclusive({
      srcObjectPath,
      dstObjectPath,
      dstEntityId: asEntityId("019a0e6e-2053-7f24-8c06-05ccd32ec7e6"),
      timestamp: undefined,
    }))
      .rejects
      .toThrow(ObjectExistsError);
  });

  test("すでに存在するメタデータへコピーしようとしてエラー", async ({ expect, metadata }) => {
    const srcObjectPath = ObjectPath.parse("path/to/file.txt");
    const dstObjectPath = ObjectPath.parse("path/to/copied.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: srcObjectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    });
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("0198a275-bad6-7e39-8541-ed79afda8c84"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: dstObjectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    });

    await expect(metadata.copyExclusive({
      srcObjectPath,
      dstObjectPath,
      dstEntityId: asEntityId("019a0e6e-2053-7f24-8c06-05ccd32ec7e6"),
      timestamp: undefined,
    }))
      .rejects
      .toThrow(ObjectExistsError);
  });
});

describe("update", () => {
  test("メタデータを更新できる", async ({ expect, metadata }) => {
    const objectPath = ObjectPath.parse("path/to/file.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([1, 2, 3]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: asTimestamp("2025-10-23T10:00:00.987Z"),
      objectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: "old description",
      userMetadata: {
        old: "metadata",
      },
    });

    // mimeType を更新
    await expect(metadata.update({
      objectPath,
      mimeType: asMimeType("application/json"),
      timestamp: undefined,
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
    }))
      .resolves
      .toBeUndefined();
    await expect(metadata.read({
      select: {
        mimeType: true,
        recordType: true,
      },
      where: {
        objectPath,
      },
    }))
      .resolves
      .toStrictEqual({
        mimeType: "application/json",
        recordType: "UPDATE_METADATA",
      });

    // timestamp を更新
    await expect(metadata.update({
      objectPath,
      mimeType: undefined,
      timestamp: asTimestamp("2025-10-23T10:00:00.123Z"),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
    }))
      .resolves
      .toBeUndefined();
    await expect(metadata.read({
      select: {
        lastModifiedAt: true,
        recordTimestamp: true,
      },
      where: {
        objectPath,
      },
    }))
      .resolves
      .toStrictEqual({
        lastModifiedAt: Date.parse("2025-10-23T10:00:00.123Z"),
        recordTimestamp: Date.parse("2025-10-23T10:00:00.123Z"),
      });

    // description を更新
    await expect(metadata.update({
      objectPath,
      mimeType: undefined,
      timestamp: undefined,
      objectTags: undefined,
      description: "new description",
      userMetadata: undefined,
    }))
      .resolves
      .toBeUndefined();
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
      timestamp: undefined,
      objectTags: undefined,
      description: undefined,
      userMetadata: {
        old: null,
        new: "metadata",
      },
    }))
      .resolves
      .toBeUndefined();
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
      timestamp: undefined,
      objectTags: undefined,
      description: null,
      userMetadata: undefined,
    }))
      .resolves
      .toBeUndefined();
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
      timestamp: undefined,
      objectTags: undefined,
      description: undefined,
      userMetadata: null,
    }))
      .resolves
      .toBeUndefined();
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

  test("存在しないメタデータを更新しようとしてエラー", async ({ expect, metadata }) => {
    const objectPath = ObjectPath.parse("path/to/file.txt");

    await expect(metadata.update({
      objectPath,
      mimeType: undefined,
      timestamp: undefined,
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("説明文を更新したあと検索に反映される", async ({ expect, metadata }) => {
    const objectPath = ObjectPath.parse("i/j/k.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([1, 2, 3]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: "old",
      userMetadata: null,
    });

    await expect(Array.fromAsync(
      await metadata.search({
        dirPath: asObjectDirectoryPath(["i", "j"]),
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
      timestamp: undefined,
      objectTags: undefined,
      description: "new",
      userMetadata: undefined,
    }))
      .resolves
      .toBeUndefined();
    await expect.soft(Array.fromAsync(
      await metadata.search({
        dirPath: asObjectDirectoryPath(["i", "j"]),
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
        dirPath: asObjectDirectoryPath(["i", "j"]),
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
  test("メタデータを更新できる", async ({ expect, metadata }) => {
    const objectPath = ObjectPath.parse("path/to/file.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([1, 2, 3]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: asTimestamp("2025-10-23T10:00:00.987Z"),
      objectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: "old description",
      userMetadata: {
        old: "metadata",
      },
    });

    await expect(metadata.updateExclusive({
      objectPath,
      expect: {
        checksum: asChecksum("00000000000000000000000000000000"),
      },
      checksum: {
        value: asChecksum("22222222222222222222222222222222"),
        state: asHashState([4, 5, 6]),
      },
      entityId: asEntityId("019a0e91-69de-70ac-8019-dd22e7a20bf4"),
      mimeType: asMimeType("application/json"),
      numParts: asNumParts(1),
      partSize: asPartSize(10e6),
      timestamp: asTimestamp("2025-10-23T10:00:00.123Z"),
      objectSize: asObjectSize(128),
      objectTags: asObjectTags(["foo"]),
      description: "new description",
      userMetadata: {
        new: "metadata",
      },
    }))
      .resolves
      .toBeUndefined();
    await expect(metadata.read({
      select: {
        mimeType: true,
        numParts: true,
        partSize: true,
        objectTags: true,
        recordType: true,
        description: true,
        userMetadata: true,
        lastModifiedAt: true,
        recordTimestamp: true,
      },
      where: {
        objectPath,
      },
    }))
      .resolves
      .toStrictEqual({
        mimeType: "application/json",
        numParts: 1,
        partSize: 10e6,
        objectTags: ["foo"],
        recordType: "UPDATE_METADATA",
        description: "new description",
        userMetadata: {
          new: "metadata",
        },
        lastModifiedAt: Date.parse("2025-10-23T10:00:00.123Z"),
        recordTimestamp: Date.parse("2025-10-23T10:00:00.123Z"),
      });
    await expect(metadata.readDetail({ objectPath }))
      .resolves
      .toStrictEqual({
        size: 128,
        checksum: {
          value: "22222222222222222222222222222222",
          state: [4, 5, 6],
        },
        entityId: "019a0e91-69de-70ac-8019-dd22e7a20bf4",
        numParts: 1,
        partSize: 10e6,
      });
  });

  test("オブジェクトサイズがパート構成以下だとエラー", async ({ expect, metadata }) => {
    await expect(metadata.updateExclusive({
      expect: {
        checksum: asChecksum("00000000000000000000000000000000"),
      },
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: undefined,
      mimeType: undefined,
      numParts: asNumParts(1),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
    }))
      .rejects
      .toThrow(ObjectSizeTooSamllError);
  });

  test("オブジェクトサイズがパート構成より大さいとエラー", async ({ expect, metadata }) => {
    await expect(metadata.updateExclusive({
      expect: {
        checksum: asChecksum("00000000000000000000000000000000"),
      },
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: undefined,
      mimeType: undefined,
      numParts: asNumParts(1),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(5e6 + 1),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
    }))
      .rejects
      .toThrow(ObjectSizeTooLargeError);
  });

  test("サイズ上限を超える説明文で作成しようとしてエラー", async ({ expect, metadata }) => {
    await expect(metadata.updateExclusive({
      expect: {
        checksum: asChecksum("00000000000000000000000000000000"),
      },
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: undefined,
      mimeType: undefined,
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: undefined,
      description: "0".repeat(50 + 1),
      userMetadata: undefined,
    }))
      .rejects
      .toThrow(InvalidInputError);
  });

  test("サイズ上限を超えるユーザー定義メタデータで更新しようとしてエラー", async ({ expect, metadata }) => {
    await expect(metadata.updateExclusive({
      expect: {
        checksum: asChecksum("00000000000000000000000000000000"),
      },
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: undefined,
      mimeType: undefined,
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath: ObjectPath.parse("file.txt"),
      objectSize: asObjectSize(0),
      objectTags: undefined,
      description: undefined,
      userMetadata: ["0".repeat(50)],
    }))
      .rejects
      .toThrow(InvalidInputError);
  });

  test("存在しないメタデータを更新しようとしてエラー", async ({ expect, metadata }) => {
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
      numParts: asNumParts(1),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectSize: asObjectSize(128),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("期待するチェックサムと異なるメタデータを更新しようとしてエラー", async ({ expect, metadata }) => {
    const objectPath = ObjectPath.parse("path/to/file.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([1, 2, 3]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: asTimestamp("2025-10-23T10:00:00.987Z"),
      objectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: "old description",
      userMetadata: {
        old: "metadata",
      },
    });

    await expect(metadata.updateExclusive({
      objectPath,
      expect: {
        checksum: asChecksum("11111111111111111111111111111111"),
      },
      checksum: {
        value: asChecksum("22222222222222222222222222222222"),
        state: asHashState([4, 5, 6]),
      },
      entityId: asEntityId("019a0e91-69de-70ac-8019-dd22e7a20bf4"),
      mimeType: asMimeType("application/json"),
      numParts: asNumParts(1),
      partSize: asPartSize(10e6),
      timestamp: asTimestamp("2025-10-23T10:00:00.123Z"),
      objectSize: asObjectSize(128),
      objectTags: asObjectTags(["foo"]),
      description: "new description",
      userMetadata: {
        new: "metadata",
      },
    }))
      .rejects
      .toThrow(ChecksumMismatchError);
  });
});

/***************************************************************************************************
 *
 * 削除系
 *
 **************************************************************************************************/

describe("trash, listInTrash", () => {
  test("削除フラグを立てられる", async ({ expect, metadata }) => {
    const objectPath = ObjectPath.parse("file.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    });

    await expect(metadata.trash({
      objectPath,
      timestamp: undefined,
    }))
      .resolves
      .toStrictEqual({
        entityId: "01989d2b-9d77-7988-ac2d-23659f27b88f",
        objectId: expect.stringMatching(v.UUIDv7_REGEX),
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
      await metadata.list({
        select: undefined,
        where: {
          dirPath: asObjectDirectoryPath([]),
          isObject: undefined,
        },
        skip: undefined,
        take: undefined,
        orderBy: {
          name: {
            type: undefined,
            collate: undefined,
          },
          preferObject: undefined,
        },
      }),
    ))
      .resolves
      .toStrictEqual([]);
    await expect(Array.fromAsync(
      await metadata.listInTrash({
        select: {
          entityId: true,
        },
        where: {
          dirPath: asObjectDirectoryPath([]),
        },
        skip: undefined,
        take: undefined,
        orderBy: {
          name: {
            type: undefined,
            collate: undefined,
          },
        },
      }),
    ))
      .resolves
      .toStrictEqual([
        {
          isObject: true,
          name: "file.txt",
          entityId: "01989d2b-9d77-7988-ac2d-23659f27b88f",
        },
      ]);
  });

  test("存在しないメタデータに削除フラグを立てようとしてエラー", async ({ expect, metadata }) => {
    const objectPath = ObjectPath.parse("file.txt");

    await expect(metadata.trash({
      objectPath,
      timestamp: undefined,
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("すでに削除フラグが立てられたメタデータに削除フラグを立てようとしてエラー", async ({ expect, metadata }) => {
    const objectPath = ObjectPath.parse("file.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: null,
      userMetadata: null,
    });

    await expect(metadata.trash({
      objectPath,
      timestamp: undefined,
    }))
      .resolves
      .toStrictEqual({
        entityId: "01989d2b-9d77-7988-ac2d-23659f27b88f",
        objectId: expect.stringMatching(v.UUIDv7_REGEX),
      });
    await expect(metadata.trash({
      objectPath,
      timestamp: undefined,
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("削除フラグを立てたあと検索に反映される", async ({ expect, metadata }) => {
    const objectPath = ObjectPath.parse("file.txt");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId: asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f"),
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: "foo",
      userMetadata: null,
    });

    await expect(Array.fromAsync(
      await metadata.search({
        dirPath: asObjectDirectoryPath([]),
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
    await expect(metadata.trash({
      objectPath,
      timestamp: undefined,
    }))
      .resolves
      .toStrictEqual({
        entityId: "01989d2b-9d77-7988-ac2d-23659f27b88f",
        objectId: expect.stringMatching(v.UUIDv7_REGEX),
      });
    await expect(Array.fromAsync(
      await metadata.search({
        dirPath: asObjectDirectoryPath([]),
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
  test("メタデータを削除できる", async ({ expect, metadata }) => {
    const objectPath = ObjectPath.parse("file.txt");
    const entityId = asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId,
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: "foo",
      userMetadata: null,
    });
    const { id } = await metadata.read({
      select: {
        id: true,
      },
      where: {
        objectPath,
      },
    });

    await expect(metadata.delete({ objectId: id }))
      .resolves
      .toBeUndefined();
    await expect(metadata.read({
      select: {},
      where: {
        objectPath,
      },
    }))
      .rejects
      .toThrow(ObjectNotFoundError);
  });

  test("存在しないメタデータを削除しようとしてもエラーはでない", async ({ expect, metadata }) => {
    const objectId = asObjectId("01989d2b-9d77-7988-ac2d-23659f27b88f");

    await expect(metadata.delete({ objectId }))
      .resolves
      .toBeUndefined();
  });

  test("削除フラグが経っているメタデータを削除できる", async ({ expect, metadata }) => {
    const objectPath = ObjectPath.parse("file.txt");
    const entityId = asEntityId("01989d2b-9d77-7988-ac2d-23659f27b88f");
    await metadata.createExclusive({
      checksum: {
        value: asChecksum("00000000000000000000000000000000"),
        state: asHashState([]),
      },
      entityId,
      mimeType: asMimeType("text/plain"),
      numParts: asNumParts(0),
      partSize: asPartSize(5e6),
      timestamp: undefined,
      objectPath,
      objectSize: asObjectSize(0),
      objectTags: asObjectTags([]),
      description: "foo",
      userMetadata: null,
    });
    const {
      objectId,
      ...trashOut
    } = await metadata.trash({
      objectPath,
      timestamp: undefined,
    });

    expect(trashOut).toStrictEqual({
      entityId,
    });
    await expect(metadata.delete({ objectId }))
      .resolves
      .toBeUndefined();
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

/***************************************************************************************************
 *
 * ユーティリティー
 *
 **************************************************************************************************/

const asUint = (x: number) => v.parse(UintSchema(), x);
const asChecksum = (x: string) => v.parse(ChecksumSchema(), x);
const asEntityId = (x: string) => v.parse(EntityIdSchema(), x);
const asMimeType = (x: string) => v.parse(MimeTypeSchema(), x);
const asNumParts = (x: number) => v.parse(NumPartsSchema(), x);
const asObjectId = (x: string) => v.parse(ObjectIdSchema(), x);
const asPartSize = (x: number) => v.parse(PartSizeSchema(), x);
const asHashState = (x: number[]) => v.parse(HashStateSchema(), x);
const asTimestamp = (x: string) => v.parse(TimestampSchema(), x);
const asBucketName = (x: string) => v.parse(BucketNameSchema(), x);
const asObjectSize = (x: number) => v.parse(ObjectSizeSchema(), x);
const asObjectTags = (x: string[]) => v.parse(ObjectTagsSchema(), x);
const asObjectDirectoryPath = (x: string[]) => v.parse(ObjectDirectoryPathSchema(), x);
