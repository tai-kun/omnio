import type { AsyncmuxLock } from "asyncmux";
import { describe, test as vitest, vi } from "vitest";
import md5 from "../../src/core/_md5.js";
import Metadata from "../../src/core/metadata.js";
import ObjectFileWriteStream from "../../src/core/object-file-write-stream.js";
import DuckdbNodeNeo from "../../src/envs/node/database/duckdb-node-neo.js";
import MemoryFileSystem from "../../src/envs/shared/file-system/memory-file-system.js";
import ConsoleLogger from "../../src/envs/shared/logger/console-logger.js";
import VoidLogger from "../../src/envs/shared/logger/void-logger.js";
import PassThroughTextSearch from "../../src/envs/shared/text-search/pass-through-text-search.js";
import { ObjectExistsError } from "../../src/shared/errors.js";
import { type ILogger, LogLevel } from "../../src/shared/logger.js";
import ObjectPath from "../../src/shared/object-path.js";
import {
  BucketNameSchema,
  EntityIdSchema,
  NumPartsSchema,
  ObjectSizeSchema,
  OpenModeSchema,
  type PartSize,
  PartSizeSchema,
  UintSchema,
} from "../../src/shared/schemas.js";
import type { IStorage } from "../../src/shared/storage.js";
import * as v from "../../src/shared/valibot.js";

const test = vitest.extend<{
  fs: MemoryFileSystem;
  storage: IStorage;
  logger: ILogger;
  metadata: Metadata;
}>({
  async fs({}, use) {
    const fs = new MemoryFileSystem();
    fs.open();
    await use(fs);
    fs.close();
  },
  async storage({ fs }, use) {
    const storage = fs.getDirectoryHandle("test", { create: true });
    await use(storage);
  },
  async logger({}, use) {
    const logger = __DEBUG__
      ? new ConsoleLogger(LogLevel.DEBUG)
      : new VoidLogger();
    use(logger);
  },
  async metadata({ logger }, use) {
    const database = new DuckdbNodeNeo(":memory:");
    const metadata = new Metadata({
      json: JSON,
      logger,
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

describe("constructor", () => {
  test("インスタンスを構築できる", async ({ expect, logger, metadata, storage }) => {
    const lock = {
      unlock: vi.fn(),
      [Symbol.dispose]: vi.fn(),
    } as unknown as AsyncmuxLock;
    const h = await md5.create();
    await using w = new ObjectFileWriteStream({
      currentSize: undefined,
      type: undefined,
      bucketName: asBucketName("test"),
      objectPath: ObjectPath.parse("foo.txt"),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
      timestamp: undefined,
      flag: asOpenMode("w"),
      newEntityId: asEntityId("019a2eb9-32ba-7d57-a39e-f35c32b1ac77"),
      oldEntityId: undefined,
      expect: undefined,
      currentNumParts: undefined,
      partSize: asPartSize(5e6),
      omnio: { closed: false },
      logger,
      metadata,
      lock,
      storage,
      hash: h,
    });

    expect.soft(w.bucketName).toBe("test");
    expect.soft(w.objectPath.toString()).toBe("foo.txt");
    expect.soft(w.flag).toBe("w");

    expect.soft(w.type).toBe(undefined);
    expect.soft(w.objectTags).toBe(undefined);
    expect.soft(w.description).toBe(undefined);
    expect.soft(w.userMetadata).toBe(undefined);
    expect.soft(w.timestamp).toBe(undefined);

    expect.soft(w.closed).toBe(false);
    expect.soft(w.size).toBe(0);
    expect.soft(w.bytesWritten).toBe(0);
  });
});

describe("'w' モード", () => {
  const openMode = asOpenMode("w");

  test("ストリームにデータを書き込める", async ({ expect, fs, storage, logger, metadata }) => {
    const lock = {
      unlock: vi.fn(),
      [Symbol.dispose]: vi.fn(),
    } as unknown as AsyncmuxLock;
    const h = await md5.create();
    await using w = new ObjectFileWriteStream({
      currentSize: undefined,
      type: undefined,
      bucketName: asBucketName("test"),
      objectPath: ObjectPath.parse("foo.txt"),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
      timestamp: undefined,
      flag: openMode,
      newEntityId: asEntityId("019a2eb9-32ba-7d57-a39e-f35c32b1ac77"),
      oldEntityId: undefined,
      expect: undefined,
      currentNumParts: undefined,
      partSize: asPartSize(5e6),
      omnio: { closed: false },
      logger,
      metadata,
      lock,
      storage,
      hash: h,
    });

    await w.write("foo");

    expect(w.bytesWritten).toBe(3);

    await w.write("bar");

    expect(w.bytesWritten).toBe(6);

    await w.write("baz");

    expect(w.bytesWritten).toBe(9);

    await w.close();

    await expect(w.close()).rejects.toThrow(undefined);
    await expect(w.abort()).rejects.toThrow(undefined);
    expect(lock.unlock).toHaveBeenCalledTimes(1);
    expect(lock[Symbol.dispose]).toHaveBeenCalledTimes(0);
    expect(w.closed).toBe(true);
    expect(fs.tree()).toStrictEqual({
      "test": {
        "019a2eb9-32ba-7d57-a39e-f35c32b1ac77": {
          "1": {
            chunks: [
              [102, 111, 111], // foo
              [98, 97, 114], // bar
              [98, 97, 122], // baz
            ],
          },
        },
      },
    });
    await expect(metadata.exists({ objectPath: ObjectPath.parse("foo.txt") }))
      .resolves
      .toStrictEqual({
        exists: true,
      });
  });

  test("書き込みの総量が partSize を超えると別のファイルに書き込まれる", async ({ expect, fs, storage, logger, metadata }) => {
    const lock = {
      unlock: vi.fn(),
      [Symbol.dispose]: vi.fn(),
    } as unknown as AsyncmuxLock;
    const h = await md5.create();
    await using w = new ObjectFileWriteStream({
      currentSize: undefined,
      type: undefined,
      bucketName: asBucketName("test"),
      objectPath: ObjectPath.parse("foo.txt"),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
      timestamp: undefined,
      flag: openMode,
      newEntityId: asEntityId("019a2eb9-32ba-7d57-a39e-f35c32b1ac77"),
      oldEntityId: undefined,
      expect: undefined,
      currentNumParts: undefined,
      partSize: 7 as PartSize,
      omnio: { closed: false },
      logger,
      metadata,
      lock,
      storage,
      hash: h,
    });

    await w.write("foo");

    expect(w.bytesWritten).toBe(3);

    await w.write("bar");

    expect(w.bytesWritten).toBe(6);

    await w.write("baz");

    expect(w.bytesWritten).toBe(9);

    await w.close();

    expect(fs.tree()).toStrictEqual({
      "test": {
        "019a2eb9-32ba-7d57-a39e-f35c32b1ac77": {
          "1": {
            chunks: [
              [102, 111, 111], // foo
              [98, 97, 114], // bar
              [98], // b
            ],
          },
          "2": {
            chunks: [
              [97, 122], // az
            ],
          },
        },
      },
    });
  });

  test("同じオブジェクトパスに書き込むと上書き", async ({ expect, fs, storage, logger, metadata }) => {
    for (let i = 0; i < 2; i++) {
      const lock = {
        unlock: vi.fn(),
        [Symbol.dispose]: vi.fn(),
      } as unknown as AsyncmuxLock;
      const h = await md5.create();
      await using w = new ObjectFileWriteStream({
        currentSize: undefined,
        type: undefined,
        bucketName: asBucketName("test"),
        objectPath: ObjectPath.parse("foo.txt"),
        objectTags: undefined,
        description: undefined,
        userMetadata: undefined,
        timestamp: undefined,
        flag: openMode,
        newEntityId: asEntityId("019a2eb9-32ba-7d57-a39e-f35c32b1ac77"),
        oldEntityId: undefined,
        expect: undefined,
        currentNumParts: undefined,
        partSize: asPartSize(5e6),
        omnio: { closed: false },
        logger,
        metadata,
        lock,
        storage,
        hash: h,
      });

      await w.write("foo");
    }

    expect(fs.tree()).toStrictEqual({
      "test": {
        "019a2eb9-32ba-7d57-a39e-f35c32b1ac77": {
          "1": {
            chunks: [
              [102, 111, 111], // foo
            ],
          },
        },
      },
    });
  });

  test("書き込みを中断できる", async ({ expect, fs, storage, logger, metadata }) => {
    const lock = {
      unlock: vi.fn(),
      [Symbol.dispose]: vi.fn(),
    } as unknown as AsyncmuxLock;
    const h = await md5.create();
    await using w = new ObjectFileWriteStream({
      currentSize: undefined,
      type: undefined,
      bucketName: asBucketName("test"),
      objectPath: ObjectPath.parse("foo.txt"),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
      timestamp: undefined,
      flag: openMode,
      newEntityId: asEntityId("019a2eb9-32ba-7d57-a39e-f35c32b1ac77"),
      oldEntityId: undefined,
      expect: undefined,
      currentNumParts: undefined,
      partSize: asPartSize(5e6),
      omnio: { closed: false },
      logger,
      metadata,
      lock,
      storage,
      hash: h,
    });

    await w.write("foo");

    expect(fs.tree()).toStrictEqual({
      "test": {
        "019a2eb9-32ba-7d57-a39e-f35c32b1ac77": {
          "1": {
            chunks: [],
          },
          "1.crswap": {
            chunks: [
              [102, 111, 111], // foo
            ],
          },
        },
      },
    });

    const error = new Error("test");
    await w.abort(error);

    await expect(w.close()).rejects.toThrow(error);
    await expect(w.abort()).rejects.toThrow(error);
    expect(lock.unlock).toHaveBeenCalledTimes(1);
    expect(w.closed).toBe(true);
    expect(fs.tree()).toStrictEqual({
      "test": {},
    });
    await expect(metadata.exists({ objectPath: ObjectPath.parse("foo.txt") }))
      .resolves
      .toStrictEqual({
        exists: false,
      });
  });
});

describe("'a' モード", () => {
  const openMode = asOpenMode("a");

  test("追記できる", async ({ expect, fs, storage, logger, metadata }) => {
    {
      const lock = {
        unlock: vi.fn(),
        [Symbol.dispose]: vi.fn(),
      } as unknown as AsyncmuxLock;
      const h = await md5.create();
      await using w = new ObjectFileWriteStream({
        currentSize: undefined,
        type: undefined,
        bucketName: asBucketName("test"),
        objectPath: ObjectPath.parse("foo.txt"),
        objectTags: undefined,
        description: undefined,
        userMetadata: undefined,
        timestamp: undefined,
        flag: openMode,
        newEntityId: asEntityId("019a2eb9-32ba-7d57-a39e-f35c32b1ac77"),
        oldEntityId: undefined,
        expect: undefined,
        currentNumParts: undefined,
        partSize: 7 as PartSize,
        omnio: { closed: false },
        logger,
        metadata,
        lock,
        storage,
        hash: h,
      });
      await w.write("foo");
      await w.write("bar");
      await w.close();

      expect(fs.tree()).toStrictEqual({
        "test": {
          "019a2eb9-32ba-7d57-a39e-f35c32b1ac77": {
            "1": {
              chunks: [
                [102, 111, 111], // foo
                [98, 97, 114], // bar
              ],
            },
          },
        },
      });
    }
    {
      const lock = {
        unlock: vi.fn(),
        [Symbol.dispose]: vi.fn(),
      } as unknown as AsyncmuxLock;
      const { state } = await md5.digest(
        new Uint8Array([
          ...[102, 111, 111], // foo
          ...[98, 97, 114], // bar
        ]),
      );
      const h = await md5.create(state);
      await using w = new ObjectFileWriteStream({
        currentSize: asObjectSize(6),
        type: undefined,
        bucketName: asBucketName("test"),
        objectPath: ObjectPath.parse("foo.txt"),
        objectTags: undefined,
        description: undefined,
        userMetadata: undefined,
        timestamp: undefined,
        flag: openMode,
        newEntityId: asEntityId("019a2eb9-32ba-7d57-a39e-f35c32b1ac77"),
        oldEntityId: undefined,
        expect: undefined,
        currentNumParts: asNumParts(1),
        partSize: 7 as PartSize,
        omnio: { closed: false },
        logger,
        metadata,
        lock,
        storage,
        hash: h,
      });

      expect(w.size).toBe(6);
      expect(w.bytesWritten).toBe(0);
      expect(fs.tree()).toStrictEqual({
        "test": {
          "019a2eb9-32ba-7d57-a39e-f35c32b1ac77": {
            "1": {
              chunks: [
                [102, 111, 111], // foo
                [98, 97, 114], // bar
              ],
            },
          },
        },
      });

      await w.write("baz");

      expect(w.size).toBe(9);
      expect(w.bytesWritten).toBe(3);
      expect(fs.tree()).toStrictEqual({
        "test": {
          "019a2eb9-32ba-7d57-a39e-f35c32b1ac77": {
            "1": {
              chunks: [
                [102, 111, 111], // foo
                [98, 97, 114], // bar
                [98], // b
              ],
            },
            "2": {
              chunks: [],
            },
            "2.crswap": {
              chunks: [
                [97, 122], // az
              ],
            },
          },
        },
      });

      await w.close();

      expect(fs.tree()).toStrictEqual({
        "test": {
          "019a2eb9-32ba-7d57-a39e-f35c32b1ac77": {
            "1": {
              chunks: [
                [102, 111, 111], // foo
                [98, 97, 114], // bar
                [98], // b
              ],
            },
            "2": {
              chunks: [
                [97, 122], // az
              ],
            },
          },
        },
      });
      expect(lock.unlock).toHaveBeenCalledTimes(1);
    }
  });
});

describe("'wx' モード", () => {
  const openMode = asOpenMode("wx");

  test("ストリームにデータを書き込める", async ({ expect, fs, storage, logger, metadata }) => {
    const lock = {
      unlock: vi.fn(),
      [Symbol.dispose]: vi.fn(),
    } as unknown as AsyncmuxLock;
    const h = await md5.create();
    await using w = new ObjectFileWriteStream({
      currentSize: undefined,
      type: undefined,
      bucketName: asBucketName("test"),
      objectPath: ObjectPath.parse("foo.txt"),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
      timestamp: undefined,
      flag: openMode,
      newEntityId: asEntityId("019a2eb9-32ba-7d57-a39e-f35c32b1ac77"),
      oldEntityId: undefined,
      expect: undefined,
      currentNumParts: undefined,
      partSize: asPartSize(5e6),
      omnio: { closed: false },
      logger,
      metadata,
      lock,
      storage,
      hash: h,
    });

    await w.write("foo");
    await w.close();

    expect(fs.tree()).toStrictEqual({
      "test": {
        "019a2eb9-32ba-7d57-a39e-f35c32b1ac77": {
          "1": {
            chunks: [
              [102, 111, 111], // foo
            ],
          },
        },
      },
    });
    await expect(metadata.exists({ objectPath: ObjectPath.parse("foo.txt") }))
      .resolves
      .toStrictEqual({
        exists: true,
      });
  });

  test("存在するパスに書き込もうとしてエラー", async ({ expect, storage, logger, metadata }) => {
    {
      const lock = {
        unlock: vi.fn(),
        [Symbol.dispose]: vi.fn(),
      } as unknown as AsyncmuxLock;
      const h = await md5.create();
      await using w = new ObjectFileWriteStream({
        currentSize: undefined,
        type: undefined,
        bucketName: asBucketName("test"),
        objectPath: ObjectPath.parse("foo.txt"),
        objectTags: undefined,
        description: undefined,
        userMetadata: undefined,
        timestamp: undefined,
        flag: openMode,
        newEntityId: asEntityId("019a2eb9-32ba-7d57-a39e-f35c32b1ac77"),
        oldEntityId: undefined,
        expect: undefined,
        currentNumParts: undefined,
        partSize: asPartSize(5e6),
        omnio: { closed: false },
        logger,
        metadata,
        lock,
        storage,
        hash: h,
      });

      await w.write("foo");
    }

    const lock = {
      unlock: vi.fn(),
      [Symbol.dispose]: vi.fn(),
    } as unknown as AsyncmuxLock;
    const h = await md5.create();
    await using w = new ObjectFileWriteStream({
      currentSize: undefined,
      type: undefined,
      bucketName: asBucketName("test"),
      objectPath: ObjectPath.parse("foo.txt"),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
      timestamp: undefined,
      flag: openMode,
      newEntityId: asEntityId("019a2eb9-32ba-7d57-a39e-f35c32b1ac77"),
      oldEntityId: undefined,
      expect: undefined,
      currentNumParts: undefined,
      partSize: asPartSize(5e6),
      omnio: { closed: false },
      logger,
      metadata,
      lock,
      storage,
      hash: h,
    });

    await w.write("foo");

    await expect(w.close())
      .rejects
      .toThrow(ObjectExistsError);
  });
});

describe("'ax' モード", () => {
  const openMode = asOpenMode("ax");

  test("ストリームにデータを書き込める", async ({ expect, fs, storage, logger, metadata }) => {
    const lock = {
      unlock: vi.fn(),
      [Symbol.dispose]: vi.fn(),
    } as unknown as AsyncmuxLock;
    const h = await md5.create();
    await using w = new ObjectFileWriteStream({
      currentSize: undefined,
      type: undefined,
      bucketName: asBucketName("test"),
      objectPath: ObjectPath.parse("foo.txt"),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
      timestamp: undefined,
      flag: openMode,
      newEntityId: asEntityId("019a2eb9-32ba-7d57-a39e-f35c32b1ac77"),
      oldEntityId: undefined,
      expect: undefined,
      currentNumParts: undefined,
      partSize: asPartSize(5e6),
      omnio: { closed: false },
      logger,
      metadata,
      lock,
      storage,
      hash: h,
    });

    await w.write("foo");
    await w.close();

    expect(fs.tree()).toStrictEqual({
      "test": {
        "019a2eb9-32ba-7d57-a39e-f35c32b1ac77": {
          "1": {
            chunks: [
              [102, 111, 111], // foo
            ],
          },
        },
      },
    });
    await expect(metadata.exists({ objectPath: ObjectPath.parse("foo.txt") }))
      .resolves
      .toStrictEqual({
        exists: true,
      });
  });

  test("存在するパスに書き込もうとしてエラー", async ({ expect, storage, logger, metadata }) => {
    {
      const lock = {
        unlock: vi.fn(),
        [Symbol.dispose]: vi.fn(),
      } as unknown as AsyncmuxLock;
      const h = await md5.create();
      await using w = new ObjectFileWriteStream({
        currentSize: undefined,
        type: undefined,
        bucketName: asBucketName("test"),
        objectPath: ObjectPath.parse("foo.txt"),
        objectTags: undefined,
        description: undefined,
        userMetadata: undefined,
        timestamp: undefined,
        flag: openMode,
        newEntityId: asEntityId("019a2eb9-32ba-7d57-a39e-f35c32b1ac77"),
        oldEntityId: undefined,
        expect: undefined,
        currentNumParts: undefined,
        partSize: asPartSize(5e6),
        omnio: { closed: false },
        logger,
        metadata,
        lock,
        storage,
        hash: h,
      });

      await w.write("foo");
    }

    const lock = {
      unlock: vi.fn(),
      [Symbol.dispose]: vi.fn(),
    } as unknown as AsyncmuxLock;
    const h = await md5.create();
    await using w = new ObjectFileWriteStream({
      currentSize: undefined,
      type: undefined,
      bucketName: asBucketName("test"),
      objectPath: ObjectPath.parse("foo.txt"),
      objectTags: undefined,
      description: undefined,
      userMetadata: undefined,
      timestamp: undefined,
      flag: openMode,
      newEntityId: asEntityId("019a2eb9-32ba-7d57-a39e-f35c32b1ac77"),
      oldEntityId: undefined,
      expect: undefined,
      currentNumParts: undefined,
      partSize: asPartSize(5e6),
      omnio: { closed: false },
      logger,
      metadata,
      lock,
      storage,
      hash: h,
    });

    await w.write("foo");

    await expect(w.close())
      .rejects
      .toThrow(ObjectExistsError);
  });
});

/***************************************************************************************************
 *
 * ユーティリティー
 *
 **************************************************************************************************/

const asUint = (x: number) => v.parse(UintSchema(), x);
const asEntityId = (x: string) => v.parse(EntityIdSchema(), x);
const asNumParts = (x: number) => v.parse(NumPartsSchema(), x);
const asOpenMode = (x: string) => v.parse(OpenModeSchema(), x);
const asPartSize = (x: number) => v.parse(PartSizeSchema(), x);
const asBucketName = (x: string) => v.parse(BucketNameSchema(), x);
const asObjectSize = (x: number) => v.parse(ObjectSizeSchema(), x);
