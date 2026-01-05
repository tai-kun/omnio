import type { AsyncmuxLock } from "asyncmux";
import { test, vi } from "vitest";
import ObjectFileReadStream from "../../src/core/object-file-read-stream.js";
import MemoryFileSystem from "../../src/envs/shared/file-system/memory-file-system.js";
import utf8 from "../../src/shared/_utf8.js";
import ObjectPath from "../../src/shared/object-path.js";
import {
  BucketNameSchema,
  ChecksumSchema,
  MimeTypeSchema,
  NumPartsSchema,
  ObjectIdSchema,
  ObjectSizeSchema,
  TimestampSchema,
} from "../../src/shared/schemas.js";
import * as v from "../../src/shared/valibot.js";

test("オブジェクトをパート毎に読み込める", async ({ expect }) => {
  const fs = new MemoryFileSystem();
  fs.open();
  const storage = fs.getDirectoryHandle("test", { create: true });
  {
    const f = storage.getDirectoryHandle("019a2eb9-32ba-7d57-a39e-f35c32b1ac77", { create: true });
    const w1 = await f.getFileHandle("1", { create: true })
      .createWritable({ keepExistingData: false });
    w1.write(utf8.encode("foo"));
    w1.write(utf8.encode("bar"));
    w1.close();
    const w2 = await f.getFileHandle("2", { create: true })
      .createWritable({ keepExistingData: false });
    w2.write(utf8.encode("baz"));
    w2.close();

    expect(fs.tree()).toStrictEqual({
      "test": {
        "019a2eb9-32ba-7d57-a39e-f35c32b1ac77": {
          "1": {
            chunks: [
              [102, 111, 111], // foo
              [98, 97, 114], // bar
            ],
          },
          "2": {
            chunks: [
              [98, 97, 122], // baz
            ],
          },
        },
      },
    });
  }

  const lock = {
    unlock: vi.fn(),
    [Symbol.dispose]: vi.fn(),
  } as unknown as AsyncmuxLock;
  using r = new ObjectFileReadStream({
    objectId: asObjectId("019a2eb5-8e4d-79ad-a8d6-237f3d46a5d7"),
    size: asObjectSize(9),
    type: asMimeType("text/plain"),
    lastModified: asTimestamp("2025-10-30T10:00:00.000Z"),
    bucketName: asBucketName("test"),
    objectPath: ObjectPath.parse("foo.txt"),
    checksum: asChecksum("6df23dc03f9b54cc38a0fc1483df6e21"),
    numParts: asNumParts(2),
    omnio: { closed: false },
    lock,
    entityHandle: storage.getDirectoryHandle("019a2eb9-32ba-7d57-a39e-f35c32b1ac77", {
      create: false,
    }),
    objectTags: undefined,
    description: undefined,
    userMetadata: null,
  });

  expect(r.objectId).toBe("019a2eb5-8e4d-79ad-a8d6-237f3d46a5d7");
  expect(r.size).toBe(9);
  expect(r.type).toBe("text/plain");
  expect(r.lastModified).toBe(Date.parse("2025-10-30T10:00:00.000Z"));
  expect(r.bucketName).toBe("test");
  expect(r.objectPath.toString()).toBe("foo.txt");
  expect(r.checksum).toBe("6df23dc03f9b54cc38a0fc1483df6e21");
  expect(r.numParts).toBe(2);
  await expect(Array.fromAsync(r))
    .resolves
    .toStrictEqual([
      new Uint8Array([
        ...[102, 111, 111], // foo
        ...[98, 97, 114], // bar
      ]),
      new Uint8Array([
        ...[98, 97, 122], // baz
      ]),
    ]);

  fs.close();
});

/***************************************************************************************************
 *
 * ユーティリティー
 *
 **************************************************************************************************/

const asNumParts = (x: number) => v.parse(NumPartsSchema(), x);
const asObjectId = (x: string) => v.parse(ObjectIdSchema(), x);
const asMimeType = (x: string) => v.parse(MimeTypeSchema(), x);
const asChecksum = (x: string) => v.parse(ChecksumSchema(), x);
const asTimestamp = (x: string) => v.parse(TimestampSchema(), x);
const asBucketName = (x: string) => v.parse(BucketNameSchema(), x);
const asObjectSize = (x: number) => v.parse(ObjectSizeSchema(), x);
