import { describe, test } from "vitest";
import md5 from "../../src/core/_md5.js";

describe("digest", () => {
  test("同じデータに対しては常に同じ MD5 値を返す", async ({ expect }) => {
    const data = new TextEncoder().encode("こんにちは");

    const digest1 = await md5.digest(data);
    const digest2 = await md5.digest(data);

    expect(digest1.value).toBe(digest2.value);
    expect(digest1.value).toMatch(/^[0-9a-f]{32}$/);
    expect(digest1.state).toStrictEqual(digest2.state);
    expect(digest1.state).toBeInstanceOf(Array);
  });

  test("異なるデータに対しては異なる MD5 値を返す", async ({ expect }) => {
    const data1 = new TextEncoder().encode("foo");
    const data2 = new TextEncoder().encode("bar");

    const digest1 = await md5.digest(data1);
    const digest2 = await md5.digest(data2);

    expect(digest1.value).not.toBe(digest2.value);
    expect(digest1.value).toMatch(/^[0-9a-f]{32}$/);
    expect(digest2.value).toMatch(/^[0-9a-f]{32}$/);
  });

  test("長さの異なるゼロ値ではそれぞれ異なる MD5 値を返す", async ({ expect }) => {
    const data0 = new Uint8Array([]);
    const data1 = new Uint8Array([0]);
    const data2 = new Uint8Array([0, 0]);

    const digest0 = await md5.digest(data0);
    const digest1 = await md5.digest(data1);
    const digest2 = await md5.digest(data2);

    expect(digest0.value).not.toBe(digest1.value);
    expect(digest0.value).not.toBe(digest2.value);
    expect(digest1.value).not.toBe(digest2.value);
  });
});

describe("create", () => {
  test("ストリームで複数回 update しても一括 digest の結果と一致する", async ({ expect }) => {
    const part1 = new TextEncoder().encode("Hello, ");
    const part2 = new TextEncoder().encode("world!");
    const all = new TextEncoder().encode("Hello, world!");

    const hashStream = await md5.create();
    hashStream.update(part1);
    hashStream.update(part2);
    const streamedDigest = hashStream.digest();
    const directDigest = await md5.digest(all);

    expect(streamedDigest.value).toBe(directDigest.value);
    expect(streamedDigest.value).toMatch(/^[0-9a-f]{32}$/);
  });

  test("空データに対する digest が期待通りのハッシュになる", async ({ expect }) => {
    const empty = new Uint8Array();
    const h = await md5.digest(empty);
    const expectedMD5 = "d41d8cd98f00b204e9800998ecf8427e"; // MD5("") の既知値

    expect(h.value).toBe(expectedMD5);
  });

  test("ハッシュの計算を途中から再開できる", async ({ expect }) => {
    const part1 = new TextEncoder().encode("Hello, ");
    const part2 = new TextEncoder().encode("world!");
    const all = new TextEncoder().encode("Hello, world!");

    let state: readonly number[];
    {
      const hashStream = await md5.create();
      hashStream.update(part1);
      const streamedDigest = hashStream.digest();
      state = streamedDigest.state;
    }

    let value: string;
    {
      const hashStream = await md5.create(state);
      hashStream.update(part2);
      const streamedDigest = hashStream.digest();
      value = streamedDigest.value;
    }

    const directDigest = await md5.digest(all);

    expect(value).toBe(directDigest.value);
  });
});
