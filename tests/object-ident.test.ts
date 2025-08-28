import { test } from "vitest";
import { type BucketName, ObjectIdent, ObjectPath } from "../src/index.js";

const bucketName = "my-bucket" as BucketName;

test("constructor() はプロパティに正しく値を設定すする", ({ expect }) => {
  const path = new ObjectPath("x/y/z.png");
  const fileIdent = new ObjectIdent({ bucketName, objectPath: path });

  expect(fileIdent.bucketName).toBe("my-bucket");
  expect(fileIdent.objectPath).toBe(path);
});

test("toJSON() は正しい JSON オブジェクトを返す", ({ expect }) => {
  const path = new ObjectPath("dir/file.txt");
  const fileIdent = new ObjectIdent({ bucketName, objectPath: path });

  expect(fileIdent.toJSON()).toStrictEqual({
    bucketName: "my-bucket",
    objectPath: path,
  });
  expect(JSON.parse(JSON.stringify(fileIdent))).toStrictEqual({
    bucketName: "my-bucket",
    objectPath: "dir/file.txt",
  });
});

test("toString() は 'バケット名:ファイルパス' の形式で返す", ({ expect }) => {
  const path = new ObjectPath("a/b/c.txt");
  const fileIdent = new ObjectIdent({ bucketName, objectPath: path });

  expect(`${fileIdent}`).toBe("my-bucket:a/b/c.txt");
});
