import { test } from "vitest";
import { NodeDb } from "../src/db/node-db.js";
import { NodeFs } from "../src/fs/node-fs.js";
import { Omnio } from "../src/index.js";
import jsonify from "../src/jsonify.js";

test("READEM.md", async ({ expect }) => {
  const omnio = new Omnio({
    fs: new NodeFs(),
    db: new NodeDb(),
    bucketName: "example",
  });

  await omnio.open();

  await omnio.putObject("path/to/movie.mp4", new Uint8Array(), {
    description:
      "この動画では、新商品「○○」の紹介が行われている。冒頭では商品の外観や付属品が映し出され、"
      + "その後、主要な機能や特徴について説明が加えられている。",
    userMetadata: {
      src: "https://example.com/movie.mp4",
    },
  });

  {
    const object = await omnio.getObject("path/to/movie.mp4", {
      load: {
        userMetadata: true,
      },
    });

    expect(object instanceof File).toBe(true);
    expect(object.userMetadata).toStrictEqual({
      src: "https://example.com/movie.mp4",
    });
  }
  {
    const dirPath = ["path"];
    const query = "商品";
    const objectList = await omnio.searchObjects(dirPath, query, { recursive: true });
    const objects = await Array.fromAsync(objectList);

    // console.log(objects);
    expect(jsonify(objects)).toStrictEqual([
      {
        objectPath: "path/to/movie.mp4",
        description:
          "この動画では、新商品「○○」の紹介が行われている。冒頭では商品の外観や付属品が映し出され、"
          + "その後、主要な機能や特徴について説明が加えられている。",
        searchScore: expect.any(Number),
      },
    ]);
  }

  await omnio.close();
});
