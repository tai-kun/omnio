# Omnio

データをメタデータと一緒に管理するライブラリです。Node.js と ブラウザーで動きます。

メタデータにはユーザー定義の構造化された値以外に説明文も含めることができ、説明文を対象に検索できます。メタデータの管理には DuckDB を使用しています。

データの実体は Node.js ならローカル、ブラウザなら OPFS に保存します。自作すれば保存先を任意のストレージ (S3 など)　にすることができます。

Node.js: 

```ts
import { Omnio } from "omnio";
import { NodeDb } from "omnio/db/node";
import { NodeFs } from "omnio/fs/node";

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

  console.log(object instanceof File);  //-> true
  console.log(object.userMetadata);  //-> { src: "https://example.com/movie.mp4" }
}
{
  const query = "商品";
  const objectList = await omnio.searchObjects(query, { recursive: true });
  const objects = await Array.fromAsync(objectList);

  console.log(objects);
  //-> [
  //   {
  //     objectPath: "path/to/movie.mp4",
  //     description: "この動画では、新商品「○○」の紹介が行われている。冒頭では商品の外観や付属品が映し出され、その後、主要な機能や特徴について説明が加えられている。",
  //     searchScore: 0.3681230632208837
  //   }
  // ]
}

await omnio.close();
```

ブラウザー:

```ts
import { Omnio } from "omnio";
import { WasmDb } from "omnio/db/wasm";
import { Opfs } from "omnio/fs/opfs";
```
