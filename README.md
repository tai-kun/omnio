# Omnio

**Omnio** は、メタデータと説明文を活用してファイルを管理・検索できる**メタデータ駆動型のオブジェクトストレージライブラリー**です。  

Node.js とブラウザーの両方で動作し、ファイル本体とそのメタデータを一貫した API で扱うことができます。

## ✨ 特徴

- 🧩 **メタデータ中心設計** — ファイルに構造化メタデータと説明文を付与可能  
- 🔍 **説明文検索** — 自然言語による検索が可能  
- 🗂️ **ストレージ抽象化** — ローカルFS・OPFS・独自バックエンドに対応  
- 🪶 **組み込みDB** — DuckDB（Node / WASM）を利用した高速検索  
- 🌐 **クロス環境対応** — Node.js とブラウザーの両方で同じコードが動作  
- 🧱 **拡張性の高い構成** — `Fs` や `Db` を独自実装に差し替え可能

## 🚀 インストール

```bash
npm install omnio
```

## 💡 使い方

### Node.js での例

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

const object = await omnio.getObject("path/to/movie.mp4", {
  load: {
    userMetadata: true,
  },
});

console.log(object instanceof File);  //-> true
console.log(object.userMetadata);  //-> { src: "https://example.com/movie.mp4" }

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

await omnio.close();
```

### ブラウザーでの例

[オリジンプライベートファイルシステム](https://developer.mozilla.org/docs/Web/API/File_System_API/Origin_private_file_system) (OPFS) を使って、ブラウザー上でデータを永続化できます。

```ts
import { Omnio } from "omnio";
import { WasmDb } from "omnio/db/wasm";
import { Opfs } from "omnio/fs/opfs";

const omnio = new Omnio({
  fs: new Opfs(),
  db: new WasmDb(),
  bucketName: "web-data",
});
```

### メモリー上のファイルシステム (Node.js)

データをローカルファイルに書き込まず、メモリー上で管理します。テストで使用することを想定しています。

```ts
import { Omnio } from "omnio";
import { WasmDb } from "omnio/db/wasm";
import { MemoryFs } from "omnio/fs/memory";

const omnio = new Omnio({
  fs: new MemoryFs(),
  db: new WasmDb(),
  bucketName: "test-data",
});
```
