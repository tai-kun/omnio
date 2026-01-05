import * as duckdb from "@duckdb/duckdb-wasm";
import ehWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import mvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdbWasmEh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import duckdbWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import { describe, test as vitest } from "vitest";
import Omnio from "../../src/core/omnio.js";
import { OmnioClosedError } from "../../src/shared/errors.js";
import unreachable from "../../src/shared/unreachable.js";

const date = new Date();
let counter = 0;
const test = vitest.extend<{ omnio: Omnio }>({
  async omnio({}, use) {
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
      let omnio: Omnio;
      switch (__FILE_SYSTEM__) {
        case "memory": {
          const {
            default: setupOnBrowserIoMemoryDbDuckdbWasm,
          } = await import("../../src/envs/browser/setup/setup-on-browser-io-memory-db-wasm.js");
          omnio = new Omnio(
            setupOnBrowserIoMemoryDbDuckdbWasm(duckdbBundle, { bucketName: "test" }),
          );
          break;
        }

        case "opfs": {
          const {
            default: setupOnBrowserIoOpfsDbDuckdbWasm,
          } = await import("../../src/envs/browser/setup/setup-on-browser-io-opfs-db-wasm.js");
          omnio = new Omnio(setupOnBrowserIoOpfsDbDuckdbWasm(duckdbBundle, { bucketName: "test" }));
          break;
        }

        default:
          unreachable(__FILE_SYSTEM__ as never);
      }

      await omnio.open();
      await use(omnio);
      await omnio.close();
    } else {
      const {
        default: setupOnNodeIoMemoryDbDuckdbNodeNeo,
      } = await import("../../src/envs/node/setup/setup-on-node-io-memory-db-node-neo.js");
      let omnio: Omnio;
      switch (__FILE_SYSTEM__) {
        case "memory":
          omnio = new Omnio(setupOnNodeIoMemoryDbDuckdbNodeNeo({ bucketName: "test" }));
          break;

        case "local": {
          const { tmpdir } = await import("node:os");
          const { join } = await import("node:path");
          const {
            default: setupOnNodeIoLocalDbDuckdbNodeNeo,
          } = await import("../../src/envs/node/setup/setup-on-node-io-local-db-node-neo.js");
          omnio = new Omnio(setupOnNodeIoLocalDbDuckdbNodeNeo({
            rootDir: join(tmpdir(), "omnio-test", date.getTime().toString(36), String(++counter)),
            bucketName: "test",
          }));
          break;
        }

        default:
          unreachable(__FILE_SYSTEM__ as never);
      }

      await omnio.open();
      await use(omnio);
      await omnio.close();
    }
  },
});

describe("ライフサイクルと状態管理", () => {
  test("`.closed` が正しいシステム状態を反映する", async ({ expect, omnio }) => {
    expect(omnio.closed).toBe(false);

    await omnio.close();

    expect(omnio.closed).toBe(true);
  });

  test("closed 時に API メソッドを呼び出した場合、OmnioClosedError が投げられる", async ({ expect, omnio }) => {
    await omnio.close();

    await expect(omnio.getObject(""))
      .rejects
      .toThrow(OmnioClosedError);
  });
});

describe("作成", () => {
  test("オブジェクトを作成できる", async ({ expect, omnio }) => {
    await omnio.putObject("foo.txt", "foo");
    const file = await omnio.getObject("foo.txt");

    expect(file.bucketName).toBe("test");
    expect(file.objectPath.toString()).toBe("foo.txt");
    expect(file.type).toBe("text/plain");
    expect(file.size).toBe(3);
    await expect(file.text())
      .resolves
      .toBe("foo");
  });
});

describe.todo("読み取り");

describe.todo("更新");

describe.todo("削除");
