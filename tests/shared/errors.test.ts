import { setGlobalConfig } from "valibot";
import { beforeEach, describe, test } from "vitest";
import { TypeError } from "../../src/shared/errors.js";

beforeEach(() => {
  setGlobalConfig({ lang: "en" });
});

describe("TypeError", () => {
  test("globalThis.Error を継承している", ({ expect }) => {
    expect(new TypeError("string", 1)).toBeInstanceOf(globalThis.Error);
  });

  test("言語別にメッセージが変わる", ({ expect }) => {
    expect(new TypeError("string", 1).message).toBe("Expected string, but got number");

    setGlobalConfig({ lang: "ja" });

    expect(new TypeError("string", 1).message).toBe("string を期待しましたが、number を得ました");
  });
});
