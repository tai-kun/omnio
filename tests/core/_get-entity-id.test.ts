import { test } from "vitest";
import getEntityId from "../../src/core/_get-entity-id.js";
import { UUIDv7_REGEX } from "../../src/shared/valibot.js";

test("エンティティ ID は UUID v7 形式である", ({ expect }) => {
  expect(getEntityId()).toMatch(UUIDv7_REGEX);
});
