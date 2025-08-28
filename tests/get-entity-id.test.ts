import { UUID_REGEX } from "valibot";
import { test } from "vitest";
import getEntityId from "../src/get-entity-id.js";

test("エンティティ ID は UUID 形式である", ({ expect }) => {
  expect(getEntityId()).toMatch(UUID_REGEX);
});
