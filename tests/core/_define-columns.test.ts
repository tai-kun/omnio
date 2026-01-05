import * as v from "valibot";
import { test } from "vitest";
import defineColumns from "../../src/core/_define-columns.js";

test("単一の標準カラムが正しく定義されること", ({ expect }) => {
  const schema = v.string();
  const columns = defineColumns({
    userName: ["user_name", schema],
  });
  const column = columns[0]!;

  expect(columns).toHaveLength(1);
  expect(column.key).toBe("userName");
  expect(column.schema).toBe(schema);

  const sqlNoTable = column.build();

  expect(sqlNoTable.text).toBe(`user_name AS "userName"`);
  expect(sqlNoTable.values).toEqual([]);

  const sqlWithTable = column.build("u");

  expect(sqlWithTable.text).toBe(`u.user_name AS "userName"`);
  expect(sqlWithTable.values).toEqual([]);
});

test("Timestamp 型のカラムが正しく定義されること", ({ expect }) => {
  const columns = defineColumns({
    createdAt: ["created_at", "Timestamp"],
  });
  const column = columns[0]!;

  expect(columns).toHaveLength(1);
  expect(column.key).toBe("createdAt");

  const sqlNoTable = column.build();

  expect(sqlNoTable.text).toBe(`(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS "createdAt"`);
  expect(sqlNoTable.values).toEqual([]);

  const sqlWithTable = column.build("t");

  expect(sqlWithTable.text)
    .toBe(`(EXTRACT(EPOCH FROM t.created_at) * 1000)::BIGINT AS "createdAt"`);
  expect(sqlWithTable.values).toEqual([]);
});
