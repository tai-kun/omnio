import { beforeEach, test } from "vitest";
import jsonify from "../../src/shared/jsonify.js";

// テストスイートの前に、全てのカスタムマッピングをクリアする。
beforeEach(() => {
  jsonify.clear();
});

test("プリミティブ型の値を JSON に変換できる", ({ expect }) => {
  expect(jsonify("Hello")).toBe("Hello");
  expect(jsonify(123)).toBe(123);
  expect(jsonify(true)).toBe(true);
  expect(jsonify(null)).toBe(null);
});

test("`BigInt` を `Number` に変換できる", ({ expect }) => {
  expect(jsonify(123n)).toBe(123);
  expect(jsonify(-456n)).toBe(-456);
});

test("`Number.MAX_SAFE_INTEGER` を超える `BigInt` はエラーを投げる", ({ expect }) => {
  const largeBigInt = BigInt(Number.MAX_SAFE_INTEGER) + 1n;

  expect(() => jsonify(largeBigInt)).toThrow(RangeError);
});

test("`Number.MIN_SAFE_INTEGER` を下回る `BigInt` はエラーを投げる", ({ expect }) => {
  const smallBigInt = BigInt(Number.MIN_SAFE_INTEGER) - 1n;

  expect(() => jsonify(smallBigInt)).toThrow(RangeError);
});

test("通常のオブジェクトを JSON に変換できる", ({ expect }) => {
  const obj = {
    name: "田中",
    age: 30,
    isStudent: false,
  };

  expect(jsonify(obj)).toEqual(obj);
});

test("配列を JSON に変換できる", ({ expect }) => {
  const arr = [
    1,
    "foo",
    true,
    {
      id: 1,
    },
  ];

  expect(jsonify(arr)).toEqual(arr);
});

test("`Map` をオブジェクトに変換できる", ({ expect }) => {
  const map = new Map<any, any>([
    ["a", 1],
    ["b", "two"],
  ]);

  expect(jsonify(map)).toEqual({
    a: 1,
    b: "two",
  });
});

test("`Map` のキーと値も再帰的に変換される", ({ expect }) => {
  const mapWithBigInt = new Map([
    [1n, "value1"],
    [2n, "value2"],
  ]);

  expect(jsonify(mapWithBigInt)).toEqual({
    "1": "value1",
    "2": "value2",
  });
});

test("`Set` を配列に変換できる", ({ expect }) => {
  const set = new Set([1, 2, "three"]);

  expect(jsonify(set)).toEqual([1, 2, "three"]);
});

test("カスタムクラスを登録して JSON に変換できる", ({ expect }) => {
  // `register` を使ってカスタムクラスの変換を定義するテストである。
  class MyValueObject {
    value: string;

    constructor(value: string) {
      this.value = value;
    }
  }

  jsonify.register(MyValueObject, function() {
    return {
      custom: `custom_value_${this.value}`,
    };
  });

  const instance = new MyValueObject("test");

  expect(jsonify(instance)).toEqual({
    custom: "custom_value_test",
  });
});

test("複数のカスタムクラスを登録して変換できる", ({ expect }) => {
  class ClassA {
    a: number;

    constructor(a: number) {
      this.a = a;
    }
  }

  class ClassB {
    b: string;

    constructor(b: string) {
      this.b = b;
    }
  }

  jsonify.register(ClassA, function() {
    return `ClassA:${this.a}`;
  });
  jsonify.register(ClassB, function() {
    return `ClassB:${this.b}`;
  });

  const obj = {
    itemA: new ClassA(100),
    itemB: new ClassB("hello"),
  };

  expect(jsonify(obj)).toEqual({
    itemA: "ClassA:100",
    itemB: "ClassB:hello",
  });
});

test("ネストされたオブジェクト内のカスタムクラスを変換できる", ({ expect }) => {
  class NestedClass {
    id: number;

    constructor(id: number) {
      this.id = id;
    }
  }

  jsonify.register(NestedClass, function() {
    return `Nested_${this.id}`;
  });

  const obj = {
    data: {
      item: new NestedClass(99),
    },
  };

  expect(jsonify(obj)).toEqual({
    data: {
      item: "Nested_99",
    },
  });
});

test("`unregister` 関数で登録を解除できる", ({ expect }) => {
  class TempClass {
    value: string;

    constructor(value: string) {
      this.value = value;
    }
  }

  jsonify.register(TempClass, () => ({
    message: "registered",
  }));

  const obj1 = new TempClass("test1");

  expect(jsonify(obj1)).toEqual({
    message: "registered",
  });

  const result = jsonify.unregister(TempClass);

  expect(result).toBe(true);

  const obj2 = new TempClass("test2");

  expect(jsonify(obj2)).toEqual({
    value: "test2",
  });
});

test("存在しないクラスの登録解除は `false` を返す", ({ expect }) => {
  class NonExistentClass {}
  const result = jsonify.unregister(NonExistentClass);

  expect(result).toBe(false);
});

test("`clear` 関数で全ての登録を解除できる", ({ expect }) => {
  class ClassC {
    value: string;

    constructor(value: string) {
      this.value = value;
    }
  }

  class ClassD {
    value: string;

    constructor(value: string) {
      this.value = value;
    }
  }

  jsonify.register(ClassC, () => ({
    message: "ClassC registered",
  }));
  jsonify.register(ClassD, () => ({
    message: "ClassD registered",
  }));

  expect(jsonify(new ClassC("test"))).toEqual({
    message: "ClassC registered",
  });
  expect(jsonify(new ClassD("test"))).toEqual({
    message: "ClassD registered",
  });

  jsonify.clear();

  expect(jsonify(new ClassC("test"))).toEqual({
    value: "test",
  });
  expect(jsonify(new ClassD("test"))).toEqual({
    value: "test",
  });
});
