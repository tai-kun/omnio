import { test } from "vitest";
import mutex from "../src/mutex.js";

const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));

test("書き込みは直列", async ({ expect }) => {
  class Runner {
    private logs: string[];

    constructor(logs: string[]) {
      this.logs = logs;
    }

    async runWithoutMutex(ms: number, value: string) {
      await sleep(ms);
      this.logs.push(value);
      this.logs;
    }

    @mutex
    async runWithMutex(ms: number, value: string) {
      await sleep(ms);
      this.logs.push(value);
    }
  }

  const logs: string[] = [];
  const runner = new Runner(logs);
  // without mutex
  await Promise.all([
    runner.runWithoutMutex(600, "A"),
    runner.runWithoutMutex(300, "B"),
    runner.runWithoutMutex(0, "C"),
  ]);
  // with mutex
  await Promise.all([
    runner.runWithMutex(600, "A"),
    runner.runWithMutex(300, "B"),
    runner.runWithMutex(0, "C"),
  ]);

  expect(logs).toStrictEqual([
    // without mutex
    "C",
    "B",
    "A",
    // with mutex
    "A",
    "B",
    "C",
  ]);
});

test("読み取りは並行", async ({ expect }) => {
  class Runner {
    private logs: string[];

    constructor(logs: string[]) {
      this.logs = logs;
    }

    async runWithoutMutex(ms: number, value: string) {
      await sleep(ms);
      this.logs.push(value);
    }

    @mutex.readonly
    async runWithMutex(ms: number, value: string) {
      await sleep(ms);
      this.logs.push(value);
    }
  }

  const logs: string[] = [];
  const runner = new Runner(logs);
  // without mutex
  await Promise.all([
    runner.runWithoutMutex(600, "A"),
    runner.runWithoutMutex(300, "B"),
    runner.runWithoutMutex(0, "C"),
  ]);
  // with mutex
  await Promise.all([
    runner.runWithMutex(600, "A"),
    runner.runWithMutex(300, "B"),
    runner.runWithMutex(0, "C"),
  ]);

  expect(logs).toStrictEqual([
    // without mutex
    "C",
    "B",
    "A",
    // with mutex
    "C",
    "B",
    "A",
  ]);
});

test("直列と並行の組み合わせは直列", async ({ expect }) => {
  class Runner {
    private logs: string[];

    constructor(logs: string[]) {
      this.logs = logs;
    }

    @mutex
    async write(ms: number, value: string) {
      await sleep(ms);
      this.logs.push("W:" + value);
    }

    @mutex.readonly
    async read(ms: number, value: string) {
      await sleep(ms);
      this.logs.push("R:" + value);
    }
  }

  const logs: string[] = [];
  const runner = new Runner(logs);
  await Promise.all([
    runner.write(300, "A"),
    runner.write(0, "B"),
    runner.read(600, "A"),
    runner.read(300, "B"),
    runner.write(0, "C"),
    runner.read(0, "B"),
  ]);

  expect(logs).toStrictEqual([
    "W:A",
    "W:B",
    "R:B",
    "R:A",
    "W:C",
    "R:B",
  ]);
});
