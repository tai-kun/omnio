import {
  _addIssue as addIssue,
  type BaseIssue,
  type BaseValidation,
  type ErrorMessage,
  setSpecificMessage,
} from "valibot";
import singleton from "./_singleton.js";
import utf8_ from "./_utf8.js";
import quoteString from "./quote-string.js";

/***************************************************************************************************
 *
 * uuidv7
 *
 **************************************************************************************************/

export const UUIDv7_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;

export interface Uuidv7Issue<TInput extends string> extends BaseIssue<TInput> {
  readonly kind: "validation";
  readonly type: "uuidv7";
  readonly expected: null;
  readonly received: `"${string}"`;
  readonly requirement: RegExp;
}

export interface Uuidv7Action<
  TInput extends string,
  TMessage extends ErrorMessage<Uuidv7Issue<TInput>> | undefined,
> extends BaseValidation<TInput, TInput, Uuidv7Issue<TInput>> {
  readonly type: "uuidv7";
  readonly reference: typeof uuidv7;
  readonly expects: null;
  readonly requirement: RegExp;
  readonly message: TMessage;
}

export function uuidv7<TInput extends string>(): Uuidv7Action<TInput, undefined>;

export function uuidv7<
  TInput extends string,
  const TMessage extends ErrorMessage<Uuidv7Issue<TInput>> | undefined,
>(message: TMessage): Uuidv7Action<TInput, TMessage>;

export function uuidv7(
  message?: ErrorMessage<Uuidv7Issue<string>>,
): Uuidv7Action<string, ErrorMessage<Uuidv7Issue<string>> | undefined> {
  return {
    kind: "validation",
    type: "uuidv7",
    reference: uuidv7,
    async: false,
    expects: null,
    requirement: UUIDv7_REGEX,
    message,
    "~run"(dataset, config) {
      if (dataset.typed && !this.requirement.test(dataset.value)) {
        addIssue(this, "uuidv7", dataset, config);
      }

      return dataset;
    },
  };
}

/*#__PURE__*/ setSpecificMessage(
  uuidv7,
  issue => `Invalid UUID v7: Received ${issue.received}`,
  "en",
);
/*#__PURE__*/ setSpecificMessage(
  uuidv7,
  issue => `無効な UUID v7: ${issue.received} を受け取りました`,
  "ja",
);

/***************************************************************************************************
 *
 * utf8
 *
 **************************************************************************************************/

const getUtf8ByteCountCache = () => (singleton("valibot__utf8_byte_count_cache", () => ({
  value: "",
  count: 0,
  timer: null as ReturnType<typeof setTimeout> | null,
})));

function cacheUtf8ByteCount(value: string, bytesCount: number): void {
  const cache = getUtf8ByteCountCache();
  if (cache.timer !== null) {
    clearTimeout(cache.timer);
  }

  const TTL = 1e3;
  cache.value = value;
  cache.count = bytesCount;
  cache.timer = setTimeout(() => {
    cache.value = "";
    cache.count = 0;
    cache.timer = null;
  }, TTL);
}

function getByteCount(input: string): number {
  if (input === "") {
    return 0;
  }

  const cache = getUtf8ByteCountCache();
  if (cache.value === input) {
    return cache.count;
  }

  const bytesCount = utf8_.encode(input).length;
  cacheUtf8ByteCount(input, bytesCount);

  return bytesCount;
}

export interface Utf8Issue<TInput extends string> extends BaseIssue<TInput> {
  readonly kind: "validation";
  readonly type: "utf8";
  readonly expected: string;
  readonly received: string;
}

export interface Utf8Action<
  TInput extends string,
  TMessage extends ErrorMessage<Utf8Issue<TInput>> | undefined,
> extends BaseValidation<TInput, TInput, Utf8Issue<TInput>> {
  readonly type: "utf8";
  readonly reference: typeof utf8;
  readonly expects: null;
  readonly message: TMessage;
}

export function utf8<TInput extends string>(): Utf8Action<TInput, undefined>;

export function utf8<
  TInput extends string,
  const TMessage extends ErrorMessage<Utf8Issue<TInput>> | undefined,
>(message: TMessage): Utf8Action<TInput, TMessage>;

export function utf8(
  message?: ErrorMessage<Utf8Issue<string>>,
): Utf8Action<string, ErrorMessage<Utf8Issue<string>> | undefined> {
  return {
    kind: "validation",
    type: "utf8",
    reference: utf8,
    async: false,
    expects: null,
    message,
    "~run"(dataset, config) {
      if (dataset.typed) {
        const input = utf8_.encode(dataset.value);
        cacheUtf8ByteCount(dataset.value, input.length);
        if (!utf8_.isValidUtf8(input)) {
          addIssue(this, "utf8", dataset, config);
        }
      }

      return dataset;
    },
  };
}

/*#__PURE__*/ setSpecificMessage(
  utf8,
  issue => `Invalid UTF-8: Received ${issue.received}`,
  "en",
);
/*#__PURE__*/ setSpecificMessage(
  utf8,
  issue => `無効な UTF-8: ${issue.received} を受け取りました`,
  "ja",
);

/***************************************************************************************************
 *
 * maxBytes
 *
 **************************************************************************************************/

export interface MaxBytesIssue<
  TInput extends string,
  TRequirement extends number,
> extends BaseIssue<TInput> {
  readonly kind: "validation";
  readonly type: "max_bytes";
  readonly expected: `<=${TRequirement}`;
  readonly received: `${number}`;
  readonly requirement: TRequirement;
}

export interface MaxBytesAction<
  TInput extends string,
  TRequirement extends number,
  TMessage extends ErrorMessage<MaxBytesIssue<TInput, TRequirement>> | undefined,
> extends BaseValidation<TInput, TInput, MaxBytesIssue<TInput, TRequirement>> {
  readonly type: "max_bytes";
  readonly reference: typeof maxBytes;
  readonly expects: `<=${TRequirement}`;
  readonly requirement: TRequirement;
  readonly message: TMessage;
}

export function maxBytes<
  TInput extends string,
  const TRequirement extends number,
>(requirement: TRequirement): MaxBytesAction<TInput, TRequirement, undefined>;

export function maxBytes<
  TInput extends string,
  const TRequirement extends number,
  const TMessage extends ErrorMessage<MaxBytesIssue<TInput, TRequirement>> | undefined,
>(requirement: TRequirement, message: TMessage): MaxBytesAction<TInput, TRequirement, TMessage>;

export function maxBytes(
  requirement: number,
  message?: ErrorMessage<MaxBytesIssue<string, number>>,
): MaxBytesAction<string, number, ErrorMessage<MaxBytesIssue<string, number>> | undefined> {
  return {
    kind: "validation",
    type: "max_bytes",
    reference: maxBytes,
    async: false,
    expects: `<=${requirement}`,
    message,
    requirement,
    "~run"(dataset, config) {
      if (dataset.typed) {
        const length = getByteCount(dataset.value);
        if (length > this.requirement) {
          addIssue(this, "bytes", dataset, config, {
            received: `${length}`,
          });
        }
      }

      return dataset;
    },
  };
}

/*#__PURE__*/ setSpecificMessage(
  maxBytes,
  issue => `Invalid bytes: Expected ${issue.expected} but received ${issue.received}`,
  "en",
);
/*#__PURE__*/ setSpecificMessage(
  maxBytes,
  issue => `無効な bytes: ${issue.expected} を期待しましたが ${issue.received} を受け取りました`,
  "ja",
);

/***************************************************************************************************
 *
 * notStartsWith
 *
 **************************************************************************************************/

export interface NotStartsWithIssue<
  TInput extends string,
  TRequirement extends string,
> extends BaseIssue<TInput> {
  readonly kind: "validation";
  readonly type: "not_starts_with";
  readonly expected: `"${TRequirement}"`;
  readonly requirement: TRequirement;
}

export interface NotStartsWithAction<
  TInput extends string,
  TRequirement extends string,
  TMessage extends ErrorMessage<NotStartsWithIssue<TInput, TRequirement>> | undefined,
> extends BaseValidation<TInput, TInput, NotStartsWithIssue<TInput, TRequirement>> {
  readonly type: "not_starts_with";
  readonly reference: typeof notStartsWith;
  readonly expects: TRequirement;
  readonly requirement: TRequirement;
  readonly message: TMessage;
}

export function notStartsWith<
  TInput extends string,
  const TRequirement extends string,
>(requirement: TRequirement): NotStartsWithAction<TInput, TRequirement, undefined>;

export function notStartsWith<
  TInput extends string,
  const TRequirement extends string,
  const TMessage extends ErrorMessage<NotStartsWithIssue<TInput, TRequirement>> | undefined,
>(
  requirement: TRequirement,
  message: TMessage,
): NotStartsWithAction<TInput, TRequirement, TMessage>;

export function notStartsWith(
  requirement: string,
  message?: ErrorMessage<NotStartsWithIssue<string, string>>,
): NotStartsWithAction<
  string,
  string,
  ErrorMessage<NotStartsWithIssue<string, string>> | undefined
> {
  return {
    kind: "validation",
    type: "not_starts_with",
    reference: notStartsWith,
    async: false,
    expects: quoteString(requirement),
    message,
    requirement,
    "~run"(dataset, config) {
      if (dataset.typed && dataset.value.startsWith(this.requirement)) {
        addIssue(this, "start", dataset, config);
      }

      return dataset;
    },
  };
}

/*#__PURE__*/ setSpecificMessage(
  notStartsWith,
  issue => `Invalid start: Received ${issue.expected}`,
  "en",
);
/*#__PURE__*/ setSpecificMessage(
  notStartsWith,
  issue => `無効な先頭: ${issue.expected} を受け取りました`,
  "ja",
);

/***************************************************************************************************
 *
 * notEndsWith
 *
 **************************************************************************************************/

export interface NotEndsWithIssue<
  TInput extends string,
  TRequirement extends string,
> extends BaseIssue<TInput> {
  readonly kind: "validation";
  readonly type: "not_ends_with";
  readonly expected: `"${TRequirement}"`;
  readonly requirement: TRequirement;
}

export interface NotEndsWithAction<
  TInput extends string,
  TRequirement extends string,
  TMessage extends ErrorMessage<NotEndsWithIssue<TInput, TRequirement>> | undefined,
> extends BaseValidation<TInput, TInput, NotEndsWithIssue<TInput, TRequirement>> {
  readonly type: "not_ends_with";
  readonly reference: typeof notEndsWith;
  readonly expects: TRequirement;
  readonly requirement: TRequirement;
  readonly message: TMessage;
}

export function notEndsWith<
  TInput extends string,
  const TRequirement extends string,
>(requirement: TRequirement): NotEndsWithAction<TInput, TRequirement, undefined>;

export function notEndsWith<
  TInput extends string,
  const TRequirement extends string,
  const TMessage extends ErrorMessage<NotEndsWithIssue<TInput, TRequirement>> | undefined,
>(
  requirement: TRequirement,
  message: TMessage,
): NotEndsWithAction<TInput, TRequirement, TMessage>;

export function notEndsWith(
  requirement: string,
  message?: ErrorMessage<NotEndsWithIssue<string, string>>,
): NotEndsWithAction<
  string,
  string,
  ErrorMessage<NotEndsWithIssue<string, string>> | undefined
> {
  return {
    kind: "validation",
    type: "not_ends_with",
    reference: notEndsWith,
    async: false,
    expects: quoteString(requirement),
    message,
    requirement,
    "~run"(dataset, config) {
      if (dataset.typed && dataset.value.endsWith(this.requirement)) {
        addIssue(this, "start", dataset, config);
      }

      return dataset;
    },
  };
}

/*#__PURE__*/ setSpecificMessage(
  notEndsWith,
  issue => `Invalid end: Received ${issue.expected}`,
  "en",
);
/*#__PURE__*/ setSpecificMessage(
  notEndsWith,
  issue => `無効な末尾: ${issue.expected} を受け取りました`,
  "ja",
);

/***************************************************************************************************
 *
 * notMatch
 *
 **************************************************************************************************/

export interface NotMatchIssue<TInput extends string> extends BaseIssue<TInput> {
  readonly kind: "validation";
  readonly type: "notMatch";
  readonly expected: string;
  readonly received: `"${string}"`;
  readonly requirement: RegExp;
}

export interface NotMatchAction<
  TInput extends string,
  TMessage extends ErrorMessage<NotMatchIssue<TInput>> | undefined,
> extends BaseValidation<TInput, TInput, NotMatchIssue<TInput>> {
  readonly type: "notMatch";
  readonly reference: typeof notMatch;
  readonly expects: string;
  readonly requirement: RegExp;
  readonly message: TMessage;
}

export function notMatch<TInput extends string>(
  requirement: RegExp,
): NotMatchAction<TInput, undefined>;

export function notMatch<
  TInput extends string,
  const TMessage extends ErrorMessage<NotMatchIssue<TInput>> | undefined,
>(requirement: RegExp, message: TMessage): NotMatchAction<TInput, TMessage>;

export function notMatch(
  requirement: RegExp,
  message?: ErrorMessage<NotMatchIssue<string>>,
): NotMatchAction<string, ErrorMessage<NotMatchIssue<string>> | undefined> {
  return {
    kind: "validation",
    type: "notMatch",
    reference: notMatch,
    async: false,
    expects: `${requirement}`,
    requirement,
    message,
    "~run"(dataset, config) {
      if (dataset.typed && this.requirement.test(dataset.value)) {
        addIssue(this, "format", dataset, config);
      }

      return dataset;
    },
  };
}

/*#__PURE__*/ setSpecificMessage(
  notMatch,
  issue => `Invalid format: Expected not ${issue.expected} but received ${issue.received}`,
  "en",
);
/*#__PURE__*/ setSpecificMessage(
  notMatch,
  issue =>
    `無効なフォーマット: ${issue.expected} ではない形式を期待しましたが、 ${issue.received} を受け取りました`,
  "ja",
);
