import type { ErrorMeta } from "i18n-error-base";
import { tryCaptureStackTrace } from "try-capture-stack-trace";
import {
  type InferOutput,
  rawCheck,
  type RawCheckAction,
  rawTransform,
  type RawTransformAction,
  safeParse,
} from "valibot";
import {
  InvalidInputError,
  type Issue,
  UnexpectedValidationError,
  type ValidationErrorBase,
} from "./errors.js";
import isError from "./is-error.js";

/***************************************************************************************************
 *
 * 共通の型
 *
 **************************************************************************************************/

/**
 * Valibot のスキーマ型を抽出するための基本型です。
 */
type BaseSchema = typeof safeParse extends (schema: infer TSchema, ...args: any) => any ? TSchema
  : never;

/***************************************************************************************************
 *
 * 再エクスポート
 *
 **************************************************************************************************/

export {
  array,
  boolean,
  brand,
  finite,
  instance,
  literal,
  maxLength,
  maxValue,
  minLength,
  minValue,
  notValue,
  nullable,
  number,
  object,
  optional,
  pipe,
  readonly,
  regex,
  safeInteger,
  string,
  trim,
  union,
  unknown,
} from "valibot";
export type { Brand, InferInput, InferOutput, ObjectEntries } from "valibot";

export * from "./valibot-extra.js";

/***************************************************************************************************
 *
 * check
 *
 **************************************************************************************************/

export function check<TInput>(operation: (input: unknown) => boolean): RawCheckAction<TInput> {
  return rawCheck<TInput>(({ dataset, addIssue }) => {
    const input = dataset.value;
    try {
      return operation(input);
    } catch (ex) {
      let message: string;
      if (isError(ex)) {
        message = `${ex.name}: ${ex.message}`;
      } else {
        try {
          message = JSON.stringify(ex);
        } catch {
          message = String(ex);
        }
      }

      addIssue({
        input,
        message,
      });

      return false;
    }
  });
}

/***************************************************************************************************
 *
 * transform
 *
 **************************************************************************************************/

export function transform<TInput, TOutput>(
  operation: (input: TInput) => TOutput,
): RawTransformAction<TInput, TOutput> {
  return rawTransform<TInput, TOutput>(({ dataset, addIssue, NEVER }) => {
    const input = dataset.value;
    try {
      return operation(input);
    } catch (ex) {
      let message: string;
      if (isError(ex)) {
        message = `${ex.name}: ${ex.message}`;
      } else {
        try {
          message = JSON.stringify(ex);
        } catch {
          message = String(ex);
        }
      }

      addIssue({
        input,
        message,
      });

      return NEVER;
    }
  });
}

/***************************************************************************************************
 *
 * parse
 *
 **************************************************************************************************/

interface IParseError {
  new(issues: [Issue, ...Issue[]], input: unknown): ValidationErrorBase<ErrorMeta>;
}

export function parse<const TSchema extends BaseSchema>(
  schema: TSchema,
  input: unknown,
  Error: IParseError = InvalidInputError,
): InferOutput<TSchema> {
  const result = safeParse(schema, input);
  if (result.success) {
    return result.output;
  }

  const error = new Error(result.issues, input);
  tryCaptureStackTrace(error, parse);
  throw error;
}

export function expect<const TSchema extends BaseSchema>(
  schema: TSchema,
  input: unknown,
): InferOutput<TSchema> {
  const result = safeParse(schema, input);
  if (result.success) {
    return result.output;
  }

  const error = new UnexpectedValidationError(result.issues, input);
  tryCaptureStackTrace(error, expect);
  throw error;
}
