declare const __DEBUG__: boolean;
declare const __CLIENT__: boolean;
declare const __SERVER__: boolean;

// @ts-ignore
declare module globalThis {
  interface ArrayConstructor {
    isArray(arg: readonly any[] | any): arg is readonly any[];
  }
}
