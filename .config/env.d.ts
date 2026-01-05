declare const __DEBUG__: boolean;
declare const __CLIENT__: boolean;
declare const __SERVER__: boolean;
declare const __FILE_SYSTEM__: "memory" | "local" | "opfs";

// @ts-ignore
declare module globalThis {
  interface ArrayConstructor {
    isArray(arg: readonly any[] | any): arg is readonly any[];
  }
}

declare module "*?url" {
  const src: string;
  export default src;
}
