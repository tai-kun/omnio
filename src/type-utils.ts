/**
 * エディターにおけるオブジェクトの型表示をきれいにします。
 *
 * @template T オブジェクトの型です。
 */
type $Simplify<T> = { [P in keyof T]: T[P] } & {};

/**
 * T または PromiseLike<T> の型を定義します。これは、非同期処理を扱う場合に便利です。
 *
 * @template T 解決される値の型です。
 */
export type Awaitable<T> = T | PromiseLike<T>;

/**
 * オブジェクトのプロパティーの型を取得します。
 *
 * @template TObject 対象のオブジェクトです。
 * @template TProperty プロパティーのキーです。
 * @template TNotSet プロパティーが存在しない場合の型です。
 * @returns 指定されたプロパティーの型、またはプロパティーが存在しない場合は TNotSet の型です。
 */
export type $Get<
  TObject,
  TProperty extends keyof any,
  TNotSet = undefined,
> = TObject extends { readonly [_ in TProperty]: infer V } ? V : TNotSet;

/**
 * オブジェクトから、指定したプロパティーのみを選択した型を生成します。
 *
 * @template TObject 対象のオブジェクトです。
 * @template TSelect 行データから選択するプロパティーを、真偽値で指定したオブジェクトです。
 * @returns 指定されたプロパティーのみを持つ新しい型、または TSelect が undefined の場合は TObject の型です。
 */
export type $Select<
  TObject,
  TSelect,
> = TSelect extends undefined ? TObject : $Simplify<
  & {
    [
      P in {
        [P in keyof TSelect]: TSelect[P] extends true ? P : never;
      }[keyof TSelect & keyof TObject]
    ]-?: TObject[P];
  }
  & {
    [
      P in {
        [P in keyof TSelect]: (boolean | undefined) extends TSelect[P] ? P : never;
      }[keyof TSelect & keyof TObject]
    ]+?: TObject[P];
  }
>;
