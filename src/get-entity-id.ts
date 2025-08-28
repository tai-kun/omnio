import { v4 as randomUUID } from "uuid";
import * as v from "valibot";
import * as schemas from "./schemas.js";

/**
 * 実際に保存されるファイルの識別子を生成します。この識別子は、ファイルの保存や取得に使用されます。
 *
 * @returns UUID v4 形式の、実際に保存されるファイルの識別子です。
 */
export default function getEntityId(): v.InferOutput<typeof schemas.EntityId> {
  return v.parse(schemas.EntityId, randomUUID());
}
