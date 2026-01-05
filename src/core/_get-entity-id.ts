import { v7 } from "uuid";
import type { EntityId } from "../shared/schemas.js";

/**
 * 実際に保存されるファイルの識別子を生成します。この識別子は、ファイルの保存や取得に使用されます。
 *
 * @returns UUID v7 形式の、実際に保存されるファイルの識別子です。
 */
export default function getEntityId(): EntityId {
  return v7() as EntityId;
}
