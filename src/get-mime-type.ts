import mime from "mime";
import * as v from "valibot";
import * as schemas from "./schemas.js";

/**
 * ファイルパスから MIME タイプを取得します。
 *
 * @param path ファイルパスです。
 * @param defaultType MIME タイプが特定できない場合に返されるデフォルト値です。
 * デフォルトは `"application/octet-stream"` です。
 * @returns 取得した MIME タイプです。
 */
export default function getMimeType(
  path: string,
  defaultType: string | undefined = "application/octet-stream",
): v.InferOutput<typeof schemas.MimeType> {
  const ct = mime.getType(path) ?? defaultType;
  return v.parse(schemas.MimeType, ct);
}
