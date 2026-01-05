import mime from "mime";
import { MimeType, MimeTypeSchema } from "../shared/schemas.js";
import * as v from "../shared/valibot.js";

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
): MimeType {
  return v.parse(MimeTypeSchema(), mime.getType(path) ?? defaultType);
}
