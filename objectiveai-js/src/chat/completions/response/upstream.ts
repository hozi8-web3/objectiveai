import z from "zod";
import { convert, type JSONSchema } from "../../../json_schema";

export const UpstreamSchema = z
  .enum(["unknown", "open_router", "claude_agent_sdk"])
  .describe("The upstream provider that served the request.");
export type Upstream = z.infer<typeof UpstreamSchema>;
export const UpstreamJsonSchema: JSONSchema = convert(UpstreamSchema);
