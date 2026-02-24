import z from "zod";
import { convert, type JSONSchema } from "../../json_schema";

export const UpstreamSchema = z
  .enum(["unknown", "open_router", "claude_agent_sdk"])
  .describe("The upstream provider that served the request.")
  .meta({ title: "Upstream" });
export type Upstream = z.infer<typeof UpstreamSchema>;
export const UpstreamJsonSchema: JSONSchema = convert(UpstreamSchema);

export const UpstreamsSchema = z
  .array(UpstreamSchema)
  .optional()
  .nullable()
  .describe("Available upstreams for this request.")
  .meta({ title: "Upstreams" });
export type Upstreams = z.infer<typeof UpstreamsSchema>;
export const UpstreamsJsonSchema: JSONSchema = convert(UpstreamsSchema);
