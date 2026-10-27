import { z } from "zod";
import { api, ApiError } from "./client";
import { operationSchema } from "./types";

const firstResultSchema = operationSchema.extend({
  secret: z.string().nullable().optional(),
});

export async function submitOperation(
  input: {
    path: string;
    method: "POST" | "PUT" | "PATCH" | "DELETE";
    body: object;
    id: string;
    beforeSubmit: (id: string) => void;
  },
  client = api,
) {
  input.beforeSubmit(input.id);
  const topicId = input.path.match(/^\/api\/v1\/topics\/([^/]+)/)?.at(1);
  const result = await client.request(input.path, firstResultSchema, {
    scope: topicId
      ? `topic:${decodeURIComponent(topicId)}`
      : input.path.split("/").at(3),
    method: input.method,
    body: { ...input.body, operationId: input.id },
  });
  if (result.id !== input.id)
    throw new ApiError("invalid", "operation_identity_mismatch");
  return result;
}
