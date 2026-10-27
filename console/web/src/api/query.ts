import { useQuery } from "@tanstack/react-query";
import type { ZodType } from "zod";
import { api, ApiError } from "./client";
import { useSession } from "./session";

export function useResource<T>(
  scope: string,
  path: string,
  schema: ZodType<T>,
  polling = false,
  allowed = true,
  parameters: Record<string, string | undefined> = {},
) {
  const session = useSession();
  const query = useQuery({
    queryKey: ["protected", session.epoch, scope, path, parameters],
    queryFn: ({ signal }) => {
      if (!session.session)
        throw new ApiError("authentication", "authentication_required");
      if (!allowed || session.denied.has(scope))
        throw new ApiError("forbidden", "permission_denied");
      return api.request(path, schema, { scope, signal, query: parameters });
    },
    enabled: Boolean(session.session) && allowed && !session.denied.has(scope),
    retry: false,
    refetchOnWindowFocus: false,
    staleTime: 15_000,
    refetchInterval: polling ? 30_000 : false,
    refetchIntervalInBackground: false,
  });
  return {
    ...query,
    error: session.denied.has(scope)
      ? (session.deniedErrors.get(scope) ??
        new ApiError("forbidden", "permission_denied"))
      : query.error,
  };
}
