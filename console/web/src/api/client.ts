import type { ZodType } from "zod";

export type Failure =
  | "authentication"
  | "forbidden"
  | "unavailable"
  | "invalid"
  | "conflict"
  | "stale";
type Diagnostics = { requestId: string | null; traceId: string | null };
export class ApiError extends Error {
  readonly kind: Failure;
  readonly code: string;
  readonly status: number;
  readonly requestId: string | null;
  readonly traceId: string | null;
  constructor(
    kind: Failure,
    code: string,
    status = 0,
    diagnostics?: Diagnostics,
  ) {
    super(code);
    this.name = "ApiError";
    this.kind = kind;
    this.code = code;
    this.status = status;
    this.requestId = diagnostics?.requestId ?? null;
    this.traceId = diagnostics?.traceId ?? null;
  }
}
export type AccessEvent =
  | { kind: "authentication"; error: ApiError }
  | { kind: "forbidden"; scope: string; error: ApiError };
type Listener = (event: AccessEvent) => void;

function diagnostics(headers: Headers): Diagnostics {
  const request = headers.get("x-request-id") ?? "";
  const requestId =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      request,
    )
      ? request.toLowerCase()
      : null;
  const trace = /^00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$/.exec(
    headers.get("traceparent") ?? "",
  );
  const traceId = trace?.at(1),
    spanId = trace?.at(2);
  return {
    requestId,
    traceId:
      traceId && spanId && !/^0+$/.test(traceId) && !/^0+$/.test(spanId)
        ? traceId
        : null,
  };
}

export function normalizeTimes(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(normalizeTimes);
  if (value === null || typeof value !== "object") return value;
  const result: Record<string, unknown> = Object.create(null);
  for (const [key, child] of Object.entries(value))
    result[key] = key === "propertiesBase64" ? child : normalizeTimes(child);
  for (const [source, target] of [
    ["observedAtMs", "observedAt"],
    ["observedFromMs", "observedFrom"],
    ["observedUntilMs", "observedUntil"],
    ["expiresAtMs", "expiresAt"],
    ["credentialExpiresAtMs", "credentialExpiresAt"],
    ["lastHeartbeatMs", "lastHeartbeat"],
    ["createdAtMs", "createdAt"],
    ["sampledAtMs", "sampledAt"],
  ]) {
    const raw = result[source ?? ""];
    if (source && target && source in result)
      result[target] =
        typeof raw === "string" &&
        /^\d{1,16}$/.test(raw) &&
        BigInt(raw) <= 8_640_000_000_000_000n
          ? new Date(Number(raw)).toISOString()
          : null;
  }
  return result;
}

export class ApiClient {
  private csrfToken: string | null = null;
  private generation = 0;
  private scopeVersions = new Map<string, number>();
  private pending = new Map<AbortController, string>();
  private listeners = new Set<Listener>();

  private readonly fetcher: typeof fetch;
  constructor(
    fetcher: typeof fetch = (input, init) => globalThis.fetch(input, init),
  ) {
    this.fetcher = fetcher;
  }

  onAccessFailure(listener: Listener) {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  clearIdentity() {
    this.csrfToken = null;
    this.generation += 1;
    for (const controller of this.pending.keys()) controller.abort();
    this.pending.clear();
    this.scopeVersions.clear();
  }

  setCsrfToken(token: string) {
    this.csrfToken = token;
  }

  private forbid(scope: string, error: ApiError) {
    this.scopeVersions.set(scope, (this.scopeVersions.get(scope) ?? 0) + 1);
    for (const [controller, requestScope] of this.pending) {
      if (requestScope === scope) controller.abort();
    }
    for (const listener of this.listeners)
      listener({ kind: "forbidden", scope, error });
  }

  async request<T>(
    path: string,
    schema: ZodType<T>,
    options: {
      scope?: string;
      signal?: AbortSignal;
      method?: "GET" | "POST" | "DELETE" | "PUT" | "PATCH";
      body?: unknown;
      query?: Record<string, string | undefined>;
      publicSession?: boolean;
    } = {},
  ): Promise<T> {
    if (
      !path.startsWith("/api/v1/") ||
      path.startsWith("//") ||
      /[?#]/.test(path) ||
      new URL(path, "https://console.invalid").pathname !== path
    ) {
      throw new ApiError("invalid", "invalid_api_path");
    }
    const scope = options.scope ?? "session";
    const generation = this.generation;
    const version = this.scopeVersions.get(scope) ?? 0;
    const controller = new AbortController();
    const abort = () => controller.abort();
    options.signal?.addEventListener("abort", abort, { once: true });
    if (options.signal?.aborted) controller.abort();
    this.pending.set(controller, scope);
    const method = options.method ?? "GET";
    if (method !== "GET" && !options.publicSession && !this.csrfToken) {
      this.pending.delete(controller);
      options.signal?.removeEventListener("abort", abort);
      throw new ApiError("authentication", "csrf_token_required");
    }
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(options.query ?? {}))
      if (value !== undefined && value !== "") params.set(key, value);
    const url = params.size ? `${path}?${params}` : path;
    try {
      const response = await this.fetcher(url, {
        method,
        credentials: "same-origin",
        cache: "no-store",
        redirect: "error",
        signal: controller.signal,
        headers: {
          Accept: "application/json",
          ...(method !== "GET" && this.csrfToken && !options.publicSession
            ? { "X-CSRF-Token": this.csrfToken }
            : {}),
          ...(options.body === undefined
            ? {}
            : { "Content-Type": "application/json" }),
        },
        body:
          options.body === undefined ? undefined : JSON.stringify(options.body),
      });
      const metadata = diagnostics(response.headers);
      if (
        generation !== this.generation ||
        version !== (this.scopeVersions.get(scope) ?? 0)
      ) {
        throw new ApiError("stale", "stale_identity_response");
      }
      if (response.status === 401) {
        const failure = new ApiError(
          "authentication",
          "authentication_required",
          401,
          metadata,
        );
        this.clearIdentity();
        for (const listener of this.listeners)
          listener({ kind: "authentication", error: failure });
        throw failure;
      }
      if (response.status === 403) {
        const failure = new ApiError(
          "forbidden",
          "permission_denied",
          403,
          metadata,
        );
        this.forbid(scope, failure);
        throw failure;
      }
      if (!response.ok) {
        const error: unknown = await response.json().catch(() => null);
        const code =
          typeof error === "object" &&
          error !== null &&
          "code" in error &&
          typeof error.code === "string"
            ? error.code
            : "request_failed";
        throw new ApiError(
          response.status >= 500
            ? "unavailable"
            : response.status === 409
              ? "conflict"
              : "invalid",
          code,
          response.status,
          metadata,
        );
      }
      const data: unknown =
        response.status === 204
          ? null
          : await response.json().catch(() => {
              throw new ApiError(
                "invalid",
                "invalid_response",
                response.status,
                metadata,
              );
            });
      if (
        generation !== this.generation ||
        version !== (this.scopeVersions.get(scope) ?? 0)
      ) {
        throw new ApiError("stale", "stale_identity_response");
      }
      const parsed = schema.safeParse(normalizeTimes(data));
      if (!parsed.success)
        throw new ApiError(
          "invalid",
          "invalid_response",
          response.status,
          metadata,
        );
      return parsed.data;
    } catch (error) {
      if (error instanceof ApiError) throw error;
      if (controller.signal.aborted)
        throw new ApiError("stale", "request_cancelled");
      throw new ApiError("unavailable", "network_unavailable");
    } finally {
      this.pending.delete(controller);
      options.signal?.removeEventListener("abort", abort);
    }
  }
}

export const api = new ApiClient();
