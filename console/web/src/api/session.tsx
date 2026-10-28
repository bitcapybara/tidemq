import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { api, ApiError } from "./client";
import { sessionSchema, type Session } from "./types";

interface SessionContextValue {
  session: Session | null;
  error: ApiError | null;
  loading: boolean;
  authenticating: boolean;
  loginFailure: ApiError | null;
  logoutFailure: ApiError | null;
  epoch: number;
  denied: ReadonlySet<string>;
  deniedErrors: ReadonlyMap<string, ApiError>;
  refresh: () => void;
  login: (token: string) => Promise<void>;
  logout: () => Promise<void>;
}
const SessionContext = createContext<SessionContextValue | null>(null);

function authorizationFingerprint(session: Session): string {
  return JSON.stringify([
    session.principal.id,
    session.cluster.id,
    session.tenant,
    session.namespace,
    session.permissions,
    session.resourceGrants,
    session.operator,
    session.credentialId,
    session.capabilities,
  ]);
}

export function SessionProvider({ children }: { children: ReactNode }) {
  const client = useQueryClient();
  const [epoch, setEpoch] = useState(0);
  const [denied, setDenied] = useState<ReadonlySet<string>>(new Set());
  const [deniedErrors, setDeniedErrors] = useState<
    ReadonlyMap<string, ApiError>
  >(new Map());
  const [authenticationFailure, setAuthenticationFailure] =
    useState<ApiError | null>(null);
  const [blocked, setBlocked] = useState(false);
  const [authenticating, setAuthenticating] = useState(false);
  const [loginFailure, setLoginFailure] = useState<ApiError | null>(null);
  const [logoutFailure, setLogoutFailure] = useState<ApiError | null>(null);
  const loginPending = useRef(false);
  const identity = useRef<string | null>(null);
  const identityChannel = useRef<BroadcastChannel | null>(null);
  const clearProtected = useCallback(() => {
    api.clearIdentity();
    void client.cancelQueries({
      predicate: (entry) => entry.queryKey[0] === "protected",
    });
    client.removeQueries({
      predicate: (entry) => entry.queryKey[0] === "protected",
    });
    setDenied(new Set());
    setDeniedErrors(new Map());
    setEpoch((previous) => previous + 1);
  }, [client]);

  const query = useQuery({
    queryKey: ["session"],
    queryFn: async ({ signal }) => {
      const value = await api.request("/api/v1/me", sessionSchema, { signal });
      const next = authorizationFingerprint(value);
      if (identity.current !== null && identity.current !== next)
        clearProtected();
      identity.current = next;
      api.setCsrfToken(value.csrfToken);
      setBlocked(false);
      setAuthenticationFailure(null);
      return value;
    },
    retry: false,
    refetchOnWindowFocus: false,
    staleTime: 30_000,
  });

  useEffect(
    () =>
      api.onAccessFailure((event) => {
        if (event.kind === "authentication") {
          setBlocked(true);
          identity.current = null;
          clearProtected();
          setAuthenticationFailure(event.error);
          client.setQueryData(["session"], null);
        } else {
          setDenied((previous) => new Set([...previous, event.scope]));
          setDeniedErrors(
            (previous) => new Map([...previous, [event.scope, event.error]]),
          );
          void client.cancelQueries({
            predicate: (entry) =>
              entry.queryKey[0] === "protected" &&
              entry.queryKey[2] === event.scope,
          });
          client.removeQueries({
            predicate: (entry) =>
              entry.queryKey[0] === "protected" &&
              entry.queryKey[2] === event.scope,
          });
        }
      }),
    [client, clearProtected],
  );

  useEffect(() => {
    if (typeof BroadcastChannel === "undefined") return;
    const channel = new BroadcastChannel("tidemq.console.session");
    identityChannel.current = channel;
    let revision = 0;
    channel.onmessage = (event: MessageEvent<unknown>) => {
      if (event.data !== "login" && event.data !== "logout") return;
      const current = ++revision;
      const signedIn = event.data === "login";
      identity.current = null;
      setBlocked(true);
      setAuthenticationFailure(null);
      setLoginFailure(null);
      setLogoutFailure(null);
      clearProtected();
      client.setQueryData(["session"], null);
      void client
        .cancelQueries({ queryKey: ["session"], exact: true })
        .then(() => {
          if (signedIn && current === revision)
            void client.refetchQueries({ queryKey: ["session"], exact: true });
        });
    };
    return () => {
      identityChannel.current = null;
      channel.close();
    };
  }, [client, clearProtected]);

  useEffect(() => {
    if (blocked || !query.data?.expiresAt) return;
    const deadline = Date.parse(query.data.expiresAt);
    if (!Number.isFinite(deadline)) return;
    let timer: ReturnType<typeof setTimeout>;
    const expire = () => {
      const remaining = deadline - Date.now();
      if (remaining > 0) {
        timer = setTimeout(expire, Math.min(remaining, 2_147_483_647));
        return;
      }
      setBlocked(true);
      identity.current = null;
      clearProtected();
      setAuthenticationFailure(null);
      client.setQueryData(["session"], null);
    };
    timer = setTimeout(
      expire,
      Math.max(0, Math.min(deadline - Date.now(), 2_147_483_647)),
    );
    return () => clearTimeout(timer);
  }, [blocked, query.data?.expiresAt, client, clearProtected]);

  async function login(token: string) {
    if (loginPending.current)
      throw new ApiError("invalid", "login_in_progress");
    loginPending.current = true;
    setAuthenticating(true);
    setLoginFailure(null);
    setAuthenticationFailure(null);
    setLogoutFailure(null);
    clearProtected();
    setBlocked(true);
    client.setQueryData(["session"], null);
    identityChannel.current?.postMessage("logout");
    try {
      const session = await api.request("/api/v1/session", sessionSchema, {
        method: "POST",
        body: { token },
        publicSession: true,
      });
      identity.current = authorizationFingerprint(session);
      api.setCsrfToken(session.csrfToken);
      client.setQueryData(["session"], session);
      setBlocked(false);
      identityChannel.current?.postMessage("login");
    } catch (error) {
      setLoginFailure(
        error instanceof ApiError
          ? error
          : new ApiError("unavailable", "login_unavailable"),
      );
      throw error;
    } finally {
      loginPending.current = false;
      setAuthenticating(false);
    }
  }

  async function logout() {
    setLogoutFailure(null);
    identityChannel.current?.postMessage("logout");
    try {
      if (logoutFailure) {
        const current = await api.request("/api/v1/me", sessionSchema);
        api.setCsrfToken(current.csrfToken);
      }
      await api.request("/api/v1/session", z.unknown(), { method: "DELETE" });
    } catch (error) {
      if (error instanceof ApiError && error.kind === "authentication") return;
      setLogoutFailure(
        error instanceof ApiError
          ? error
          : new ApiError("unavailable", "logout_unavailable"),
      );
      throw error;
    } finally {
      clearProtected();
      setBlocked(true);
      identity.current = null;
      client.setQueryData(["session"], null);
    }
  }

  return (
    <SessionContext
      value={{
        session: !blocked && query.data ? query.data : null,
        error: blocked
          ? (authenticationFailure ??
            (query.error instanceof ApiError ? query.error : null) ??
            new ApiError("authentication", "authentication_required"))
          : query.error instanceof ApiError
            ? query.error
            : null,
        loading: query.isPending,
        authenticating,
        loginFailure,
        logoutFailure,
        epoch,
        denied,
        deniedErrors,
        refresh: () => {
          void query.refetch();
        },
        login,
        logout,
      }}
    >
      {children}
    </SessionContext>
  );
}

export function useSession() {
  const value = useContext(SessionContext);
  if (!value) throw new Error("SessionProvider is required");
  return value;
}
