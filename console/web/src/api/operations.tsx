import {
  createContext,
  useCallback,
  useContext,
  useState,
  type ReactNode,
} from "react";
import { useLocation, useNavigate } from "react-router";
import { useQueryClient } from "@tanstack/react-query";
import { ApiError } from "./client";
import { type Operation } from "./types";
import { useSession } from "./session";
import { operationId } from "@/features/logic";
import { submitOperation } from "./write";
import { redactDeniedOperations } from "./operation-access";

export interface LocalOperation {
  phase: "submitting" | "recover" | "result";
  result?: Operation;
  secret?: string;
  error?: ApiError;
  returnTo: string;
  resourceTo?: string;
  epoch: number;
}
type Store = Record<string, LocalOperation>;
const OperationContext = createContext<{
  entries: Store;
  set: (id: string, value: LocalOperation) => void;
  forgetSecret: (id: string) => void;
} | null>(null);

export function OperationProvider({ children }: { children: ReactNode }) {
  const { denied } = useSession();
  const [stored, setEntries] = useState<Store>({});
  const entries = redactDeniedOperations(stored, denied);
  if (entries !== stored) setEntries(entries);
  const set = useCallback(
    (id: string, value: LocalOperation) =>
      setEntries((previous) => ({ ...previous, [id]: value })),
    [],
  );
  const forgetSecret = useCallback(
    (id: string) =>
      setEntries((previous) => {
        const entry = previous[id];
        if (!entry?.secret) return previous;
        const { secret: discarded, ...safe } = entry;
        void discarded;
        return { ...previous, [id]: safe };
      }),
    [],
  );
  return (
    <OperationContext value={{ entries, set, forgetSecret }}>
      {children}
    </OperationContext>
  );
}

export function useOperations() {
  const value = useContext(OperationContext);
  if (!value) throw new Error("OperationProvider is required");
  return value;
}

export function useOperationRunner() {
  const store = useOperations();
  const session = useSession();
  const navigate = useNavigate();
  const location = useLocation();
  const client = useQueryClient();
  const pendingId =
    typeof location.state === "object" &&
    location.state !== null &&
    "pendingOperationId" in location.state &&
    typeof location.state.pendingOperationId === "string"
      ? location.state.pendingOperationId
      : null;
  async function run(
    path: string,
    method: "POST" | "PUT" | "PATCH" | "DELETE",
    body: object,
  ) {
    const id = operationId();
    const startedEpoch = session.epoch;
    const returnTo = location.pathname + location.search;
    try {
      const response = await submitOperation({
        path,
        method,
        body,
        id,
        beforeSubmit: () => {
          store.set(id, { phase: "submitting", returnTo, epoch: startedEpoch });
          navigate(returnTo, {
            replace: true,
            state: { pendingOperationId: id },
          });
          navigate(`/operations/${id}`);
        },
      });
      const { secret, ...result } = response;
      const resourceTo =
        result.status === "succeeded" && result.resourceId && method === "POST"
          ? path === "/api/v1/topics"
            ? `/topics/${result.resourceId}`
            : path === "/api/v1/principals"
              ? `/identities?principalId=${result.resourceId}`
              : undefined
          : undefined;
      store.set(id, {
        phase: "result",
        result,
        resourceTo,
        secret:
          window.location.pathname === `/operations/${id}`
            ? (secret ?? undefined)
            : undefined,
        returnTo,
        epoch: startedEpoch,
      });
      void client.invalidateQueries({
        predicate: (query) => query.queryKey[0] === "protected",
      });
      void client.invalidateQueries({ queryKey: ["session"] });
    } catch (error) {
      if (error instanceof ApiError && error.kind === "stale") return;
      store.set(id, {
        phase: "recover",
        error:
          error instanceof ApiError
            ? error
            : new ApiError("unavailable", "unknown_operation_result"),
        returnTo,
        epoch: startedEpoch,
      });
    }
  }
  return { run, pendingId };
}
