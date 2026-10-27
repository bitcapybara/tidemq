import { useLayoutEffect, type ReactNode } from "react";
import { render } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter, Route, Routes } from "react-router";
import { SessionProvider, useSession } from "@/api/session";
import { OperationProvider } from "@/api/operations";
import { LocaleProvider } from "@/locales/locale";

export function renderConsole(ui: ReactNode, path = "/", route = "*", state?: object) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 }, mutations: { retry: false } } });
  let session: ReturnType<typeof useSession>;
  function Capture() {
    const current = useSession();
    useLayoutEffect(() => { session = current; });
    return null;
  }
  const view = render(
    <QueryClientProvider client={client}>
      <LocaleProvider>
        <SessionProvider>
          <MemoryRouter initialEntries={[state ? { pathname: path, state } : path]}>
            <Capture />
            <OperationProvider>
              <Routes><Route path={route} element={ui} /></Routes>
            </OperationProvider>
          </MemoryRouter>
        </SessionProvider>
      </LocaleProvider>
    </QueryClientProvider>,
  );
  return { ...view, client, session: () => session! };
}
