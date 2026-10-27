import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { expect, it } from "vitest";
import { z } from "zod";
import { useResource } from "@/api/query";
import { useSession } from "@/api/session";
import { renderConsole } from "../render";
import { apiUrl, server } from "../network/server";

function ManualRead() {
  const session = useSession();
  const query = useResource("restricted", "/api/v1/restricted", z.string(), false, false);
  return <>
    <span>{session.loading ? "Checking session" : "Session ready"}</span>
    <button onClick={() => void query.refetch()}>Manual read</button>
    {query.error && <p role="alert">{query.error.message}</p>}
  </>;
}

it.each([true, false])("blocks even explicit refetches when authenticated=%s but access is absent", async (authenticated) => {
  if (!authenticated) server.use(http.get(apiUrl("/me"), () => new HttpResponse(null, { status: 401 })));
  let requests = 0;
  server.use(http.get(apiUrl("/restricted"), () => { requests++; return HttpResponse.json("private"); }));
  renderConsole(<ManualRead />);
  await screen.findByText("Session ready");
  await userEvent.setup().click(screen.getByRole("button", { name: "Manual read" }));
  await waitFor(() => expect(screen.getByRole("alert")).toHaveTextContent(authenticated ? "permission_denied" : "authentication_required"));
  expect(requests).toBe(0);
});
