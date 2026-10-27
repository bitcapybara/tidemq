import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { Route, Routes } from "react-router";
import { expect, it, vi } from "vitest";
import { MessageBrowser, SendMessage } from "@/features/messages";
import { OperationPage } from "@/features/operations";
import { renderConsole } from "../render";
import { apiUrl, server } from "../network/server";
import * as data from "../network/data";

const browser = <MessageBrowser topicId={data.ids.topic} shards={[data.shard]} allowed />;

it("scans bounded offsets, keeps exact filters out of URLs/query keys, paginates and clears filters for context", async () => {
  const requests: Record<string, unknown>[] = [];
  server.use(http.post(apiUrl("/topics/:id/messages"), async ({ request }) => {
    requests.push(await request.json() as Record<string, unknown>);
    return HttpResponse.json(data.messages);
  }));
  const user = userEvent.setup();
  const view = renderConsole(browser);
  await screen.findByText("message-42");
  expect(requests).toEqual([{ shard: data.ids.shard, limit: 32 }]);
  await user.type(screen.getByLabelText("Start offset (inclusive)"), "40");
  await user.type(screen.getByLabelText("End offset (exclusive)"), "80");
  await user.type(screen.getByLabelText("MessageId (locate with context)"), "message-42");
  await user.click(screen.getByText(/Exact key \/ property filters/));
  await user.click(screen.getByRole("checkbox", { name: "Match exact key (including empty)" }));
  await user.type(screen.getByLabelText("Exact key value"), "private-key");
  await user.clear(screen.getByLabelText("Property object (string values)"));
  await user.click(screen.getByLabelText("Property object (string values)"));
  await user.paste(JSON.stringify({ region: "east" }));
  await user.click(screen.getByRole("button", { name: "Inspect range" }));
  await waitFor(() => expect(requests).toHaveLength(2));
  expect(requests.at(-1)).toMatchObject({ startOffset: "40", endOffset: "80", messageId: "message-42", keyBase64: btoa("private-key"), propertiesBase64: { region: btoa("east") }, limit: 32 });
  expect(window.location.href).not.toContain("private-key");
  expect(JSON.stringify(view.client.getQueryCache().getAll().map((q) => q.queryKey))).not.toMatch(/private-key|cHJpdmF0ZS1rZXk=/);
  await user.click(screen.getByRole("button", { name: "Continue" }));
  await waitFor(() => expect(requests).toHaveLength(3));
  expect(requests.at(-1)).toMatchObject({ startOffset: "43", observedEnd: "100", cursor: "scan-cursor", keyBase64: btoa("private-key") });
  await user.click(screen.getByRole("button", { name: "Previous range" }));
  await waitFor(() => expect(requests).toHaveLength(4));
  expect(requests.at(-1)?.startOffset).toBe("40");
  await user.click(screen.getByRole("button", { name: "Before / after" }));
  await waitFor(() => expect(requests).toHaveLength(5));
  expect(requests.at(-1)).toEqual({ shard: data.ids.shard, startOffset: "26", endOffset: "58", observedEnd: "100", limit: 32 });
  expect(screen.getByLabelText("Exact key value")).toHaveValue("");
  await user.click(screen.getByRole("button", { name: "Retained start" }));
  await waitFor(() => expect(requests).toHaveLength(6));
  expect(requests.at(-1)).toMatchObject({ startOffset: "0", endOffset: "100", observedEnd: "100" });
  await user.click(screen.getByRole("button", { name: "Latest window" }));
  await waitFor(() => expect(requests).toHaveLength(7));
  expect(requests.at(-1)).toEqual({ shard: data.ids.shard, limit: 32 });
});

it("rejects invalid ranges and encodings without issuing another request", async () => {
  let requests = 0;
  server.use(http.post(apiUrl("/topics/:id/messages"), () => { requests++; return HttpResponse.json(data.messages); }));
  const user = userEvent.setup();
  renderConsole(browser);
  await screen.findByText("message-42");
  await user.type(screen.getByLabelText("Start offset (inclusive)"), "10");
  await user.type(screen.getByLabelText("End offset (exclusive)"), "5");
  await user.click(screen.getByRole("button", { name: "Inspect range" }));
  expect((await screen.findAllByText(/The request could not be accepted/)).at(0)).toBeVisible();
  expect(requests).toBe(1);
  await user.clear(screen.getByLabelText("End offset (exclusive)"));
  await user.click(screen.getByText(/Exact key \/ property filters/));
  await user.selectOptions(screen.getByLabelText("Value encoding"), "base64");
  await user.click(screen.getByRole("checkbox", { name: "Match exact key (including empty)" }));
  await user.type(screen.getByLabelText("Exact key value"), "invalid-base64");
  await user.click(screen.getByRole("button", { name: "Inspect range" }));
  expect(requests).toBe(1);
  await user.clear(screen.getByLabelText("Exact key value"));
  await user.click(screen.getByRole("button", { name: "Inspect range" }));
  await waitFor(() => expect(requests).toBe(2));
});

it("decodes a selected message in four formats and keeps truncation separate from original size", async () => {
  server.use(http.post(apiUrl("/topics/:id/messages"), () => HttpResponse.json({ ...data.messages, items: [{ ...data.message, payloadTruncated: true, metadataTruncated: true }] })));
  const user = userEvent.setup();
  renderConsole(browser);
  await user.click(await screen.findByRole("button", { name: "42" }));
  const content = screen.getByRole("region", { name: "Message content" });
  expect(content).toHaveTextContent(/Payload preview is truncated/);
  expect(content).toHaveTextContent(/Key \/ property preview is truncated/);
  for (const [format, text] of [["json", '"order": 42'], ["base64", data.message.payloadBase64], ["hex", "7b 22 6f 72 64 65 72 22 3a 34 32 7d"]]) {
    await user.selectOptions(within(content).getByLabelText("Payload format"), format!);
    expect(content).toHaveTextContent(text!);
  }
  await user.selectOptions(within(content).getByLabelText("Key / property format"), "base64");
  expect(content).toHaveTextContent("ZWFzdA==");
  await user.click(within(content).getByRole("button", { name: "Close" }));
  expect(screen.queryByRole("region", { name: "Message content" })).not.toBeInTheDocument();
});

it("shows decode failures and distinguishes missing metadata from an empty key", async () => {
  server.use(http.post(apiUrl("/topics/:id/messages"), () => HttpResponse.json({ ...data.messages, items: [{ ...data.message, payloadBase64: "/w==", keyBase64: null, propertiesBase64: {} }] })));
  const user = userEvent.setup();
  renderConsole(browser);
  await user.click(await screen.findByRole("button", { name: "42" }));
  const content = screen.getByRole("region", { name: "Message content" });
  expect(content).toHaveTextContent("Cannot decode with this format");
  expect(content).toHaveTextContent("Missing");
  expect(content).toHaveTextContent("No properties in this preview.");
});

it("cancels an in-flight scan, shows cancellation and allows a new bounded scan", async () => {
  let finish: (() => void) | undefined;
  const pending = new Promise<void>((resolve) => { finish = resolve; });
  server.use(http.post(apiUrl("/topics/:id/messages"), async () => { await pending; return HttpResponse.json(data.messages); }));
  const user = userEvent.setup();
  renderConsole(browser);
  await waitFor(() => expect(screen.getByRole("button", { name: "Cancel scan" })).toBeEnabled());
  expect(screen.getByText("Loading…")).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Cancel scan" }));
  expect(await screen.findByText(/The scan was cancelled/)).toBeVisible();
  await act(async () => { finish?.(); });
  server.use(http.post(apiUrl("/topics/:id/messages"), () => HttpResponse.json(data.messages)));
  await user.click(screen.getByRole("button", { name: "Latest window" }));
  expect(await screen.findByText("message-42")).toBeVisible();
});

it("opens only the selected shard group and changes back to an individual shard", async () => {
  const requests = vi.fn();
  server.use(http.post(apiUrl("/topics/:id/messages"), () => { requests(); return HttpResponse.json(data.messages); }));
  const user = userEvent.setup();
  renderConsole(browser, "/?shard=all");
  expect(requests).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: `${data.ids.shard} · current` }));
  expect(await screen.findByText("message-42")).toBeVisible();
  await user.click(screen.getByRole("button", { name: `${data.ids.shard} · current` }));
  expect(screen.queryByRole("table")).not.toBeInTheDocument();
  await user.selectOptions(screen.getByLabelText("Shard view"), data.ids.shard);
  expect(await screen.findByText("message-42")).toBeVisible();
});

it.each(["/?shard=bad-id", `/?shard=${data.ids.shard}&offset=-1`])("refuses an invalid deep link %s", async (path) => {
  renderConsole(browser, path);
  expect((await screen.findAllByText(/The request could not be accepted/)).at(0)).toBeVisible();
  expect(screen.queryByText("message-42")).not.toBeInTheDocument();
});

it("can inspect a known shard outside the loaded topology", async () => {
  renderConsole(browser, `/?shard=${data.ids.other}&offset=1`);
  expect(await screen.findByText("message-42")).toBeVisible();
  expect(screen.getByRole("option", { name: /not in loaded topology/ })).toBeInTheDocument();
});

it("requires content permission before scanning", async () => {
  renderConsole(<MessageBrowser topicId={data.ids.topic} shards={[data.shard]} allowed={false} />);
  expect(await screen.findByText(/You do not have access to this resource/)).toBeVisible();
  expect(screen.queryByRole("button", { name: "Inspect range" })).not.toBeInTheDocument();
});

it("validates JSON before sending and preserves raw payload/key bytes in the write body", async () => {
  const user = userEvent.setup();
  let body: Record<string, unknown> | undefined;
  server.use(http.post(apiUrl("/topics/:id/send"), async ({ request }) => { body = await request.json() as Record<string, unknown>; return HttpResponse.json(data.operation(String(body.operationId))); }));
  renderConsole(<Routes>
    <Route path="/operations/:id" element={<OperationPage />} />
    <Route path="*" element={<SendMessage topicId={data.ids.topic} topicName="Orders" onClose={() => undefined} />} />
  </Routes>);
  await user.selectOptions(screen.getByLabelText("Payload format"), "json");
  await user.type(screen.getByLabelText("Payload", { exact: true }), "invalid-json");
  await user.click(screen.getByRole("button", { name: "Confirm and submit" }));
  expect(screen.getByRole("alert")).toHaveTextContent("Check the entered values");
  expect(body).toBeUndefined();
  await user.clear(screen.getByLabelText("Payload", { exact: true }));
  await user.click(screen.getByLabelText("Payload", { exact: true }));
  await user.paste('{"order":42}');
  await user.click(screen.getByRole("checkbox", { name: "Include a key (empty values are allowed)" }));
  await user.type(screen.getByLabelText("Key", { exact: true }), " key ");
  await user.click(screen.getByRole("button", { name: "Confirm and submit" }));
  expect(await screen.findByText("Succeeded")).toBeVisible();
  expect(body).toMatchObject({ payloadBase64: btoa('{"order":42}'), keyBase64: btoa(" key "), propertiesBase64: {} });
});
