import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import * as data from "./data";

export const apiUrl = (path: string) => `http://localhost:5173/api/v1${path}`;
export const handlers = [
  http.get(apiUrl("/me"), () => HttpResponse.json(data.session)),
  http.get(apiUrl("/capabilities"), () => HttpResponse.json(data.session.capabilities)),
  http.post(apiUrl("/session"), () => HttpResponse.json(data.session)),
  http.delete(apiUrl("/session"), () => new HttpResponse(null, { status: 204 })),
  http.get(apiUrl("/overview"), () => HttpResponse.json(data.overview)),
  http.get(apiUrl("/topics"), () => HttpResponse.json(data.page([data.topic]))),
  http.get(apiUrl("/topics/:id"), () => HttpResponse.json(data.topicDetail)),
  http.get(apiUrl("/topics/:id/shards"), () => HttpResponse.json(data.page([{ ...data.shard, id: data.ids.other, sealed: true }]))),
  http.get(apiUrl("/topics/:id/producers"), () => HttpResponse.json(data.page([{ ...data.member, registration: "1" }]))),
  http.post(apiUrl("/topics/:id/messages"), () => HttpResponse.json(data.messages)),
  http.get(apiUrl("/subscriptions"), () => HttpResponse.json(data.page([data.subscription]))),
  http.get(apiUrl("/subscriptions/:id/progress"), () => HttpResponse.json(data.page([data.progress]))),
  http.get(apiUrl("/subscriptions/:id/members"), () => HttpResponse.json(data.page([data.member]))),
  http.get(apiUrl("/principals"), () => HttpResponse.json(data.page([data.principal]))),
  http.get(apiUrl("/credentials"), () => HttpResponse.json(data.page([data.credential]))),
  http.get(apiUrl("/grants"), () => HttpResponse.json(data.page([data.grant]))),
  http.get(apiUrl("/tenants"), () => HttpResponse.json(data.page([{ id: data.ids.tenant, name: "Tenant" }]))),
  http.get(apiUrl("/namespaces"), () => HttpResponse.json(data.page([{ id: data.ids.namespace, name: "Namespace" }]))),
  http.get(apiUrl("/components"), () => HttpResponse.json(data.page([data.component]))),
  http.get(apiUrl("/components/:id"), () => HttpResponse.json(data.component)),
  http.get(apiUrl("/components/:id/instances"), () => HttpResponse.json(data.page([data.instance]))),
  http.get(apiUrl("/cluster"), () => HttpResponse.json(data.page([data.node]))),
  http.get(apiUrl("/alerts"), () => HttpResponse.json(data.page([{ id: "alert", name: "High backlog", state: "firing", detail: "Queue needs attention", url: "https://example.test/alert", runbookUrl: "https://example.test/runbook" }]))),
  http.get(apiUrl("/operations/:id"), ({ params }) => HttpResponse.json(data.operation(String(params.id)))),
];
export const server = setupServer(...handlers);
