import assert from 'node:assert/strict';
import http from 'node:http';
import test from 'node:test';
import { listen, proxyRequest } from './console-browser-live.mjs';

test('the browser fixture proxy preserves origin, cookies, bodies and status without mocking the API', async () => {
  const upstream = http.createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    assert.equal(request.headers.origin, 'https://127.0.0.1:8443');
    assert.equal(request.headers.cookie, '__Host-test=opaque');
    assert.equal(request.headers.host, '127.0.0.1:8443');
    assert.equal(body, '{"operationId":"original-operation"}');
    response.writeHead(409, { 'content-type': 'application/json', 'set-cookie': '__Host-test=new; Secure; HttpOnly; Path=/' });
    response.end('{"error":"operation_conflict"}');
  });
  const upstreamPort = await listen(upstream);
  const proxy = http.createServer((request, response) => proxyRequest(upstreamPort, request, response));
  const proxyPort = await listen(proxy);
  try {
    const result = await new Promise((resolve, reject) => {
      const request = http.request({ hostname: '127.0.0.1', port: proxyPort, path: '/api/v1/operations', method: 'POST',
        headers: { origin: 'https://127.0.0.1:8443', host: '127.0.0.1:8443', cookie: '__Host-test=opaque', 'content-type': 'application/json' } },
      async (response) => {
        let body = '';
        for await (const chunk of response) body += chunk;
        resolve({ status: response.statusCode, cookie: response.headers['set-cookie'], body });
      });
      request.once('error', reject);
      request.end('{"operationId":"original-operation"}');
    });
    assert.deepEqual(result, { status: 409, cookie: ['__Host-test=new; Secure; HttpOnly; Path=/'], body: '{"error":"operation_conflict"}' });
  } finally {
    proxy.closeAllConnections();
    upstream.closeAllConnections();
    await Promise.all([new Promise((resolve) => proxy.close(resolve)), new Promise((resolve) => upstream.close(resolve))]);
  }
});

test('an unavailable Console produces an explicit proxy failure', async () => {
  const stopped = http.createServer();
  const unused = await listen(stopped);
  await new Promise((resolve) => stopped.close(resolve));
  const proxy = http.createServer((request, response) => proxyRequest(unused, request, response));
  const port = await listen(proxy);
  try {
    const response = await fetch(`http://127.0.0.1:${port}/api/v1/session`);
    assert.equal(response.status, 502);
    assert.equal(await response.text(), 'The test Console is unavailable');
  } finally {
    proxy.closeAllConnections();
    await new Promise((resolve) => proxy.close(resolve));
  }
});
