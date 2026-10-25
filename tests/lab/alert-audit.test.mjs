import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {setTimeout} from 'node:timers/promises';

test('alert audit validates payloads and retains bounded current and previous files', async t => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'tidemq-alert-audit-'));
  const child = spawn(process.execPath, [new URL('../../scripts/lab/alert-audit.mjs', import.meta.url).pathname], {
    env: {...process.env, ALERT_AUDIT_DIRECTORY: directory}, stdio: 'ignore',
  });
  t.after(async () => { child.kill(); await fs.rm(directory, {recursive: true, force: true}); });
  let ready = false;
  for (let attempt = 0; attempt < 100; attempt++) {
    try { await fetch('http://127.0.0.1:19094/'); ready = true; break; } catch { await setTimeout(20); }
  }
  assert.ok(ready, 'The local audit receiver must become ready');
  const send = body => fetch('http://127.0.0.1:19094/alerts', {method: 'POST', body});
  assert.equal((await send('{')).status, 400);
  assert.equal((await send('{"status":"firing"}')).status, 400);
  assert.equal((await send('{"status":"firing","alerts":[]}')).status, 200);
  const recorded = JSON.parse(await fs.readFile(path.join(directory, 'events.jsonl'), 'utf8'));
  assert.equal(recorded.status, 'firing');
  assert.ok(recorded.receivedAt);
  await fs.writeFile(path.join(directory, 'events.jsonl'), 'x'.repeat(4_194_305));
  assert.equal((await send('{"status":"resolved","alerts":[]}')).status, 200);
  assert.equal((await fs.stat(path.join(directory, 'previous.jsonl'))).size, 4_194_305);
  assert.equal(JSON.parse(await fs.readFile(path.join(directory, 'events.jsonl'), 'utf8')).status, 'resolved');
});
