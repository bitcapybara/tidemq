import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { runInNewContext } from 'node:vm';

const workflow = readFileSync(new URL('../../.github/workflows/docs.yml', import.meta.url), 'utf8');
const deploy = workflow.split('\n  deploy:\n')[1] ?? '';
const condition = deploy.match(/^    if: (.+)$/m)?.[1];

test('main documentation pushes publish after a successful build', () => {
  assert.ok(condition, 'The Pages job must have an explicit deployment condition');
  assert.match(deploy, /^    needs: build$/m);
  assert.equal(runInNewContext(condition, {
    github: { event_name: 'push', ref: 'refs/heads/main' }, inputs: {},
  }), true);
});

test('Pages only accepts main pushes or an explicit manual main publication', () => {
  assert.ok(condition, 'The Pages job must have an explicit deployment condition');
  const cases = [
    ['workflow_dispatch', 'refs/heads/main', true, true],
    ['workflow_dispatch', 'refs/heads/main', false, false],
    ['workflow_dispatch', 'refs/heads/feature', true, false],
    ['push', 'refs/heads/feature', false, false],
    ['push', 'refs/tags/main', false, false],
    ['pull_request', 'refs/pull/1/merge', true, false],
    ['pull_request', 'refs/heads/main', true, false],
    ['pull_request_target', 'refs/heads/main', true, false],
  ];
  for (const [event, ref, publish, expected] of cases) {
    assert.equal(runInNewContext(condition, {
      github: { event_name: event, ref }, inputs: { publish },
    }), expected, `${event} on ${ref} with publish=${publish}`);
  }
});
