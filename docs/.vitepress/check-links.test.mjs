import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { validateSite } from './check-links.mjs';

test('validates project paths, assets and encoded fragments, and rejects broken links', async () => {
  const root = await mkdtemp(join(tmpdir(), 'tidemq-docs-links-'));
  try {
    await mkdir(join(root, 'guide'));
    await writeFile(join(root, 'asset.css'), 'body {}');
    await writeFile(join(root, 'index.html'), '<a href="/tidemq/guide/page.html?x=1#%E6%81%A2%E5%A4%8D">Guide</a><link href="/tidemq/asset.css"><a href="https://example.test/">External</a>');
    await writeFile(join(root, 'guide/page.html'), '<h1 id="恢复">Recovery</h1><a href="../">Home</a>');
    const valid = await validateSite(root);
    assert.deepEqual(valid.errors, []);
    assert.equal(valid.pages, 2);
    assert.equal(valid.links, 3);
    await writeFile(join(root, 'index.html'), '<a href="/tidemq/guide/page.html#missing">Fragment</a><a href="/tidemq/absent.html">Missing</a><a href="/other/">Wrong base</a>');
    const invalid = await validateSite(root);
    assert.equal(invalid.errors.length, 3);
    assert.match(invalid.errors.join('\n'), /Missing fragment/);
    assert.match(invalid.errors.join('\n'), /Link escapes the site base/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('rejects empty output', async () => {
  const root = await mkdtemp(join(tmpdir(), 'tidemq-docs-empty-'));
  try {
    assert.deepEqual((await validateSite(root)).errors, ['No generated HTML pages found']);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
