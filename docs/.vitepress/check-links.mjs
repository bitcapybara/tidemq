import { readFile, readdir, stat } from 'node:fs/promises';
import { resolve, relative, sep, join } from 'node:path';
import { pathToFileURL } from 'node:url';

async function htmlFiles(directory) {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await htmlFiles(path));
    else if (entry.name.endsWith('.html')) files.push(path);
  }
  return files;
}

export async function validateSite(directory, base = '/tidemq/') {
  const root = resolve(directory);
  const pages = await htmlFiles(root);
  const ids = new Map();
  const errors = [];
  let links = 0;
  for (const page of pages) {
    const html = await readFile(page, 'utf8');
    ids.set(page, new Set([...html.matchAll(/\bid="([^"]*)"/g)].map((match) => match[1])));
  }
  for (const page of pages) {
    const html = await readFile(page, 'utf8');
    const source = relative(root, page).split(sep).join('/');
    for (const match of html.matchAll(/\s(?:href|src)=["']([^"']*)["']/g)) {
      const href = match[1].replaceAll('&amp;', '&');
      if (/^(?:[a-z][a-z\d+.-]*:|\/\/)/i.test(href)) continue;
      links += 1;
      const label = source + ' -> ' + href;
      try {
        const url = new URL(href, 'https://docs.invalid' + base + source);
        if (!url.pathname.startsWith(base)) throw new Error('Link escapes the site base');
        let target = resolve(root, decodeURIComponent(url.pathname.slice(base.length)));
        if (target !== root && !target.startsWith(root + sep)) {
          throw new Error('Link escapes the output directory');
        }
        if ((await stat(target)).isDirectory()) target = join(target, 'index.html');
        await stat(target);
        if (url.hash && target.endsWith('.html')) {
          const anchor = decodeURIComponent(url.hash.slice(1));
          if (!ids.get(target)?.has(anchor)) throw new Error('Missing fragment: ' + anchor);
        }
      } catch (error) {
        errors.push(label + ': ' + error.message);
      }
    }
  }
  if (pages.length === 0) errors.push('No generated HTML pages found');
  return { pages: pages.length, links, errors };
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  const result = await validateSite(resolve(import.meta.dirname, 'dist'));
  if (result.errors.length > 0) {
    console.error(result.errors.slice(0, 20).join('\n'));
    console.error('Invalid local links: ' + result.errors.length);
    process.exitCode = 1;
  } else {
    console.log('Validated ' + result.pages + ' pages and ' + result.links + ' local links and fragments.');
  }
}
