import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';

export function collectFailureEvidence(root, output) {
  fs.mkdirSync(output, {recursive: true, mode: 0o700});
  const files = [];
  const entries = [];
  for (const directory of fs.readdirSync(root, {withFileTypes: true})) {
    if (!directory.isDirectory() || !/^tidemq-runtime-[a-f0-9-]+$/.test(directory.name)) continue;
    for (const file of fs.readdirSync(path.join(root, directory.name), {withFileTypes: true})) {
      if (!file.isFile() || files.length >= 40) continue;
      const json = /-(failure|diagnostics|metrics)\.json$/.test(file.name)
        || /^consumer-recovery-(after-quorum|timeout)\.json$/.test(file.name);
      const log = /^(node|process)-[0-9]+(?:-(?:stdout|stderr))?\.(log|stdout|stderr)$/.test(file.name);
      if (!json && !log) continue;
      const source = path.join(root, directory.name, file.name);
      const size = fs.statSync(source).size;
      if (json && size > 2 * 1024 * 1024) continue;
      const buffer = Buffer.alloc(json ? size : Math.min(size, 256 * 1024));
      const tailOffset = json ? 0 : Math.max(0, size - buffer.length);
      const fd = fs.openSync(source, 'r');
      let copiedBytes;
      try { copiedBytes = fs.readSync(fd, buffer, 0, buffer.length, tailOffset); }
      finally { fs.closeSync(fd); }
      const destination = path.join(output, directory.name);
      fs.mkdirSync(destination, {recursive: true, mode: 0o700});
      fs.writeFileSync(path.join(destination, file.name), buffer.subarray(0, copiedBytes), {mode: 0o600});
      const relative = `${directory.name}/${file.name}`;
      files.push(relative);
      entries.push({path: relative, sourceBytes: size, copiedBytes, tailOffset, truncated: tailOffset > 0 || copiedBytes < size});
    }
  }
  fs.writeFileSync(path.join(output, 'manifest.json'), JSON.stringify({files, entries}, null, 2) + '\n');
  return files;
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const [root, output] = process.argv.slice(2);
  if (!root || !output) throw new Error('Usage: failure-evidence.mjs temporary-root output-directory');
  collectFailureEvidence(root, output);
}
