#!/usr/bin/env bash
set -euo pipefail
umask 077
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
[[ $# == 1 && ! -e "$1" && -d "$(dirname -- "$1")" ]] || {
  printf 'Usage: build-console.sh new-output-directory-with-existing-parent\n' >&2; exit 2;
}
for tool in node tar; do
  command -v "$tool" >/dev/null || { printf 'Console packaging requires %s\n' "$tool" >&2; exit 2; }
done
node -e 'const [major,minor]=process.versions.node.split(".").map(Number); if (!((major===22 && minor>=22)||major===24||major>=26)) { console.error("Console packaging requires Node 22.22+, 24, or 26+"); process.exit(2); }'
for file in package.json package-lock.json; do
  [[ -s "$repo/console/web/$file" ]] || { printf 'Console source or lock file is missing: %s\n' "$file" >&2; exit 2; }
done
temporary=$(mktemp -d "${TMPDIR:-/tmp}/tidemq-console-build.XXXXXXXX")
trap 'rm -rf "$temporary"' EXIT
mkdir "$temporary/web"
tar -C "$repo/console/web" --exclude='./node_modules' --exclude='./dist' --exclude='./.git' \
  --exclude='./.artifacts' --exclude='./coverage' --exclude='.DS_Store' --exclude='*.tsbuildinfo' --exclude='*.log' \
  --exclude='./.env' --exclude='./.env.*' --exclude='./.npmrc' --exclude='*.pem' --exclude='*.key' -cf - . \
  | tar -C "$temporary/web" -xf -
npm_command=(npm)
if ! command -v npm >/dev/null; then
  command -v curl >/dev/null || { printf 'Bootstrapping npm requires curl\n' >&2; exit 2; }
  archive="$temporary/npm.tgz"
  # Verified against the official npm registry metadata, before extraction or execution.
  integrity='7iKzNfy8lWYs3zq4oFPa8EXZz5xt9gQNKJZau3B1ErLBb6bF7sBJ00x09485DOvRT2l5Gerbl3VlZNT57MxJVA=='
  curl --fail --silent --show-error --location --proto '=https' --max-time 60 --max-filesize 16777216 \
    https://registry.npmjs.org/npm/-/npm-11.6.2.tgz -o "$archive"
  node --input-type=module - "$archive" "$integrity" <<'JS'
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
const [, , file, expected] = process.argv;
if (createHash('sha512').update(readFileSync(file)).digest('base64') !== expected) {
  console.error('Downloaded npm archive failed SHA512 verification'); process.exit(2);
}
JS
  mkdir "$temporary/npm"
  tar -xzf "$archive" -C "$temporary/npm"
  npm_command=(node "$temporary/npm/package/bin/npm-cli.js")
fi
node --input-type=module - "$temporary/web" > "$temporary/source.sha256" <<'JS'
import {readdirSync,readFileSync} from 'node:fs';
import {join,relative} from 'node:path';
import {createHash} from 'node:crypto';
const root=process.argv[2], hash=createHash('sha256');
function visit(directory) {
  for (const entry of readdirSync(directory,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name,'en'))) {
    const file=join(directory,entry.name);
    if (entry.isSymbolicLink()) throw new Error('Console sources must not contain symlinks');
    if (entry.isDirectory()) visit(file);
    else if (entry.isFile()) hash.update(relative(root,file)).update('\0').update(readFileSync(file)).update('\0');
  }
}
visit(root); console.log(hash.digest('hex'));
JS
(
  cd "$temporary/web"
  "${npm_command[@]}" ci --include=dev --no-audit --no-fund
  "${npm_command[@]}" run build
)
[[ -s "$temporary/web/dist/index.html" && -s "$temporary/web/dist/.vite/manifest.json" ]] || {
  printf 'Console build did not produce index.html and the Vite manifest\n' >&2; exit 1;
}
mkdir -- "$1"
output=$(cd -- "$1" && pwd -P)
cp -R "$temporary/web/dist/." "$output/"
chmod 0700 "$output"
node --input-type=module - "$output" "$(cat "$temporary/source.sha256")" "$(node --version)" "$("${npm_command[@]}" --version)" <<'JS'
import {writeFileSync} from 'node:fs';
import {join} from 'node:path';
const [, , output,sourceSha256,nodeVersion,npmVersion]=process.argv;
writeFileSync(join(output,'.tidemq-build.json'),JSON.stringify({sourceSha256,nodeVersion,npmVersion})+'\n',{mode:0o600});
JS
printf 'Console static build packaged with its source hash: %s\n' "$output"
