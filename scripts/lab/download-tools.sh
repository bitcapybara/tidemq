#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
mode=${1:-all}
[[ $# -le 1 && "$mode" =~ ^(all|actions)$ ]] || { printf 'Usage: download-tools.sh [all|actions]\n' >&2; exit 2; }
verify_archive() {
  if command -v sha256sum >/dev/null; then
    printf '%s  %s\n' "$1" "$2" | sha256sum -c - >/dev/null
  else
    printf '%s  %s\n' "$1" "$2" | shasum -a 256 -c - >/dev/null
  fi
}
install -d -m 0755 "$lab_state/tools"
while IFS=$'\t' read -r name url digest member raw; do
  archive="$lab_state/tools/$name.archive"
  if [[ ! -f "$archive" ]]; then
    if command -v aria2c >/dev/null; then
      aria2c --continue=true --split=16 --max-connection-per-server=16 --min-split-size=1M \
        --file-allocation=none --console-log-level=warn --summary-interval=0 --download-result=hide \
        --dir="$lab_state/tools" --out="$name.archive.part" "$url"
    else
      curl --fail --location --retry 3 --proto '=https' "$url" -o "$archive.part"
    fi
    verify_archive "$digest" "$archive.part"
    mv "$archive.part" "$archive"
  fi
  verify_archive "$digest" "$archive"
  if [[ "$member" != - ]]; then
    if [[ "$url" == *.zip ]]; then unzip -p "$archive" "$member" > "$lab_state/tools/$name";
    else tar -xOf "$archive" "$member" > "$lab_state/tools/$name"; fi
    chmod 0755 "$lab_state/tools/$name"
  elif [[ "$raw" == true ]]; then
    install -m 0755 "$archive" "$lab_state/tools/$name"
  fi
  printf 'Verified pinned binary archive: %s\n' "$name"
done < <(jq -r --arg mode "$mode" 'to_entries[] | select($mode == "all" or .value.action) | [.key,.value.url,.value.sha256,(.value.member // "-"),(.value.raw // false)] | @tsv' "$lab_repo/deploy/lab/binaries.json")
