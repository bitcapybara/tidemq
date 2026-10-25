#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
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
    printf '%s  %s\n' "$digest" "$archive.part" | sha256sum --check --status
    mv "$archive.part" "$archive"
  fi
  printf '%s  %s\n' "$digest" "$archive" | sha256sum --check --status
  if [[ "$member" != - ]]; then
    if [[ "$url" == *.zip ]]; then unzip -p "$archive" "$member" > "$lab_state/tools/$name";
    else tar -xOf "$archive" "$member" > "$lab_state/tools/$name"; fi
    chmod 0755 "$lab_state/tools/$name"
  elif [[ "$raw" == true ]]; then
    install -m 0755 "$archive" "$lab_state/tools/$name"
  fi
  printf 'Verified pinned binary archive: %s\n' "$name"
done < <(jq -r 'to_entries[] | [.key,.value.url,.value.sha256,(.value.member // "-"),(.value.raw // false)] | @tsv' "$lab_repo/deploy/lab/binaries.json")
