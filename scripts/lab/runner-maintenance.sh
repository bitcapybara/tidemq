#!/usr/bin/env bash
set -euo pipefail

empty_cache() {
  local directory=$1
  [[ -d "$directory" && ! -L "$directory" && $(realpath -e "$directory") == "$directory" ]] || {
    printf 'Refusing a missing or redirected cache: %s\n' "$directory" >&2; return 1;
  }
  find "$directory" -xdev -mindepth 1 -maxdepth 1 -exec rm -rf --one-file-system -- {} +
}

bound_cache() {
  local directory=$1 bytes=$2 days=$3 size
  [[ -e "$directory" || -L "$directory" ]] || return 0
  [[ -d "$directory" && ! -L "$directory" && $(realpath -e "$directory") == "$directory" ]] || return 1
  find "$directory" -xdev -mindepth 1 -maxdepth 1 -mtime "+$days" -exec rm -rf --one-file-system -- {} +
  size=$(du -sx -B1 "$directory" | cut -f 1)
  if (( size > bytes )); then empty_cache "$directory"; fi
}

free_bytes() { df --output=avail -B1 "$1" | tail -n 1 | tr -d ' '; }

clean_checkouts() {
  local directory=$1 workspace=${2:-$1/tidemq/tidemq} relative repository
  [[ -d "$directory" && ! -L "$directory" && $(realpath -e "$directory") == "$directory" ]] || return 1
  [[ "$workspace" == "$directory/"* && $(realpath -m "$workspace") == "$workspace" ]] || return 1
  relative=${workspace#"$directory/"}
  repository=${workspace%/*}
  [[ "$relative" != _* && ${repository%/*} == "$directory" ]] || return 1
  # Keep these directories present even if a Worker starts between the timer check and its hook.
  mkdir -p -- "$workspace"
  find "$directory" -mindepth 1 -maxdepth 1 ! -name '_*' ! -path "$repository" -exec rm -rf --one-file-system -- {} +
  find "$repository" -mindepth 1 -maxdepth 1 ! -path "$workspace" -exec rm -rf --one-file-system -- {} +
  empty_cache "$workspace"
}

sweep() {
  local repo=$1 phase=$2 run runs status=0 directory
  runs=$(kubectl --request-timeout=15s get namespaces -l tidemq.lab/ci=true -o json |
    jq -er '[.items[].metadata.name] | join("\n")') || return 1
  if [[ -d "$TIDEMQ_LAB_HOME/runs" ]]; then
    while IFS= read -r -d '' directory; do
      [[ ! -f "$directory/.tidemq-ci" ]] || runs+=$'\n'"${directory##*/}"
    done < <(find "$TIDEMQ_LAB_HOME/runs" -mindepth 1 -maxdepth 1 -type d -print0)
  fi
  while IFS= read -r run; do
    [[ -n "$run" ]] || continue
    [[ "$run" =~ ^ci-[0-9]+-[0-9]+$ ]] || { printf 'Invalid disposable CI identity: %s\n' "$run" >&2; status=1; continue; }
    bash "$repo/scripts/lab/cleanup-ci.sh" "$run" || status=1
  done < <(printf '%s\n' "$runs" | sort -u)

  # Job hooks run outside the checkout, after artifact uploads and action post steps.
  clean_checkouts /srv/tidemq/work "${GITHUB_WORKSPACE:-}"
  # A new Worker can prepare actions before its started hook acquires our lock.
  if [[ "$phase" == after ]]; then bound_cache /srv/tidemq/work/_actions "$ACTION_CACHE_MAX_BYTES" "$CACHE_MAX_DAYS"; fi
  # Runner file commands may still be open during a hook; only remove old temporary entries.
  if [[ -d /srv/tidemq/work/_temp ]]; then
    find /srv/tidemq/work/_temp -xdev -mindepth 1 -maxdepth 1 -mtime +1 -exec rm -rf --one-file-system -- {} +
  fi
  bound_cache /srv/tidemq/cargo-target "$TARGET_MAX_BYTES" "$CACHE_MAX_DAYS"
  for directory in /srv/tidemq/rust/cargo/registry /srv/tidemq/rust/cargo/git; do
    bound_cache "$directory" "$CARGO_CACHE_MAX_BYTES" "$CACHE_MAX_DAYS"
  done
  bound_cache /srv/tidemq/tmp "$TMP_MAX_BYTES" 1
  find /srv/tidemq/actions-runner/_diag -type f -mtime +7 -delete
  if (( $(du -sx -B1 /srv/tidemq/actions-runner/_diag | cut -f 1) > DIAGNOSTIC_MAX_BYTES )); then
    find /srv/tidemq/actions-runner/_diag -type f -mtime +1 -delete
  fi
  [[ $(docker --host unix:///var/run/docker.sock info --format '{{.DockerRootDir}}') == /srv/tidemq/docker ]] || return 1
  [[ -z $(docker --host unix:///var/run/docker.sock ps -q) ]] || {
    printf 'Refusing Docker cleanup while a runner container is active\n' >&2; return 1;
  }
  docker --host unix:///var/run/docker.sock container prune --force
  docker --host unix:///var/run/docker.sock image prune --all --force
  docker --host unix:///var/run/docker.sock builder prune --all --force
  if (( $(free_bytes /) < ROOT_MIN_FREE_BYTES || $(free_bytes /srv/tidemq) < DATA_MIN_FREE_BYTES )); then
    for directory in /srv/tidemq/cargo-target /srv/tidemq/rust/cargo/registry /srv/tidemq/rust/cargo/git /srv/tidemq/tmp; do
      [[ ! -d "$directory" ]] || empty_cache "$directory"
    done
  fi
  df -B1 / /srv/tidemq
  (( $(free_bytes /) >= ROOT_MIN_FREE_BYTES && $(free_bytes /srv/tidemq) >= DATA_MIN_FREE_BYTES )) || {
    printf 'Runner free disk budget is insufficient; persistent data was preserved\n' >&2; return 1;
  }
  return "$status"
}

main() {
  local phase=${1:-} repo policy_name
  [[ "$phase" =~ ^(before|after|idle|sweep)$ && $(cat /etc/tidemq-lab-role) == runner && $(id -un) == actions ]] || exit 2
  mountpoint -q /srv/tidemq
  repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
  export TIDEMQ_LAB_HOME=/home/actions/.config/tidemq-lab
  export KUBECONFIG="$TIDEMQ_LAB_HOME/kubeconfig"
  export HOME=/home/actions PATH=/usr/local/bin:/usr/bin:/bin:/srv/tidemq/tools
  unset DOCKER_CONTEXT DOCKER_TLS_VERIFY DOCKER_CERT_PATH
  source "$repo/deploy/lab/runner-maintenance.env"
  for policy_name in TARGET_MAX_BYTES CARGO_CACHE_MAX_BYTES ACTION_CACHE_MAX_BYTES TMP_MAX_BYTES DIAGNOSTIC_MAX_BYTES ROOT_MIN_FREE_BYTES DATA_MIN_FREE_BYTES CACHE_MAX_DAYS; do
    [[ ${!policy_name} =~ ^[0-9]{1,12}$ ]] || exit 2
  done
  cd /
  if [[ "$phase" == sweep ]]; then
    [[ ${TIDEMQ_LAB_LEASE_HELD:-} == 1 ]] || exit 2
    [[ ${2:-} =~ ^(before|after|idle)$ ]] || exit 2
    sweep "$repo" "$2"
    return
  fi
  exec 9>/srv/tidemq/maintenance/cleanup.lock
  if [[ "$phase" == idle ]]; then
    flock -n 9 || return 0
    if pgrep -u actions -f '[R]unner.Worker' >/dev/null; then printf 'Runner is busy; idle cleanup skipped\n'; return; fi
  else
    flock -w 600 9
  fi
  printf 'Runner cleanup started: %s\n' "$phase"
  timeout --signal=TERM --kill-after=30s 10m bash "$repo/scripts/lab/with-lease.sh" bash "$repo/scripts/lab/runner-maintenance.sh" sweep "$phase"
  printf 'Runner cleanup completed: %s\n' "$phase"
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then main "$@"; fi
