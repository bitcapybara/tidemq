#!/usr/bin/env bash
set -euo pipefail

if [[ -z ${TIDEMQ_NEXTEST_IMAGE:-} ]]; then
  exec cargo nextest run "$@"
fi
[[ $(uname -s) == Linux && $(id -u) == 1001 && $(id -g) == 1001 ]] || {
  printf 'Isolated nextest requires the Linux actions user\n' >&2; exit 2;
}
umask 077
[[ $TIDEMQ_NEXTEST_IMAGE =~ ^sha256:[a-f0-9]{64}$ &&
   ${GITHUB_RUN_ID:-} =~ ^[0-9]+$ && ${GITHUB_RUN_ATTEMPT:-} =~ ^[0-9]+$ ]] || {
  printf 'An immutable test image and numeric CI identity are required\n' >&2; exit 2;
}
build_arguments=("$@")
discovery_arguments=()
execution_arguments=()
metadata_flags=(--format-version 1 --locked)
while (( $# > 0 )); do
  argument=$1
  shift
  case $argument in
    --workspace|--lib|--bins|--examples|--all-targets|--locked)
      discovery_arguments+=("$argument") ;;
    -p|--package|--test|--features|-F)
      (( $# > 0 )) || { printf 'Missing nextest build argument value\n' >&2; exit 2; }
      discovery_arguments+=("$argument" "$1")
      if [[ $argument == --features || $argument == -F ]]; then metadata_flags+=("$argument" "$1"); fi
      shift ;;
    --profile|-P|-E|--filterset|--run-ignored)
      (( $# > 0 )) || { printf 'Missing nextest selection argument value\n' >&2; exit 2; }
      discovery_arguments+=("$argument" "$1")
      execution_arguments+=("$argument" "$1")
      shift ;;
    --profile=*|--filterset=*|--run-ignored=*|--ignore-default-filter)
      discovery_arguments+=("$argument")
      execution_arguments+=("$argument") ;;
    --test-threads|-j|--retries)
      (( $# > 0 )) || { printf 'Missing nextest runner argument value\n' >&2; exit 2; }
      execution_arguments+=("$argument" "$1")
      shift ;;
    --test-threads=*|--retries=*|--fail-fast|--no-fail-fast|--no-capture)
      execution_arguments+=("$argument") ;;
    --)
      discovery_arguments+=(-- "$@")
      execution_arguments+=(-- "$@")
      break ;;
    -*)
      printf 'Unsupported argument in the isolated CI nextest entry\n' >&2; exit 2 ;;
    *)
      discovery_arguments+=("$argument")
      execution_arguments+=("$argument") ;;
  esac
done

workspace=$(pwd -P)
target=${CARGO_TARGET_DIR:?An absolute Cargo target directory is required}
temporary=${TMPDIR:?An absolute test temporary directory is required}
for directory in "$workspace" "$target" "$temporary"; do
  [[ $directory == /* && $directory != / && $directory != *','* &&
     $directory != *$'\n'* && $directory != *$'\r'* && -d $directory &&
     $(cd -- "$directory" && pwd -P) == "$directory" ]] || {
    printf 'Test mounts must be canonical absolute directories\n' >&2; exit 2;
  }
  for private in /home/actions /srv/tidemq/actions-runner /srv/tidemq/container-runner/state /srv/tidemq/work/_temp; do
    [[ $directory != "$private" && $directory != "$private/"* && $private != "$directory/"* ]] || {
      printf 'Refusing a mount that exposes runner private state\n' >&2; exit 2;
    }
  done
done
unset DOCKER_CONTEXT DOCKER_HOST DOCKER_TLS_VERIFY DOCKER_CERT_PATH
docker=(docker --host unix:///var/run/docker.sock)
[[ $("${docker[@]}" info --format '{{.CgroupDriver}} {{.CgroupVersion}}') == 'systemd 2' &&
   $("${docker[@]}" image inspect "$TIDEMQ_NEXTEST_IMAGE" --format '{{.Id}}') == "$TIDEMQ_NEXTEST_IMAGE" ]] || {
  printf 'The installed test image and independent systemd cgroups are required\n' >&2; exit 2;
}

mkdir -p target/ci
metadata=$(mktemp -d "$workspace/target/ci/nextest.XXXXXX")
token=${metadata##*.}
name="tidemq-nextest-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT-$token"
container=
cleanup() {
  local result=$?
  trap - EXIT TERM INT
  if [[ -n $container ]]; then
    if ! "${docker[@]}" rm --force "$container" >/dev/null; then
      printf 'Owned nextest container cleanup failed; maintenance must reclaim it\n' >&2
      (( result != 0 )) || result=1
    fi
  fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

# Reuse both metadata files so the execution account cannot launch another Cargo build.
cargo nextest run --no-run "${build_arguments[@]}"
cargo nextest list --list-type binaries-only --message-format json "${discovery_arguments[@]}" > "$metadata/binaries.json"
cargo metadata "${metadata_flags[@]}" > "$metadata/cargo.json"

options=(create --name "$name" --label tidemq.lab/nextest=true
  --label "tidemq.lab/nextest-run=$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
  --label "tidemq.lab/nextest-token=$token" --user 1001:1001 --init --read-only
  --cgroup-parent system.slice --memory 6442450944 --memory-swap 6442450944 --cpus 4
  --pids-limit 1024 --cap-drop ALL --security-opt no-new-privileges:true
  --workdir "$workspace"
  --tmpfs /home/actions:rw,exec,uid=1001,gid=1001,mode=0700,size=64m
  --tmpfs /tmp:rw,noexec,uid=1001,gid=1001,mode=0700,size=64m)
for directory in "$workspace" "$target" "$temporary"; do
  options+=(--mount "type=bind,src=$directory,dst=$directory")
done
# Only public tool installation files are exposed; no runner home, registration or lab keys.
options+=(--mount type=bind,src=/srv/tidemq/rust/cargo/bin,dst=/opt/tidemq-rust/cargo/bin,readonly
  --mount type=bind,src=/srv/tidemq/rust/rustup,dst=/opt/tidemq-rust/rustup,readonly
  --mount type=bind,src=/srv/tidemq/tools,dst=/srv/tidemq/tools,readonly
  --mount type=bind,src=/srv/tidemq/actions-runner/externals/node24,dst=/opt/actions-runner/externals/node24,readonly)
environment=(CI GITHUB_SHA GITHUB_RUN_ID GITHUB_RUN_ATTEMPT
  CARGO_TARGET_DIR CARGO_LLVM_COV_TARGET_DIR CARGO_LLVM_COV_SHOW_ENV CARGO_LLVM_COV
  CARGO_BUILD_JOBS CARGO_INCREMENTAL CARGO_PROFILE_DEV_DEBUG CARGO_PROFILE_TEST_DEBUG
  CARGO_TERM_COLOR CARGO_HOME RUSTUP_HOME RUSTUP_TOOLCHAIN RUSTC_WRAPPER RUSTFLAGS
  CARGO_ENCODED_RUSTFLAGS RUSTDOCFLAGS LLVM_PROFILE_FILE TMPDIR RUST_LOG RUST_BACKTRACE
  TIDEMQ_OTELCOL_BIN TIDEMQ_REQUIRE_EXTERNAL TIDEMQ_KEEP_TEST_EVIDENCE TIDEMQ_CONSOLE_BROWSER_STATIC_DIR)
while IFS= read -r variable; do
  [[ $variable != TIDEMQ_TEST_* && $variable != NEXTEST_* ]] || environment+=("$variable")
done < <(compgen -e)
for variable in "${environment[@]}"; do
  [[ ! ${!variable+x} ]] || options+=(--env "$variable")
done
printf 'Nextest execution uses image %s, memory=6442450944 cpu=4, source/target/profile paths unchanged\n' "$TIDEMQ_NEXTEST_IMAGE"
container=$("${docker[@]}" "${options[@]}" --entrypoint /usr/bin/bash "$TIDEMQ_NEXTEST_IMAGE" --noprofile --norc -c '
  receipt=$1; shift
  {
    printf "cgroup\n"; cat /proc/self/cgroup
    for counter in memory.max memory.current cpu.max; do
      printf "%s=" "$counter"; cat "/sys/fs/cgroup/$counter"
    done
    awk '"'"'$1 == "anon" || $1 == "file" || $1 == "shmem"'"'"' /sys/fs/cgroup/memory.stat
  } > "$receipt"
  exec "$@"
' nextest "$metadata/scope.txt" /srv/tidemq/tools/cargo-nextest nextest run \
  --binaries-metadata "$metadata/binaries.json" --cargo-metadata "$metadata/cargo.json" "${execution_arguments[@]}")
[[ $container =~ ^[a-f0-9]{64}$ ]] || { container=; exit 2; }
"${docker[@]}" start --attach "$container" &
wait "$!"
result=$("${docker[@]}" wait "$container")
[[ $result =~ ^[0-9]+$ && $result -le 255 ]] || exit 2
exit "$result"
