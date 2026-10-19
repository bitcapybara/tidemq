#!/usr/bin/env bash
set -euo pipefail
umask 077

script_dir=$(cd "$(dirname "$0")" && pwd -P)
# Sourcing defines functions only. Every Docker call is replaced below.
source "$script_dir/docker-validate.sh"
task_dir=$(mktemp -d /tmp/tidemq-docker-harness-test.XXXXXXXX)
cleanup() {
  local file pid arguments
  for file in "$task_dir"/*.pid; do
    [[ -f $file ]] || continue
    pid=$(cat "$file")
    [[ $pid =~ ^[0-9]+$ && $pid -gt 1 ]] || continue
    arguments=$(ps -o args= -p "$pid" 2> /dev/null) || continue
    if [[ $arguments == *"$task_dir/"* ]]; then kill -KILL "$pid" 2> /dev/null || true; fi
  done
  rm -rf "$task_dir"
}
trap cleanup EXIT
check() { "$@" || { printf 'Harness self-test failed: %s\n' "$*" >&2; exit 1; }; }
reject() { if "$@" > /dev/null 2>&1; then printf 'Unexpected self-test success: %s\n' "$*" >&2; exit 1; fi; }

project=tidemq-validation-rf1-0123456789abcdef
expected_image="sha256:$(printf '%064d' 0)"
id=$(printf '%064d' 1)
evidence="$task_dir/evidence"; case_dir="$task_dir/case"
mkdir "$evidence" "$case_dir"
printf 'COMPOSE_PROJECT_NAME=%s\n' "$project" > "$task_dir/project.env"
[[ $(read_project "$task_dir/project.env") == "$project" ]]
for text in "COMPOSE_PROJECT_NAME=$project\n\n" "COMPOSE_PROJECT_NAME=$project" "COMPOSE_PROJECT_NAME=business\n" "COMPOSE_PROJECT_NAME=$project\r\n"; do
  printf '%b' "$text" > "$task_dir/invalid.env"
  reject read_project "$task_dir/invalid.env"
done
ln -s "$task_dir/project.env" "$task_dir/symlink.env"
reject read_project "$task_dir/symlink.env"
[[ $(quorum_voters metadata-quorum) == '11 12' ]]
[[ $(quorum_voters storage-quorum) == '21 22' ]]
reject quorum_voters storage-kill
reject quorum_voters unknown

mock_project=$project; mock_service=node-1; mock_image=$expected_image
mock_running=false; mock_paused=false; mock_status=exited:0
validator_case=selftest.123; validator_name="$project-$validator_case"
mock_name="/$validator_name"; mock_case=$validator_case; mock_collision=
calls="$task_dir/calls"; : > "$calls"
dk() {
  local command=$1; shift
  case "$command" in
    inspect)
      if [[ $1 == --type && ${3:-} == --format ]]; then
        [[ $5 == "$validator_name" ]] || return 2
        printf '%s\n' "$id"
      elif [[ $1 == --type ]]; then
        jq -n --arg id "$id" --arg image "$mock_image" --arg project "$mock_project" --arg service "$mock_service" --arg name "$mock_name" --arg case "$mock_case" \
          '[{Id:$id,Image:$image,Name:$name,Config:{Labels:{"com.docker.compose.project":$project,"com.docker.compose.service":$service,"io.tidemq.validation.case":$case}}}]'
      else
        case "$2" in
          '{{.State.Running}}') printf '%s\n' "$mock_running" ;;
          '{{.State.Paused}}') printf '%s\n' "$mock_paused" ;;
          '{{.State.Status}}:{{.State.ExitCode}}') printf '%s\n' "$mock_status" ;;
          '{{.HostConfig.Memory}}:{{.HostConfig.MemorySwap}}') printf '402653184:402653184\n' ;;
          '{{json .State}}') jq -n --argjson running "$mock_running" --arg status "${mock_status%:*}" --argjson code "${mock_status#*:}" '{Running:$running,Status:$status,ExitCode:$code}' ;;
          *) return 2 ;;
        esac
      fi
      ;;
    exec) shift; "$@" ;;
    container) printf '%s\n' "$*" > "$task_dir/name-filter"; printf '%s\n' "$mock_collision" ;;
    kill|start|pause|unpause|update)
      printf '%s %s\n' "$command" "$*" >> "$calls"
      case "$command" in
        start) mock_running=true ;;
        unpause) mock_paused=false ;;
        kill) if [[ $* == *'--signal KILL'* ]]; then mock_running=false; mock_status=exited:137; fi ;;
      esac
      ;;
    *) printf 'Unexpected mocked Docker command: %s\n' "$command" >&2; return 2 ;;
  esac
}
check verify "$id" node-1
check mutate "$id" node-1 kill --signal KILL
[[ $(wc -l < "$calls") -eq 1 ]]
: > "$calls"
mock_project=business; reject mutate "$id" node-1 kill --signal KILL; mock_project=$project
mock_service=node-2; reject mutate "$id" node-1 pause; mock_service=node-1
mock_image="sha256:$(printf '%064d' 2)"; reject mutate "$id" node-1 start; mock_image=$expected_image
[[ ! -s $calls ]]

validator=$id; mock_service=validator
check validator_name_free
grep -Fq "name=^/$project-selftest\\.123$" "$task_dir/name-filter"
mock_collision=$id; reject validator_name_free; mock_collision=
launch_attempted=true; validator=
check recover_validator_id
[[ $validator == "$id" && $launch_state == verified_after_launch_failure && $completed == false ]]
mock_case=other-case; validator=
reject recover_validator_id
[[ $launch_state == identity_mismatch ]]
mock_case=$validator_case; validator=$id
mock_name=/other-container; reject verify "$validator" validator; mock_name="/$validator_name"
check marker_write fault-injected
[[ -f $case_dir/fault-injected && ! -L $case_dir/fault-injected && ! -s $case_dir/fault-injected ]]
reject marker_write fault-injected
ln -s "$case_dir/fault-injected" "$case_dir/recovered"
reject marker_write recovered
printf '{"schema_version":1}\n' > "$case_dir/waiting-fault.json"
check marker_read waiting-fault.json 4096 "$task_dir/read.json"
cmp "$case_dir/waiting-fault.json" "$task_dir/read.json"
head -c 4097 /dev/zero > "$case_dir/waiting-fault.json"
reject marker_read waiting-fault.json 4096 "$task_dir/read.json"
mock_project=business; reject marker_write recovered; mock_project=$project

for node in 21 22 23; do
  jq -n --argjson node "$node" '{node_id:$node,data_groups:[{group_id:2,leader:23,term:4}]}' > "$task_dir/storage-$node.json"
done
check valid_storage 21 "$task_dir/storage-21.json"
reject valid_storage 22 "$task_dir/storage-21.json"

topic=12345678-1234-4321-8123-0123456789ab
for node in 11 12 13; do
  jq -n --arg partition "$topic/0" '{source:"local_applied",partition:$partition,group_id:2,phase:"active",owner_node:31,epoch:2,route_generation:4,revision:7,grant_metadata_revision:5}' > "$task_dir/owner-$node.json"
done
management() {
  if [[ $2 == /diagnostics ]]; then cp "$task_dir/storage-$1.json" "$3"; else cp "$task_dir/owner-$1.json" "$3"; fi
}
check storage_consensus before
[[ $(jq -r .leader "$evidence/storage-before.json") == 23 ]]
jq '.data_groups[0].leader = 31' "$task_dir/storage-21.json" > "$task_dir/storage-change.json"
reject valid_storage 21 "$task_dir/storage-change.json"
check storage_consensus before
jq '.data_groups[0].term = 5' "$task_dir/storage-22.json" > "$task_dir/storage-change.json"
mv "$task_dir/storage-change.json" "$task_dir/storage-22.json"
jq '.data_groups[0].term = 6' "$task_dir/storage-23.json" > "$task_dir/storage-change.json"
mv "$task_dir/storage-change.json" "$task_dir/storage-23.json"
reject storage_consensus before
check owner_consensus before
[[ $(jq -r .owner_node "$evidence/owner-before.json") == 31 ]]
jq '.owner_node = 32' "$task_dir/owner-13.json" > "$task_dir/owner-change.json"
mv "$task_dir/owner-change.json" "$task_dir/owner-13.json"
check owner_consensus before
jq '.phase = "granted"' "$task_dir/owner-12.json" > "$task_dir/owner-change.json"
mv "$task_dir/owner-change.json" "$task_dir/owner-12.json"
reject owner_consensus before
jq '.revision = 4' "$task_dir/owner-11.json" > "$task_dir/owner-change.json"
reject valid_owner "$task_dir/owner-change.json"

require_unavailable=false
printf '{"schema_version":1,"require_unavailable":false,"ack_frontier":72,"ledger":{"committed":72,"received":72,"acknowledged":72,"frontier":72},"batch":{"receipt_seen":true}}\n' > "$evidence/fault-observed.json"
check validate_stage fault-observed.json
jq '.ledger.frontier = 71' "$evidence/fault-observed.json" > "$task_dir/stage.json"
mv "$task_dir/stage.json" "$evidence/fault-observed.json"
reject validate_stage fault-observed.json

mock_service=node-1; affected_ids=(); affected_services=(); affected_actions=()
original_memory=402653184; original_swap=402653184; mock_running=false
remember "$id" node-1 oom
check restore_all
grep -q "^update --memory 402653184 --memory-swap 402653184 $id$" "$calls"
grep -q "^start $id$" "$calls"
[[ ${affected_actions[0]} == done && $mock_running == true ]]
affected_actions[0]=pause; mock_paused=true; mock_project=business; : > "$calls"
reject restore_all
[[ ! -s $calls && $mock_paused == true ]]
mock_project=$project
check restore_all
[[ $mock_paused == false ]]

mock_service=validator; mock_status=exited:1
reject wait_exit
mock_status=exited:0
check wait_exit
expected=64; message_bytes=1024; settle_ms=0; staged=false; require_unavailable=false
printf '{"schema_version":1,"status":"passed","client_close_completed":true,"same_consumer_session_verified":true,"configuration":{"messages":64,"message_bytes":1024,"replay_settle_ms":0,"stage_fault":false,"require_unavailable":false},"ledger":{"expected":64,"committed":64,"received":64,"acknowledged":64,"frontier":64}}\n' > "$evidence/report.json"
check validate_report
jq '.ledger.acknowledged = 63' "$evidence/report.json" > "$task_dir/report.json"
mv "$task_dir/report.json" "$evidence/report.json"
reject validate_report
mock_running=true; mock_status=running:0; : > "$calls"
check terminate_validator
grep -q '^kill --signal TERM ' "$calls"
grep -q '^kill --signal KILL ' "$calls"
[[ $(cat "$evidence/validator-exit.txt") == exited:137 && $mock_running == false ]]

timeout_tool=
for candidate in timeout gtimeout; do
  if command -v "$candidate" > /dev/null && [[ $("$candidate" --version) == *'GNU coreutils'* ]]; then
    timeout_tool=$(command -v "$candidate"); break
  fi
done
if [[ -n $timeout_tool ]]; then
  mkdir "$task_dir/fake"
  ln -s "$timeout_tool" "$task_dir/fake/timeout"
  cat > "$task_dir/fake/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$$" > "$SELFTEST_DIR/docker.pid"
"$SELFTEST_DIR/fake/plugin" &
wait
EOF
  cat > "$task_dir/fake/plugin" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$$" > "$SELFTEST_DIR/plugin.pid"
bash -c 'exec -a "$1" sleep 30' sh "$SELFTEST_DIR/sleeper" &
printf '%s\n' "$!" > "$SELFTEST_DIR/sleeper.pid"
wait
EOF
  chmod 0755 "$task_dir/fake/docker" "$task_dir/fake/plugin"
  if PATH="$task_dir/fake:$PATH" SELFTEST_DIR="$task_dir" bash -c \
    'source "$1"; OP_DEADLINE=$((SECONDS + 2)); dk compose version' sh "$script_dir/docker-validate.sh" > "$task_dir/timeout.log" 2>&1; then
    printf 'The fake plugin did not time out\n' >&2; exit 1
  fi
  for file in "$task_dir/docker.pid" "$task_dir/plugin.pid" "$task_dir/sleeper.pid"; do
    [[ -f $file ]] || { printf 'Fake process did not start\n' >&2; exit 1; }
    pid=$(cat "$file")
    until_time=$((SECONDS + 2))
    while kill -0 "$pid" 2> /dev/null; do
      process_state=$(ps -o stat= -p "$pid" 2> /dev/null) || break
      [[ $process_state != *Z* ]] || break
      [[ $SECONDS -lt $until_time ]] || { printf 'A fake Docker/plugin descendant survived the process-group deadline\n' >&2; exit 1; }
      sleep 0.1
    done
  done
  printf 'GNU timeout process-group test passed: fake Docker, plugin and sleeper stopped.\n'
elif [[ $(uname -s) == Linux ]]; then
  printf 'GNU timeout is required for the Linux process-group self-test\n' >&2
  exit 1
else
  printf 'SKIP: GNU timeout process-group test requires GNU coreutils; run it on the target Linux host.\n'
fi
printf 'Harness helper self-tests passed: project/identity/name guards, uncertain launch recovery, bounded markers, leader/owner checks, durable stages, scoped recovery and exit/report rejection. No Docker was used.\n'
