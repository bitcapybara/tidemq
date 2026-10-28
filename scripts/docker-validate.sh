#!/usr/bin/env bash
set -euo pipefail
umask 077

project= state= evidence= network_id= validator= validator_name= validator_case= case_dir= expected_image= topic=
launch_attempted=false
launch_state=not_started validator_stopped=false
original_memory= original_swap= completed=false phase=preflight
affected_ids=() affected_services=() affected_actions=()

fail() { printf 'Docker validation failed: %s\n' "$*" >&2; return 1; }
dk() {
  local seconds=10 remaining
  if [[ ${OP_DEADLINE:-0} -gt 0 ]]; then
    remaining=$((OP_DEADLINE - SECONDS))
    if [[ $remaining -le 0 ]]; then
      printf 'Docker command deadline exhausted before invocation\n' >&2
      return 124
    fi
    if [[ $remaining -lt $seconds ]]; then seconds=$remaining; fi
  fi
  timeout --signal=KILL "${seconds}s" docker "$@"
}
dc() { COMPOSE_PROFILES= dk compose --project-name "$project" --env-file "$state/project.env" -f "$state/compose.yaml" "$@"; }

read_project() {
  local file=$1 line
  [[ -f $file && ! -L $file && $(wc -c < "$file") -le 128 && $(wc -l < "$file") -eq 1 ]] || return 1
  IFS= read -r line < "$file"
  [[ $line =~ ^COMPOSE_PROJECT_NAME=tidemq-validation-(rf1|rf3)-[0-9a-f]{16}$ ]] || return 1
  printf '%s\n' "${line#COMPOSE_PROJECT_NAME=}"
}
verify() {
  local id=$1 service=$2 document
  [[ $id =~ ^[0-9a-f]{64}$ && $expected_image =~ ^sha256:[0-9a-f]{64}$ ]] || return 1
  document=$(dk inspect --type container "$id") || return 1
  jq -e --arg id "$id" --arg project "$project" --arg service "$service" --arg image "$expected_image" '
    length == 1 and .[0].Id == $id and .[0].Image == $image and
    .[0].Config.Labels["com.docker.compose.project"] == $project and
    .[0].Config.Labels["com.docker.compose.service"] == $service
  ' <<< "$document" > /dev/null || return 1
  if [[ $service == validator ]]; then
    [[ -n $validator_name && -n $validator_case ]] || return 1
    jq -e --arg name "/$validator_name" --arg case "$validator_case" '
      .[0].Name == $name and .[0].Config.Labels["io.tidemq.validation.case"] == $case
    ' <<< "$document" > /dev/null || return 1
  fi
}
validator_name_free() {
  local escaped=${validator_name//./\\.} existing
  [[ $validator_name =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]+$ ]] || return 1
  existing=$(dk container ls --all --no-trunc --filter "name=^/$escaped$" --format '{{.ID}}') || return 1
  [[ -z $existing ]]
}
recover_validator_id() {
  local candidate OP_DEADLINE=$((SECONDS + 5))
  [[ $launch_attempted == true && -n $validator_name ]] || return 1
  while [[ $SECONDS -lt $OP_DEADLINE ]]; do
    if candidate=$(dk inspect --type container --format '{{.Id}}' "$validator_name" 2> "$evidence/validator-lookup-error"); then
      verify "$candidate" validator || { launch_state=identity_mismatch; return 1; }
      validator=$candidate
      launch_state=verified_after_launch_failure
      printf '%s\n' "$validator" > "$evidence/validator-id.txt"
      return 0
    fi
    sleep 0.2
  done
  launch_state=unresolved
  return 1
}
container_id() {
  local service=$1 id
  id=$(dc ps --all --quiet "$service") || return 1
  verify "$id" "$service" || return 1
  printf '%s\n' "$id"
}
mutate() {
  local id=$1 service=$2 command=$3
  shift 3
  verify "$id" "$service" || { fail "Refusing $command for an unverified $service container"; return 1; }
  dk "$command" "$@" "$id"
}
remember() {
  affected_ids+=("$1"); affected_services+=("$2"); affected_actions+=("$3")
}
quorum_voters() {
  case "$1" in
    metadata-quorum) printf '11 12\n' ;;
    storage-quorum) printf '21 22\n' ;;
    *) return 1 ;;
  esac
}

marker_read() {
  local name=$1 limit=$2 destination=$3
  verify "$validator" validator || return 1
  dk exec "$validator" /bin/sh -c '
    test -f "$1" && test ! -L "$1" || exit 3
    test "$(wc -c < "$1")" -le "$2" || exit 4
    cat "$1"
  ' sh "$case_dir/$name" "$limit" > "$destination"
}
marker_write() {
  local name=$1
  case "$name" in fault-injected|recovered) ;; *) return 1 ;; esac
  verify "$validator" validator || { fail 'Refusing a marker in an unverified validator'; return 1; }
  dk exec "$validator" /bin/sh -c '
    test -d "$1" && test ! -L "$1" || exit 1
    test ! -e "$1/$2" && test ! -L "$1/$2" || exit 1
    umask 077
    set -C
    : > "$1/$2"
  ' sh "$case_dir" "$name"
}
wait_marker() {
  local name=$1 OP_DEADLINE=$((SECONDS + 120))
  while [[ $SECONDS -lt $OP_DEADLINE ]]; do
    if marker_read "$name" 4096 "$evidence/$name.partial"; then
      jq -e 'type == "object" and .schema_version == 2' "$evidence/$name.partial" > /dev/null || return 1
      mv "$evidence/$name.partial" "$evidence/$name"
      return 0
    fi
    verify "$validator" validator || return 1
    [[ $(dk inspect --format '{{.State.Running}}' "$validator") == true ]] || return 1
    sleep 0.2
  done
  fail "Timed out waiting for $name"
}
wait_exit() {
  local OP_DEADLINE=$((SECONDS + 120)) status
  while [[ $SECONDS -lt $OP_DEADLINE ]]; do
    verify "$validator" validator || return 1
    status=$(dk inspect --format '{{.State.Status}}:{{.State.ExitCode}}' "$validator") || return 1
    if [[ $status == exited:* || $status == dead:* ]]; then
      printf '%s\n' "$status" > "$evidence/validator-exit.txt"
      [[ $status == exited:0 ]]
      return
    fi
    sleep 0.2
  done
  fail 'Validator exit exceeded 120 seconds'
}
management() {
  local node=$1 path=$2 destination=$3
  verify "$network_id" network || { fail 'Cached network holder identity verification failed'; return 1; }
  dk exec "$network_id" curl -q --fail --silent --show-error --noproxy '*' --connect-timeout 1 --max-time 2 \
    "http://127.0.0.1:$((8400 + node * 10))$path" | head -c 262144 > "$destination"
}
wait_ready() {
  local node=$1 OP_DEADLINE=$((SECONDS + 120))
  while [[ $SECONDS -lt $OP_DEADLINE ]]; do
    if management "$node" /readyz "$evidence/ready-$node.txt"; then return 0; fi
    sleep 0.2
  done
  fail "Node $node did not become ready within 120 seconds"
}
valid_storage() {
  jq -e --argjson node "$1" '
    .node_id == $node and (.data_groups | length) == 1 and .data_groups[0].group_id == 2 and
    (.data_groups[0].leader == 21 or .data_groups[0].leader == 22 or .data_groups[0].leader == 23) and
    (.data_groups[0].term | type == "number" and . > 0 and . <= 9007199254740991 and floor == .)
  ' "$2" > /dev/null
}
storage_consensus() {
  local phase=$1 node file files=()
  for node in 21 22 23; do
    file="$evidence/storage-$phase-$node.json"
    if management "$node" /diagnostics "$file" && valid_storage "$node" "$file"; then files+=("$file"); fi
  done
  [[ ${#files[@]} -ge 2 ]] || return 1
  jq -se '
    map(.data_groups[0] | {group_id,leader,term}) | group_by([.group_id,.leader,.term]) |
    map(select(length >= 2)) | if length == 1 then .[0][0] else error("No unique 2/3 storage leader agreement") end
  ' "${files[@]}" > "$evidence/storage-$phase.json"
}
wait_storage_transfer() {
  local prior_leader=$1 prior_term=$2 OP_DEADLINE=$((SECONDS + 120))
  while [[ $SECONDS -lt $OP_DEADLINE ]]; do
    if storage_consensus after && jq -e --argjson old "$prior_leader" --argjson term "$prior_term" \
      '.leader != $old and .term > $term' "$evidence/storage-after.json" > /dev/null; then return 0; fi
    sleep 0.2
  done
  fail 'No agreed storage leader transfer with a greater term while the original leader was offline'
}
validate_stage() {
  local name=$1
  if [[ $name == waiting-fault.json ]]; then
    jq -e --argjson unavailable "$require_unavailable" '
      .schema_version == 2 and .partition_id == 0 and .business_acknowledged_before == 64 and
      .fault_batch_first_sequence == 64 and .fault_batch_messages == 8 and .require_unavailable == $unavailable
    ' "$evidence/$name" > /dev/null
  else
    jq -e --argjson unavailable "$require_unavailable" '
      .schema_version == 2 and .require_unavailable == $unavailable and
      (if $unavailable then
        .business_acknowledged_before == 64 and .ledger.committed == 64 and .ledger.received == 64 and .ledger.acknowledged == 64 and .ledger.frontier == 64 and
        .batch.receipt_seen == false and .batch.original_pending > 0
      else
        .business_acknowledged_before == 72 and .ledger.committed == 72 and .ledger.received == 72 and
        .ledger.acknowledged == 72 and .ledger.frontier == 72 and .batch.receipt_seen == true
      end)
    ' "$evidence/$name" > /dev/null
  fi
}
valid_owner() {
  jq -e --arg partition "$topic/0" '
    .source == "local_applied" and .partition == $partition and .group_id == 2 and .phase == "active" and
    (.owner_node == 31 or .owner_node == 32) and
    ([.epoch,.route_generation,.revision,.grant_metadata_revision] | all(type == "number" and . >= 0 and . <= 9007199254740991 and floor == .)) and
    .epoch > 0 and .route_generation > 0 and .revision >= .grant_metadata_revision
  ' "$1" > /dev/null
}
owner_consensus() {
  local phase=$1 node file files=()
  for node in 11 12 13; do
    file="$evidence/owner-$phase-$node.json"
    if management "$node" "/ownership/$topic/0" "$file" && valid_owner "$file"; then files+=("$file"); fi
  done
  [[ ${#files[@]} -ge 2 ]] || return 1
  jq -se '
    group_by([.partition,.group_id,.phase,.owner_node,.epoch,.route_generation]) |
    map(select(length >= 2)) | if length == 1 then .[0][0] else error("No unique 2/3 owner agreement") end
  ' "${files[@]}" > "$evidence/owner-$phase.json"
}
wait_owner_transfer() {
  local prior_owner=$1 prior_epoch=$2 OP_DEADLINE=$((SECONDS + 120))
  while [[ $SECONDS -lt $OP_DEADLINE ]]; do
    if owner_consensus after && jq -e --argjson old "$prior_owner" --argjson epoch "$prior_epoch" \
      '.owner_node != $old and .epoch > $epoch' "$evidence/owner-after.json" > /dev/null; then return 0; fi
    sleep 0.2
  done
  fail 'No agreed Active owner transfer with a greater epoch while the original Broker was offline'
}

snapshot() {
  local phase=$1 node id failed=0 OP_DEADLINE=$((SECONDS + 30))
  for node in $node_ids; do
    id=$(container_id "node-$node") || { failed=1; continue; }
    dk inspect --type container "$id" | jq '.[0] | {Id,Image,Mounts,Labels:.Config.Labels,Memory:.HostConfig.Memory,MemorySwap:.HostConfig.MemorySwap,State}' \
      > "$evidence/$phase-node-$node-state.json" || failed=1
    dk logs --tail 200 --timestamps "$id" 2>&1 | head -c 262144 > "$evidence/$phase-node-$node.log" || \
      printf 'Log capture failed or reached its byte bound\n' > "$evidence/$phase-node-$node.log-error"
    management "$node" /metrics "$evidence/$phase-node-$node.metrics" 2> "$evidence/$phase-node-$node.metrics-error" || failed=1
    management "$node" /diagnostics "$evidence/$phase-node-$node.diagnostics.json" 2> "$evidence/$phase-node-$node.diagnostics-error" || failed=1
  done
  case "$phase" in before|after) return "$failed" ;; *) return 0 ;; esac
}
collect_driver() {
  [[ -n $validator ]] || return 0
  verify "$validator" validator || return 1
  dk logs --tail 200 --timestamps "$validator" 2>&1 | head -c 262144 > "$evidence/validator.log" || true
  dk inspect --format '{{json .State}}' "$validator" > "$evidence/validator-state.json" || true
  dk cp "$validator:$case_dir/report.json" "$evidence/report.json" 2> "$evidence/report-copy-error" || return 1
  [[ -f $evidence/report.json && ! -L $evidence/report.json && $(wc -c < "$evidence/report.json") -le 65536 ]]
}
terminate_validator() {
  local OP_DEADLINE=$((SECONDS + 5)) term_until=$((SECONDS + 2)) running document
  verify "$validator" validator || return 1
  running=$(dk inspect --format '{{.State.Running}}' "$validator") || return 1
  if [[ $running == true ]]; then
    mutate "$validator" validator kill --signal TERM > /dev/null || true
    while [[ $SECONDS -lt $term_until ]]; do
      running=$(dk inspect --format '{{.State.Running}}' "$validator") || return 1
      [[ $running == true ]] || break
      sleep 0.2
    done
    if [[ $running == true ]]; then mutate "$validator" validator kill --signal KILL > /dev/null || return 1; fi
  fi
  while [[ $SECONDS -lt $OP_DEADLINE ]]; do
    document=$(dk inspect --format '{{json .State}}' "$validator") || return 1
    printf '%s\n' "$document" > "$evidence/validator-state.json"
    if jq -e '.Running == false and (.Status == "exited" or .Status == "dead")' <<< "$document" > /dev/null; then
      jq -r '.Status + ":" + (.ExitCode | tostring)' <<< "$document" > "$evidence/validator-exit.txt"
      validator_stopped=true
      return 0
    fi
    sleep 0.2
  done
  fail 'Validator did not stop within the shared five-second cleanup deadline'
}
capture_cgroup() {
  local phase=$1 id=$2 field
  verify "$id" node-1 || return 1
  for field in memory.max memory.current memory.events; do
    dk exec "$id" cat "/sys/fs/cgroup/$field" | head -c 4096 > "$evidence/oom-$phase-$field" || return 1
  done
  [[ $(cat "$evidence/oom-$phase-memory.max") == "$original_memory" ]]
}
restore_all() {
  local index id service action running paused limits failed=0 OP_DEADLINE=$((SECONDS + 30))
  for ((index=${#affected_ids[@]}-1; index>=0; index--)); do
    id=${affected_ids[$index]}; service=${affected_services[$index]}; action=${affected_actions[$index]}
    [[ $action != done ]] || continue
    if ! verify "$id" "$service"; then failed=1; continue; fi
    if [[ $action == pause ]]; then
      paused=$(dk inspect --format '{{.State.Paused}}' "$id") || { failed=1; continue; }
      [[ $paused == true || $paused == false ]] || { failed=1; continue; }
      if [[ $paused == true ]]; then
        mutate "$id" "$service" unpause > /dev/null || { failed=1; continue; }
      fi
    else
      if [[ $action == oom ]]; then
        mutate "$id" "$service" update --memory "$original_memory" --memory-swap "$original_swap" > /dev/null || { failed=1; continue; }
        limits=$(dk inspect --format '{{.HostConfig.Memory}}:{{.HostConfig.MemorySwap}}' "$id") || { failed=1; continue; }
        [[ $limits == "$original_memory:$original_swap" ]] || { failed=1; continue; }
      fi
    fi
    running=$(dk inspect --format '{{.State.Running}}' "$id") || { failed=1; continue; }
    [[ $running == true || $running == false ]] || { failed=1; continue; }
    if [[ $running != true ]]; then mutate "$id" "$service" start > /dev/null || { failed=1; continue; }; fi
    affected_actions[$index]=done
  done
  return "$failed"
}
fault_active() {
  local index id service action document
  for ((index=0; index<${#affected_ids[@]}; index++)); do
    id=${affected_ids[$index]}; service=${affected_services[$index]}; action=${affected_actions[$index]}
    verify "$id" "$service" || return 1
    document=$(dk inspect --format '{{json .State}}' "$id") || return 1
    printf '%s\n' "$document" > "$evidence/fault-active-$service.json"
    case "$action" in
      pause) jq -e '.Running == true and .Paused == true' <<< "$document" > /dev/null || return 1 ;;
      kill) jq -e '.Running == false and .Paused == false' <<< "$document" > /dev/null || return 1 ;;
      oom) jq -e '.Running == false and .OOMKilled == true' <<< "$document" > /dev/null || return 1 ;;
      *) return 1 ;;
    esac
  done
}
finish() {
  local status=$? OP_DEADLINE=0
  trap - EXIT INT TERM
  if ! restore_all; then status=1; completed=false; fi
  if [[ $completed != true && -n $evidence ]]; then
    if [[ $launch_attempted == true ]] && ! verify "$validator" validator; then
      recover_validator_id || printf 'The exact validator launch identity could not be recovered\n' > "$evidence/validator-recovery-error"
    fi
    if [[ -n $validator ]] && verify "$validator" validator; then
      if ! terminate_validator; then status=1; fi
      collect_driver || true
    fi
    snapshot failure || true
    jq -n --arg project "$project" --arg case "${case_name:-unknown}" --arg image "$expected_image" --arg phase "$phase" --arg validator "$validator" \
      --arg name "$validator_name" --arg label "$validator_case" --arg launch "$launch_state" --argjson stopped "$validator_stopped" --argjson exit_code "$status" \
      '{status:"failed",project:$project,case:$case,image_id:$image,validator:$validator,validator_name:$name,case_label:$label,launch_state:$launch,validator_stopped:$stopped,failure_phase:$phase,exit_code:$exit_code}' > "$evidence/harness-report.json"
    printf 'Docker validation failed at %s (exit %s)\n' "$phase" "$status" >&2
  fi
  if [[ -n $evidence ]]; then printf 'Docker validation evidence: %s\n' "$evidence"; fi
  exit "$status"
}
validate_report() {
  jq -e --argjson expected "$expected" --argjson bytes "$message_bytes" --argjson settle "$settle_ms" \
    --argjson staged "$staged" --argjson unavailable "$require_unavailable" '
    .schema_version == 2 and .status == "passed" and .client_close_completed == true and .original_consumer_reused == true and
    .configuration.messages == 64 and .configuration.message_bytes == $bytes and .configuration.replay_settle_ms == $settle and
    .configuration.stage_fault == $staged and .configuration.require_unavailable == $unavailable and
    .ledger.expected == $expected and .ledger.committed == $expected and .ledger.received == $expected and
    .ledger.acknowledged == $expected and .ledger.frontier == $expected
  ' "$evidence/report.json" > /dev/null
}

main() {
  [[ $# -eq 2 && $(uname -s) == Linux ]] || { fail 'Usage on Linux: docker-validate.sh STATE smoke|replay|storage-kill|broker-kill|metadata-quorum|storage-quorum|gateway-kill|oom'; return 1; }
  for tool in docker timeout jq awk head wc mktemp stat cat mv mkdir basename sleep; do command -v "$tool" > /dev/null || return 1; done
  [[ -n ${TIDEMQ_IMAGE:-} && -n ${RUSTFS_BINARY:-} ]] || { fail 'TIDEMQ_IMAGE and RUSTFS_BINARY are required'; return 1; }
  state=$(cd "$1" && pwd -P); case_name=$2
  project=$(read_project "$state/project.env") || { fail 'project.env must be one canonical validation project line'; return 1; }
  mode=${project#tidemq-validation-}; mode=${mode%%-*}
  case "$mode:$case_name" in
    rf1:smoke|rf1:replay|rf1:oom|rf3:smoke|rf3:replay|rf3:storage-kill|rf3:broker-kill|rf3:metadata-quorum|rf3:storage-quorum|rf3:gateway-kill) ;;
    *) fail 'Unsupported case for this topology'; return 1 ;;
  esac
  expected_image=$(dk image inspect --format '{{.Id}}' "$TIDEMQ_IMAGE")
  [[ $expected_image =~ ^sha256:[0-9a-f]{64}$ ]] || return 1
  node_ids=1; gateway=1
  if [[ $mode == rf3 ]]; then node_ids='11 12 13 21 22 23 31 32 41 42'; gateway=41; fi
  [[ ! -L $state/evidence ]] || return 1
  if [[ ! -d $state/evidence ]]; then mkdir -m 0700 "$state/evidence"; fi
  [[ $(stat -c %a "$state/evidence") == 700 ]] || return 1
  evidence=$(mktemp -d "$state/evidence/$case_name.XXXXXXXX")
  trap finish EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  local node id config
  network_id=$(container_id network)
  for node in $node_ids; do
    id=$(container_id "node-$node")
    [[ $(dk inspect --format '{{.State.Running}}:{{.State.Paused}}' "$id") == true:false ]] || return 1
  done
  config=$(dc --profile tools config --format json)
  jq -e --arg image "$TIDEMQ_IMAGE" '.services.validator | .image == $image and .pull_policy == "never" and .network_mode == "service:network"' <<< "$config" > /dev/null
  snapshot before
  if [[ $mode == rf1 ]]; then
    wait_ready 1
    id=$(container_id node-1)
    dk exec "$id" curl -q --fail --silent --show-error --noproxy '*' --connect-timeout 1 --max-time 2 \
      --proto '=https' --cacert /run/tidemq/ca.pem https://localhost:8443/ \
      | head -c 262144 > "$evidence/combined-https-index.html"
  fi
  phase=warmup
  message_bytes=1024; settle_ms=0; staged=false; require_unavailable=false; expected=64
  local args=(--messages 64)
  if [[ $case_name == replay ]]; then message_bytes=65536; settle_ms=5000; fi
  case "$case_name" in
    smoke|replay) ;;
    *) staged=true; expected=80; args+=(--stage-fault) ;;
  esac
  case "$case_name" in metadata-quorum|storage-quorum|gateway-kill|oom) require_unavailable=true; args+=(--require-unavailable) ;; esac
  validator_case=$(basename "$evidence")
  validator_name="$project-$validator_case"
  case_dir="/var/lib/tidemq/$validator_case"
  validator_name_free || { fail 'Validator name exists or its absence could not be confirmed'; return 1; }
  launch_attempted=true
  launch_state=uncertain
  validator=$(dc --profile tools run --detach --no-deps --name "$validator_name" --label "io.tidemq.validation.case=$validator_case" \
    validator --endpoint "127.0.0.1:$((7400 + gateway * 10))" \
    --ca /run/client/ca.pem --token-file /run/client/access.token \
    --output-dir "$case_dir" --message-bytes "$message_bytes" --replay-settle-ms "$settle_ms" "${args[@]}")
  verify "$validator" validator
  launch_state=verified
  printf '%s\n' "$validator" > "$evidence/validator-id.txt"
  if [[ $staged == true ]]; then
    wait_marker waiting-fault.json
    validate_stage waiting-fault.json
    phase=injection
    topic=$(jq -er '.topic_id | select(test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))' "$evidence/waiting-fault.json")
    local target= prior_owner= prior_epoch= prior_term= OP_DEADLINE
    case "$case_name" in
      storage-kill)
        storage_consensus before
        target=$(jq -er '.leader' "$evidence/storage-before.json")
        prior_term=$(jq -er '.term' "$evidence/storage-before.json")
        ;;
      broker-kill)
        owner_consensus before
        prior_owner=$(jq -er '.owner_node' "$evidence/owner-before.json")
        prior_epoch=$(jq -er '.epoch' "$evidence/owner-before.json")
        target=$prior_owner
        ;;
      gateway-kill) target=41 ;;
      oom) target=1 ;;
      metadata-quorum|storage-quorum)
        for node in $(quorum_voters "$case_name"); do
          id=$(container_id "node-$node"); remember "$id" "node-$node" pause
          mutate "$id" "node-$node" pause > /dev/null
        done
        sleep 4
        ;;
    esac
    if [[ -n $target ]]; then
      id=$(container_id "node-$target")
      if [[ $case_name == oom ]]; then
        original_memory=$(dk inspect --format '{{.HostConfig.Memory}}' "$id")
        original_swap=$(dk inspect --format '{{.HostConfig.MemorySwap}}' "$id")
        [[ $original_memory == 402653184 && $original_swap == "$original_memory" ]] || return 1
        printf '%s:%s\n' "$original_memory" "$original_swap" > "$evidence/oom-original-limits.txt"
        capture_cgroup before "$id"
        remember "$id" "node-$target" oom
        mutate "$id" "node-$target" update --memory 6291456 --memory-swap 6291456 > /dev/null
        dk inspect --format '{{.HostConfig.Memory}}:{{.HostConfig.MemorySwap}}' "$id" > "$evidence/oom-lowered-limits.txt"
        [[ $(cat "$evidence/oom-lowered-limits.txt") == 6291456:6291456 ]] || return 1
        OP_DEADLINE=$((SECONDS + 120))
        until [[ $(dk inspect --format '{{.State.OOMKilled}}:{{.State.Running}}:{{.State.ExitCode}}' "$id") == true:false:137 ]]; do
          [[ $SECONDS -lt $OP_DEADLINE ]] || return 1
          sleep 0.2
        done
        dk inspect --format '{{json .State}}' "$id" > "$evidence/oom-killed-state.json"
        dk inspect --type container "$id" | jq '.[0] | {Memory:.HostConfig.Memory,MemorySwap:.HostConfig.MemorySwap,State}' > "$evidence/oom-lowered-state.json"
        jq -e '.Memory == 6291456 and .MemorySwap == 6291456 and .State.OOMKilled == true and .State.Running == false and .State.ExitCode == 137' \
          "$evidence/oom-lowered-state.json" > /dev/null
        OP_DEADLINE=0
      else
        remember "$id" "node-$target" kill
        mutate "$id" "node-$target" kill --signal KILL > /dev/null
      fi
      printf '%s\n' "$target" > "$evidence/fault-target-node.txt"
    fi
    fault_active
    marker_write fault-injected
    phase=fault-observation
    wait_marker fault-observed.json
    validate_stage fault-observed.json
    fault_active
    if [[ $case_name == storage-kill ]]; then wait_storage_transfer "$target" "$prior_term"; fi
    if [[ $case_name == broker-kill ]]; then wait_owner_transfer "$prior_owner" "$prior_epoch"; fi
    fault_active
    snapshot during
    phase=recovery
    restore_all
    for node in "${affected_services[@]}"; do wait_ready "${node#node-}"; done
    if [[ $case_name == oom ]]; then capture_cgroup restored "$id"; fi
    marker_write recovered
  fi
  phase=exit-and-report
  wait_exit
  collect_driver
  validate_report
  snapshot after
  jq -n --arg project "$project" --arg case "$case_name" --arg image "$expected_image" --arg validator "$validator" \
    '{status:"passed",project:$project,case:$case,image_id:$image,validator:$validator,scope:"one_host_shared_network_namespace"}' > "$evidence/harness-report.json"
  completed=true
  printf 'Docker validation passed: %s\n' "$case_name"
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then main "$@"; fi
