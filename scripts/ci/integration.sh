#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
features=tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen
mode=${1:-all}
[[ "$mode" =~ ^(all|normal|faults)$ ]] || exit 2
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export TIDEMQ_TEST_REGISTRY_RPC_LOG=1
mkdir -p target/ci
export TMPDIR=$(mktemp -d "${TMPDIR:-/tmp}/tidemq-ci-integration.XXXXXX")
cleanup() {
  local result=$?
  trap - EXIT
  if (( result != 0 )); then
    node scripts/ci/failure-evidence.mjs "$TMPDIR" target/ci/failures || true
    [[ ! -f target/nextest/ci/junit.xml ]] || cp target/nextest/ci/junit.xml target/ci/failed-junit.xml
  fi
  rm -rf -- "$TMPDIR"
  exit "$result"
}
trap cleanup EXIT
target_dir=${CARGO_TARGET_DIR:-$PWD/target}
if [[ "$mode" != faults ]]; then
node --test tests/lab/*.test.mjs
bash scripts/ci/nextest.sh --workspace --all-targets --locked --features "$features" --profile ci -E 'kind(test)'
cp target/nextest/ci/junit.xml target/ci/integration-junit.xml
fi
if [[ "$mode" != normal ]]; then
# The dedicated binaries require test-hooks. For shared binaries, select only
# hook-specific cases and the two tests with distinct hook-enabled variants.
fault_filter='
  binary(=runtime_reclamation) |
  binary(=runtime_consumer_reads) |
  binary(=runtime_consumer_close) |
  binary(=runtime_group_watch) |
  binary(=sdk_consumer_long_poll) |
  test(=broker::application::group_watch::tests::local_committed_commands_publish_observations_without_notifying_rejections) |
  test(=runtime::auth::local_tests::metadata_authorization_does_not_reenter_its_only_core_handler) |
  test(=runtime::consumer_read_test_hooks::tests::persistent_fault_matches_scope_records_original_identity_and_releases) |
  test(=runtime::data::consumer_tests::consumer_retirement::a_stale_storage_leader_hint_cannot_turn_original_read_retirement_into_forbidden) |
  test(=runtime::data::consumer_tests::consumer_retirement::an_unknown_committed_read_retirement_recovers_through_a_stale_storage_leader_hint) |
  test(/\Aruntime::group_watch_reply_pressure::/) |
  test(=runtime::read_trace_test_hooks::tests::scopes_isolate_reused_ids_and_bound_registration_and_event_retention) |
  test(=scalable::data_membership::accepted_data_replacement_survives_controller_restart_before_native_prepare_and_keeps_one_original_plan) |
  test(=scalable::metadata_membership::cancelled_metadata_waiter_and_controller_restart_resume_one_original_membership_plan) |
  test(=scalable::metadata_membership::metadata_replacement_checkpoints_the_new_voter_and_old_seed_restart_keeps_original_sdk_working) |
  test(=scalable::node_drain::lost_local_retirement_proof_survives_a_restart_without_reopening_business_admission) |
  test(=scalable::registry::lost_split_reply_resumes_its_original_targets_after_new_capacity_becomes_preferable) |
  test(=scalable::registry::lost_topic_creation_recovers_its_original_identity_after_placement_load_changes) |
  test(=scalable::retention::creating_subscription_protects_available_history_across_metadata_restart) |
  test(=scalable::sdk::cancelled_partial_batch_keeps_the_known_receipt_and_original_pending_handles) |
  test(=scalable::transfer::broker_ack_delegation_refreshes_the_original_operation_when_the_authority_group_changes) |
  test(=scalable::transfer::operator_sdk_transfers_live_authority_and_tenant_credentials_cannot_move_or_inspect_it) |
  test(=topic_deletion::a_lost_publish_reply_recovers_its_original_success_after_topic_removal)'
bash scripts/ci/nextest.sh -p tidemq --lib --test runtime_queue --test runtime_consumer_reads --test runtime_consumer_close --test runtime_reclamation --test runtime_group_watch --test sdk_consumer_long_poll \
  --locked --features "$features,test-hooks" --profile ci -E "$fault_filter"
cp target/nextest/ci/junit.xml target/ci/runtime-faults-junit.xml
if [[ ${TIDEMQ_REQUIRE_EXTERNAL:-0} == 1 ]]; then
  [[ -x ${TIDEMQ_TEST_RUSTFS_BIN:-} && -x ${TIDEMQ_OTELCOL_BIN:-} ]] || { printf 'Verified external dependency binaries are required\n' >&2; exit 1; }
  "$TIDEMQ_TEST_RUSTFS_BIN" --version > target/ci/rustfs-version.txt
  "$TIDEMQ_OTELCOL_BIN" --version > target/ci/collector-version.txt
  sha256sum "$TIDEMQ_TEST_RUSTFS_BIN" "$TIDEMQ_OTELCOL_BIN" > target/ci/external-binary-sha256.txt
  cargo build --locked --example deploy_validation
  export TIDEMQ_TEST_DEPLOY_VALIDATION="$target_dir/debug/examples/deploy_validation"
  # These tests own independent dependency processes and data directories under tests/.
  bash scripts/ci/nextest.sh -p tidemq --all-targets --locked --features "$features,test-hooks" \
    --profile ci --run-ignored only -E 'kind(test)'
  cp target/nextest/ci/junit.xml target/ci/external-services-junit.xml
fi
fi
