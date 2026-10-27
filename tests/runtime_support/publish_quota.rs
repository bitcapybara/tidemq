use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use base64::{Engine, engine::general_purpose::STANDARD};
use tidemq_protocol::{
    Id,
    console_operation::{Operation, Status, WriteCommand, WriteRequest},
};
use tidemq_sdk::{Client, ResourceLimits};
use tokio::sync::Notify;

use super::*;
use crate::{
    config::Role,
    metadata::{AllocationRequest, QuotaGrant},
    runtime::data::consumer_tests::fixture::{Fixture, deadline},
};

static PUBLISHES: Mutex<BTreeMap<Id, Arc<PublishPause>>> = Mutex::new(BTreeMap::new());

#[derive(Clone, Copy, Eq, PartialEq)]
enum PauseMode {
    FirstPreflight,
    EveryPreflight,
    Observe,
    LoseCompletion,
}

struct PublishPause {
    mode: PauseMode,
    requests: Mutex<Vec<(PublishRequest, Instant)>>,
    writers: Mutex<Vec<(u64, u64)>>,
    failures: Mutex<Vec<super::super::Failure>>,
    ready: Notify,
    resume: Notify,
    refused: Notify,
}

struct InstalledPause {
    producer: Id,
    state: Arc<PublishPause>,
}

impl InstalledPause {
    fn new(producer: Id) -> Self {
        Self::with_mode(producer, PauseMode::FirstPreflight)
    }

    fn with_mode(producer: Id, mode: PauseMode) -> Self {
        let state = Arc::new(PublishPause {
            mode,
            requests: Mutex::new(Vec::new()),
            writers: Mutex::new(Vec::new()),
            failures: Mutex::new(Vec::new()),
            ready: Notify::new(),
            resume: Notify::new(),
            refused: Notify::new(),
        });
        assert!(
            PUBLISHES
                .lock()
                .unwrap()
                .insert(producer, state.clone())
                .is_none()
        );
        Self { producer, state }
    }
}

impl Drop for InstalledPause {
    fn drop(&mut self) {
        PUBLISHES.lock().unwrap().remove(&self.producer);
        self.state.resume.notify_one();
    }
}

pub(super) async fn before_publish(request: &PublishRequest, deadline: Instant) {
    let pause = PUBLISHES
        .lock()
        .unwrap()
        .get(&request.identity.producer)
        .cloned();
    if let Some(pause) = pause {
        let first = {
            let mut requests = pause.requests.lock().unwrap();
            requests.push((request.clone(), deadline));
            requests.len() == 1
        };
        if pause.mode == PauseMode::EveryPreflight
            || pause.mode == PauseMode::FirstPreflight && first
        {
            pause.ready.notify_one();
            pause.resume.notified().await;
        }
    }
}

pub(super) fn after_publish(
    request: &PublishRequest,
    result: &Result<DataResponse>,
) -> Option<Error> {
    let pause = PUBLISHES
        .lock()
        .unwrap()
        .get(&request.identity.producer)
        .cloned();
    let pause = pause?;
    let injected = (pause.mode == PauseMode::LoseCompletion
        && result.is_ok()
        && pause.failures.lock().unwrap().is_empty())
    .then(Error::unknown);
    if let Some(error) = injected.as_ref().or_else(|| result.as_ref().err()) {
        pause.failures.lock().unwrap().push(error.failure());
        pause.refused.notify_one();
    }
    injected
}

pub(super) fn writer_for_publish(request: &PublishRequest, writer: (u64, u64)) {
    if let Some(pause) = PUBLISHES.lock().unwrap().get(&request.identity.producer) {
        pause.writers.lock().unwrap().push(writer);
    }
}

struct Prepared {
    fixture: Fixture,
    client: Client,
    service: Arc<Service>,
    tenant: tidemq_sdk::TenantId,
    partition: crate::metadata::PartitionKey,
    request: WriteRequest,
}

async fn prepared() -> Prepared {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("quota-race").await.unwrap();
    client
        .admin()
        .set_tenant_limits(
            tenant,
            ResourceLimits::builder()
                .publish_bytes_per_second(16 * 1024 * 1024)
                .consume_bytes_per_second(16 * 1024 * 1024)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = client
        .admin()
        .create_topic("quota-race/app/events")
        .await
        .unwrap();
    let topic: Id = topic.id().into();
    let service = fixture.running.get(&1).unwrap().service.clone();
    let partition = crate::metadata::PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let request = WriteRequest {
        operation_id: Id::new_v7().unwrap(),
        command: WriteCommand::Send {
            topic,
            payload_base64: STANDARD.encode(vec![b'q'; 4096]),
            key_base64: Some(STANDARD.encode("original-key")),
            properties_base64: BTreeMap::from([(
                "source".into(),
                STANDARD.encode("quota-interleave"),
            )]),
        },
    };
    Prepared {
        fixture,
        client,
        service,
        tenant,
        partition,
        request,
    }
}

async fn quota_transition(
    service: &Service,
    current: &QuotaGrant,
    rate: u64,
    frozen: bool,
) -> QuotaGrant {
    // Keep this quota interleaving independent of the separately reviewed control-lease boundary.
    service
        .data_lease_for(2, current.partition, deadline(), Duration::from_secs(1))
        .await
        .unwrap();
    let mut capacity = current.capacity;
    capacity.publish_bytes_per_second = rate;
    let prepare = serde_json::json!({"Prepare": AllocationRequest {
        partition: current.partition, tenant: current.tenant, namespace: current.namespace,
        expected_version: current.version, capacity, frozen,
    }});
    let response = service
        .quota_control_command(1, &serde_json::to_vec(&prepare).unwrap(), deadline())
        .await
        .unwrap();
    let response: serde_json::Value = serde_json::from_slice(&response).unwrap();
    let grant: QuotaGrant = serde_json::from_value(response.get("Grant").unwrap().clone()).unwrap();
    let installed = service
        .quota_data_command(
            2,
            &serde_json::to_vec(&serde_json::json!({"Install": grant})).unwrap(),
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<QuotaGrant>(&installed).unwrap(),
        grant
    );
    let confirmed = service
        .quota_control_command(
            1,
            &serde_json::to_vec(&serde_json::json!({"Installed": grant})).unwrap(),
            deadline(),
        )
        .await
        .unwrap();
    let confirmed: serde_json::Value = serde_json::from_slice(&confirmed).unwrap();
    assert_eq!(
        serde_json::from_value::<QuotaGrant>(confirmed.get("Grant").unwrap().clone()).unwrap(),
        grant
    );
    grant
}

async fn current_grant(prepared: &Prepared) -> QuotaGrant {
    prepared
        .service
        .data
        .get(&2)
        .unwrap()
        .state
        .try_with_application(|state| state.tenant_quota(prepared.partition).cloned().unwrap())
        .await
        .unwrap()
}

async fn committed_end(prepared: &Prepared) -> u64 {
    prepared
        .service
        .data
        .get(&2)
        .unwrap()
        .state
        .try_with_application(|state| state.producer(prepared.partition).unwrap().committed_end())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_send_reconfirms_a_grant_shrunk_after_preflight_with_the_original_native_request() {
    let prepared = prepared().await;
    let operation = prepared.request.operation_id;
    let pause = InstalledPause::new(operation);
    let until = deadline();
    let client = prepared.client.clone();
    let request = prepared.request.clone();
    let pending = tokio::spawn(async move { client.console_write(&request).await });
    tokio::time::timeout_at(until, pause.state.ready.notified())
        .await
        .unwrap();
    let guard = prepared.service.tenant_quota_serial.lock().await;
    let original = current_grant(&prepared).await;
    assert!(original.capacity.publish_bytes_per_second > 4096 && !original.frozen);
    let frozen = quota_transition(
        &prepared.service,
        &original,
        original.capacity.publish_bytes_per_second,
        true,
    )
    .await;
    let shrunk = quota_transition(&prepared.service, &frozen, 1, false).await;
    assert!(shrunk.version > original.version && !shrunk.frozen);
    pause.state.resume.notify_one();
    tokio::time::timeout_at(until, pause.state.refused.notified())
        .await
        .unwrap();
    drop(guard);
    let result: Operation = pending.await.unwrap().unwrap();
    assert_eq!(
        result.status,
        Status::Succeeded,
        "The original operation failed: {:?}",
        result.reason
    );
    assert_eq!(result.id, operation);
    assert_eq!(committed_end(&prepared).await, 1);
    let attempted = pause.state.requests.lock().unwrap().clone();
    assert!(attempted.len() > 1);
    let original_native = attempted.first().unwrap();
    assert!(attempted.iter().all(|attempt| attempt == original_native));
    let writers = pause.state.writers.lock().unwrap().clone();
    assert_eq!(writers.len(), attempted.len());
    assert!(
        writers
            .iter()
            .all(|writer| writer == writers.first().unwrap())
    );
    assert!(attempted.len() <= MAX_PUBLISH_QUOTA_ATTEMPTS);
    assert_eq!(
        prepared
            .service
            .producer_metrics
            .snapshot()
            .quota_retries_total,
        attempted.len() as u64 - 1
    );
    eprintln!(
        "The original Console send recovered after {} identical native attempts",
        attempted.len()
    );
    let failures = pause.state.failures.lock().unwrap().clone();
    assert!(!failures.is_empty());
    assert!(failures.iter().all(|failure| failure.code
        == i32::from(tidemq_protocol::common::ErrorCode::QuotaExceeded)
        && failure.outcome == i32::from(tidemq_protocol::common::Outcome::Rejected)
        && failure.retryable
        && !failure.original_outcome_known));
    drop(failures);
    drop(pause);
    let replay = prepared
        .client
        .console_write(&prepared.request)
        .await
        .unwrap();
    assert_eq!(replay.status, result.status);
    assert_eq!(replay.id, result.id);
    assert_eq!(replay.receipt, result.receipt);
    assert_eq!(replay.resource_id, result.resource_id);
    assert_eq!(committed_end(&prepared).await, 1);
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_grant_changes_stop_at_the_bound_with_one_original_intent_and_deadline() {
    let prepared = prepared().await;
    let pause = InstalledPause::with_mode(prepared.request.operation_id, PauseMode::EveryPreflight);
    let client = prepared.client.clone();
    let request = prepared.request.clone();
    let until = deadline();
    let started = Instant::now();
    let pending = tokio::spawn(async move { client.console_write(&request).await });
    for _ in 0..MAX_PUBLISH_QUOTA_ATTEMPTS {
        tokio::time::timeout_at(until, pause.state.ready.notified())
            .await
            .unwrap();
        let guard = prepared.service.tenant_quota_serial.lock().await;
        let current = current_grant(&prepared).await;
        let frozen = quota_transition(
            &prepared.service,
            &current,
            current.capacity.publish_bytes_per_second,
            true,
        )
        .await;
        quota_transition(&prepared.service, &frozen, 1, false).await;
        pause.state.resume.notify_one();
        tokio::time::timeout_at(until, pause.state.refused.notified())
            .await
            .unwrap();
        drop(guard);
    }
    let result = pending.await.unwrap().unwrap();
    assert_eq!(result.status, Status::Rejected);
    assert_eq!(result.reason.as_deref(), Some("quota_exceeded"));
    assert_eq!(committed_end(&prepared).await, 0);
    let attempts = pause.state.requests.lock().unwrap().clone();
    assert_eq!(attempts.len(), MAX_PUBLISH_QUOTA_ATTEMPTS);
    assert_eq!(
        prepared
            .service
            .producer_metrics
            .snapshot()
            .quota_retries_total,
        MAX_PUBLISH_QUOTA_ATTEMPTS as u64 - 1
    );
    assert!(
        attempts
            .iter()
            .all(|attempt| attempt == attempts.first().unwrap())
    );
    assert!(started.elapsed() >= Duration::from_millis(270));
    assert!(Instant::now() < attempts.first().unwrap().1);
    drop(pause);
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_real_quota_limit_is_terminal_even_after_the_policy_recovers() {
    let prepared = prepared().await;
    prepared
        .client
        .admin()
        .set_tenant_limits(
            prepared.tenant,
            ResourceLimits::builder()
                .publish_bytes_per_second(1)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let pause = InstalledPause::with_mode(prepared.request.operation_id, PauseMode::Observe);
    let refused = prepared
        .client
        .console_write(&prepared.request)
        .await
        .unwrap();
    assert_eq!(refused.status, Status::Rejected);
    assert_eq!(refused.reason.as_deref(), Some("quota_exceeded"));
    assert!(pause.state.requests.lock().unwrap().is_empty());
    assert_eq!(committed_end(&prepared).await, 0);
    assert_eq!(
        prepared
            .service
            .producer_metrics
            .snapshot()
            .quota_retries_total,
        0
    );
    prepared
        .client
        .admin()
        .set_tenant_limits(prepared.tenant, ResourceLimits::default())
        .await
        .unwrap();
    let replay = prepared
        .client
        .console_write(&prepared.request)
        .await
        .unwrap();
    assert_eq!(replay.status, refused.status);
    assert_eq!(replay.reason, refused.reason);
    assert_eq!(replay.id, refused.id);
    assert!(pause.state.requests.lock().unwrap().is_empty());
    assert_eq!(committed_end(&prepared).await, 0);
    assert_eq!(
        prepared
            .service
            .producer_metrics
            .snapshot()
            .quota_retries_total,
        0
    );
    drop(pause);
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_disabled_tenant_does_not_reach_or_retry_native_publish() {
    let prepared = prepared().await;
    prepared
        .client
        .admin()
        .set_tenant_enabled(prepared.tenant, false)
        .await
        .unwrap();
    let pause = InstalledPause::with_mode(prepared.request.operation_id, PauseMode::Observe);
    match prepared.client.console_write(&prepared.request).await {
        Ok(result) => {
            assert_eq!(result.status, Status::Rejected);
            assert!(matches!(
                result.reason.as_deref(),
                Some("quota_exceeded" | "forbidden")
            ));
        }
        Err(error) => {
            assert_eq!(error.kind(), tidemq_sdk::ErrorKind::PermissionDenied);
            assert!(!error.outcome_unknown());
        }
    }
    assert!(pause.state.requests.lock().unwrap().is_empty());
    assert_eq!(committed_end(&prepared).await, 0);
    assert_eq!(
        prepared
            .service
            .producer_metrics
            .snapshot()
            .quota_retries_total,
        0
    );
    drop(pause);
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_native_completion_is_not_automatically_resubmitted() {
    let prepared = prepared().await;
    let pause = InstalledPause::with_mode(prepared.request.operation_id, PauseMode::LoseCompletion);
    let result = prepared
        .client
        .console_write(&prepared.request)
        .await
        .unwrap();
    assert_eq!(result.status, Status::Unknown);
    assert_eq!(pause.state.requests.lock().unwrap().len(), 1);
    assert_eq!(committed_end(&prepared).await, 1);
    assert_eq!(
        prepared
            .service
            .producer_metrics
            .snapshot()
            .quota_retries_total,
        0
    );
    let failures = pause.state.failures.lock().unwrap().clone();
    assert_eq!(failures.len(), 1);
    assert_eq!(
        failures.first().unwrap().outcome,
        i32::from(Outcome::Unknown)
    );
    drop(pause);
    let recovered = prepared
        .client
        .console_operation(prepared.request.operation_id)
        .await
        .unwrap();
    assert_eq!(recovered.status, Status::Succeeded);
    assert_eq!(recovered.id, result.id);
    assert_eq!(committed_end(&prepared).await, 1);
    assert_eq!(
        prepared
            .service
            .producer_metrics
            .snapshot()
            .quota_retries_total,
        0
    );
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_native_publisher_session_table_is_rejected_without_retries() {
    let prepared = prepared().await;
    let capacity = crate::producer::ProducerLimits::default().max_sessions;
    let mut clients = Vec::new();
    for _ in 0..capacity.div_ceil(16) {
        clients.push(prepared.fixture.client(1).await);
    }
    let mut producers = Vec::new();
    for index in 0..capacity {
        let producer = clients
            .get(index / 16)
            .unwrap()
            .new_producer("quota-race/app/events")
            .build()
            .await
            .unwrap();
        producer.send("occupied-session").await.unwrap();
        producers.push(producer);
    }
    assert_eq!(committed_end(&prepared).await, capacity as u64);
    let pause = InstalledPause::new(prepared.request.operation_id);
    let before = prepared
        .service
        .producer_metrics
        .snapshot()
        .quota_retries_total;
    let until = deadline();
    let client = prepared.client.clone();
    let request = prepared.request.clone();
    let pending = tokio::spawn(async move { client.console_write(&request).await });
    tokio::time::timeout_at(until, pause.state.ready.notified())
        .await
        .unwrap();
    // Keep admission stable so only the real publisher table limit decides this attempt.
    let serial = prepared.service.tenant_quota_serial.lock().await;
    prepared
        .service
        .data_lease_for(2, prepared.partition, until, Duration::from_secs(1))
        .await
        .unwrap();
    pause.state.resume.notify_one();
    let result = pending.await.unwrap().unwrap();
    drop(serial);
    let attempts = pause.state.requests.lock().unwrap().clone();
    let writes = pause.state.writers.lock().unwrap().len();
    let failures = pause.state.failures.lock().unwrap().clone();
    let retries = prepared
        .service
        .producer_metrics
        .snapshot()
        .quota_retries_total
        - before;
    let end = committed_end(&prepared).await;
    eprintln!(
        "Real native publisher capacity: live_sessions={capacity}, native_attempts={writes}, quota_retries_delta={retries}, status={:?}, committed_end={end}",
        result.status
    );
    while let Some(producer) = producers.pop() {
        producer.close().await.unwrap();
    }
    let replay = prepared
        .client
        .console_write(&prepared.request)
        .await
        .unwrap();
    assert_eq!(replay.status, result.status);
    assert_eq!(replay.reason, result.reason);
    assert_eq!(replay.id, result.id);
    assert_eq!(pause.state.writers.lock().unwrap().len(), writes);
    assert_eq!(
        prepared
            .service
            .producer_metrics
            .snapshot()
            .quota_retries_total
            - before,
        retries
    );
    drop(pause);
    for client in clients {
        client.close().await.unwrap();
    }
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
    assert_eq!(result.status, Status::Rejected);
    assert_eq!(result.reason.as_deref(), Some("quota_exceeded"));
    assert_eq!(end, capacity as u64);
    assert_eq!(
        failures.first().unwrap().message,
        crate::producer::Rejection::SessionCapacity.to_string()
    );
    assert!(
        attempts
            .iter()
            .all(|attempt| attempt == attempts.first().unwrap())
    );
    assert_eq!(
        writes, 1,
        "A real publisher capacity refusal must not resubmit the original intent"
    );
    assert_eq!(retries, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_hot_data_capacity_is_rejected_once_without_quota_retries() {
    let mut prepared = prepared().await;
    prepared
        .client
        .admin()
        .set_tenant_limits(
            prepared.tenant,
            ResourceLimits::builder()
                .publish_bytes_per_second(128 * 1024 * 1024)
                .consume_bytes_per_second(16 * 1024 * 1024)
                .backlog_bytes(128 * 1024 * 1024)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let target_bytes = prepared
        .service
        .config
        .protocol
        .max_message_bytes
        .min(1024 * 1024) as usize;
    prepared.request.command = WriteCommand::Send {
        topic: prepared.partition.topic_id,
        payload_base64: STANDARD.encode(vec![b'c'; target_bytes]),
        key_base64: None,
        properties_base64: BTreeMap::new(),
    };
    let consumer = prepared
        .client
        .new_stream_consumer("quota-race/app/events", "retain-hot")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let mut producers = Vec::new();
    for _ in 0..16 {
        producers.push(
            prepared
                .client
                .new_producer("quota-race/app/events")
                .build()
                .await
                .unwrap(),
        );
    }
    let chunk = Bytes::from(vec![b'h'; target_bytes / 4]);
    producers
        .first()
        .unwrap()
        .send(tidemq_sdk::Message::new(chunk.clone()))
        .await
        .unwrap();
    let held = tokio::time::timeout(Duration::from_secs(15), consumer.receive())
        .await
        .unwrap()
        .unwrap();
    let replica = prepared.service.data.get(&2).unwrap();
    let maximum = replica
        .state
        .try_with_application(|state| state.config().unwrap().max_hot_bytes)
        .await
        .unwrap();
    let mut remaining = maximum;
    for index in 1..=maximum as usize / chunk.len() {
        remaining = replica
            .state
            .try_with_application(|state| {
                maximum.saturating_sub(state.hot_bytes()).saturating_sub(
                    state
                        .producer(prepared.partition)
                        .unwrap()
                        .publish_state_bytes(),
                )
            })
            .await
            .unwrap();
        if remaining < target_bytes as u64 {
            break;
        }
        producers
            .get(index % producers.len())
            .unwrap()
            .send(tidemq_sdk::Message::new(chunk.clone()))
            .await
            .unwrap();
    }
    assert!(
        remaining < target_bytes as u64,
        "The fixture did not reach native hot capacity"
    );
    let before_end = committed_end(&prepared).await;
    let before = prepared
        .service
        .producer_metrics
        .snapshot()
        .quota_retries_total;
    let pause = InstalledPause::new(prepared.request.operation_id);
    let client = prepared.client.clone();
    let request = prepared.request.clone();
    let until = deadline();
    let pending = tokio::spawn(async move { client.console_write(&request).await });
    tokio::time::timeout_at(until, pause.state.ready.notified())
        .await
        .unwrap();
    let serial = prepared.service.tenant_quota_serial.lock().await;
    prepared
        .service
        .data_lease_for(2, prepared.partition, until, Duration::from_secs(1))
        .await
        .unwrap();
    pause.state.resume.notify_one();
    let result = pending.await.unwrap().unwrap();
    drop(serial);
    let writes = pause.state.writers.lock().unwrap().len();
    let failure = pause
        .state
        .failures
        .lock()
        .unwrap()
        .first()
        .unwrap()
        .clone();
    let retries = prepared
        .service
        .producer_metrics
        .snapshot()
        .quota_retries_total
        - before;
    let end = committed_end(&prepared).await;
    eprintln!(
        "Real native hot capacity: maximum={maximum}, remaining_upper_bound={remaining}, message_bytes={target_bytes}, native_attempts={writes}, quota_retries_delta={retries}"
    );
    drop(pause);
    consumer.ack_cumulative(&held).await.unwrap();
    consumer.close().await.unwrap();
    for producer in producers {
        producer.close().await.unwrap();
    }
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
    assert_eq!(result.status, Status::Rejected);
    assert_eq!(result.reason.as_deref(), Some("quota_exceeded"));
    assert_eq!(
        failure.message,
        crate::producer::Rejection::DataCapacity.to_string()
    );
    assert_eq!(end, before_end);
    assert_eq!(writes, 1);
    assert_eq!(retries, 0);
}

#[test]
fn retry_eligibility_keeps_the_exact_tenant_source_and_original_failure_contract() {
    use crate::broker::{DataRejection as D, QuotaRejection as Q};
    let exhausted = super::super::data::data_rejection(D::Quota(Q::Exhausted));
    assert!(unrecorded_tenant_grant_refusal(&exhausted));
    let external = Error::reject_with(
        ErrorCode::QuotaExceeded,
        Outcome::Rejected,
        true,
        Q::Exhausted.to_string(),
    );
    let expected = serde_json::to_value(external.failure()).unwrap();
    assert_eq!(serde_json::to_value(exhausted.failure()).unwrap(), expected);
    assert_eq!(exhausted.to_string(), external.to_string());
    for reason in [
        D::Quota(Q::Occupied),
        D::PartitionCapacity,
        D::Producer(crate::producer::Rejection::SessionCapacity),
        D::Producer(crate::producer::Rejection::DataCapacity),
        D::Transfer(crate::broker::TransferDataRejection::Capacity),
    ] {
        let error = super::super::data::data_rejection(reason);
        assert_eq!(error.failure().code, i32::from(ErrorCode::QuotaExceeded));
        assert!(!unrecorded_tenant_grant_refusal(&error));
    }
    assert!(!unrecorded_tenant_grant_refusal(&external));
    for field in ["known", "unknown", "permanent"] {
        let mut error = super::super::data::data_rejection(D::Quota(Q::Exhausted));
        let Error::TenantGrantExhausted { failure } = &mut error else {
            panic!("Tenant grant provenance was lost");
        };
        match field {
            "known" => failure.original_outcome_known = true,
            "unknown" => failure.outcome = i32::from(Outcome::Unknown),
            "permanent" => failure.retryable = false,
            _ => unreachable!(),
        }
        assert!(!unrecorded_tenant_grant_refusal(&error));
    }
    let admitted = super::super::data::data_rejection(D::Quota(Q::Exhausted)).admitted();
    assert!(!unrecorded_tenant_grant_refusal(&admitted));
    assert_eq!(admitted.failure().outcome, i32::from(Outcome::Unknown));
    let forwarded: super::super::Failure = serde_json::from_value(expected).unwrap();
    assert!(!unrecorded_tenant_grant_refusal(&Error::Rejected {
        failure: forwarded
    }));
}

#[test]
fn quota_retry_events_keep_the_parent_span_and_only_fixed_safe_fields() {
    let buffer = crate::observability::logging::tests::Buffer(Arc::default());
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let parent = tracing::info_span!("producer_submission");
        let _entered = parent.enter();
        quota_retry_event(2, 10, "commit");
    });
    let output = buffer.0.lock().unwrap().clone();
    let event: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let fields = event.get("fields").unwrap();
    assert_eq!(fields.get("event_code").unwrap(), "producer_quota_retry");
    assert_eq!(fields.get("attempt").unwrap(), 2);
    assert_eq!(
        fields.get("max_attempts").unwrap(),
        MAX_PUBLISH_QUOTA_ATTEMPTS
    );
    assert_eq!(fields.get("backoff_ms").unwrap(), 10);
    assert_eq!(fields.get("stage").unwrap(), "commit");
    assert_eq!(
        event.get("span").unwrap().get("name").unwrap(),
        "producer_submission"
    );
    for field in ["token", "body", "payload", "proof", "producer", "topic"] {
        assert!(fields.get(field).is_none());
    }
}
