use super::*;
use std::{sync::Mutex, time::Duration};
use tidemq_protocol::{ProtocolLimits, framing::decode_payload};
use tidemq_sdk::Client;
use tokio::sync::Notify;

use crate::{
    config::Role,
    metadata::{Command, ResourceLeaseCommand, Response},
    runtime::{GroupConfig, data::consumer_tests::fixture::*},
};

const TOKEN: &str = "runtime-test-access-token-with-at-least-32-characters";
const TOPIC: &str = "test/app/console-recovery";

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum Phase {
    Begin,
    Identity,
    Complete,
}

type MetadataKey = (Id, Phase);
static METADATA: Mutex<BTreeMap<MetadataKey, Arc<MetadataPause>>> = Mutex::new(BTreeMap::new());
static REFUSALS: Mutex<BTreeMap<Id, Arc<RefusalPause>>> = Mutex::new(BTreeMap::new());

struct MetadataPause {
    attempts: Mutex<Vec<Command>>,
    ready: Notify,
    resume: Notify,
}

struct InstalledMetadataPause {
    key: MetadataKey,
    pause: Arc<MetadataPause>,
}

impl InstalledMetadataPause {
    fn new(operation: Id, phase: Phase) -> Self {
        let key = (operation, phase);
        let pause = Arc::new(MetadataPause {
            attempts: Mutex::new(Vec::new()),
            ready: Notify::new(),
            resume: Notify::new(),
        });
        assert!(
            METADATA
                .lock()
                .unwrap()
                .insert(key, pause.clone())
                .is_none()
        );
        Self { key, pause }
    }
}

impl Drop for InstalledMetadataPause {
    fn drop(&mut self) {
        let mut pauses = METADATA.lock().unwrap();
        if pauses
            .get(&self.key)
            .is_some_and(|pause| Arc::ptr_eq(pause, &self.pause))
        {
            pauses.remove(&self.key);
        }
        self.pause.resume.notify_one();
    }
}

fn metadata_key(command: &Command) -> Option<MetadataKey> {
    match &command.action {
        metadata::Action::Console(ConsoleCommand::Begin { record, .. }) => {
            Some((record.id, Phase::Begin))
        }
        metadata::Action::Console(ConsoleCommand::Complete { id, .. }) => {
            Some((*id, Phase::Complete))
        }
        metadata::Action::Identity(_) => Some((command.operation_id, Phase::Identity)),
        _ => None,
    }
}

pub(in crate::runtime) async fn before_metadata_write(command: &Command) {
    let Some(key) = metadata_key(command) else {
        return;
    };
    let pause = METADATA.lock().unwrap().get(&key).cloned();
    let Some(pause) = pause else {
        return;
    };
    let first = {
        let mut attempts = pause.attempts.lock().unwrap();
        attempts.push(command.clone());
        attempts.len() == 1
    };
    if first {
        pause.ready.notify_one();
        pause.resume.notified().await;
    }
}

struct RefusalPause {
    attempts: Mutex<Vec<(ConsoleRecord, usize, super::super::Failure)>>,
    ready: Notify,
    resume: Notify,
}

struct InstalledRefusalPause {
    operation: Id,
    pause: Arc<RefusalPause>,
}

impl InstalledRefusalPause {
    fn new(operation: Id) -> Self {
        let pause = Arc::new(RefusalPause {
            attempts: Mutex::new(Vec::new()),
            ready: Notify::new(),
            resume: Notify::new(),
        });
        assert!(
            REFUSALS
                .lock()
                .unwrap()
                .insert(operation, pause.clone())
                .is_none()
        );
        Self { operation, pause }
    }
}

impl Drop for InstalledRefusalPause {
    fn drop(&mut self) {
        let mut pauses = REFUSALS.lock().unwrap();
        if pauses
            .get(&self.operation)
            .is_some_and(|pause| Arc::ptr_eq(pause, &self.pause))
        {
            pauses.remove(&self.operation);
        }
        self.pause.resume.notify_one();
    }
}

pub(in crate::runtime) async fn after_native_refusal(
    record: &ConsoleRecord,
    attempt: usize,
    error: &Error,
) {
    let pause = REFUSALS.lock().unwrap().get(&record.id).cloned();
    let Some(pause) = pause else {
        return;
    };
    let first = {
        let mut attempts = pause.attempts.lock().unwrap();
        attempts.push((record.clone(), attempt, error.failure()));
        attempts.len() == 1
    };
    if first {
        pause.ready.notify_one();
        pause.resume.notified().await;
    }
}

async fn setup() -> (Fixture, Client, Id, Id) {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture
        .configs
        .get_mut(&1)
        .unwrap()
        .runtime
        .groups
        .push(GroupConfig {
            group_id: 3,
            voters: Vec::new(),
            standby: Vec::new(),
        });
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("test").await.unwrap();
    let namespace = client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    (fixture, client, tenant.into(), namespace.into())
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

async fn root_access(service: &Service) -> Access {
    let credential = service
        .metadata
        .as_ref()
        .unwrap()
        .state
        .try_with_application(|state| {
            state
                .auth()
                .authenticate(
                    metadata::TokenDigest::of(TOKEN).unwrap(),
                    super::super::data::unix_millis().unwrap(),
                )
                .unwrap()
                .id
        })
        .await
        .unwrap();
    Access {
        principal: Arc::from(format!("credential:{credential}")),
        scope: Arc::from("*"),
        session: Some(Id::new_v7().unwrap()),
        tenant: None,
    }
}

fn console_frame(operation: Id, command: WriteCommand) -> Frame {
    let request = WriteRequest {
        operation_id: operation,
        command,
    };
    tidemq_protocol::framing::new_frame(
        FrameKind::ConsoleWrite,
        TrafficClass::Bulk,
        Id::new_v7().unwrap(),
        operation,
        &public::ConsoleWrite {
            json: serde_json::to_vec(&request).unwrap().into(),
        },
        5_000,
        &ProtocolLimits::default(),
    )
    .unwrap()
}

fn principal_command(tenant: Id, namespace: Id, name: &str) -> WriteCommand {
    WriteCommand::CreatePrincipal {
        name: name.into(),
        tenant,
        namespace: Some(namespace),
        permissions: vec!["produce".into()],
    }
}

fn decode_operation(frame: Frame) -> Operation {
    let reply: public::ConsoleOperationResult =
        decode_payload(frame.payload, &ProtocolLimits::default()).unwrap();
    serde_json::from_slice(&reply.json).unwrap()
}

async fn conflict_with_resource_lease(
    service: &Service,
    pause: &InstalledMetadataPause,
    until: Instant,
) -> Command {
    tokio::time::timeout_at(until, pause.pause.ready.notified())
        .await
        .unwrap();
    let original = pause
        .pause
        .attempts
        .lock()
        .unwrap()
        .first()
        .unwrap()
        .clone();
    let replica = service.metadata.as_ref().unwrap();
    let competing = Command::resource_lease(
        Id::new_v7().unwrap(),
        original.expected_revision,
        ResourceLeaseCommand::Release {
            id: Id::new_v7().unwrap(),
            owner: service.config.node_id,
            now_ms: super::super::data::unix_millis().unwrap(),
        },
    );
    assert!(matches!(
        replica
            .group
            .commit(competing, until)
            .await
            .unwrap()
            .response,
        Response::Applied {
            outcome: metadata::Outcome::ResourceLease(None),
            ..
        }
    ));
    assert!(matches!(
        replica
            .group
            .commit(original.clone(), until)
            .await
            .unwrap()
            .response,
        Response::Rejected {
            reason: metadata::Rejection::RevisionConflict,
            ..
        }
    ));
    original
}

async fn assert_original_receipt(client: &Client, operation: &Operation) {
    assert_eq!(operation.status, Status::Succeeded);
    assert!(operation.receipt.is_some());
    let recovered = client.console_operation(operation.id).await.unwrap();
    assert_eq!(recovered.id, operation.id);
    assert_eq!(recovered.status, Status::Succeeded);
    assert_eq!(recovered.resource_id, operation.resource_id);
    assert_eq!(recovered.receipt, operation.receipt);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_retries_a_real_resource_lease_revision_conflict_with_one_principal() {
    let (fixture, client, tenant, namespace) = setup().await;
    let service = fixture.running.get(&1).unwrap().service.clone();
    let operation = Id::new_v7().unwrap();
    let frame = console_frame(
        operation,
        principal_command(tenant, namespace, "cas-principal"),
    );
    let access = root_access(&service).await;
    let pause = InstalledMetadataPause::new(operation, Phase::Identity);
    let until = deadline();
    let handler = service.clone();
    let pending =
        tokio::spawn(async move { handler.console_write_public(&access, &frame, until).await });
    let stale = conflict_with_resource_lease(&service, &pause, until).await;
    assert_eq!(stale.operation_id, operation);
    assert!(
        service
            .metadata
            .as_ref()
            .unwrap()
            .state
            .try_with_application(|state| state.auth().principal(operation).is_none())
            .await
            .unwrap()
    );
    pause.pause.resume.notify_one();
    let result = decode_operation(
        tokio::time::timeout_at(until, pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    );
    assert_eq!(result.id, operation);
    assert_eq!(result.resource_id, Some(operation));
    let attempts = pause.pause.attempts.lock().unwrap().clone();
    assert_eq!(attempts.len(), 2);
    for attempt in &attempts {
        assert_eq!(attempt.operation_id, operation);
        let metadata::Action::Identity(request) = &attempt.action else {
            panic!("Expected the original identity mutation");
        };
        let metadata::Action::Identity(original) = &stale.action else {
            panic!("Expected the stale identity mutation");
        };
        assert!(request.action == original.action);
        assert!(request.actor == original.actor);
        assert!(request.request_digest == original.request_digest);
    }
    assert_eq!(
        attempts.get(1).unwrap().expected_revision,
        stale.expected_revision.checked_add(1).unwrap()
    );
    assert!(Instant::now() < until);
    drop(pause);
    assert_original_receipt(&client, &result).await;
    let principals = client
        .admin()
        .principals(tenant.into())
        .next_page()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(principals.len(), 1);
    assert_eq!(principals.first().unwrap().id, operation.into());
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_begin_and_complete_only_refresh_their_cas_guard() {
    let (fixture, client, tenant, namespace) = setup().await;
    let service = fixture.running.get(&1).unwrap().service.clone();
    for (phase, name) in [
        (Phase::Begin, "cas-begin"),
        (Phase::Complete, "cas-complete"),
    ] {
        let operation = Id::new_v7().unwrap();
        let frame = console_frame(operation, principal_command(tenant, namespace, name));
        let access = root_access(&service).await;
        let pause = InstalledMetadataPause::new(operation, phase);
        let until = deadline();
        let handler = service.clone();
        let pending =
            tokio::spawn(async move { handler.console_write_public(&access, &frame, until).await });
        let stale = conflict_with_resource_lease(&service, &pause, until).await;
        let saved = service
            .metadata
            .as_ref()
            .unwrap()
            .state
            .try_with_application(|state| state.console_record(operation).cloned())
            .await
            .unwrap();
        match phase {
            Phase::Begin => assert!(saved.is_none()),
            Phase::Complete => assert!(saved.unwrap().receipt.is_none()),
            Phase::Identity => panic!("Console receipt test requires a receipt phase"),
        }
        pause.pause.resume.notify_one();
        let result = decode_operation(
            tokio::time::timeout_at(until, pending)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        );
        let attempts = pause.pause.attempts.lock().unwrap().clone();
        assert_eq!(attempts.len(), 2);
        let retry = attempts.get(1).unwrap();
        assert_eq!(retry.operation_id, stale.operation_id);
        assert_eq!(
            retry.expected_revision,
            stale.expected_revision.checked_add(1).unwrap()
        );
        let mut refreshed = stale;
        refreshed.expected_revision = retry.expected_revision;
        assert!(*retry == refreshed, "Only the outer CAS guard may change");
        assert!(Instant::now() < until);
        drop(pause);
        assert_original_receipt(&client, &result).await;
    }
    client.close().await.unwrap();
    fixture.close().await;
}

async fn committed_end(service: &Service, group: u64, partition: metadata::PartitionKey) -> u64 {
    service
        .data
        .get(&group)
        .unwrap()
        .state
        .try_with_application(|state| state.producer(partition).unwrap().committed_end())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_accepted_console_send_recovers_a_native_refusal_without_rebinding() {
    let (fixture, client, _, _) = setup().await;
    let topic: Id = client
        .admin()
        .create_topic(TOPIC)
        .await
        .unwrap()
        .id()
        .into();
    let service = fixture.running.get(&1).unwrap().service.clone();
    let partition = metadata::PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let route = service.route(partition, deadline()).await.unwrap();
    assert_eq!(committed_end(&service, route.group_id, partition).await, 0);
    let operation = Id::new_v7().unwrap();
    let command = WriteCommand::Send {
        topic,
        payload_base64: STANDARD.encode("one recovered console message"),
        key_base64: None,
        properties_base64: BTreeMap::new(),
    };
    let frame = console_frame(operation, command.clone());
    let access = root_access(&service).await;
    let pause = InstalledRefusalPause::new(operation);
    let until = deadline();
    service
        .draining
        .store(true, std::sync::atomic::Ordering::Release);
    let handler = service.clone();
    // Admission already happened: only the real native Publish dispatch observes draining.
    let pending =
        tokio::spawn(async move { handler.console_write_public(&access, &frame, until).await });
    tokio::time::timeout_at(until, pause.pause.ready.notified())
        .await
        .unwrap();
    let attempts = pause.pause.attempts.lock().unwrap().clone();
    assert_eq!(attempts.len(), 1);
    let (record, _, failure) = attempts.first().unwrap();
    assert_eq!(record.id, operation);
    assert_eq!(failure.code, i32::from(ErrorCode::Unavailable));
    assert_eq!(failure.outcome, i32::from(Outcome::Rejected));
    assert!(failure.retryable);
    assert!(!failure.original_outcome_known);
    let bound = console_publish(record, &command).unwrap();
    assert_eq!(bound.producer_id.as_ref(), operation.as_bytes());
    assert_eq!(bound.sequence, 1);
    assert_eq!(committed_end(&service, route.group_id, partition).await, 0);
    service
        .draining
        .store(false, std::sync::atomic::Ordering::Release);
    pause.pause.resume.notify_one();
    let result = decode_operation(
        tokio::time::timeout_at(until, pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    );
    assert_eq!(result.id, operation);
    assert_eq!(result.resource_id, Some(topic));
    assert_eq!(committed_end(&service, route.group_id, partition).await, 1);
    let saved = service
        .metadata
        .as_ref()
        .unwrap()
        .state
        .try_with_application(|state| state.console_record(operation).unwrap().clone())
        .await
        .unwrap();
    assert!(saved.binding == record.binding);
    assert!(saved.digest == record.digest);
    assert!(console_publish(&saved, &command).unwrap() == bound);
    assert_eq!(pause.pause.attempts.lock().unwrap().len(), 1);
    assert!(Instant::now() < until);
    drop(pause);
    assert_original_receipt(&client, &result).await;
    assert_eq!(committed_end(&service, route.group_id, partition).await, 1);
    client.close().await.unwrap();
    fixture.close().await;
}

#[test]
fn retryable_refusals_are_rejected_and_unknown_execution_stays_unknown() {
    let record = ConsoleRecord {
        id: Id::new_v7().unwrap(),
        actor: Id::new_v7().unwrap(),
        credential: Id::new_v7().unwrap(),
        digest: [1; 32],
        created_ms: 1,
        expires_ms: u64::MAX,
        binding: ConsoleBinding::CreateTopic("test/app/classification".into()),
        target: "test/app/classification".into(),
        scope: "test/app/classification".into(),
        receipt: None,
    };
    let mut recorded_refusal = Error::unavailable().failure();
    recorded_refusal.original_outcome_known = true;
    for (error, retry) in [
        (Error::unavailable(), true),
        (
            super::super::wire::execution_error(crate::execution::Error::NotSubmitted {
                reason: "Admission closed before handoff".into(),
            }),
            true,
        ),
        (
            Error::reject(ErrorCode::QuotaExceeded, Outcome::Rejected, true),
            false,
        ),
        (
            Error::reject(ErrorCode::Forbidden, Outcome::Rejected, false),
            false,
        ),
        (
            Error::reject(ErrorCode::Unavailable, Outcome::Rejected, false),
            false,
        ),
        (
            Error::Rejected {
                failure: recorded_refusal,
            },
            false,
        ),
    ] {
        assert_eq!(retry_console_refusal(&error), retry);
        let result = execution_result(&record, 2, Err(error));
        assert_eq!(result.status, Status::Rejected);
        assert!(result.receipt.is_none());
    }
    for error in [
        Error::unknown(),
        super::super::wire::execution_error(crate::execution::Error::Unknown {
            reason: "Deadline expired after handoff".into(),
        }),
        super::super::wire::execution_error(crate::execution::Error::Fatal {
            reason: "Application failed after persistence".into(),
        }),
        Error::unavailable().admitted(),
        Error::operation(
            "test network uncertainty",
            std::io::Error::other("no authoritative reply"),
        ),
    ] {
        assert!(
            !retry_console_refusal(&error),
            "Unknown execution must not be replayed"
        );
        let result = execution_result(&record, 2, Err(error));
        assert_eq!(result.status, Status::Unknown);
        assert!(result.receipt.is_none());
        assert!(result.resource_id.is_none());
    }
}
