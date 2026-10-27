use std::{
    collections::BTreeMap,
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use tidemq_protocol::{
    Id, ProtocolLimits,
    common::{ErrorCode, Frame, FrameKind, Outcome, TrafficClass},
    framing::decode_payload,
    public,
};
use tidemq_sdk::Client;
use tokio::{sync::Notify, time::Instant};

use super::super::{Error, data::unix_millis, service::Service};
use crate::{
    broker::{DataCommand, DataResponse, WriterFence},
    config::{Config, Role, TlsConfig},
    metadata::PartitionKey,
    runtime::{GroupConfig, PeerConfig, RunningNode, RuntimeConfig},
    subscription::GroupCommand,
};

// Shared fixtures include helpers used by the runtime and external S3 integration suites.
#[allow(dead_code)]
#[path = "../../tests/runtime_support/fixture.rs"]
pub(in crate::runtime) mod fixture;
#[allow(dead_code)]
#[path = "../../tests/lifecycle_support/rustfs.rs"]
mod rustfs;
#[path = "../../tests/s3_support/mod.rs"]
mod s3;
use fixture::*;

#[path = "metadata_replay_tests.rs"]
mod metadata_replay_tests;

#[path = "../../tests/runtime_support/ack_priority.rs"]
mod ack_priority;

const TOKEN: &str = "runtime-test-access-token-with-at-least-32-characters";
const TOPIC: &str = "test/app/consumer-authority";
static COMMIT: Mutex<Option<Arc<CommitPause>>> = Mutex::new(None);
static METADATA_WRITE: Mutex<Option<Arc<MetadataWritePause>>> = Mutex::new(None);

struct MetadataWritePause {
    operation: Id,
    attempts: Mutex<Vec<(u64, crate::metadata::ConsumerCommand)>>,
    ready: Notify,
    resume: Notify,
}

struct InstalledMetadataWritePause(Arc<MetadataWritePause>);

impl InstalledMetadataWritePause {
    fn new(operation: Id) -> Self {
        let pause = Arc::new(MetadataWritePause {
            operation,
            attempts: Mutex::new(Vec::new()),
            ready: Notify::new(),
            resume: Notify::new(),
        });
        assert!(
            METADATA_WRITE
                .lock()
                .unwrap()
                .replace(pause.clone())
                .is_none()
        );
        Self(pause)
    }
}

impl Drop for InstalledMetadataWritePause {
    fn drop(&mut self) {
        let mut slot = METADATA_WRITE.lock().unwrap();
        if slot
            .as_ref()
            .is_some_and(|pause| Arc::ptr_eq(pause, &self.0))
        {
            slot.take();
        }
        self.0.resume.notify_one();
    }
}

pub(in crate::runtime) async fn before_consumer_meta_write(
    operation: Id,
    revision: u64,
    command: &crate::metadata::ConsumerCommand,
) {
    let pause = METADATA_WRITE
        .lock()
        .unwrap()
        .as_ref()
        .filter(|pause| pause.operation == operation)
        .cloned();
    let Some(pause) = pause else {
        return;
    };
    let first = {
        let mut attempts = pause.attempts.lock().unwrap();
        attempts.push((revision, command.clone()));
        attempts.len() == 1
    };
    if first {
        pause.ready.notify_one();
        pause.resume.notified().await;
    }
}

struct CommitPause {
    partition: PartitionKey,
    expires: Mutex<Option<std::time::Instant>>,
    ready: Notify,
    resume: Notify,
}

struct InstalledCommitPause(Arc<CommitPause>);

impl InstalledCommitPause {
    fn new(partition: PartitionKey) -> Self {
        let pause = Arc::new(CommitPause {
            partition,
            expires: Mutex::new(None),
            ready: Notify::new(),
            resume: Notify::new(),
        });
        assert!(COMMIT.lock().unwrap().replace(pause.clone()).is_none());
        Self(pause)
    }
}

impl Drop for InstalledCommitPause {
    fn drop(&mut self) {
        let mut slot = COMMIT.lock().unwrap();
        if slot
            .as_ref()
            .is_some_and(|pause| Arc::ptr_eq(pause, &self.0))
        {
            slot.take();
        }
        self.0.resume.notify_one();
    }
}

pub(in crate::runtime) async fn after_group_commit(
    partition: PartitionKey,
    expires: std::time::Instant,
) {
    let pause = {
        let mut slot = COMMIT.lock().unwrap();
        if slot
            .as_ref()
            .is_some_and(|pause| pause.partition == partition)
        {
            slot.take()
        } else {
            None
        }
    };
    if let Some(pause) = pause {
        *pause.expires.lock().unwrap() = Some(expires);
        pause.ready.notify_one();
        pause.resume.notified().await;
    }
}

async fn setup() -> (Fixture, Client) {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("test").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    (fixture, client)
}

async fn access(service: &Service, frame: &Frame) -> crate::gateway::Access {
    let credential = service
        .metadata
        .as_ref()
        .unwrap()
        .state
        .with_application(|state| {
            state
                .auth()
                .authenticate(
                    crate::metadata::TokenDigest::of(TOKEN).unwrap(),
                    unix_millis().unwrap(),
                )
                .unwrap()
                .id
        })
        .await;
    service
        .dynamic_access(credential, Id::new_v7().unwrap(), frame, deadline())
        .await
        .unwrap()
        .0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_consumer_observation_retries_a_real_resource_lease_revision_conflict() {
    use crate::metadata::{
        Command, ConsumerCommand, ConsumerOutcome, Outcome as MetadataOutcome,
        ResourceLeaseCommand, Response,
    };
    use tidemq_protocol::topic::{InitialPosition, SubscriptionKind};

    let (fixture, client) = setup().await;
    let topic = client.admin().create_topic(TOPIC).await.unwrap();
    let service = fixture.running.get(&1).unwrap().service.clone();
    let guard = service.metadata_mutation.lock().await;
    let subscription = Id::new_v7().unwrap();
    service
        .consumer_meta_write(
            subscription,
            ConsumerCommand::Create {
                topic: topic.id().to_string().parse().unwrap(),
                name: "cas-observation".into(),
                model: SubscriptionKind::Stream,
                initial: InitialPosition::EarliestAvailable,
            },
            deadline(),
        )
        .await
        .unwrap();
    let row = service.consumer_row(subscription).await.unwrap();
    let plan = row.members.plan().clone();
    let domain = *plan.domains.first().unwrap();
    let now_ms = unix_millis().unwrap();
    service
        .install_consumer_domain(
            &row,
            domain,
            plan.version.checked_mul(2).unwrap(),
            row.members.grants(now_ms).unwrap(),
            deadline(),
        )
        .await
        .unwrap();
    let command = ConsumerCommand::Observe {
        subscription,
        version: plan.version,
        fenced: false,
        domain,
        now_ms,
    };
    let operation = Id::new_v7().unwrap();
    let pause = InstalledMetadataWritePause::new(operation);
    let original_deadline = Instant::now() + Duration::from_secs(5);
    let handler = service.clone();
    let original = command.clone();
    let pending = tokio::spawn(async move {
        handler
            .consumer_meta_write(operation, original, original_deadline)
            .await
    });
    tokio::time::timeout_at(original_deadline, pause.0.ready.notified())
        .await
        .unwrap();
    let revision = pause.0.attempts.lock().unwrap().first().unwrap().0;
    let replica = service.metadata.as_ref().unwrap();
    let competing = Command::resource_lease(
        Id::new_v7().unwrap(),
        revision,
        ResourceLeaseCommand::Release {
            id: Id::new_v7().unwrap(),
            owner: 1,
            now_ms,
        },
    );
    let response = replica.raft.client_write(competing).await.unwrap().data;
    assert!(matches!(
        response,
        Response::Applied {
            outcome: MetadataOutcome::ResourceLease(None),
            ..
        }
    ));
    let stale = Command::consumer(operation, revision, command.clone());
    let response = replica.raft.client_write(stale).await.unwrap().data;
    assert!(matches!(
        response,
        Response::Rejected {
            reason: crate::metadata::Rejection::RevisionConflict,
            ..
        }
    ));
    assert_eq!(
        service
            .consumer_row(subscription)
            .await
            .unwrap()
            .members
            .plan(),
        &plan
    );
    pause.0.resume.notify_one();
    let outcome = tokio::time::timeout_at(original_deadline, pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let ConsumerOutcome::Plan(observed) = outcome else {
        panic!("Consumer observation returned a different outcome");
    };
    assert!(observed.fenced);
    assert!(!observed.active);
    assert_eq!(observed.version, plan.version);
    assert!(observed.installed.is_empty());
    let attempts = pause.0.attempts.lock().unwrap().clone();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts.first().unwrap().0, revision);
    assert_eq!(attempts.get(1).unwrap().0, revision.checked_add(1).unwrap());
    assert!(attempts.iter().all(|(_, attempted)| *attempted == command));
    assert!(Instant::now() < original_deadline);
    drop(pause);
    service
        .resume_consumer(subscription, deadline())
        .await
        .unwrap();
    assert!(
        service
            .consumer_row(subscription)
            .await
            .unwrap()
            .members
            .plan()
            .active
    );
    drop(guard);
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_consumer_mutation_expiry_preserves_unknown_semantics_and_source() {
    let (fixture, client) = setup().await;
    let topic = client.admin().create_topic(TOPIC).await.unwrap();
    let queue = client
        .new_queue_consumer(TOPIC, "durable")
        .build()
        .await
        .unwrap();
    queue.close().await.unwrap();
    let subscription = client
        .admin()
        .queue_subscription(TOPIC, "durable")
        .await
        .unwrap()
        .id()
        .to_string()
        .parse()
        .unwrap();
    let partition = PartitionKey {
        topic_id: topic.id().to_string().parse().unwrap(),
        partition: 0,
    };
    let service = fixture.running.get(&1).unwrap().service.clone();
    let route = service.route(partition, deadline()).await.unwrap();
    let pause = InstalledCommitPause::new(partition);
    let handler = service.clone();
    let pending = tokio::spawn(async move {
        handler
            .group_commit(&route, GroupCommand::OpenGate { subscription }, deadline())
            .await
    });
    tokio::time::timeout_at(deadline(), pause.0.ready.notified())
        .await
        .unwrap();
    assert!(
        service
            .data
            .get(&2)
            .unwrap()
            .state
            .with_application(|state| {
                state
                    .consumer_group_cursor(partition, subscription)
                    .unwrap()
                    .gate_open()
            })
            .await
    );
    let expires = pause.0.expires.lock().unwrap().unwrap();
    tokio::time::sleep_until(expires.into()).await;
    pause.0.resume.notify_one();
    let error = pending.await.unwrap().unwrap_err();
    assert_eq!(error.failure().code, i32::from(ErrorCode::Unavailable));
    assert_eq!(error.failure().outcome, i32::from(Outcome::Unknown));
    let report = format!("{:#}", anyhow::Error::new(error));
    assert!(report.contains("verify committed consumer writer"));
    assert_eq!(
        report.matches("Control authorization has expired").count(),
        1
    );
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_advanced_retention_floor_starts_new_subscriptions_at_available_history() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    let config = fixture.configs.get_mut(&1).unwrap();
    config.runtime.retention.minimum_age_ms = 0;
    config.runtime.archive_interval_ms = 60_000;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("test").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = client.admin().create_topic(TOPIC).await.unwrap();
    let queue = client
        .new_queue_consumer(TOPIC, "durable")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(TOPIC).build().await.unwrap();
    let receipt = producer.send("retained-until-acknowledged").await.unwrap();
    let delivery = queue.receive().await.unwrap();
    assert_eq!(delivery.message_id(), receipt.message_id());
    queue.ack(&delivery).await.unwrap();
    let service = &fixture.running.get(&1).unwrap().service;
    let partition = PartitionKey {
        topic_id: topic.id().to_string().parse().unwrap(),
        partition: 0,
    };
    let lease = service.data_lease(2, partition, deadline()).await.unwrap();
    let now_ms = unix_millis().unwrap().checked_add(2000).unwrap();
    let status = service
        .data
        .get(&2)
        .unwrap()
        .state
        .with_application(|state| state.retention_status(partition, now_ms).unwrap())
        .await;
    assert_eq!(status.maximum_floor, 1);
    assert!(matches!(
        service
            .data_write(
                2,
                DataCommand::Retention {
                    partition,
                    writer: WriterFence {
                        node_id: lease.grant().owner_node,
                        owner_epoch: lease.grant().epoch
                    },
                    command: crate::retention::Command {
                        expected_revision: status.revision,
                        before: 1,
                        now_ms
                    },
                },
                deadline()
            )
            .await
            .unwrap(),
        DataResponse::Retention(_)
    ));
    let late = client
        .new_queue_consumer(TOPIC, "after-retention")
        .build()
        .await
        .unwrap();
    let next = producer.send("after-retention").await.unwrap();
    let available = late.receive().await.unwrap();
    assert_eq!(available.message_id(), next.message_id());
    assert_eq!(available.payload(), b"after-retention");
    late.ack(&available).await.unwrap();
    late.close().await.unwrap();
    queue.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
