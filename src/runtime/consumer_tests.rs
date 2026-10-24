use std::{
    collections::BTreeMap,
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

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
use tidemq_sdk::{Bytes, Client};
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
mod fixture;
#[allow(dead_code)]
#[path = "../../tests/lifecycle_support/rustfs.rs"]
mod rustfs;
#[path = "../../tests/s3_support/mod.rs"]
mod s3;
use fixture::*;

#[path = "metadata_replay_tests.rs"]
mod metadata_replay_tests;

const TOKEN: &str = "runtime-test-access-token-with-at-least-32-characters";
const TOPIC: &str = "test/app/consumer-authority";
static COMMIT: Mutex<Option<Arc<CommitPause>>> = Mutex::new(None);

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
    fixture.enable_dynamic_auth().await;
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
async fn committed_consumer_mutation_expiry_preserves_unknown_semantics_and_source() {
    let (fixture, client) = setup().await;
    let topic = client.admin().create_logical_topic(TOPIC).await.unwrap();
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
async fn an_advanced_logical_retention_floor_rejects_expired_positions() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.enable_dynamic_auth().await;
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
    let topic = client.admin().create_logical_topic(TOPIC).await.unwrap();
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
    assert!(
        matches!(service.require_retained_offset(2, partition, 0, deadline()).await,
        Err(Error::Rejected { failure }) if failure.code == i32::from(ErrorCode::OffsetExpired)
            && failure.outcome == i32::from(Outcome::Rejected) && !failure.retryable)
    );
    service
        .require_retained_offset(2, partition, 1, deadline())
        .await
        .unwrap();
    queue.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}
