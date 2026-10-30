use std::sync::Arc;
use std::time::Duration;

use crate::config::Role;
use crate::metadata::{PartitionKey, TokenDigest};
use crate::ownership::remote::Query;
use crate::runtime::{data::consumer_tests::fixture, service::Service};
use tidemq_protocol::Id;
use tokio::time::Instant;

pub(in crate::runtime) const TOKEN: &str = "runtime-test-access-token-with-at-least-32-characters";

pub(in crate::runtime) struct Fixture {
    pub runtime: fixture::Fixture,
    pub client: tidemq_sdk::Client,
    pub service: Arc<Service>,
    pub credential: Id,
    pub partition: PartitionKey,
    pub shard: Id,
}

impl Fixture {
    pub async fn new() -> Self {
        Self::configured(false, false).await
    }

    pub async fn retention() -> Self {
        Self::configured(true, false).await
    }

    async fn separated() -> Self {
        Self::configured(false, true).await
    }

    async fn configured(retention: bool, separated: bool) -> Self {
        let mut runtime = if separated {
            fixture::Fixture::new(
                &[
                    (1, Role::Metadata),
                    (2, Role::Metadata),
                    (3, Role::Metadata),
                    (4, Role::Storage),
                    (5, Role::Storage),
                    (6, Role::Storage),
                    (7, Role::Broker),
                    (8, Role::Gateway),
                ],
                &[1, 2, 3],
                &[4, 5, 6],
            )
            .await
        } else {
            fixture::Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await
        };
        for config in runtime.configs.values_mut() {
            config.runtime.groups.push(crate::runtime::GroupConfig {
                group_id: 3,
                voters: if separated { vec![4, 5, 6] } else { Vec::new() },
                standby: Vec::new(),
            });
        }
        if retention {
            for config in runtime.configs.values_mut() {
                config.runtime.retention.minimum_age_ms = 1;
                config.runtime.archive_interval_ms = 60_000;
            }
        }
        runtime.start_all().await;
        let client = runtime.client(if separated { 8 } else { 1 }).await;
        let tenant = client.admin().create_tenant("typed").await.unwrap();
        client
            .admin()
            .create_namespace(tenant, "app")
            .await
            .unwrap();
        let topic = client
            .admin()
            .create_topic("typed/app/events")
            .await
            .unwrap();
        let topic: Id = topic.id().into();
        let node = runtime.running.get(&1).unwrap();
        let leader = node
            .service
            .leader(
                node.service.config.runtime.metadata_group,
                tokio::time::Instant::now() + std::time::Duration::from_secs(3),
            )
            .await
            .unwrap();
        let service = runtime.running.get(&leader).unwrap().service.clone();
        let (credential, shard) = service
            .metadata
            .as_ref()
            .unwrap()
            .state
            .try_with_application(|state| {
                (
                    state
                        .auth()
                        .authenticate(
                            TokenDigest::of(TOKEN).unwrap(),
                            crate::runtime::data::unix_millis().unwrap(),
                        )
                        .unwrap()
                        .id,
                    state.topology().domains(topic).next().unwrap().route.id,
                )
            })
            .await
            .unwrap();
        Self {
            runtime,
            client,
            service,
            credential,
            partition: PartitionKey {
                topic_id: topic,
                partition: 0,
            },
            shard,
        }
    }

    pub async fn close(self) {
        self.client.close().await.unwrap();
        self.runtime.close().await;
    }
}

#[tokio::test]
async fn local_peer_rejects_framed_work_before_constructing_an_envelope() {
    let fixture = Fixture::new().await;
    let mut rpc = fixture.service.rpc.clone();
    rpc.limits.max_control_frame_bytes = 1;
    let result = rpc
        .call(
            1,
            1,
            super::Request {
                kind: super::super::auth::AUTH_QUERY,
                ..Default::default()
            },
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .await;
    fixture.close().await;
    assert!(matches!(result, Err(super::Error::Invalid { .. })));
}

async fn sdk_lifecycle(fixture: Fixture) {
    use std::time::Duration;
    const TOPIC: &str = "typed/app/events";
    let stream = fixture
        .client
        .new_stream_consumer(TOPIC, "history")
        .build()
        .await
        .unwrap();
    let producer = fixture.client.new_producer(TOPIC).build().await.unwrap();
    for phase in 0..2 {
        let payload = format!("typed-lifecycle-{phase}");
        let receipt = producer
            .send(tidemq_sdk::Message::new(payload.clone()).key("ordered"))
            .await
            .unwrap();
        let message = tokio::time::timeout(Duration::from_secs(15), stream.receive())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.message_id(), receipt.message_id());
        assert_eq!(message.payload(), payload.as_bytes());
        stream.ack_cumulative(&message).await.unwrap();
        if phase == 0 {
            fixture
                .client
                .admin()
                .split_topic_at(TOPIC, 1 << 63)
                .await
                .unwrap();
        }
    }
    producer.close().await.unwrap();
    stream.close().await.unwrap();
    fixture
        .client
        .admin()
        .delete_stream_subscription(TOPIC, "history")
        .await
        .unwrap();
    fixture.client.admin().delete_topic(TOPIC).await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_native_sdk_preserves_stream_split_gates_and_deletion() {
    let fixture = Fixture::new().await;
    assert!(fixture.service.rpc.is_local());
    assert_eq!(fixture.runtime.running.len(), 1);
    sdk_lifecycle(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn separated_rf3_native_sdk_preserves_stream_split_gates_and_deletion() {
    let fixture = Fixture::separated().await;
    assert!(!fixture.service.rpc.is_local());
    assert_eq!(fixture.runtime.running.len(), 8);
    assert_eq!(
        fixture
            .service
            .metadata
            .as_ref()
            .unwrap()
            .current_placement()
            .unwrap()
            .raft_voters()
            .unwrap()
            .len(),
        3
    );
    for group in [2, 3] {
        assert_eq!(
            fixture
                .runtime
                .running
                .get(&4)
                .unwrap()
                .service
                .data
                .get(&group)
                .unwrap()
                .current_placement()
                .unwrap()
                .raft_voters()
                .unwrap()
                .len(),
            3
        );
    }
    sdk_lifecycle(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn separated_rf3_authority_query_honors_short_deadline_and_releases_rpc_path() {
    let fixture = Fixture::separated().await;
    let gateway = fixture.runtime.running.get(&8).unwrap().service.clone();
    let request = || super::Request::control(super::AUTHORITY, &Query::Group(2)).unwrap();

    let warm = gateway
        .rpc
        .call(1, 1, request(), Instant::now() + Duration::from_secs(3))
        .await;
    assert!(
        warm.is_ok(),
        "The remote authority path did not warm successfully: {warm:?}"
    );

    let started = Instant::now();
    let expired = gateway
        .rpc
        .call(1, 1, request(), Instant::now() + Duration::from_millis(1))
        .await;
    assert!(
        expired.is_err(),
        "A one-millisecond authority call unexpectedly succeeded"
    );
    assert!(started.elapsed() < Duration::from_secs(2));

    let recovered = gateway
        .rpc
        .call(1, 1, request(), Instant::now() + Duration::from_secs(3))
        .await;
    assert!(
        recovered.is_ok(),
        "A fresh authority query could not reuse the RPC path after expiry: {recovered:?}"
    );
    fixture.close().await;
}
