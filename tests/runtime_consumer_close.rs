//! Original consumer Join recovery against real QUIC, metadata Raft, and data Raft.
use std::{
    collections::BTreeMap,
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
    time::Duration,
};

use bytes::Bytes;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use tidemq::{
    config::{Config, Role, TlsConfig},
    runtime::{
        GroupConfig, PeerConfig, RunningNode, RuntimeConfig,
        consumer_join_hook::{InstalledJoinPause, Point},
    },
};
use tidemq_protocol::{
    Id,
    console::{Query, Reply},
};
use tidemq_sdk::Client;
use tokio::time::Instant;

#[allow(dead_code)]
#[path = "runtime_support/fixture.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "lifecycle_support/rustfs.rs"]
mod rustfs;
#[path = "s3_support/mod.rs"]
mod s3;
#[allow(dead_code)]
#[path = "runtime_support/udp_proxy.rs"]
mod udp_proxy;
use fixture::*;

const TOKEN: &str = "runtime-test-access-token-with-at-least-32-characters";
const TOPIC: &str = "close/app/events";

struct CloseFixture {
    runtime: Fixture,
    root: Client,
    client: Client,
    proxy: udp_proxy::Proxy,
    first: tidemq_sdk::QueueConsumer,
    second: tidemq_sdk::QueueConsumer,
    topic: Id,
    subscription: Id,
    member: Id,
    epoch: u64,
}

impl CloseFixture {
    async fn new() -> Self {
        let mut runtime = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        runtime.start_all().await;
        let root = runtime.client(1).await;
        let tenant = root.admin().create_tenant("close").await.unwrap();
        root.admin().create_namespace(tenant, "app").await.unwrap();
        let topic = root.admin().create_topic(TOPIC).await.unwrap().id().into();
        let backend = runtime.configs.get(&1).unwrap().listen;
        let proxy = udp_proxy::Proxy::start([backend, backend]).await;
        let client = Client::builder(
            proxy.address.to_string(),
            tidemq_sdk::TlsTrust::ca_pem(runtime.roots.clone()),
            tidemq_sdk::Credentials::token(TOKEN),
        )
        .request_timeout(Duration::from_secs(10))
        .close_timeout(Duration::from_secs(5))
        .build()
        .await
        .unwrap();
        let first = client
            .new_queue_consumer(TOPIC, "jobs")
            .prefetch_messages(1)
            .build()
            .await
            .unwrap();
        let Reply::Subscriptions(page) = root
            .console_read(Query::Subscriptions {
                topic: Some(topic),
                after: None,
                limit: 32,
            })
            .await
            .unwrap()
        else {
            panic!("Expected durable subscriptions");
        };
        let subscription = page.items.iter().find(|row| row.name == "jobs").unwrap().id;
        let observed = members(&root, topic, subscription).await;
        assert_eq!(observed.len(), 1);
        let member = observed.first().unwrap().id;
        let epoch = observed.first().unwrap().epoch.0;
        let second = root
            .new_queue_consumer(TOPIC, "jobs")
            .prefetch_messages(1)
            .build()
            .await
            .unwrap();
        Self {
            runtime,
            root,
            client,
            proxy,
            first,
            second,
            topic,
            subscription,
            member,
            epoch,
        }
    }

    async fn expire_original(&self) {
        self.proxy.set_mtu(0);
        let until = Instant::now() + Duration::from_secs(30);
        loop {
            let rows = members(&self.root, self.topic, self.subscription).await;
            if rows.iter().any(|row| {
                row.id == self.member
                    && row.epoch.0 == self.epoch
                    && !row.lease_valid
                    && row.status.eq_ignore_ascii_case("expired")
            }) {
                return;
            }
            assert!(
                Instant::now() < until,
                "The original member did not durably expire under the unchanged lease"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

async fn members(
    client: &Client,
    topic: Id,
    subscription: Id,
) -> Vec<tidemq_protocol::console::Member> {
    let Reply::Members(page) = client
        .console_read(Query::Members {
            topic,
            subscription,
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected durable member observations");
    };
    page.items
}

async fn original_rejoin_closes(point: Point) {
    let fixture = CloseFixture::new().await;
    fixture.expire_original().await;
    let pause = InstalledJoinPause::install(fixture.topic, fixture.member, point).unwrap();
    fixture.proxy.set_mtu(1200);
    let identity = pause
        .reached(Instant::now() + Duration::from_secs(20))
        .await
        .unwrap();
    assert_eq!(identity.member, fixture.member);
    assert_eq!(identity.subscription, fixture.subscription);
    assert!(identity.epoch > fixture.epoch);
    if point == Point::Reply {
        let observed = pause.snapshot().unwrap();
        let (changed, status) = (observed.changed_identity, observed.original_status);
        assert!(!changed);
        assert_eq!(status.unwrap().0, identity.epoch);
    }
    let close = fixture.first.close();
    tokio::pin!(close);
    if point == Point::BeforeCommit {
        tokio::select! {
            result = pause.replayed(Instant::now() + Duration::from_secs(5)) => result.unwrap(),
            result = &mut close => panic!("Close did not resolve its accepted original Join: {result:?}"),
        }
        pause.resume();
    }
    let report = close
        .await
        .expect("The original Join must close within the original five-second budget");
    assert!(report.local_cleanup_complete());
    assert_eq!(report.members_left(), 1);
    assert_eq!(report.unresolved_joins(), 0);
    assert_eq!(report.unresolved_acks(), 0);
    assert_eq!(report.unresolved_reads(), 0);
    pause.resume();
    pause
        .finished(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let observed = pause.snapshot().unwrap();
    let (attempts, changed, status) = (
        observed.attempts,
        observed.changed_identity,
        observed.original_status,
    );
    assert!(attempts >= 2);
    assert!(
        !changed,
        "Recovery changed the original operation, digest, client, credential, or epoch"
    );
    eprintln!(
        "RF1 original Join closed: point={point:?}, operation={}, member={}, old_epoch={}, joined_epoch={}, attempts={attempts}, original_status={status:?}, evidence={}",
        identity.operation,
        identity.member,
        fixture.epoch,
        identity.epoch,
        fixture.runtime.directory.display()
    );
    let rows = members(&fixture.root, fixture.topic, fixture.subscription).await;
    assert!(
        !rows
            .iter()
            .any(|row| row.id == fixture.member && row.epoch.0 == identity.epoch)
    );
    let other = fixture
        .second
        .close()
        .await
        .expect("The other original consumer must retain its five-second close budget");
    assert!(other.local_cleanup_complete());
    assert_eq!(other.members_left(), 1);
    assert_eq!(other.unresolved_joins(), 0);
    let client = fixture.client.close().await.unwrap();
    assert!(client.local_cleanup_complete());
    assert_eq!(client.unresolved_joins(), 0);
    assert_eq!(
        fixture
            .client
            .transport_metrics()
            .snapshot()
            .active_connections,
        0
    );
    let root = fixture.root.close().await.unwrap();
    assert!(root.local_cleanup_complete());
    assert_eq!(root.unresolved_joins(), 0);
    assert_eq!(
        fixture
            .root
            .transport_metrics()
            .snapshot()
            .active_connections,
        0
    );
    drop(pause);
    fixture.proxy.close().await;
    fixture.runtime.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf1_close_recovers_the_committed_original_join_when_its_reply_is_unseen() {
    original_rejoin_closes(Point::Reply).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf1_close_recovers_the_original_inflight_join_before_its_late_commit() {
    original_rejoin_closes(Point::BeforeCommit).await;
}
