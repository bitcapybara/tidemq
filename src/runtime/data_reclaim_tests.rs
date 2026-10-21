use super::*;

use std::collections::BTreeMap;
use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use tidemq::config::{Config, Role, TlsConfig};
use tidemq::runtime::{GroupConfig, PeerConfig, RunningNode, RuntimeConfig};
use tidemq_sdk::{
    Bytes, Client, ClientConfig, ClientTls, Consumer, ConsumerConfig, Id, PeerTarget, Producer,
    public,
};
use tokio::sync::Notify;

// These shared integration fixtures also contain helpers used by other runtime/S3 suites.
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
const MESSAGES: u64 = 8;
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static HOOK: Mutex<Option<Arc<HotDeliveryHook>>> = Mutex::new(None);

struct HotDeliveryHook {
    partition: PartitionKey,
    offset: u64,
    point: HookPoint,
    hot_pages: AtomicUsize,
    ready: Notify,
    resume: Notify,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum HookPoint {
    BeforeHotDelivery,
    AfterDurableDelivery,
    RejectHotRead,
    CountHotPages,
}

struct InstalledHook(Arc<HotDeliveryHook>);

impl InstalledHook {
    fn new(partition: PartitionKey, offset: u64, point: HookPoint) -> Self {
        let hook = Arc::new(HotDeliveryHook {
            partition,
            offset,
            point,
            hot_pages: AtomicUsize::new(0),
            ready: Notify::new(),
            resume: Notify::new(),
        });
        assert!(HOOK.lock().unwrap().replace(hook.clone()).is_none());
        Self(hook)
    }
}

impl Drop for InstalledHook {
    fn drop(&mut self) {
        let mut slot = HOOK.lock().unwrap();
        if slot.as_ref().is_some_and(|hook| Arc::ptr_eq(hook, &self.0)) {
            slot.take();
        }
        self.0.resume.notify_one();
    }
}

fn take_hook(
    partition: PartitionKey,
    offset: u64,
    point: HookPoint,
) -> Option<Arc<HotDeliveryHook>> {
    let mut slot = HOOK.lock().unwrap();
    if slot.as_ref().is_some_and(|hook| {
        hook.partition == partition && hook.offset == offset && hook.point == point
    }) {
        slot.take()
    } else {
        None
    }
}

pub(super) async fn pause_hot_delivery(partition: PartitionKey, offset: u64, deadline: Instant) {
    if let Some(hook) = take_hook(partition, offset, HookPoint::BeforeHotDelivery) {
        hook.ready.notify_one();
        let _ = tokio::time::timeout_at(deadline, hook.resume.notified()).await;
    }
}

pub(super) fn lose_delivery_reply(partition: PartitionKey, offset: u64) -> bool {
    take_hook(partition, offset, HookPoint::AfterDurableDelivery).is_some()
}

pub(super) fn reject_hot_read(partition: PartitionKey, offset: u64) -> bool {
    take_hook(partition, offset, HookPoint::RejectHotRead).is_some()
}

pub(super) fn observe_hot_page(partition: PartitionKey) {
    if let Some(hook) = HOOK
        .lock()
        .unwrap()
        .as_ref()
        .filter(|hook| hook.partition == partition && hook.point == HookPoint::CountHotPages)
    {
        hook.hot_pages.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_read_reuses_its_bounded_hot_page_and_preserves_durable_ack_identity() {
    let _serial = SERIAL.lock().await;
    let mut fixture = Fixture::new(
        &[
            (1, Role::Combined),
            (2, Role::Combined),
            (3, Role::Combined),
        ],
        &[1, 2, 3],
        &[1, 2, 3],
    )
    .await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let (topic, subscription) = create(&client).await;
    let partition = PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
        .await
        .unwrap();
    let consumer = Consumer::claim(
        client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    producer
        .produce(
            (0_u64..8)
                .map(|identity| public::Message {
                    key: Bytes::copy_from_slice(&identity.to_be_bytes()),
                    body: Bytes::from_static(b"bounded-hot-page"),
                })
                .collect(),
        )
        .await
        .unwrap();
    let hook = InstalledHook::new(partition, 0, HookPoint::CountHotPages);
    let started = Instant::now();
    let first_count = consumer.fill().await.unwrap();
    let first_elapsed = started.elapsed();
    let hot_pages = hook.0.hot_pages.load(Ordering::Relaxed);
    drop(hook);
    let mut seen = std::collections::BTreeSet::new();
    let mut frontier = 0;
    for _ in 0..8 {
        let mut pending = Vec::new();
        while let Some(delivery) = consumer.next().await.unwrap() {
            assert_eq!(delivery.key(), &delivery.offset().to_be_bytes());
            assert_eq!(delivery.body(), b"bounded-hot-page");
            assert!(seen.insert(delivery.offset()));
            pending.push(delivery);
        }
        if !pending.is_empty() {
            frontier = consumer
                .ack_many(&pending)
                .await
                .unwrap()
                .acknowledged_before;
        }
        if seen.len() == 8 {
            break;
        }
        consumer.fill().await.unwrap();
    }
    let assignment = client
        .admin()
        .subscription_status(topic, 0, subscription)
        .await
        .unwrap()
        .assignment
        .unwrap();
    client.close().await;
    drop(consumer);
    drop(producer);
    fixture.close().await;
    assert!(first_count > 1);
    assert_eq!(seen, (0..8).collect());
    assert_eq!(frontier, 8);
    assert_eq!(assignment.acknowledged_before, 8);
    assert_eq!(assignment.cold_before, 0);
    eprintln!(
        "Original live Read returned {first_count} deliveries in {first_elapsed:?} using {hot_pages} successful quorum-backed hot pages"
    );
    assert_eq!(
        hot_pages, 2,
        "A bounded data page and the original empty probe must share the same Read"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_read_retains_page_charge_until_its_owned_work_finishes() {
    let _serial = SERIAL.lock().await;
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let (topic, subscription) = create(&client).await;
    let partition = PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
        .await
        .unwrap();
    let consumer = Consumer::claim(
        client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    producer
        .produce(vec![public::Message {
            key: Bytes::new(),
            body: Bytes::from_static(b"cancelled-hot-page"),
        }])
        .await
        .unwrap();
    let service = fixture.running.get(&1).unwrap().service.clone();
    let broker = service.brokers.get(&(2, 1)).unwrap().clone();
    let before = service.budget.available_data_bytes();
    let hook = InstalledHook::new(partition, 0, HookPoint::BeforeHotDelivery);
    let until = deadline();
    let cancellation = tidemq_sdk::CancellationToken::new();
    let context = tidemq_sdk::RequestContext::new(until, cancellation.clone());
    let filling = consumer.clone();
    let pending = tokio::spawn(async move { context.scope(filling.fill()).await });
    tokio::time::timeout_at(until, hook.0.ready.notified())
        .await
        .unwrap();
    let page_charge = service.config.protocol.max_frame_bytes as usize - 8192 + 512;
    assert!(service.budget.available_data_bytes() <= before - page_charge);
    let mut draining = Box::pin(broker.drain(until.into_std()));
    let still_held = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(draining.as_mut(), cx).is_pending())
    })
    .await;
    assert!(
        still_held,
        "The page must retain its original admission while paused"
    );
    cancellation.cancel();
    let result = pending.await.unwrap();
    assert!(
        matches!(&result, Err(tidemq_sdk::Error::RequestUnknown { source })
            if matches!(source.as_ref(), tidemq_sdk::Error::Cancelled)),
        "Cancellation after submission must retain its unknown outcome: {result:?}"
    );
    // Caller cancellation does not require an already owned request to stop immediately.
    hook.0.resume.notify_one();
    tokio::time::timeout_at(until, draining)
        .await
        .expect("Finishing owned work must release the page within the original deadline")
        .unwrap();
    // The lane and memory permits share one Admission, released after owned work finishes.
    drop(hook);
    let issued = service
        .data
        .get(&2)
        .unwrap()
        .state
        .with_application(|state| {
            state
                .subscriptions(partition)
                .unwrap()
                .diagnostics(subscription)
                .unwrap()
                .next_delivery_offset
        })
        .await;
    client.close().await;
    drop(consumer);
    drop(producer);
    drop(service);
    fixture.close().await;
    assert!(issued <= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the verified RustFS 1.0.3 executable in TIDEMQ_TEST_RUSTFS_BIN"]
async fn first_hot_delivery_reloads_cold_after_concurrent_local_reclamation() {
    run_reclaimed_read(0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the verified RustFS 1.0.3 executable in TIDEMQ_TEST_RUSTFS_BIN"]
async fn hot_reclamation_returns_only_the_durable_prefix_before_resuming_cold() {
    run_reclaimed_read(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_delivery_after_a_durable_prefix_cannot_become_partial_success() {
    let _serial = SERIAL.lock().await;
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let (topic, subscription) = create(&client).await;
    let partition = PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
        .await
        .unwrap();
    let consumer = Consumer::claim(
        client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    producer
        .produce(vec![
            public::Message {
                key: Bytes::new(),
                body: Bytes::from_static(b"unknown-delivery"),
            };
            3
        ])
        .await
        .unwrap();
    // Lose the reply only after the second real RecordDelivery is durable and fence-checked.
    let _hook = InstalledHook::new(partition, 1, HookPoint::AfterDurableDelivery);
    let result = consumer.fill().await;
    let buffered = consumer.next().await.unwrap();
    let assignment = client
        .admin()
        .subscription_status(topic, 0, subscription)
        .await
        .unwrap()
        .assignment
        .unwrap();
    let later = consumer.recv().await.unwrap().unwrap();
    let later_offset = later.offset();
    assert!(later.key().is_empty());
    assert_eq!(later.body(), b"unknown-delivery");
    let later_ack = consumer.ack(&later).await.unwrap();
    let retries = fixture
        .running
        .get(&1)
        .unwrap()
        .service
        .data
        .get(&2)
        .unwrap()
        .state
        .with_application(|state| {
            let subscriptions = state.subscriptions(partition).unwrap();
            assert_eq!(subscriptions.protected_before(), Some(0));
            subscriptions
                .due_retries(
                    subscription,
                    unix_millis().unwrap() + subscription::Limits::default().ack_timeout_ms,
                    3,
                )
                .unwrap()
                .into_iter()
                .map(|retry| retry.offset)
                .collect::<Vec<_>>()
        })
        .await;
    client.close().await;
    drop(later);
    drop(consumer);
    drop(producer);
    drop(client);
    fixture.close().await;
    assert!(
        matches!(
            result,
            Err(tidemq_sdk::Error::Remote {
                code: ErrorCode::Unavailable,
                outcome: Outcome::Unknown,
                ..
            })
        ),
        "An uncertain delivery must fail the complete Read: {result:?}"
    );
    assert!(
        buffered.is_none(),
        "No durable prefix may enter the SDK buffer after Unknown"
    );
    assert_eq!(assignment.next_delivery_offset, 2);
    assert_eq!(assignment.issued_before, 2);
    assert_eq!(assignment.acknowledged_before, 0);
    assert_eq!(assignment.cold_before, 0);
    assert_eq!(later_offset, 2);
    assert_eq!(later_ack.acknowledged_before, 0);
    assert_eq!(retries, vec![0, 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_hot_read_rejection_cannot_hide_a_durable_delivery_prefix() {
    let _serial = SERIAL.lock().await;
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let (topic, subscription) = create(&client).await;
    let partition = PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
        .await
        .unwrap();
    let consumer = Consumer::claim(
        client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    producer
        .produce(vec![
            public::Message {
                key: Bytes::new(),
                body: Bytes::from_static(b"later-read-rejection"),
            };
            3
        ])
        .await
        .unwrap();
    let _hook = InstalledHook::new(partition, 1, HookPoint::RejectHotRead);
    let result = consumer.fill().await;
    assert!(
        matches!(
            result,
            Err(tidemq_sdk::Error::Remote {
                code: ErrorCode::Unavailable,
                outcome: Outcome::Unknown,
                ..
            })
        ),
        "A failed later read cannot hide already durable deliveries: {result:?}"
    );
    assert!(consumer.next().await.unwrap().is_none());
    let assignment = client
        .admin()
        .subscription_status(topic, 0, subscription)
        .await
        .unwrap()
        .assignment
        .unwrap();
    let mut later = Vec::new();
    for _ in 0..2 {
        let delivery = consumer.recv().await.unwrap().unwrap();
        assert_eq!(delivery.body(), b"later-read-rejection");
        later.push(delivery);
    }
    let later_offsets = later
        .iter()
        .map(|delivery| delivery.offset())
        .collect::<std::collections::BTreeSet<_>>();
    let ack = consumer.ack_many(&later).await.unwrap();
    let (due_now, due_after_lease) = fixture
        .running
        .get(&1)
        .unwrap()
        .service
        .data
        .get(&2)
        .unwrap()
        .state
        .with_application(|state| {
            let subscriptions = state.subscriptions(partition).unwrap();
            assert_eq!(subscriptions.protected_before(), Some(0));
            let now = unix_millis().unwrap();
            (
                subscriptions.due_retries(subscription, now, 3).unwrap(),
                subscriptions
                    .due_retries(
                        subscription,
                        now + subscription::Limits::default().ack_timeout_ms,
                        3,
                    )
                    .unwrap()
                    .into_iter()
                    .map(|retry| retry.offset)
                    .collect::<Vec<_>>(),
            )
        })
        .await;
    client.close().await;
    drop(later);
    drop(consumer);
    drop(producer);
    drop(client);
    fixture.close().await;
    assert_eq!(assignment.next_delivery_offset, 1);
    assert_eq!(assignment.issued_before, 1);
    assert_eq!(assignment.acknowledged_before, 0);
    assert_eq!(assignment.cold_before, 0);
    assert_eq!(later_offsets, (1..3).collect());
    assert_eq!(ack.acknowledged_before, 0);
    assert!(due_now.is_empty());
    assert_eq!(due_after_lease, vec![0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_real_logical_retention_floor_still_rejects_the_expired_offset() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    let config = fixture.configs.get_mut(&1).unwrap();
    config.runtime.retention.minimum_age_ms = 0;
    config.runtime.archive_interval_ms = 60_000;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let (topic, subscription) = create(&client).await;
    let partition = PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
        .await
        .unwrap();
    let consumer = Consumer::claim(
        client.clone(),
        topic,
        0,
        subscription,
        ConsumerConfig::default(),
    )
    .await
    .unwrap();
    producer
        .produce(vec![public::Message {
            key: Bytes::new(),
            body: Bytes::from_static(b"retained-until-acknowledged"),
        }])
        .await
        .unwrap();
    assert_eq!(consumer.fill().await.unwrap(), 1);
    let delivered = consumer.next().await.unwrap().unwrap();
    assert_eq!(
        consumer.ack(&delivered).await.unwrap().acknowledged_before,
        1
    );
    let service = &fixture.running.get(&1).unwrap().service;
    let lease = service.data_lease(2, partition, deadline()).await.unwrap();
    let now_ms = unix_millis().unwrap().checked_add(2000).unwrap();
    let status = service
        .data
        .get(&2)
        .unwrap()
        .state
        .with_application(|state| {
            let protected = state.subscriptions(partition).unwrap().protected_before();
            state
                .retention(partition)
                .unwrap()
                .status(now_ms, protected)
        })
        .await;
    assert_eq!(status.maximum_floor, 1);
    let response = service
        .data_write(
            2,
            DataCommand::Retention {
                partition,
                writer: WriterFence {
                    node_id: lease.grant().owner_node,
                    owner_epoch: lease.grant().epoch,
                },
                command: crate::retention::Command {
                    expected_revision: status.revision,
                    before: 1,
                    now_ms,
                },
            },
            deadline(),
        )
        .await
        .unwrap();
    assert!(matches!(response, DataResponse::Retention(_)));
    let expired = service
        .require_retained_offset(2, partition, 0, deadline())
        .await;
    let valid = service
        .require_retained_offset(2, partition, 1, deadline())
        .await;
    let seek = consumer.seek(0, Id::random().unwrap()).await;
    client.close().await;
    drop(delivered);
    drop(consumer);
    drop(producer);
    drop(client);
    fixture.close().await;
    assert!(matches!(expired, Err(Error::Rejected { failure })
        if failure.code == i32::from(ErrorCode::OffsetExpired)
            && failure.outcome == i32::from(Outcome::Rejected) && !failure.retryable));
    assert!(valid.is_ok());
    assert!(matches!(
        seek,
        Err(tidemq_sdk::Error::Remote {
            code: ErrorCode::OffsetExpired,
            outcome: Outcome::Rejected,
            retryable: false,
            ..
        })
    ));
}

async fn run_reclaimed_read(paused_offset: u64) {
    let _serial = SERIAL.lock().await;
    let rustfs = rustfs::RustFs::start().await;
    rustfs.create_bucket().await;
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    enable_objects(&mut fixture, &rustfs).await;
    fixture
        .configs
        .get_mut(&1)
        .unwrap()
        .runtime
        .archive_interval_ms = 60_000;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let (topic, subscription) = create(&client).await;
    let partition = PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let producer = Producer::open(client.clone(), topic, 0, Id::random().unwrap())
        .await
        .unwrap();
    // Claim before writing: the same live assignment must keep H=0 across local reclamation.
    let consumer = Arc::new(
        Consumer::claim(
            client.clone(),
            topic,
            0,
            subscription,
            ConsumerConfig::default(),
        )
        .await
        .unwrap(),
    );
    let messages = (0..MESSAGES)
        .map(|offset| public::Message {
            key: Bytes::copy_from_slice(&offset.to_be_bytes()),
            body: Bytes::from(vec![0x5a; 1024]),
        })
        .collect();
    assert_eq!(
        producer.produce(messages).await.unwrap().message_count,
        MESSAGES as u32
    );
    let service = &fixture.running.get(&1).unwrap().service;
    let coordinator = service.objects.get(&2).unwrap();
    // Perform real S3 publication and all voter checkpoints before the narrow race window.
    let prepared = coordinator
        .prepare(
            vec![crate::lifecycle::RangeRequest {
                partition,
                offsets: tidemq_protocol::state::OffsetRange::new(0, MESSAGES).unwrap(),
            }],
            Vec::new(),
            deadline().into_std(),
        )
        .await
        .unwrap();
    let object = prepared.object_id();
    coordinator
        .register(&prepared, deadline().into_std())
        .await
        .unwrap();
    coordinator
        .authorize(object, deadline().into_std())
        .await
        .unwrap();
    coordinator
        .upload(&prepared, deadline().into_std())
        .await
        .unwrap();
    coordinator
        .publish(object, deadline().into_std())
        .await
        .unwrap();
    coordinator
        .checkpoint(object, deadline().into_std())
        .await
        .unwrap();

    let hook = InstalledHook::new(partition, paused_offset, HookPoint::BeforeHotDelivery);
    let until = deadline();
    let reading = {
        let consumer = consumer.clone();
        tokio::spawn(async move {
            tidemq_sdk::RequestContext::with_deadline(until)
                .scope(consumer.fill())
                .await
        })
    };
    tokio::time::timeout_at(until, hook.0.ready.notified())
        .await
        .unwrap();
    // The runtime now owns a valid hot body, but has not submitted its RecordDelivery.
    coordinator
        .reclaim(partition, MESSAGES, until.into_std())
        .await
        .unwrap();
    let boundaries = service
        .data
        .get(&2)
        .unwrap()
        .state
        .with_application(|state| {
            let producer = state.producer(partition).unwrap();
            (producer.local_reclaim_end(), producer.retained_floor())
        })
        .await;
    assert_eq!(boundaries, (MESSAGES, 0));
    hook.0.resume.notify_one();
    let first = tokio::time::timeout_at(until, reading)
        .await
        .unwrap()
        .unwrap();
    let result = consume_all(&consumer, first, paused_offset, until).await;
    let assignment = client
        .admin()
        .subscription_status(topic, 0, subscription)
        .await
        .unwrap()
        .assignment
        .unwrap();
    let metrics = http(
        fixture.running.get(&1).unwrap().management_address,
        "/metrics",
    )
    .await;
    let conflicts = metrics
        .lines()
        .find_map(|line| line.strip_prefix("tidemq_runtime_read_hot_reclaim_conflicts_total "))
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0);
    client.close().await;
    drop(consumer);
    drop(producer);
    drop(client);
    fixture.close().await;
    eprintln!(
        "Reclaimed hot delivery offset {paused_offset}: conflicts={conflicts}, result={result:?}"
    );
    assert_eq!(
        conflicts, 1,
        "The regression must hit the actual hot delivery rejection once"
    );
    result.expect("Local body reclamation must not masquerade as logical OffsetExpired");
    assert_eq!(assignment.cold_before, 0);
    assert_eq!(assignment.acknowledged_before, MESSAGES);
}

async fn consume_all(
    consumer: &Consumer,
    first: tidemq_sdk::Result<usize>,
    paused_offset: u64,
    until: Instant,
) -> std::result::Result<(), String> {
    let mut count =
        first.map_err(|error| format!("The first active Read was rejected: {error:?}"))?;
    if paused_offset > 0 {
        assert_eq!(count as u64, paused_offset);
    } else {
        assert!(count > 0);
    }
    let mut next = 0;
    while next < MESSAGES {
        let mut page = Vec::new();
        while let Some(delivery) = consumer.next().await.unwrap() {
            assert_eq!(delivery.offset(), next);
            assert_eq!(delivery.key(), &next.to_be_bytes());
            assert_eq!(delivery.body(), vec![0x5a; 1024].as_slice());
            next += 1;
            page.push(delivery);
        }
        assert_eq!(page.len(), count);
        if !page.is_empty() {
            let ack = tidemq_sdk::RequestContext::with_deadline(until)
                .scope(consumer.ack_many(&page))
                .await
                .map_err(|error| format!("The original delivery ACK was rejected: {error:?}"))?;
            assert_eq!(ack.acknowledged_before, next);
        }
        drop(page);
        if next < MESSAGES {
            count = tidemq_sdk::RequestContext::with_deadline(until)
                .scope(consumer.fill())
                .await
                .map_err(|error| format!("The original consumer could not resume: {error:?}"))?;
        }
    }
    assert_eq!(next, MESSAGES);
    Ok(())
}
