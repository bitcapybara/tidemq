#![cfg(feature = "test-hooks")]
//! Root-package integration target: real SDK, QUIC and local WAL; no transport peer mock.

use bytes::Bytes;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use std::{
    collections::BTreeMap,
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
    time::Duration,
};
use tidemq::{
    config::{Config, Role, TlsConfig},
    metadata::PartitionKey,
    runtime::{GroupConfig, PeerConfig, RunningNode, RuntimeConfig},
};
use tidemq_protocol::Id;
use tidemq_sdk::{Client, ConsumerMode, Message, RequestContext};
use tokio::time::Instant;

#[allow(dead_code)]
#[path = "../../../../tests/runtime_support/fixture.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "../../../../tests/runtime_support/group_watch.rs"]
mod group_watch;
#[allow(dead_code)]
#[path = "../../../../tests/lifecycle_support/rustfs.rs"]
mod rustfs;
#[path = "../../../../tests/s3_support/mod.rs"]
mod s3;
use fixture::*;

const TOKEN: &str = "runtime-test-access-token-with-at-least-32-characters";
const TOPIC: &str = "test/app/sdk-protected";
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn standalone() -> Fixture {
    let mut runtime = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    runtime.configs.get_mut(&1).unwrap().runtime.groups = (2..=10)
        .map(|group_id| GroupConfig {
            group_id,
            voters: Vec::new(),
            standby: Vec::new(),
        })
        .collect();
    runtime.start_all().await;
    runtime
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_handoff_preserves_original_read_retirement_and_cumulative_ack() {
    let _serial = SERIAL.lock().await;
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter("tidemq_sdk::consumer_recovery=debug")
        .try_init();
    let runtime = standalone().await;
    let client = runtime
        .client_with_timeouts(1, Duration::from_secs(30), Duration::from_secs(5))
        .await;
    let tenant = client.admin().create_tenant("test").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    const TOPIC_HANDOFF: &str = "test/app/sdk-handoff";
    client.admin().create_topic(TOPIC_HANDOFF).await.unwrap();
    let first = client
        .new_stream_consumer(TOPIC_HANDOFF, "history")
        .prefetch_messages(64)
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(TOPIC_HANDOFF).build().await.unwrap();
    for index in 0..40 {
        producer
            .send(Message::new(format!("old-{index}")).key(format!("key-{index}")))
            .await
            .unwrap();
    }
    let mut old = Vec::new();
    for _ in 0..40 {
        old.push(
            tokio::time::timeout(Duration::from_secs(15), first.receive())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let second = client
        .new_stream_consumer(TOPIC_HANDOFF, "history")
        .build()
        .await
        .unwrap();
    let handed_off = tokio::time::timeout(Duration::from_secs(30), second.receive())
        .await
        .unwrap()
        .unwrap();
    let revoked = old
        .iter()
        .find(|message| message.message_id() == handed_off.message_id())
        .unwrap();
    second.ack_cumulative(&handed_off).await.unwrap();
    for index in 0..40 {
        producer
            .send(Message::new(format!("new-{index}")).key(format!("key-{index}")))
            .await
            .unwrap();
    }
    let retained = tokio::time::timeout(Duration::from_secs(30), first.receive())
        .await
        .unwrap()
        .unwrap();
    assert!(retained.payload().starts_with(b"new-"));
    first.ack_cumulative(&retained).await.unwrap();
    assert!(matches!(
        first.ack_cumulative(revoked).await,
        Err(tidemq_sdk::Error::StaleDelivery)
    ));
    producer.close().await.unwrap();
    second.close().await.unwrap();
    first.close().await.unwrap();
    drop((old, handed_off, retained));
    client.close().await.unwrap();
    assert_eq!(client.metrics().snapshot().admitted_bytes, 0);
    runtime.close().await;
}

fn key_for(shard: &tidemq_sdk::ShardInfo, topic: Id) -> Vec<u8> {
    (0_u64..100_000)
        .map(|sequence| sequence.to_be_bytes().to_vec())
        .find(|key| {
            let hash = tidemq_protocol::topic::routing_hash(Some(key), topic, 0).unwrap();
            hash >= shard.hash_start && shard.hash_end.is_none_or(|end| hash < end)
        })
        .expect("A bounded key search must reach the active shard")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_child_read_waits_for_its_gate_without_changing_original_identity() {
    use tidemq::runtime::consumer_read_test_hooks::InstalledReadReplyFault;
    let _serial = SERIAL.lock().await;
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter("tidemq_sdk::consumer_recovery=debug")
        .try_init();
    let runtime = standalone().await;
    let client = runtime.client(1).await;
    let tenant = client.admin().create_tenant("test").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    const GATED: &str = "test/app/sdk-gated-child";
    let topic = client.admin().create_topic(GATED).await.unwrap();
    let topic_id: Id = topic.id().to_string().parse().unwrap();
    let consumer = client
        .new_stream_consumer(GATED, "gated")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(GATED).build().await.unwrap();
    let old = producer.send(Message::new("predecessor")).await.unwrap();
    let held = tokio::time::timeout(Duration::from_secs(15), consumer.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.message_id(), old.message_id());
    client
        .admin()
        .split_topic_at(GATED, 1_u64 << 63)
        .await
        .unwrap();
    let active: Vec<_> = client
        .admin()
        .topic_shards(GATED)
        .await
        .unwrap()
        .into_iter()
        .filter(|shard| !shard.sealed)
        .collect();
    let shard = active.first().unwrap();
    let key = key_for(shard, topic_id);
    let wire = group_watch::WireClient::connect(&runtime, 1).await;
    let layout_reply = wire
        .call(group_watch::frame(
            tidemq_protocol::common::FrameKind::GetTopicLayout,
            Id::new_v7().unwrap(),
            &tidemq_protocol::public::GetTopicLayout {
                topic_id: Bytes::copy_from_slice(topic_id.as_bytes()),
                ..Default::default()
            },
        ))
        .await;
    let layout: tidemq_protocol::public::TopicLayoutResult = group_watch::decoded(
        &layout_reply,
        tidemq_protocol::common::FrameKind::TopicLayoutResult,
    );
    let child = layout
        .active
        .iter()
        .find(|entry| {
            entry.shard_id.as_ref() == shard.id.to_string().parse::<Id>().unwrap().as_bytes()
        })
        .unwrap();
    let child_id = Id::from_slice(&child.shard_id).unwrap();
    let cluster = runtime.configs.get(&1).unwrap().cluster_id.parse().unwrap();
    let fault = InstalledReadReplyFault::install(
        cluster,
        vec![PartitionKey {
            topic_id,
            partition: child.birth_partition,
        }],
    )
    .unwrap();
    let sent = producer
        .send(Message::new("successor").key(key))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            assert!(
                consumer.try_receive().unwrap().is_none(),
                "A closed child gate delivered before the predecessor was acknowledged"
            );
            let diagnostics = consumer.diagnostics().unwrap();
            if diagnostics.assigned_targets >= active.len() && diagnostics.recovery_queue > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("The child Read did not reach the closed gate");
    assert!(
        !fault
            .wait_for_targets(1, Instant::now() + Duration::from_millis(50))
            .await,
        "The child returned a successful Read before the predecessor was acknowledged"
    );
    consumer.ack_cumulative(&held).await.unwrap();
    assert!(
        fault
            .wait_for_targets(1, Instant::now() + Duration::from_secs(15))
            .await
    );
    let attempts = fault.attempts();
    assert!(!attempts.is_empty() && !fault.overflowed());
    let original = attempts.first().unwrap();
    assert!(
        attempts
            .iter()
            .all(|attempt| attempt.operation == original.operation
                && attempt.sequence == original.sequence
                && attempt.member_epoch == original.member_epoch
                && attempt.max_messages == original.max_messages
                && attempt.max_bytes == original.max_bytes)
    );
    eprintln!(
        "Gated child original identity: {}",
        serde_json::json!({
            "shard": child_id.to_string(), "operation": original.operation.to_string(),
            "sequence": original.sequence, "member_epoch": original.member_epoch,
            "attempts": attempts.len()
        })
    );
    fault.release();
    let delivery = tokio::time::timeout(Duration::from_secs(15), consumer.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.message_id(), sent.message_id());
    consumer.ack_cumulative(&delivery).await.unwrap();
    drop((held, delivery));
    let closed = consumer.close().await.unwrap();
    assert_eq!(closed.unresolved_reads(), 0);
    assert_eq!(closed.unresolved_watches(), 0);
    producer.close().await.unwrap();
    client.close().await.unwrap();
    assert_eq!(client.metrics().snapshot().admitted_bytes, 0);
    runtime.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_sdk_uses_native_protection_while_business_holds_ordinary_credit() {
    let _serial = SERIAL.lock().await;
    let runtime = standalone().await;
    let observer = runtime.running.get(&1).unwrap().watch_diagnostics();
    let client = runtime.client(1).await;
    let tenant = client.admin().create_tenant("test").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = client.admin().create_topic(TOPIC).await.unwrap();
    client
        .admin()
        .split_topic_at(TOPIC, 1_u64 << 63)
        .await
        .unwrap();
    let active: Vec<_> = client
        .admin()
        .topic_shards(TOPIC)
        .await
        .unwrap()
        .into_iter()
        .filter(|shard| !shard.sealed)
        .collect();
    assert_eq!(active.len(), 2);
    let producer = client.new_producer(TOPIC).build().await.unwrap();
    let topic_id = topic.id().to_string().parse().unwrap();
    let mut expected = std::collections::HashSet::new();
    for shard in &active {
        let receipt = producer
            .send(Message::new("protected-business").key(key_for(shard, topic_id)))
            .await
            .unwrap();
        assert!(expected.insert(receipt.message_id()));
    }
    let minimum = tidemq_protocol::ProtocolLimits::default().max_message_bytes + 512;
    let queue = client
        .new_queue_consumer(TOPIC, "native-protection")
        .prefetch_messages(2)
        .prefetch_bytes(minimum * 2)
        .build()
        .await
        .unwrap();
    assert_eq!(queue.diagnostics().unwrap().mode, ConsumerMode::Isolated);
    let until = Instant::now() + Duration::from_secs(15);
    let first = RequestContext::with_deadline(until)
        .scope(queue.receive())
        .await
        .unwrap();
    // Holding the first delivery leaves only the independently reserved message credit.
    let second = RequestContext::with_deadline(until)
        .scope(queue.receive())
        .await
        .unwrap();
    let diagnostics = queue.diagnostics().unwrap();
    assert_eq!(diagnostics.ordinary_slots_free, 0);
    assert_eq!(diagnostics.protected_slots_free, 0);
    assert!(diagnostics.protected_bytes_free < minimum as usize);
    assert_ne!(first.message_id(), second.message_id());
    for message in [&first, &second] {
        assert!(expected.remove(&message.message_id()));
        queue.ack(message).await.unwrap();
    }
    assert!(expected.is_empty());
    eprintln!(
        "Real SDK protected delivery ledger: first={} second={} diagnostics={diagnostics:?}",
        first.message_id(),
        second.message_id()
    );
    drop(first);
    drop(second);
    let report = queue.close().await.unwrap();
    assert_eq!(report.members_left(), 1);
    assert_eq!(report.unresolved_reads(), 0);
    assert_eq!(report.unresolved_watches(), 0);
    assert!(report.local_cleanup_complete());
    producer.close().await.unwrap();
    client.close().await.unwrap();
    assert_eq!(client.metrics().snapshot().active_deliveries, 0);
    assert_eq!(client.metrics().snapshot().admitted_bytes, 0);
    runtime.close().await;
    let snapshot = tokio::time::timeout_at(deadline(), observer.snapshot())
        .await
        .unwrap();
    eprintln!(
        "Real SDK Watch cleanup ledger: {}",
        serde_json::to_string(&snapshot).unwrap()
    );
    assert_eq!(snapshot.charged_bytes, 0);
    assert_eq!(snapshot.active_requests, 0);
    assert_eq!(snapshot.cleanup_tasks, 0);
    assert_eq!(snapshot.forwards, 0);
    assert_eq!(snapshot.target_contexts, 0);
    assert_eq!(snapshot.cancellation_watermarks, 0);
    assert_eq!(snapshot.timers, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eight_committed_unknown_reads_leave_the_ninth_shard_progressing() {
    use tidemq::runtime::consumer_read_test_hooks::InstalledReadReplyFault;
    use tidemq_protocol::{common::FrameKind, public};

    let _serial = SERIAL.lock().await;
    const TOPIC_UNKNOWN: &str = "test/app/sdk-unknown";
    let runtime = standalone().await;
    let observer = runtime.running.get(&1).unwrap().watch_diagnostics();
    let client = runtime.client(1).await;
    let tenant = client.admin().create_tenant("test").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = client.admin().create_topic(TOPIC_UNKNOWN).await.unwrap();
    let topic_id = topic.id().to_string().parse::<Id>().unwrap();
    for index in 1_u128..9 {
        let boundary = (((u128::from(u64::MAX) + 1) * index) / 9) as u64;
        client
            .admin()
            .split_topic_at(TOPIC_UNKNOWN, boundary)
            .await
            .unwrap();
    }
    let wire = group_watch::WireClient::connect(&runtime, 1).await;
    let layout_reply = wire
        .call(group_watch::frame(
            FrameKind::GetTopicLayout,
            Id::new_v7().unwrap(),
            &public::GetTopicLayout {
                topic_id: Bytes::copy_from_slice(topic_id.as_bytes()),
                ..Default::default()
            },
        ))
        .await;
    let layout: public::TopicLayoutResult =
        group_watch::decoded(&layout_reply, FrameKind::TopicLayoutResult);
    assert_eq!(layout.active.len(), 9);
    let blocked: Vec<_> = layout
        .active
        .iter()
        .take(8)
        .map(|shard| PartitionKey {
            topic_id,
            partition: shard.birth_partition,
        })
        .collect();
    let healthy_id = Id::from_slice(&layout.active.last().unwrap().shard_id).unwrap();
    wire.close().await;
    let shards = client.admin().topic_shards(TOPIC_UNKNOWN).await.unwrap();
    let healthy = shards
        .iter()
        .find(|shard| shard.id.to_string() == healthy_id.to_string())
        .unwrap();
    let producer = client.new_producer(TOPIC_UNKNOWN).build().await.unwrap();
    let queue = client
        .new_queue_consumer(TOPIC_UNKNOWN, "unknown-eight")
        .build()
        .await
        .unwrap();
    tokio::time::timeout_at(deadline(), async {
        loop {
            let state = queue.diagnostics().unwrap();
            if state.assigned_targets >= 9
                && state.observed_targets == state.assigned_targets
                && state.ready_targets == 0
                && state.ordinary_reads == 0
                && state.protected_reads == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("The SDK did not initialize and retire its empty target baselines");
    let cluster = runtime.configs.get(&1).unwrap().cluster_id.parse().unwrap();
    let fault = InstalledReadReplyFault::install(cluster, blocked.clone()).unwrap();
    let mut expected = std::collections::HashSet::new();
    for (index, route) in layout.active.iter().take(8).enumerate() {
        let shard_id = Id::from_slice(&route.shard_id).unwrap();
        let shard = shards
            .iter()
            .find(|shard| shard.id.to_string() == shard_id.to_string())
            .unwrap();
        let receipt = producer
            .send(Message::new("blocked-progress").key(key_for(shard, topic_id)))
            .await
            .unwrap();
        assert!(expected.insert(receipt.message_id()));
        assert!(
            fault.wait_for_targets(index + 1, deadline()).await,
            "A committed read did not reach the persistent reply fault"
        );
        tokio::time::timeout_at(deadline(), async {
            loop {
                let state = queue.diagnostics().unwrap();
                assert!(
                    !state.protected_unknown,
                    "A staged ordinary fault consumed protection"
                );
                if state.ordinary_unknown == index + 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("The SDK did not preserve its staged ordinary original read");
    }
    let mut healthy_seen = std::collections::HashSet::new();
    for _ in 0..2 {
        let receipt = producer
            .send(Message::new("healthy-progress").key(key_for(healthy, topic_id)))
            .await
            .unwrap();
        let message = RequestContext::with_deadline(deadline())
            .scope(queue.receive())
            .await
            .unwrap();
        assert_eq!(message.message_id(), receipt.message_id());
        assert!(
            healthy_seen.insert(message.message_id()),
            "The healthy shard repeated a delivery"
        );
        let diagnostics = queue.diagnostics().unwrap();
        assert_eq!(diagnostics.ordinary_unknown, 8);
        assert!(diagnostics.ordinary_reads <= 8 && diagnostics.protected_reads <= 1);
        assert!(
            diagnostics.ordinary_rpc <= 7
                && diagnostics.protected_rpc <= 1
                && diagnostics.recovery_rpc <= 2
        );
        assert!(diagnostics.protected_blocked);
        queue.ack(&message).await.unwrap();
        let clone = message.clone();
        drop(message);
        assert!(queue.diagnostics().unwrap().protected_blocked);
        drop(clone);
    }
    let attempts = fault.attempts();
    assert!(!fault.overflowed());
    let mut identities = BTreeMap::new();
    for attempt in &attempts {
        assert!(blocked.contains(&attempt.partition));
        let identity = (
            attempt.subscription,
            attempt.member,
            attempt.member_epoch,
            attempt.operation,
            attempt.sequence,
            attempt.max_messages,
            attempt.max_bytes,
        );
        if let Some(previous) = identities.insert(attempt.partition, identity) {
            assert_eq!(
                previous, identity,
                "Unknown recovery changed its original identity or credit"
            );
        }
    }
    assert_eq!(identities.len(), 8);
    eprintln!(
        "Real SDK unknown ledger: attempts={} originals={identities:?} diagnostics={:?}",
        attempts.len(),
        queue.diagnostics().unwrap()
    );
    fault.release();
    for _ in 0..8 {
        let message = RequestContext::with_deadline(deadline())
            .scope(queue.receive())
            .await
            .unwrap();
        assert!(
            expected.remove(&message.message_id()),
            "A recovered original repeated or invented a delivery"
        );
        queue.ack(&message).await.unwrap();
    }
    assert!(expected.is_empty());
    let report = queue.close().await.unwrap();
    assert_eq!(report.unresolved_reads(), 0);
    assert_eq!(report.unresolved_watches(), 0);
    assert_eq!(report.members_left(), 1);
    assert!(report.local_cleanup_complete());
    assert_eq!(client.metrics().snapshot().deliveries, 10);
    assert_eq!(client.metrics().snapshot().acknowledged, 10);
    assert_eq!(client.metrics().snapshot().active_deliveries, 0);
    producer.close().await.unwrap();
    client.close().await.unwrap();
    assert_eq!(client.metrics().snapshot().admitted_bytes, 0);
    runtime.close().await;
    let snapshot = tokio::time::timeout_at(deadline(), observer.snapshot())
        .await
        .unwrap();
    assert_eq!(snapshot.charged_bytes, 0);
    assert_eq!(snapshot.active_requests, 0);
    assert_eq!(snapshot.cleanup_tasks, 0);
    assert_eq!(snapshot.target_contexts, 0);
    assert_eq!(snapshot.cancellation_watermarks, 0);
}
