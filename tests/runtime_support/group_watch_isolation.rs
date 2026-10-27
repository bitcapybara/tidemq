use super::*;
use std::collections::BTreeSet;
use tidemq::runtime::watch_test_hooks::{
    AuthorizeQuorumPause, CoreHandlerLimit, InitialTopicGroups,
};

async fn topic_layout(wire: &WireClient, topic: Id) -> Vec<public::ShardRoute> {
    let response = wire
        .call(frame(
            FrameKind::GetTopicLayout,
            id(),
            &public::GetTopicLayout {
                topic_id: bytes(topic),
                ..Default::default()
            },
        ))
        .await;
    let layout: public::TopicLayoutResult = decoded(&response, FrameKind::TopicLayoutResult);
    assert!(layout.next_after_shard.is_empty());
    assert!(layout.history.is_empty());
    layout.active
}

async fn read_exact(member: &Member, wire: &WireClient) -> public::GroupDeliveries {
    let response = wire
        .call(frame(
            FrameKind::GroupRead,
            id(),
            &public::GroupRead {
                topic_id: member.request.topic_id.clone(),
                partition_id: 0,
                subscription_id: member.request.subscription_id.clone(),
                member_id: member.request.member_id.clone(),
                member_epoch: member.request.member_epoch,
                sequence: 1,
                max_messages: 1,
                max_bytes: 512 * 1024,
            },
        ))
        .await;
    let result: public::GroupDeliveries = decoded(&response, FrameKind::GroupDeliveries);
    assert_eq!(result.sequence, 1);
    assert_eq!(result.deliveries.len(), 1);
    result
}

async fn ack_and_retire_exact(
    member: &Member,
    wire: &WireClient,
    delivery: &public::GroupDelivery,
) {
    let response = wire
        .call(frame(
            FrameKind::GroupAck,
            id(),
            &public::GroupAck {
                topic_id: member.request.topic_id.clone(),
                partition_id: 0,
                subscription_id: member.request.subscription_id.clone(),
                member_id: member.request.member_id.clone(),
                member_epoch: member.request.member_epoch,
                sequence: 1,
                proofs: vec![delivery.proof.clone().unwrap()],
                nack_delay_ms: None,
            },
        ))
        .await;
    let result: public::GroupAckResult = decoded(&response, FrameKind::GroupAckResult);
    assert!(!result.redelivery_scheduled);
    let response = wire
        .call(frame(
            FrameKind::GroupRetireRead,
            id(),
            &public::GroupRetire {
                topic_id: member.request.topic_id.clone(),
                partition_id: 0,
                subscription_id: member.request.subscription_id.clone(),
                member_id: member.request.member_id.clone(),
                member_epoch: member.request.member_epoch,
                sequence: 1,
            },
        ))
        .await;
    let _: public::GroupRetired = decoded(&response, FrameKind::GroupRetired);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_public_watch_keeps_a_healthy_changed_target_when_another_leader_is_unreachable() {
    let _serial = SERIAL.lock().await;
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut runtime = Fixture::new(
            &[
                (1, Role::Metadata),
                (2, Role::Combined),
                (3, Role::Storage),
            ],
            &[1],
            &[2],
        )
        .await;
        for config in runtime.configs.values_mut() {
            config.runtime.groups.push(GroupConfig {
                group_id: 3,
                voters: vec![3],
                standby: Vec::new(),
            });
        }
        let cluster = runtime.configs.get(&1).unwrap().cluster_id.parse().unwrap();
        let placement = InitialTopicGroups::install(cluster, TOPIC.into(), vec![2, 3]).unwrap();
        runtime.start_all().await;
        let admin = runtime.client(2).await;
        let tenant = admin.admin().create_tenant("test").await.unwrap();
        admin.admin().create_namespace(tenant, "app").await.unwrap();
        let topic = admin.admin().create_topic(TOPIC).await.unwrap();
        let topic_id: Id = topic.id().into();
        assert_eq!(topic.shard_count(), 2);
        let wire = WireClient::connect(&runtime, 2).await;
        let routes = topic_layout(&wire, topic_id).await;
        assert_eq!(routes.len(), 2);
        assert_eq!(
            routes.iter().map(|route| route.group_id).collect::<BTreeSet<_>>(),
            BTreeSet::from([2, 3])
        );
        let healthy = routes.iter().find(|route| route.group_id == 2).unwrap();
        let broken = routes.iter().find(|route| route.group_id == 3).unwrap();
        assert_eq!(healthy.birth_partition, 0);
        let member = Member::join(&wire, topic_id, "mixed-groups").await;
        for route in &routes {
            let owner = runtime.running.get(&1).unwrap().route(
                PartitionKey { topic_id, partition: route.birth_partition }, deadline()
            ).await.unwrap();
            assert_eq!(owner.owner_node, 2, "The public entry node must own both target routes");
        }
        let mut targets = Vec::new();
        for route in &routes {
            let mut request = member.watch(Bytes::new(), 0);
            request.targets = vec![public::WatchTarget {
                shard_id: route.shard_id.clone(),
                birth_partition: route.birth_partition,
                observed_token: Bytes::new(),
            }];
            let result = wire.watch(&request).await;
            targets.push(public::WatchTarget {
                observed_token: target(&request, &result, public::WatchStatus::WatchSync)
                    .observed_token
                    .clone(),
                ..request.targets.first().unwrap().clone()
            });
        }
        let producer = admin.new_producer(TOPIC).build().await.unwrap();
        runtime.stop(3).await;
        let evicted = runtime.running.get(&2).unwrap().evict_watch_leader_hint(3).await;
        assert_eq!(evicted, Some(3), "The broken group's previously synchronized leader hint was not explicitly evicted");
        eprintln!("Mixed-group discovery fault ledger: public_entry=2 owner=2 healthy_group=2 unreachable_group=3 evicted_leader=3");
        let key = (0..128)
            .map(|index| format!("healthy-{index}"))
            .find(|key| {
                let hash = tidemq_protocol::topic::routing_hash(Some(key.as_bytes()), topic_id, 1)
                    .unwrap();
                healthy.hash_start <= hash && healthy.hash_end.is_none_or(|end| hash < end)
            })
            .expect("Could not find a deterministic key in the healthy shard range");
        producer
            .send(tidemq_sdk::Message::new("healthy-with-unreachable-peer").key(key.into_bytes()))
            .await
            .unwrap();
        let observer = runtime.running.get(&2).unwrap().watch_diagnostics();
        let before = observer.snapshot().await;
        let mut request = member.watch(Bytes::new(), 1_000);
        request.targets = targets;
        let pending = wire.submit_for(&request, Duration::from_secs(10)).await;
        let received = tokio::time::timeout(Duration::from_secs(2), pending.receive())
            .await
            .expect("An unreachable group delayed the healthy readiness result")
            .unwrap();
        assert_eq!(received.frame().operation_id, request.root_operation_id);
        let result: public::GroupWatchResult = decoded(received.frame(), FrameKind::GroupWatchResult);
        assert_eq!(result.targets.len(), 2);
        assert_eq!(
            result.targets.iter().map(|entry| &entry.shard_id).collect::<BTreeSet<_>>(),
            routes.iter().map(|route| &route.shard_id).collect()
        );
        let changed = result.targets.iter().find(|entry| entry.shard_id == healthy.shard_id).unwrap();
        assert_eq!(changed.status, i32::from(public::WatchStatus::WatchChanged));
        assert_eq!(changed.error_code, i32::from(ErrorCode::Unspecified));
        assert!(!changed.retryable);
        assert!(!changed.observed_token.is_empty());
        assert!(request.targets.iter().any(|entry| {
            entry.shard_id == changed.shard_id && entry.observed_token != changed.observed_token
        }));
        let retry = result.targets.iter().find(|entry| entry.shard_id == broken.shard_id).unwrap();
        assert_eq!(retry.status, i32::from(public::WatchStatus::WatchRetry));
        assert!(retry.retryable);
        assert!(matches!(ErrorCode::try_from(retry.error_code).unwrap(), ErrorCode::Unavailable | ErrorCode::DeadlineExceeded));
        let after = observer.snapshot().await;
        assert_eq!(after.group_reads, before.group_reads);
        assert_eq!(after.group_retires, before.group_retires);
        assert_eq!(after.groups.iter().map(|group| group.applied_index).collect::<Vec<_>>(), before.groups.iter().map(|group| group.applied_index).collect::<Vec<_>>());
        let read = read_exact(&member, &wire).await;
        let delivery = read.deliveries.first().unwrap();
        assert_eq!(delivery.body, b"healthy-with-unreachable-peer".as_slice());
        ack_and_retire_exact(&member, &wire, delivery).await;
        eprintln!("Mixed-group Watch ledger: healthy=CHANGED unreachable=RETRY read_sequence=1 ack=known retire=known");
        runtime.restart(3).await;
        member.leave(&wire).await;
        producer.close().await.unwrap();
        wire.close().await;
        admin.close().await.unwrap();
        let observers: Vec<_> = runtime.running.values().map(RunningNode::watch_diagnostics).collect();
        runtime.close().await;
        for observer in observers { assert_released(&observer.snapshot().await); }
        drop(placement);
    })
    .await
    .expect("The mixed-group Watch regression exceeded its total deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eight_real_authorizations_leave_default_metadata_core_and_single_gateway_core_available() {
    let _serial = SERIAL.lock().await;
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut runtime = Fixture::new(&[(1, Role::Combined), (2, Role::Gateway)], &[1], &[1]).await;
        let cluster = runtime.configs.get(&1).unwrap().cluster_id.parse().unwrap();
        let metadata_default = tidemq::server::DispatchConfig::default().core_handlers;
        assert_eq!(metadata_default, 8);
        let metadata_core = CoreHandlerLimit::install(cluster, 1, metadata_default).unwrap();
        let gateway_core = CoreHandlerLimit::install(cluster, 2, 1).unwrap();
        runtime.start_all().await;
        assert!(metadata_core.applied());
        assert!(gateway_core.applied());
        let admin = runtime.client(2).await;
        let tenant = admin.admin().create_tenant("test").await.unwrap();
        admin.admin().create_namespace(tenant, "app").await.unwrap();
        let topic = admin.admin().create_topic(TOPIC).await.unwrap();
        let wire = WireClient::connect(&runtime, 2).await;
        let member = Member::join(&wire, topic.id().into(), "authority-isolation").await;
        let producer = admin.new_producer(TOPIC).build().await.unwrap();
        producer.send("ack-while-eight-authorizations-wait").await.unwrap();
        let read = read_exact(&member, &wire).await;
        let sync = member.watch(Bytes::new(), 0);
        let result = wire.watch(&sync).await;
        let token = target(&sync, &result, public::WatchStatus::WatchSync).observed_token.clone();
        let observer = runtime.running.get(&1).unwrap().watch_diagnostics();
        let before = observer.snapshot().await;
        let pause = AuthorizeQuorumPause::install(cluster, 1).unwrap();
        let mut pending = Vec::new();
        for _ in 0..8 {
            let request = member.watch(token.clone(), 1_000);
            let response = wire.submit_for(&request, Duration::from_secs(30)).await;
            pending.push((request, response));
        }
        pause.wait_parked(8, deadline()).await.unwrap();
        assert_eq!(pause.parked(), 8);
        assert!(observer.snapshot().await.charged_bytes > before.charged_bytes);
        let heartbeat = public::ConsumerControl {
            action: public::ConsumerAction::HeartbeatConsumer.into(),
            heartbeat_sequence: member.status.next_heartbeat_sequence,
            ..member.request.clone()
        };
        let response = tokio::time::timeout(Duration::from_secs(3), wire.call(frame(FrameKind::ConsumerControl, id(), &heartbeat)))
            .await.expect("Eight Watch authorizations starved the ordinary heartbeat");
        let status: public::ConsumerControlResult = decoded(&response, FrameKind::ConsumerControlResult);
        assert_eq!(status.member_epoch, member.request.member_epoch);
        assert!(status.plan_active);
        assert!(status.next_heartbeat_sequence > heartbeat.heartbeat_sequence);
        tokio::time::timeout(Duration::from_secs(3), ack_and_retire_exact(&member, &wire, read.deliveries.first().unwrap()))
            .await.expect("Eight Watch authorizations starved a real ACK or retirement");
        let acknowledged = observer.snapshot().await;
        assert_eq!(acknowledged.group_reads, before.group_reads);
        assert_eq!(acknowledged.group_retires, before.group_retires + 1);
        assert_eq!(pause.parked(), 8);
        tokio::time::timeout(Duration::from_secs(3), producer.close())
            .await.expect("Eight Watch authorizations starved producer close").unwrap();
        assert_eq!(pause.parked(), 8);
        runtime.running.remove(&1).unwrap().shutdown_until(Instant::now() + Duration::from_secs(8)).await.unwrap();
        assert_eq!(pause.parked(), 0, "Shutdown retained a paused authorization task");
        assert_released(&observer.snapshot().await);
        for (request, response) in pending {
            let reply = tokio::time::timeout_at(deadline(), response.receive()).await.unwrap().unwrap();
            assert_eq!(reply.frame().operation_id, request.root_operation_id);
            if reply.frame().kind == i32::from(FrameKind::Error) {
                let error: tidemq_protocol::common::ErrorReply = decoded(reply.frame(), FrameKind::Error);
                assert_eq!(error.code, i32::from(ErrorCode::Unavailable));
                assert!(error.retryable);
                eprintln!("Intentional metadata shutdown Watch ledger: root={} code={} outcome={} retryable={}", Id::from_slice(&request.root_operation_id).unwrap(), error.code, error.outcome, error.retryable);
            } else {
                let result: public::GroupWatchResult = decoded(reply.frame(), FrameKind::GroupWatchResult);
                assert_eq!(result.targets.len(), 1);
                let result = result.targets.first().unwrap();
                assert_eq!(result.shard_id, member.shard);
                assert_eq!(result.status, i32::from(public::WatchStatus::WatchRetry));
                assert_eq!(result.error_code, i32::from(ErrorCode::Unavailable));
                assert!(result.retryable);
                eprintln!("Intentional metadata shutdown Watch ledger: root={} status=RETRY code={} retryable=true", Id::from_slice(&request.root_operation_id).unwrap(), result.error_code);
            }
        }
        pause.release();
        drop(pause);
        wire.close().await;
        admin.close().await.unwrap();
        let remaining = runtime.running.get(&2).unwrap().watch_diagnostics();
        runtime.close().await;
        assert_released(&remaining.snapshot().await);
        eprintln!("Authority isolation ledger: parked=8 metadata_core_handlers=8 gateway_core_handlers=1 heartbeat=known ack=known retire=known producer_close=known metadata_shutdown=known parked_after_shutdown=0");
        drop(metadata_core);
        drop(gateway_core);
    })
    .await
    .expect("The real authorization isolation regression exceeded its total deadline");
}
