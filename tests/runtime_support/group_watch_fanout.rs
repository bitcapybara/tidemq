use super::*;
use std::collections::BTreeSet;
use tidemq::runtime::watch_test_hooks::{Diagnostics, WatchEvent, WatchPhase};

async fn sync_after_route_change(wire: &WireClient, mut request: public::GroupWatch) -> Bytes {
    let until = Instant::now() + Duration::from_millis(RPC_MS);
    let operation = Id::from_slice(&request.root_operation_id).unwrap();
    loop {
        let mut attempt = frame(FrameKind::GroupWatch, operation, &request);
        attempt.remaining_timeout_ms =
            u64::try_from(until.saturating_duration_since(Instant::now()).as_millis())
                .unwrap()
                .max(1);
        let reply = wire.call(attempt).await;
        let result = if reply.kind == i32::from(FrameKind::Error) {
            let error: tidemq_protocol::common::ErrorReply = decoded(&reply, FrameKind::Error);
            assert!(
                error.retryable && error.code == i32::from(ErrorCode::Unavailable),
                "Route preparation returned a terminal Watch error: {error:?}"
            );
            None
        } else {
            let result: public::GroupWatchResult = decoded(&reply, FrameKind::GroupWatchResult);
            if result
                .targets
                .iter()
                .all(|target| target.status == i32::from(public::WatchStatus::WatchSync))
            {
                return target(&request, &result, public::WatchStatus::WatchSync)
                    .observed_token
                    .clone();
            }
            Some(result)
        };
        assert!(
            Instant::now() < until,
            "The original Watch could not synchronize after route preparation: {result:?}"
        );
        request.attempt = request.attempt.checked_add(1).unwrap();
        tokio::task::yield_now().await;
    }
}

async fn registrations(observer: &Diagnostics, root: Id, attempt: u64) -> Vec<WatchEvent> {
    tokio::time::timeout_at(deadline(), async {
        loop {
            let events: Vec<_> = observer
                .events()
                .into_iter()
                .filter(|event| {
                    event.root == root
                        && event.attempt == attempt
                        && event.phase == WatchPhase::Registered
                })
                .collect();
            if events
                .iter()
                .map(|event| event.group)
                .collect::<BTreeSet<_>>()
                == BTreeSet::from([2, 3])
            {
                return events;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Both data-group branches did not register on the shared leader")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_owners_fan_out_to_distinct_groups_on_one_leader_and_late_cancel_is_attempt_scoped() {
    let _serial = SERIAL.lock().await;
    tokio::time::timeout(Duration::from_secs(90), async {
        let mut runtime = Fixture::new(
            &[
                (1, Role::Metadata),
                (2, Role::Storage),
                (3, Role::Broker),
                (4, Role::Gateway),
                (5, Role::Broker),
                (6, Role::Storage),
            ],
            &[1],
            &[2],
        )
        .await;
        for config in runtime.configs.values_mut() {
            config.runtime.groups.push(GroupConfig {
                group_id: 3,
                voters: vec![6],
                standby: vec![2],
            });
        }
        runtime.start_all().await;
        let admin = runtime.client(4).await;
        let tenant = admin.admin().create_tenant("test").await.unwrap();
        admin.admin().create_namespace(tenant, "app").await.unwrap();
        let topic = admin.admin().create_topic(TOPIC).await.unwrap();
        let topic_id: Id = topic.id().into();
        // Splitting needs independent leaders; co-locate the resulting groups only afterwards.
        assert_eq!(
            admin
                .admin()
                .split_topic_at(TOPIC, 1_u64 << 63)
                .await
                .unwrap()
                .shard_count(),
            2
        );
        let changed = admin.admin().replace_data_replica(3, 6, 2).await;
        match changed {
            Ok(group) => assert_eq!(group.voters, [2]),
            Err(tidemq_sdk::Error::Administration { pending, .. }) => {
                let until = deadline();
                loop {
                    let result = tidemq_sdk::RequestContext::with_deadline(until)
                        .scope(pending.retry())
                        .await;
                    match result {
                        Ok(tidemq_sdk::AdminReceipt::DataGroupReconfigured(group)) => {
                            assert_eq!(group.voters, [2]);
                            break;
                        }
                        Err(error) if error.outcome_unknown() && Instant::now() < until => {
                            tokio::task::yield_now().await
                        }
                        other => {
                            panic!("The original data replacement could not complete: {other:?}")
                        }
                    }
                }
            }
            Err(error) => panic!("Could not prepare the shared-leader fixture: {error:?}"),
        }
        let wire = WireClient::connect(&runtime, 4).await;
        let reply = wire
            .call(frame(
                FrameKind::GetTopicLayout,
                id(),
                &public::GetTopicLayout {
                    topic_id: bytes(topic_id),
                    ..Default::default()
                },
            ))
            .await;
        let layout: public::TopicLayoutResult = decoded(&reply, FrameKind::TopicLayoutResult);
        assert_eq!(layout.active.len(), 2);
        assert_eq!(
            layout
                .active
                .iter()
                .map(|shard| shard.group_id)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([2, 3])
        );
        for (shard, owner) in layout.active.iter().zip([3, 5]) {
            let partition = PartitionKey {
                topic_id,
                partition: shard.birth_partition,
            };
            let metadata = runtime.running.get(&1).unwrap();
            let route = metadata.route(partition, deadline()).await.unwrap();
            if route.owner_node != owner {
                metadata
                    .takeover(partition, owner, route.epoch, route.generation, deadline())
                    .await
                    .unwrap();
            }
            assert_eq!(
                metadata
                    .route(partition, deadline())
                    .await
                    .unwrap()
                    .owner_node,
                owner
            );
        }
        let member = Member::join(&wire, topic_id, "fanout").await;
        let mut targets = Vec::new();
        for shard in &layout.active {
            assert!(member.status.work_shards.contains(&shard.shard_id));
            let mut request = member.watch(Bytes::new(), 0);
            request.targets = vec![public::WatchTarget {
                shard_id: shard.shard_id.clone(),
                birth_partition: shard.birth_partition,
                observed_token: Bytes::new(),
            }];
            targets.push(public::WatchTarget {
                shard_id: shard.shard_id.clone(),
                birth_partition: shard.birth_partition,
                observed_token: sync_after_route_change(&wire, request).await,
            });
        }
        let mut original = member.watch(Bytes::new(), 1_000);
        original.targets = targets;
        let root = Id::from_slice(&original.root_operation_id).unwrap();
        let leader = runtime.running.get(&2).unwrap().watch_diagnostics();
        let first = wire.submit(&original).await;
        let registered = registrations(&leader, root, 1).await;
        let first_ids = registered
            .iter()
            .map(|event| event.operation)
            .collect::<BTreeSet<_>>();
        let first_parents = registered
            .iter()
            .map(|event| event.parent)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            first_ids.len(),
            2,
            "Different groups on the same physical leader reused a child operation"
        );
        assert_eq!(
            first_parents.len(),
            2,
            "Two owners lost their independent parent identities"
        );
        assert!(!first_ids.contains(&root));
        assert!(first_parents.iter().all(Option::is_some));
        assert_eq!(leader.snapshot().await.active_requests, 2);
        let replacement = public::GroupWatch {
            attempt: 2,
            ..original.clone()
        };
        let second = wire.submit(&replacement).await;
        let retried = registrations(&leader, root, 2).await;
        assert_eq!(
            retried
                .iter()
                .map(|event| event.operation)
                .collect::<BTreeSet<_>>(),
            first_ids,
            "Retry changed stable branch identities"
        );
        wire.cancel(&original).await;
        let active = leader.snapshot().await;
        assert_eq!(
            active.active_requests, 2,
            "An old parent cancellation removed the replacement subtree"
        );
        let producer = admin.new_producer(TOPIC).build().await.unwrap();
        producer.send("fanout-wakeup").await.unwrap();
        let result = receive(second).await;
        assert_eq!(result.targets.len(), 2);
        assert_eq!(
            result
                .targets
                .iter()
                .map(|target| &target.shard_id)
                .collect::<BTreeSet<_>>(),
            original
                .targets
                .iter()
                .map(|target| &target.shard_id)
                .collect()
        );
        assert!(
            result
                .targets
                .iter()
                .any(|target| target.status == i32::from(public::WatchStatus::WatchChanged))
        );
        assert!(result.targets.iter().all(|target| matches!(
            public::WatchStatus::try_from(target.status).unwrap(),
            public::WatchStatus::WatchChanged
                | public::WatchStatus::WatchUnchanged
                | public::WatchStatus::WatchRetry
        )));
        let old = receive(first).await;
        assert!(
            old.targets
                .iter()
                .all(|target| target.status != i32::from(public::WatchStatus::WatchUnchanged))
        );
        eprintln!(
            "Shared-leader Watch branch ledger: {}",
            serde_json::to_string(&leader.events()).unwrap()
        );
        member.leave(&wire).await;
        producer.close().await.unwrap();
        wire.close().await;
        admin.close().await.unwrap();
        let observers: Vec<_> = runtime
            .running
            .values()
            .map(RunningNode::watch_diagnostics)
            .collect();
        runtime.close().await;
        for observer in observers {
            assert_released(&observer.snapshot().await);
        }
    })
    .await
    .expect("The shared-leader fan-out regression exceeded its total deadline");
}
