#![cfg(feature = "test-hooks")]

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
    runtime::{GroupConfig, PeerConfig, RunningNode, RuntimeConfig, watch_test_hooks::BeforeWait},
};
use tidemq_protocol::{
    Id,
    common::{ErrorCode, FrameKind},
    public,
};
use tidemq_sdk::Client;
use tokio::time::Instant;

// Only fixture modules are shared; importing runtime suites would execute their tests twice.
#[allow(dead_code)]
#[path = "runtime_support/fixture.rs"]
mod fixture;
#[path = "runtime_support/group_watch.rs"]
mod group_watch;
#[path = "runtime_support/group_watch_fanout.rs"]
mod group_watch_fanout;
#[path = "runtime_support/group_watch_isolation.rs"]
mod group_watch_isolation;
#[path = "runtime_support/group_watch_scale.rs"]
mod group_watch_scale;
#[allow(dead_code)]
#[path = "lifecycle_support/rustfs.rs"]
mod rustfs;
#[path = "s3_support/mod.rs"]
mod s3;
use fixture::*;
use group_watch::*;

const TOKEN: &str = "runtime-test-access-token-with-at-least-32-characters";
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_exposes_existing_backlog_without_allocating_a_delivery() {
    let _serial = SERIAL.lock().await;
    let fixture = WatchFixture::new().await;
    let producer = fixture.admin.new_producer(TOPIC).build().await.unwrap();
    producer.send("existing-backlog").await.unwrap();
    let before = activity(&fixture.metrics(1).await);
    let token = fixture.sync().await;
    assert!(!token.is_empty());
    assert_eq!(activity(&fixture.metrics(1).await), before);
    let result = fixture.member.read(&fixture.wire, 1).await;
    assert_eq!(result.deliveries.len(), 1);
    let delivery = result.deliveries.first().unwrap();
    assert_eq!(delivery.body, b"existing-backlog".as_slice());
    assert_eq!(
        delivery.proof.as_ref().unwrap().attempt,
        1,
        "Watch consumed a delivery attempt"
    );
    fixture.member.ack(&fixture.wire, delivery, 1).await;
    fixture.member.retire(&fixture.wire, 1).await;
    producer.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_timeouts_and_empty_read_retirement_do_not_create_a_watch_feedback_loop() {
    let _serial = SERIAL.lock().await;
    let fixture = WatchFixture::new().await;
    let token = fixture.sync().await;
    let read = fixture.member.read(&fixture.wire, 1).await;
    assert!(read.deliveries.is_empty());
    fixture.member.retire(&fixture.wire, 1).await;
    let before = activity(&fixture.metrics(1).await);
    assert_eq!(
        before,
        [1, 1, 1],
        "The applied-command ledger is not observing the real Read path"
    );
    let observer = fixture.runtime.running.get(&1).unwrap().watch_diagnostics();
    let applied_before = observer
        .snapshot()
        .await
        .groups
        .into_iter()
        .map(|group| (group.group, group.applied_index))
        .collect::<Vec<_>>();
    for _ in 0..3 {
        let request = fixture.member.watch(token.clone(), 40);
        let result = fixture.wire.watch(&request).await;
        assert_eq!(
            target(&request, &result, public::WatchStatus::WatchUnchanged).observed_token,
            token
        );
    }
    assert_eq!(
        activity(&fixture.metrics(1).await),
        before,
        "Watch timeout submitted a Read or RetireRead"
    );
    let after = observer.snapshot().await;
    assert_eq!(
        after
            .groups
            .iter()
            .map(|group| (group.group, group.applied_index))
            .collect::<Vec<_>>(),
        applied_before,
        "Ordinary Watch changed the applied data Raft log"
    );
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_between_version_comparison_and_wait_registration_cannot_be_lost() {
    let _serial = SERIAL.lock().await;
    let fixture = WatchFixture::new().await;
    let producer = fixture.admin.new_producer(TOPIC).build().await.unwrap();
    let token = fixture.sync().await;
    let pause = BeforeWait::install(fixture.member.partition).unwrap();
    let request = fixture.member.watch(token.clone(), 1_000);
    let pending = fixture.wire.submit(&request).await;
    pause.wait(deadline()).await.unwrap();
    producer.send("registration-race").await.unwrap();
    pause.resume();
    let result = receive(pending).await;
    assert_ne!(
        target(&request, &result, public::WatchStatus::WatchChanged).observed_token,
        token
    );
    let stale = fixture.member.watch(token, 0);
    target(
        &stale,
        &fixture.wire.watch(&stale).await,
        public::WatchStatus::WatchChanged,
    );
    let read = fixture.member.read(&fixture.wire, 1).await;
    assert_eq!(read.deliveries.len(), 1);
    assert_eq!(
        read.deliveries.first().unwrap().body,
        b"registration-race".as_slice()
    );
    fixture
        .member
        .ack(&fixture.wire, read.deliveries.first().unwrap(), 1)
        .await;
    fixture.member.retire(&fixture.wire, 1).await;
    drop(pause);
    producer.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_attempt_cancellation_cannot_remove_a_replacement_waiter() {
    let _serial = SERIAL.lock().await;
    let fixture = WatchFixture::new().await;
    let producer = fixture.admin.new_producer(TOPIC).build().await.unwrap();
    let pause = BeforeWait::install(fixture.member.partition).unwrap();
    let original = fixture.member.watch(fixture.sync().await, 1_000);
    let first = fixture.wire.submit(&original).await;
    pause.wait(deadline()).await.unwrap();
    let replacement = public::GroupWatch {
        attempt: 2,
        ..original.clone()
    };
    let second = fixture.wire.submit(&replacement).await;
    pause.wait(deadline()).await.unwrap();
    let observer = fixture.runtime.running.get(&1).unwrap().watch_diagnostics();
    assert_eq!(observer.snapshot().await.active_requests, 1);
    fixture.wire.cancel(&original).await;
    assert_stale_cancel_unmatched(&observer, &original);
    assert_eq!(
        observer.snapshot().await.active_requests,
        1,
        "A stale cancellation removed the live replacement"
    );
    producer.send("new-attempt").await.unwrap();
    pause.resume();
    let result = receive(second).await;
    target(&replacement, &result, public::WatchStatus::WatchChanged);
    receive_replaced(first, &original).await;
    drop(pause);
    producer.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_forged_member_and_duplicate_targets_have_explicit_rejections() {
    let _serial = SERIAL.lock().await;
    let fixture = WatchFixture::new().await;
    let token = fixture.sync().await;
    let mut failures = Vec::new();
    for case in 0..7 {
        let mut request = fixture.member.watch(token.clone(), 0);
        match case {
            0 => request.targets.clear(),
            1 => request
                .targets
                .push(request.targets.first().unwrap().clone()),
            2 => {
                request.targets.first_mut().unwrap().observed_token =
                    Bytes::from(vec![0; tidemq_protocol::watch::MAX_TOKEN_BYTES + 1])
            }
            3 => request.member_id = bytes(id()),
            4 => request.member_epoch += 1,
            5 => request.targets.first_mut().unwrap().shard_id = bytes(id()),
            6 => request.targets.first_mut().unwrap().birth_partition = u32::MAX,
            _ => unreachable!(),
        }
        let reply = fixture
            .wire
            .call(frame(
                FrameKind::GroupWatch,
                Id::from_slice(&request.root_operation_id).unwrap(),
                &request,
            ))
            .await;
        let error: tidemq_protocol::common::ErrorReply = decoded(&reply, FrameKind::Error);
        eprintln!("Watch rejection ledger: case={case} error={error:?}");
        if !matches!(
            ErrorCode::try_from(error.code).unwrap(),
            ErrorCode::InvalidRequest
                | ErrorCode::Forbidden
                | ErrorCode::StaleGeneration
                | ErrorCode::Fenced
        ) || error.outcome == i32::from(tidemq_protocol::common::Outcome::Unknown)
        {
            failures.push(format!("case={case}: {error:?}"));
        }
    }
    assert_eq!(activity(&fixture.metrics(1).await), [0, 0, 0]);
    fixture.close().await;
    assert!(
        failures.is_empty(),
        "Watch validation returned an invalid refusal: {failures:#?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observation_budget_refuses_explicitly_while_member_control_still_progresses() {
    let _serial = SERIAL.lock().await;
    let fixture = WatchFixture::new().await;
    let token = fixture.sync().await;
    let observer = fixture.runtime.running.get(&1).unwrap().watch_diagnostics();
    let mut pending = vec![
        fixture
            .wire
            .submit(&fixture.member.watch(token.clone(), 1_000))
            .await,
    ];
    tokio::time::timeout_at(deadline(), async {
        while observer.snapshot().await.active_requests == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("The initial real Watch never registered");
    for _ in 1..160 {
        pending.push(
            fixture
                .wire
                .submit(&fixture.member.watch(token.clone(), 1_000))
                .await,
        );
    }
    let query = public::ConsumerControl {
        action: public::ConsumerAction::ConsumerStatus.into(),
        ..fixture.member.request.clone()
    };
    let reply = fixture
        .wire
        .call(frame(FrameKind::ConsumerControl, id(), &query))
        .await;
    let status: public::ConsumerControlResult = decoded(&reply, FrameKind::ConsumerControlResult);
    assert!(status.plan_active);
    let mut refused = 0;
    let mut completed = 0;
    let mut uncertain = 0;
    for response in pending {
        let reply = tokio::time::timeout_at(deadline(), response.receive())
            .await
            .unwrap()
            .unwrap();
        if reply.frame().kind == i32::from(FrameKind::Error) {
            let error: tidemq_protocol::common::ErrorReply =
                decoded(reply.frame(), FrameKind::Error);
            let code = ErrorCode::try_from(error.code).unwrap();
            assert!(
                matches!(
                    code,
                    ErrorCode::QuotaExceeded | ErrorCode::Unavailable | ErrorCode::DeadlineExceeded
                ),
                "Unexpected protocol error under budget pressure: {error:?}"
            );
            assert!(error.retryable);
            if code == ErrorCode::QuotaExceeded {
                refused += 1;
            } else {
                uncertain += 1;
            }
        } else {
            let result: public::GroupWatchResult =
                decoded(reply.frame(), FrameKind::GroupWatchResult);
            assert_eq!(result.targets.len(), 1);
            let target = result.targets.first().unwrap();
            assert_eq!(target.shard_id, fixture.member.shard);
            match public::WatchStatus::try_from(target.status).unwrap() {
                public::WatchStatus::WatchUnchanged => {
                    assert_eq!(target.observed_token, token);
                    assert_eq!(target.error_code, i32::from(ErrorCode::Unspecified));
                    assert!(!target.retryable);
                    completed += 1;
                }
                public::WatchStatus::WatchRetry => {
                    let code = ErrorCode::try_from(target.error_code).unwrap();
                    assert!(
                        matches!(
                            code,
                            ErrorCode::QuotaExceeded
                                | ErrorCode::Unavailable
                                | ErrorCode::DeadlineExceeded
                        ),
                        "Unexpected observation failure under budget pressure: {target:?}"
                    );
                    assert!(target.retryable);
                    assert_eq!(target.observed_token, token);
                    if code == ErrorCode::QuotaExceeded {
                        refused += 1;
                    } else {
                        uncertain += 1;
                    }
                }
                other => {
                    panic!("Budget pressure returned an unexpected status {other:?}: {target:?}")
                }
            }
        }
    }
    assert!(
        refused > 0,
        "Observation pressure did not reach the charged budget"
    );
    assert!(
        completed > 0,
        "No admitted observation completed successfully"
    );
    eprintln!(
        "Watch pressure result ledger: unchanged={completed} quota_refusals={refused} explicit_uncertainty={uncertain}"
    );
    let metrics = fixture.metrics(1).await;
    assert!(metric(&metrics, "tidemq_watch_admission_rejected_total") > 0);
    assert_eq!(activity(&metrics), [0, 0, 0]);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_gateway_owner_and_storage_preserve_public_watch_and_real_delivery() {
    let _serial = SERIAL.lock().await;
    let fixture = WatchFixture::topology(
        &[
            (1, Role::Metadata),
            (2, Role::Storage),
            (3, Role::Broker),
            (4, Role::Gateway),
        ],
        &[1],
        &[2],
        4,
    )
    .await;
    let token = fixture.sync().await;
    let pause = BeforeWait::install(fixture.member.partition).unwrap();
    let request = fixture.member.watch(token, 1_000);
    let pending = fixture.wire.submit(&request).await;
    pause.wait(deadline()).await.unwrap();
    let producer = fixture.admin.new_producer(TOPIC).build().await.unwrap();
    producer.send("through-every-role").await.unwrap();
    pause.resume();
    target(
        &request,
        &receive(pending).await,
        public::WatchStatus::WatchChanged,
    );
    let read = fixture.member.read(&fixture.wire, 1).await;
    assert_eq!(read.deliveries.len(), 1);
    assert_eq!(
        read.deliveries.first().unwrap().body,
        b"through-every-role".as_slice()
    );
    fixture
        .member
        .ack(&fixture.wire, read.deliveries.first().unwrap(), 1)
        .await;
    fixture.member.retire(&fixture.wire, 1).await;
    drop(pause);
    producer.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retiring_an_already_acked_read_alone_wakes_a_different_tenant_in_the_same_data_group() {
    let _serial = SERIAL.lock().await;
    tokio::time::timeout(Duration::from_secs(90), async {
        let runtime = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        let cluster = runtime.configs.get(&1).unwrap().cluster_id.parse().unwrap();
        let _limit =
            tidemq::runtime::watch_test_hooks::ReadResultLimit::install(cluster, 2, 256).unwrap();
        let fixture = WatchFixture::start(runtime, 1).await;
        let observer = fixture.runtime.running.get(&1).unwrap().watch_diagnostics();
        assert_eq!(
            observer
                .snapshot()
                .await
                .groups
                .first()
                .unwrap()
                .available_read_results,
            Some(256),
            "The startup ReadResultLimit hook was not applied to this real data group"
        );
        fixture.sync().await;
        let other_tenant = fixture.admin.admin().create_tenant("other").await.unwrap();
        fixture
            .admin
            .admin()
            .create_namespace(other_tenant, "app")
            .await
            .unwrap();
        let b_topic = fixture
            .admin
            .admin()
            .create_topic("other/app/blocked")
            .await
            .unwrap();
        let b = Member::join(
            &fixture.wire,
            b_topic.id().to_string().parse().unwrap(),
            "blocked",
        )
        .await;
        let idle_topic = fixture
            .admin
            .admin()
            .create_topic("test/app/idle")
            .await
            .unwrap();
        let idle = Member::join(
            &fixture.wire,
            idle_topic.id().to_string().parse().unwrap(),
            "idle",
        )
        .await;
        for partition in [fixture.member.partition, b.partition, idle.partition] {
            assert_eq!(
                fixture
                    .runtime
                    .running
                    .get(&1)
                    .unwrap()
                    .route(partition, deadline())
                    .await
                    .unwrap()
                    .group_id,
                2
            );
        }
        let producer_a = fixture.admin.new_producer(TOPIC).build().await.unwrap();
        for first in (0..64).step_by(32) {
            let sent = producer_a
                .send_batch(
                    (first..first + 32)
                        .map(|sequence| tidemq_sdk::Message::new(format!("retained-{sequence}")))
                        .collect(),
                )
                .await
                .unwrap();
            assert_eq!(sent.outcomes.len(), 32);
            assert!(sent.outcomes.iter().all(Result::is_ok));
        }
        let producer_b = fixture
            .admin
            .new_producer("other/app/blocked")
            .build()
            .await
            .unwrap();
        producer_b
            .send("waiting-for-result-capacity")
            .await
            .unwrap();
        let mut retained = Vec::new();
        for index in 0..4 {
            let member = Member::join_model(
                &fixture.wire,
                fixture.member.partition.topic_id,
                &format!("retained-{index}"),
                public::ConsumerModel::StreamConsumer,
            )
            .await;
            let read = member.read_batch(&fixture.wire, 1, 64).await;
            assert_eq!(
                read.deliveries.len(),
                64,
                "The fixture did not fill its exact shared result allocation"
            );
            member.ack_batch(&fixture.wire, &read.deliveries, 1).await;
            retained.push(member);
        }
        assert_eq!(retained.len() * 64, 256);
        assert_eq!(
            observer
                .snapshot()
                .await
                .groups
                .first()
                .unwrap()
                .available_read_results,
            Some(0)
        );
        let blocked = b.read(&fixture.wire, 1).await;
        assert!(
            blocked.deliveries.is_empty(),
            "B was not blocked by the full shared result budget"
        );
        b.retire(&fixture.wire, 1).await;
        let initial = b.watch(Bytes::new(), 0);
        let synced = fixture.wire.watch(&initial).await;
        let token = target(&initial, &synced, public::WatchStatus::WatchSync)
            .observed_token
            .clone();
        let idle_sync = idle.watch(Bytes::new(), 0);
        let idle_result = fixture.wire.watch(&idle_sync).await;
        let idle_token = target(&idle_sync, &idle_result, public::WatchStatus::WatchSync)
            .observed_token
            .clone();
        let pause = BeforeWait::install(b.partition).unwrap();
        let request = b.watch(token, 1_000);
        let pending = fixture.wire.submit(&request).await;
        pause.wait(deadline()).await.unwrap();
        let before = fixture.metrics(1).await;
        // All ACKs and publishes finished before B registered; this RetireRead is the sole release.
        retained.first().unwrap().retire(&fixture.wire, 1).await;
        let after = fixture.metrics(1).await;
        assert_eq!(activity(&after)[0], activity(&before)[0]);
        assert_eq!(
            metric(&after, "tidemq_watch_result_capacity_releases_total"),
            metric(&before, "tidemq_watch_result_capacity_releases_total") + 1
        );
        pause.resume();
        target(
            &request,
            &receive(pending).await,
            public::WatchStatus::WatchChanged,
        );
        let read = b.read(&fixture.wire, 2).await;
        assert_eq!(read.deliveries.len(), 1);
        assert_eq!(
            read.deliveries.first().unwrap().body,
            b"waiting-for-result-capacity".as_slice()
        );
        b.ack(&fixture.wire, read.deliveries.first().unwrap(), 1)
            .await;
        b.retire(&fixture.wire, 2).await;
        let probe = idle.watch(idle_token.clone(), 0);
        assert_eq!(
            target(
                &probe,
                &fixture.wire.watch(&probe).await,
                public::WatchStatus::WatchUnchanged
            )
            .observed_token,
            idle_token
        );
        let releases = metric(
            &fixture.metrics(1).await,
            "tidemq_watch_result_capacity_releases_total",
        );
        retained.first().unwrap().retire(&fixture.wire, 1).await;
        assert_eq!(
            metric(
                &fixture.metrics(1).await,
                "tidemq_watch_result_capacity_releases_total"
            ),
            releases,
            "An idempotent retirement released capacity twice"
        );
        drop(pause);
        for member in retained {
            member.retire(&fixture.wire, 1).await;
            member.leave(&fixture.wire).await;
        }
        b.leave(&fixture.wire).await;
        idle.leave(&fixture.wire).await;
        producer_a.close().await.unwrap();
        producer_b.close().await.unwrap();
        fixture.close().await;
    })
    .await
    .expect("The shared-result-capacity regression exceeded its total deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_joins_an_active_observation_and_releases_every_charged_resource() {
    let _serial = SERIAL.lock().await;
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut fixture = WatchFixture::new().await;
        let observer = fixture.runtime.running.get(&1).unwrap().watch_diagnostics();
        let request = fixture.member.watch(fixture.sync().await, 1_000);
        let pause = BeforeWait::install(fixture.member.partition).unwrap();
        let pending = fixture.wire.submit(&request).await;
        pause.wait(deadline()).await.unwrap();
        let active = observer.snapshot().await;
        assert_eq!(active.active_requests, 1);
        assert!(active.charged_bytes > 0);
        assert!(active.target_contexts > 0);
        assert!(active.groups.iter().any(|group| group.dependency_edges > 0));
        fixture
            .runtime
            .running
            .remove(&1)
            .unwrap()
            .shutdown_until(deadline())
            .await
            .unwrap();
        drop(pause);
        if let Ok(reply) = tokio::time::timeout_at(deadline(), pending.receive())
            .await
            .unwrap()
        {
            if reply.frame().kind == i32::from(FrameKind::GroupWatchResult) {
                let result: public::GroupWatchResult =
                    decoded(reply.frame(), FrameKind::GroupWatchResult);
                assert!(
                    result
                        .targets
                        .iter()
                        .all(|target| target.status
                            != i32::from(public::WatchStatus::WatchUnchanged)),
                    "Shutdown falsely completed a healthy unchanged observation"
                );
            } else {
                let _: tidemq_protocol::common::ErrorReply =
                    decoded(reply.frame(), FrameKind::Error);
            }
        }
        assert_released(&observer.snapshot().await);
        fixture.wire.close().await;
        fixture.admin.close().await.unwrap();
        fixture.runtime.close().await;
    })
    .await
    .expect("Active Watch shutdown exceeded its total deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_keeps_attempt_scoped_cancellation_reachable_and_never_reports_unchanged() {
    let _serial = SERIAL.lock().await;
    tokio::time::timeout(Duration::from_secs(30), async {
        let fixture = WatchFixture::new().await;
        let request = fixture.member.watch(fixture.sync().await, 1_000);
        let pause = BeforeWait::install(fixture.member.partition).unwrap();
        let pending = fixture.wire.submit(&request).await;
        pause.wait(deadline()).await.unwrap();
        let drained = fixture.admin.admin().begin_node_drain(1).await.unwrap();
        assert!(drained.draining);
        assert!(!drained.schedulable);
        fixture.wire.cancel(&request).await;
        drop(pause);
        let reply = tokio::time::timeout_at(deadline(), pending.receive())
            .await
            .unwrap()
            .unwrap();
        if reply.frame().kind == i32::from(FrameKind::GroupWatchResult) {
            let result: public::GroupWatchResult =
                decoded(reply.frame(), FrameKind::GroupWatchResult);
            assert!(
                result
                    .targets
                    .iter()
                    .all(|target| target.status != i32::from(public::WatchStatus::WatchUnchanged)),
                "A draining runtime reported a successful unchanged observation"
            );
        } else {
            let _: tidemq_protocol::common::ErrorReply = decoded(reply.frame(), FrameKind::Error);
        }
        fixture.close().await;
    })
    .await
    .expect("The Watch drain regression exceeded its total deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_node_drain_ends_an_active_watch_without_client_cancellation() {
    let _serial = SERIAL.lock().await;
    tokio::time::timeout(Duration::from_secs(30), async {
        let fixture = WatchFixture::new().await;
        let request = fixture.member.watch(fixture.sync().await, 1_000);
        let observer = fixture.runtime.running.get(&1).unwrap().watch_diagnostics();
        let pause = BeforeWait::install(fixture.member.partition).unwrap();
        let before = activity(&fixture.metrics(1).await);
        let cached_before: Vec<_> = observer
            .snapshot()
            .await
            .groups
            .into_iter()
            .map(|group| (group.group, group.cached_targets, group.dependency_edges))
            .collect();
        let mut watch_frame = frame(
            FrameKind::GroupWatch,
            Id::from_slice(&request.root_operation_id).unwrap(),
            &request,
        );
        watch_frame.remaining_timeout_ms = 30_000;
        let pending = fixture
            .wire
            .control
            .submit(watch_frame, Instant::now() + Duration::from_secs(30))
            .await
            .unwrap();
        pause.wait(deadline()).await.unwrap();
        assert_eq!(observer.snapshot().await.active_requests, 1);
        let drained = fixture.admin.admin().begin_node_drain(1).await.unwrap();
        assert!(drained.draining);
        assert!(!drained.schedulable);

        // The pause keeps its 30-second RPC deadline; completion must precede its release.
        let reply = tokio::time::timeout_at(deadline(), pending.receive())
            .await
            .unwrap()
            .unwrap();
        if reply.frame().kind == i32::from(FrameKind::GroupWatchResult) {
            let result: public::GroupWatchResult =
                decoded(reply.frame(), FrameKind::GroupWatchResult);
            assert_eq!(result.targets.len(), 1);
            let result = result.targets.first().unwrap();
            assert_eq!(result.shard_id, fixture.member.shard);
            assert_eq!(result.status, i32::from(public::WatchStatus::WatchRetry));
            assert_eq!(result.error_code, i32::from(ErrorCode::Unavailable));
            assert!(result.retryable);
        } else {
            let error: tidemq_protocol::common::ErrorReply =
                decoded(reply.frame(), FrameKind::Error);
            assert_eq!(error.code, i32::from(ErrorCode::Unavailable));
            assert!(error.retryable);
        }
        let after = observer.snapshot().await;
        assert_eq!(after.active_requests, 0);
        assert_eq!(after.forwards, 0);
        assert_eq!(after.cleanup_tasks, 0);
        assert_eq!(
            after
                .groups
                .iter()
                .map(|group| (group.group, group.cached_targets, group.dependency_edges))
                .collect::<Vec<_>>(),
            cached_before,
            "Automatic drain leaked a cached target or dependency"
        );
        assert_eq!(activity(&fixture.metrics(1).await), before);
        drop(pause);
        fixture.close().await;
    })
    .await
    .expect("Automatic Watch drain exceeded its total deadline");
}
