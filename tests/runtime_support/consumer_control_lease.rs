use std::sync::Mutex;

use tidemq_protocol::{
    console::{Query, Reply},
    topic::SubscriptionKind,
};
use tidemq_sdk::Client;
use tokio::sync::Notify;

use super::*;
use crate::{
    config::Role,
    metadata::{AllocationRequest, QuotaGrant, Route},
    ownership::{ControlPolicy, test_lease},
    runtime::data::consumer_tests::fixture::{Fixture, deadline},
    subscription::{GroupCommand, GroupIdentity, GroupOutcome, GroupRead, MemberGrant},
};

tokio::task_local! {
    static LEASE_PROBE: Arc<LeaseProbe>;
}

struct LeaseProbe {
    partition: PartitionKey,
    ttl: Duration,
    old: Mutex<Option<Arc<ControlLease>>>,
    observed_remaining: Mutex<Option<Duration>>,
    write_deadline: Mutex<Option<Instant>>,
    ready: Notify,
    resume: Notify,
}

impl LeaseProbe {
    fn new(partition: PartitionKey, ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            partition,
            ttl,
            old: Mutex::new(None),
            observed_remaining: Mutex::new(None),
            write_deadline: Mutex::new(None),
            ready: Notify::new(),
            resume: Notify::new(),
        })
    }

    fn old_lease(&self) -> Arc<ControlLease> {
        assert!(self.observed_remaining.lock().unwrap().is_some());
        self.old.lock().unwrap().as_ref().unwrap().clone()
    }

    async fn wait_for_write(&self, deadline: Instant) -> Instant {
        tokio::time::timeout_at(deadline, self.ready.notified())
            .await
            .unwrap();
        self.write_deadline.lock().unwrap().unwrap()
    }

    async fn resume_after_old_expiry(&self, deadline: Instant) -> (Arc<ControlLease>, Instant) {
        let write_deadline = self.wait_for_write(deadline).await;
        let lease = self.old_lease();
        tokio::time::sleep_until((lease.expires_at() + Duration::from_millis(20)).into()).await;
        assert!(Instant::now() < deadline);
        self.resume.notify_one();
        (lease, write_deadline)
    }
}

pub(in crate::runtime) fn prepare_cached_lease(
    partition: PartitionKey,
    cached: &mut Option<Arc<ControlLease>>,
) {
    let _ = LEASE_PROBE.try_with(|probe| {
        if probe.partition != partition {
            return;
        }
        let mut old = probe.old.lock().unwrap();
        if old.is_some() {
            return;
        }
        // Inject under the cache lock in this task, after all pre-selection awaits.
        let lease = Arc::new(
            test_lease(
                cached.as_ref().unwrap().grant().clone(),
                ControlPolicy {
                    ttl: probe.ttl,
                    ..ControlPolicy::default()
                },
            )
            .unwrap(),
        );
        *cached = Some(lease.clone());
        *old = Some(lease);
    });
}

pub(in crate::runtime) fn observe_cached_lease(
    partition: PartitionKey,
    lease: &Arc<ControlLease>,
    valid: bool,
    remaining: Duration,
) {
    let _ = LEASE_PROBE.try_with(|probe| {
        if probe.partition != partition
            || !Arc::ptr_eq(lease, probe.old.lock().unwrap().as_ref().unwrap())
        {
            return;
        }
        assert!(
            valid,
            "The injected cache must still pass the real fence and TTL check"
        );
        assert!(
            !remaining.is_zero(),
            "The old lease must be live at cache selection"
        );
        if probe.ttl < Duration::from_secs(1) {
            assert!(remaining < Duration::from_secs(1));
        }
        assert!(
            probe
                .observed_remaining
                .lock()
                .unwrap()
                .replace(remaining)
                .is_none()
        );
        eprintln!(
            "Cache selection: valid={valid}, old_remaining_ns={}",
            remaining.as_nanos()
        );
    });
}

pub(in crate::runtime) async fn before_control_write(partition: PartitionKey, deadline: Instant) {
    let probe = LEASE_PROBE.try_with(Arc::clone).ok();
    if let Some(probe) = probe.filter(|probe| probe.partition == partition) {
        assert!(probe.observed_remaining.lock().unwrap().is_some());
        *probe.write_deadline.lock().unwrap() = Some(deadline);
        probe.ready.notify_one();
        probe.resume.notified().await;
    }
}

pub(in crate::runtime) async fn before_group_write(
    partition: PartitionKey,
    command: &GroupCommand,
    deadline: Instant,
) {
    if matches!(command, GroupCommand::RetireRead { .. }) {
        before_control_write(partition, deadline).await;
    }
}

struct Prepared {
    fixture: Fixture,
    client: Client,
    service: Arc<Service>,
    route: Route,
    subscription: tidemq_protocol::Id,
    read: GroupRead,
    retire: GroupCommand,
}

async fn service_fixture() -> (Fixture, Client, Arc<Service>, Route) {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    let config = fixture.configs.get_mut(&1).unwrap();
    config.runtime.retention.minimum_age_ms = 0;
    config.runtime.archive_interval_ms = 60_000;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("lease-test").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = client
        .admin()
        .create_topic("lease-test/app/retirement")
        .await
        .unwrap();
    let topic = topic.id().into();
    let partition = PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let service = fixture.running.get(&1).unwrap().service.clone();
    let route = service.route(partition, deadline()).await.unwrap();
    (fixture, client, service, route)
}

async fn prepared() -> Prepared {
    let (fixture, client, service, route) = service_fixture().await;
    let topic = route.partition.topic_id;
    let Reply::Topic(detail) = client.console_read(Query::Topic { topic }).await.unwrap() else {
        panic!("Expected the native shard identity");
    };
    let shard = detail.shard_details.first().unwrap().id;
    let subscription = tidemq_protocol::Id::new_v7().unwrap();
    let member = tidemq_protocol::Id::new_v7().unwrap();
    let actor = tidemq_protocol::Id::new_v7().unwrap();
    service
        .group_commit(
            &route,
            GroupCommand::Create {
                identity: GroupIdentity {
                    topic,
                    shard,
                    subscription,
                },
                model: SubscriptionKind::Queue,
                start: 0,
                signing_key: [7; 32].try_into().unwrap(),
                blocked: false,
            },
            deadline(),
        )
        .await
        .unwrap();
    let now_ms = unix_millis().unwrap();
    service
        .group_commit(
            &route,
            GroupCommand::InstallMembers {
                subscription,
                version: 1,
                grants: vec![MemberGrant {
                    member,
                    client: actor,
                    credential: actor,
                    epoch: 1,
                    expires_ms: now_ms + 60_000,
                    lanes: Vec::new(),
                    max_messages: 1,
                    max_bytes: 64 * 1024,
                }],
                now_ms,
            },
            deadline(),
        )
        .await
        .unwrap();
    let read = GroupRead {
        member,
        epoch: 1,
        sequence: 1,
        now_ms,
        max_messages: 1,
        max_bytes: 64 * 1024,
    };
    let outcome = service
        .group_commit(
            &route,
            GroupCommand::Read {
                subscription,
                request: read,
                cold: Vec::new(),
            },
            deadline(),
        )
        .await
        .unwrap();
    assert!(
        matches!(outcome, GroupOutcome::Read(result) if result.deliveries.is_empty() && !result.retired)
    );
    let retire = GroupCommand::RetireRead {
        subscription,
        member,
        epoch: 1,
        sequence: 1,
    };
    Prepared {
        fixture,
        client,
        service,
        route,
        subscription,
        read,
        retire,
    }
}

async fn retired(prepared: &Prepared) -> bool {
    prepared
        .service
        .data
        .get(&2)
        .unwrap()
        .state
        .try_with_application(|state| {
            let cursor = state
                .consumer_group_cursor(prepared.route.partition, prepared.subscription)
                .unwrap();
            assert_eq!(cursor.acknowledged_before(), 0);
            assert_eq!(cursor.member_count(), 1);
            match cursor.original_read(&prepared.read) {
                Some(Ok(result)) => {
                    assert_eq!(result.sequence, prepared.read.sequence);
                    assert!(result.deliveries.is_empty());
                    result.retired
                }
                Some(Err(crate::subscription::Rejection::OutcomeExpired)) => true,
                observed => panic!("Unexpected original Read outcome: {observed:?}"),
            }
        })
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retire_read_refreshes_a_near_expiry_control_lease_before_the_original_durable_write() {
    let prepared = prepared().await;
    assert!(!retired(&prepared).await);
    let probe = LeaseProbe::new(prepared.route.partition, Duration::from_millis(200));
    let original_deadline = deadline();
    let service = prepared.service.clone();
    let route = prepared.route.clone();
    let command = prepared.retire.clone();
    let pending = tokio::spawn(LEASE_PROBE.scope(probe.clone(), async move {
        service
            .group_commit(&route, command, original_deadline)
            .await
    }));
    let (lease, write_deadline) = probe.resume_after_old_expiry(original_deadline).await;
    let outcome = pending.await.unwrap().unwrap();
    assert!(matches!(outcome, GroupOutcome::ReadRetired));
    let refreshed = prepared
        .service
        .data_lease(2, prepared.route.partition, original_deadline)
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&lease, &refreshed));
    assert_eq!(lease.grant(), refreshed.grant());
    assert!(write_deadline <= original_deadline && write_deadline > lease.expires_at().into());
    assert!(retired(&prepared).await);
    assert!(matches!(
        prepared
            .service
            .group_commit(&prepared.route, prepared.retire.clone(), original_deadline)
            .await
            .unwrap(),
        GroupOutcome::ReadRetired
    ));
    assert!(retired(&prepared).await);
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retire_read_reuses_a_fresh_control_lease_with_the_original_writer() {
    let prepared = prepared().await;
    let lease = prepared
        .service
        .data_lease(2, prepared.route.partition, deadline())
        .await
        .unwrap();
    let original_writer = lease.grant().clone();
    assert!(matches!(
        prepared
            .service
            .group_commit(&prepared.route, prepared.retire.clone(), deadline())
            .await
            .unwrap(),
        GroupOutcome::ReadRetired
    ));
    let after = prepared
        .service
        .data_lease(2, prepared.route.partition, deadline())
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&lease, &after));
    assert_eq!(after.grant(), &original_writer);
    assert!(retired(&prepared).await);
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retirement_rejects_without_submission_when_the_original_deadline_expires_before_handoff() {
    let prepared = prepared().await;
    let probe = LeaseProbe::new(prepared.route.partition, Duration::from_secs(2));
    let original_deadline = Instant::now() + Duration::from_millis(200);
    let service = prepared.service.clone();
    let route = prepared.route.clone();
    let command = prepared.retire.clone();
    let pending = tokio::spawn(LEASE_PROBE.scope(probe.clone(), async move {
        service
            .group_commit(&route, command, original_deadline)
            .await
    }));
    let write_deadline = probe.wait_for_write(deadline()).await;
    let lease = probe.old_lease();
    assert!(write_deadline <= original_deadline);
    tokio::time::sleep_until(original_deadline + Duration::from_millis(20)).await;
    probe.resume.notify_one();
    let error = pending.await.unwrap().unwrap_err();
    assert_eq!(
        error.failure().outcome,
        i32::from(tidemq_protocol::common::Outcome::Rejected)
    );
    assert!(lease.expires_at() > std::time::Instant::now());
    assert!(!retired(&prepared).await);
    assert!(matches!(
        prepared
            .service
            .group_commit(&prepared.route, prepared.retire.clone(), deadline())
            .await
            .unwrap(),
        GroupOutcome::ReadRetired
    ));
    assert!(retired(&prepared).await);
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retirement_keeps_unknown_after_handoff_and_recovers_the_original_read() {
    let prepared = prepared().await;
    let directory = prepared.service.config.data_dir.join("group-2");
    let hook = crate::execution::LocalExecutionHook::new(&directory, "before_durable", false);
    let service = prepared.service.clone();
    let route = prepared.route.clone();
    let command = prepared.retire.clone();
    let original_deadline = Instant::now() + Duration::from_millis(200);
    let pending = tokio::spawn(async move {
        service
            .group_commit(&route, command, original_deadline)
            .await
    });
    hook.entered().await;
    let error = pending.await.unwrap().unwrap_err();
    assert_eq!(
        error.failure().outcome,
        i32::from(tidemq_protocol::common::Outcome::Unknown)
    );
    assert!(!retired(&prepared).await);
    hook.release();
    tokio::time::timeout_at(deadline(), async {
        while !retired(&prepared).await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        prepared
            .service
            .group_commit(&prepared.route, prepared.retire.clone(), deadline())
            .await
            .unwrap(),
        GroupOutcome::ReadRetired
    ));
    assert!(retired(&prepared).await);
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quota_install_refreshes_a_near_expiry_control_lease_before_the_original_raft_write() {
    let prepared = prepared().await;
    let current = prepared
        .service
        .data
        .get(&2)
        .unwrap()
        .state
        .try_with_application(|state| {
            state
                .tenant_quota(prepared.route.partition)
                .unwrap()
                .clone()
        })
        .await
        .unwrap();
    let request = AllocationRequest {
        partition: current.partition,
        tenant: current.tenant,
        namespace: current.namespace,
        expected_version: current.version,
        capacity: current.capacity,
        frozen: current.frozen,
    };
    let reply = prepared
        .service
        .quota_control_command(
            1,
            &serde_json::to_vec(&serde_json::json!({"Prepare": request})).unwrap(),
            deadline(),
        )
        .await
        .unwrap();
    let reply: serde_json::Value = serde_json::from_slice(&reply).unwrap();
    let grant: QuotaGrant = serde_json::from_value(reply.get("Grant").unwrap().clone()).unwrap();
    assert_eq!(grant.version, current.version + 1);
    let body = serde_json::to_vec(&serde_json::json!({"Install": grant})).unwrap();
    let probe = LeaseProbe::new(prepared.route.partition, Duration::from_millis(200));
    let original_deadline = deadline();
    let service = prepared.service.clone();
    let pending = tokio::spawn(LEASE_PROBE.scope(probe.clone(), async move {
        service
            .quota_data_command(2, &body, original_deadline)
            .await
    }));
    let (old, write_deadline) = probe.resume_after_old_expiry(original_deadline).await;
    let reply = pending.await.unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<QuotaGrant>(&reply).unwrap(), grant);
    let installed = prepared
        .service
        .data
        .get(&2)
        .unwrap()
        .state
        .try_with_application(|state| {
            state
                .tenant_quota(prepared.route.partition)
                .unwrap()
                .clone()
        })
        .await
        .unwrap();
    assert_eq!(installed, grant);
    let refreshed = prepared
        .service
        .data_lease(2, prepared.route.partition, original_deadline)
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&old, &refreshed));
    assert_eq!(old.grant(), refreshed.grant());
    assert!(write_deadline > old.expires_at().into() && write_deadline <= original_deadline);
    assert!(!retired(&prepared).await);
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_refreshes_a_near_expiry_control_lease_before_the_original_control_chain() {
    let (fixture, client, service, route) = service_fixture().await;
    let queue = client
        .new_queue_consumer("lease-test/app/retirement", "retention")
        .build()
        .await
        .unwrap();
    let producer = client
        .new_producer("lease-test/app/retirement")
        .build()
        .await
        .unwrap();
    let receipt = producer.send("retain-until-acknowledged").await.unwrap();
    let delivery = queue.receive().await.unwrap();
    assert_eq!(delivery.message_id(), receipt.message_id());
    queue.ack(&delivery).await.unwrap();
    let before = tokio::time::timeout_at(deadline(), async {
        loop {
            let status = service
                .data
                .get(&2)
                .unwrap()
                .state
                .try_with_application(|state| {
                    state
                        .retention_status(route.partition, unix_millis().unwrap())
                        .unwrap()
                })
                .await
                .unwrap();
            if status.maximum_floor == 1 {
                break status;
            }
            // Retention only releases a completed timestamp bucket, even with zero minimum age.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(before.floor, 0);
    assert_eq!(before.maximum_floor, 1);
    let original_deadline = deadline();
    service
        .data_lease(2, route.partition, original_deadline)
        .await
        .unwrap();
    let probe = LeaseProbe::new(route.partition, Duration::from_millis(200));
    let handler = service.clone();
    let partition = route.partition;
    let pending = tokio::spawn(LEASE_PROBE.scope(probe.clone(), async move {
        handler
            .advance_retention(2, partition, original_deadline)
            .await
    }));
    let (old, write_deadline) = probe.resume_after_old_expiry(original_deadline).await;
    pending.await.unwrap().unwrap();
    let after = service
        .data
        .get(&2)
        .unwrap()
        .state
        .try_with_application(|state| {
            state
                .retention_status(partition, unix_millis().unwrap())
                .unwrap()
        })
        .await
        .unwrap();
    assert_eq!(after.floor, 1);
    let plan = service
        .retention_control(
            crate::runtime::retention_control::Control::Inspect(partition),
            original_deadline,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(plan.command.before, 1);
    assert!(plan.completed_revision.is_some());
    let refreshed = service
        .data_lease(2, partition, original_deadline)
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&old, &refreshed));
    assert_eq!(old.grant(), refreshed.grant());
    assert!(write_deadline > old.expires_at().into() && write_deadline <= original_deadline);
    producer.close().await.unwrap();
    queue.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_lease_reuses_a_live_cache_when_the_original_budget_is_shorter() {
    let prepared = prepared().await;
    let probe = LeaseProbe::new(prepared.route.partition, Duration::from_millis(700));
    let lease = LEASE_PROBE
        .scope(
            probe.clone(),
            prepared.service.data_lease(
                2,
                prepared.route.partition,
                Instant::now() + Duration::from_millis(200),
            ),
        )
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&lease, &probe.old_lease()));
    assert!(probe.observed_remaining.lock().unwrap().unwrap() < Duration::from_secs(1));
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_lease_minimum_can_remain_stronger_than_the_shared_default() {
    let prepared = prepared().await;
    let probe = LeaseProbe::new(prepared.route.partition, Duration::from_millis(1500));
    let lease = LEASE_PROBE
        .scope(
            probe.clone(),
            prepared.service.data_lease_for(
                2,
                prepared.route.partition,
                deadline(),
                Duration::from_secs(2),
            ),
        )
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&lease, &probe.old_lease()));
    assert_eq!(lease.grant(), probe.old_lease().grant());
    assert!(
        lease
            .expires_at()
            .saturating_duration_since(std::time::Instant::now())
            >= Duration::from_secs(2)
    );
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
}
