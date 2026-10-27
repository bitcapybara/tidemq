use std::{collections::BTreeMap, sync::Mutex};

use tidemq_protocol::{
    console::{Query, Reply},
    topic::SubscriptionKind,
};
use tidemq_sdk::Client;
use tokio::sync::Notify;

use super::*;
use crate::{
    config::Role,
    metadata::Route,
    ownership::{ControlPolicy, test_lease},
    runtime::data::consumer_tests::fixture::{Fixture, deadline},
    subscription::{GroupCommand, GroupIdentity, GroupOutcome, GroupRead, MemberGrant},
};

type RetireKey = (
    PartitionKey,
    tidemq_protocol::Id,
    tidemq_protocol::Id,
    u64,
    u64,
);
static WRITES: Mutex<BTreeMap<RetireKey, Arc<WritePause>>> = Mutex::new(BTreeMap::new());

struct WritePause {
    deadline: Mutex<Option<Instant>>,
    ready: Notify,
    resume: Notify,
}

struct InstalledPause {
    key: RetireKey,
    state: Arc<WritePause>,
}

impl InstalledPause {
    fn new(partition: PartitionKey, command: &GroupCommand) -> Self {
        let key = retire_key(partition, command).unwrap();
        let state = Arc::new(WritePause {
            deadline: Mutex::new(None),
            ready: Notify::new(),
            resume: Notify::new(),
        });
        assert!(WRITES.lock().unwrap().insert(key, state.clone()).is_none());
        Self { key, state }
    }
}

impl Drop for InstalledPause {
    fn drop(&mut self) {
        WRITES.lock().unwrap().remove(&self.key);
        self.state.resume.notify_one();
    }
}

fn retire_key(partition: PartitionKey, command: &GroupCommand) -> Option<RetireKey> {
    match command {
        GroupCommand::RetireRead {
            subscription,
            member,
            epoch,
            sequence,
        } => Some((partition, *subscription, *member, *epoch, *sequence)),
        _ => None,
    }
}

pub(in crate::runtime) async fn before_group_write(
    partition: PartitionKey,
    command: &GroupCommand,
    deadline: Instant,
) {
    let pause =
        retire_key(partition, command).and_then(|key| WRITES.lock().unwrap().get(&key).cloned());
    if let Some(pause) = pause {
        *pause.deadline.lock().unwrap() = Some(deadline);
        pause.ready.notify_one();
        pause.resume.notified().await;
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

async fn prepared() -> Prepared {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
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
    let Reply::Topic(detail) = client.console_read(Query::Topic { topic }).await.unwrap() else {
        panic!("Expected the native shard identity");
    };
    let shard = detail.shard_details.first().unwrap().id;
    let partition = PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let service = fixture.running.get(&1).unwrap().service.clone();
    let route = service.route(partition, deadline()).await.unwrap();
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

async fn install_lease(prepared: &Prepared, ttl: Duration) -> Arc<ControlLease> {
    let current = prepared
        .service
        .data_lease(2, prepared.route.partition, deadline())
        .await
        .unwrap();
    let lease = Arc::new(
        test_lease(
            current.grant().clone(),
            ControlPolicy {
                ttl,
                ..ControlPolicy::default()
            },
        )
        .unwrap(),
    );
    let slot = prepared
        .service
        .leases
        .lock()
        .unwrap()
        .get(&prepared.route.partition)
        .unwrap()
        .clone();
    *slot.lock().await = Some(lease.clone());
    lease
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
async fn retire_read_refreshes_a_near_expiry_control_lease_before_the_original_raft_write() {
    let prepared = prepared().await;
    assert!(!retired(&prepared).await);
    let lease = install_lease(&prepared, Duration::from_millis(200)).await;
    let pause = InstalledPause::new(prepared.route.partition, &prepared.retire);
    let original_deadline = deadline();
    let service = prepared.service.clone();
    let route = prepared.route.clone();
    let command = prepared.retire.clone();
    let pending = tokio::spawn(async move {
        service
            .group_commit(&route, command, original_deadline)
            .await
    });
    tokio::time::timeout_at(original_deadline, pause.state.ready.notified())
        .await
        .unwrap();
    let write_deadline = pause.state.deadline.lock().unwrap().unwrap();
    tokio::time::sleep_until((lease.expires_at() + Duration::from_millis(20)).into()).await;
    pause.state.resume.notify_one();
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
    drop(pause);
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
async fn retirement_keeps_unknown_when_the_original_business_deadline_expires() {
    let prepared = prepared().await;
    let lease = install_lease(&prepared, Duration::from_secs(2)).await;
    let pause = InstalledPause::new(prepared.route.partition, &prepared.retire);
    let original_deadline = Instant::now() + Duration::from_millis(200);
    let service = prepared.service.clone();
    let route = prepared.route.clone();
    let command = prepared.retire.clone();
    let pending = tokio::spawn(async move {
        service
            .group_commit(&route, command, original_deadline)
            .await
    });
    tokio::time::timeout_at(deadline(), pause.state.ready.notified())
        .await
        .unwrap();
    assert!(pause.state.deadline.lock().unwrap().unwrap() <= original_deadline);
    tokio::time::sleep_until(original_deadline + Duration::from_millis(20)).await;
    pause.state.resume.notify_one();
    let error = pending.await.unwrap().unwrap_err();
    assert_eq!(
        error.failure().outcome,
        i32::from(tidemq_protocol::common::Outcome::Unknown)
    );
    assert!(lease.expires_at() > std::time::Instant::now());
    drop(pause);
    prepared.client.close().await.unwrap();
    prepared.fixture.close().await;
}
