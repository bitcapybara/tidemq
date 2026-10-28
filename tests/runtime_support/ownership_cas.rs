use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::*;
use crate::config::Role;
use crate::runtime::data::consumer_tests::fixture::{Fixture, deadline};
use crate::runtime::resource_leases::LeaseRequest;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Step {
    Grant,
    Fence,
    Recovery,
    Activate,
}

struct Pause {
    partition: PartitionKey,
    step: Step,
    attempts: Mutex<Vec<(Id, u64)>>,
    ready: Notify,
    resume: Notify,
}

tokio::task_local! {
    static PAUSE: Arc<Pause>;
}

pub(super) async fn before_write(command: &Command) {
    let (partition, step) = match &command.action {
        metadata::Action::GrantOwnership { partition, .. } => (*partition, Step::Grant),
        metadata::Action::ObserveFence(evidence) => (evidence.fence.grant.partition, Step::Fence),
        metadata::Action::ObserveRecovery(evidence) => {
            (evidence.recovered.fence.grant.partition, Step::Recovery)
        }
        metadata::Action::ActivateRoute(evidence) => {
            (evidence.recovered.fence.grant.partition, Step::Activate)
        }
        _ => return,
    };
    let pause = PAUSE.try_with(Arc::clone).ok();
    let Some(pause) = pause.filter(|pause| pause.partition == partition && pause.step == step)
    else {
        return;
    };
    let first = {
        let mut attempts = pause.attempts.lock().unwrap();
        attempts.push((command.operation_id(), command.expected_revision));
        attempts.len() == 1
    };
    if first {
        pause.ready.notify_one();
        pause.resume.notified().await;
    }
}

async fn takeover_survives_lease_cas(step: Step) {
    let mut fixture = Fixture::new(
        &[
            (1, Role::Metadata),
            (2, Role::Storage),
            (3, Role::Broker),
            (4, Role::Broker),
            (5, Role::Gateway),
        ],
        &[1],
        &[2],
    )
    .await;
    fixture.start_all().await;
    let client = fixture.client(5).await;
    let tenant = client.admin().create_tenant("cas").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = client.admin().create_topic("cas/app/orders").await.unwrap();
    let producer = client.new_producer("cas/app/orders").build().await.unwrap();
    producer
        .send(tidemq_sdk::Message::new("before-takeover"))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let partition = PartitionKey {
        topic_id: topic.id().into(),
        partition: 0,
    };
    let service = fixture.running.get(&1).unwrap().service.clone();
    let original = service
        .metadata
        .as_ref()
        .unwrap()
        .state
        .try_with_application(|state| state.route(partition).cloned())
        .await
        .unwrap()
        .unwrap();
    let diagnostic = service
        .metadata
        .as_ref()
        .unwrap()
        .state
        .try_with_application(|state| state.diagnose_ownership(partition))
        .await
        .unwrap()
        .unwrap();
    let target = if original.owner_node == 3 { 4 } else { 3 };
    let pause = Arc::new(Pause {
        partition,
        step,
        attempts: Mutex::new(Vec::new()),
        ready: Notify::new(),
        resume: Notify::new(),
    });
    let coordinator = service.clone();
    let until = deadline();
    let pending = tokio::spawn(PAUSE.scope(pause.clone(), async move {
        coordinator
            .execute_takeover(
                partition,
                target,
                diagnostic.epoch,
                diagnostic.route_generation,
                until,
            )
            .await
    }));
    tokio::time::timeout_at(until, pause.ready.notified())
        .await
        .unwrap();
    let expected = pause.attempts.lock().unwrap().first().unwrap().1;
    let storage = fixture.running.get(&2).unwrap().service.clone();
    let lease_id = Id::new_v7().unwrap();
    let reply = storage
        .resource_lease(
            LeaseRequest::Acquire {
                id: lease_id,
                target: metadata::ResourceLeaseTarget::ColdRead(partition),
            },
            until,
        )
        .await
        .unwrap();
    assert_eq!(reply.lease.as_ref().unwrap().id, lease_id);
    assert!(service.meta_revision().await.unwrap() > expected);
    pause.resume.notify_one();
    let route = tokio::time::timeout_at(until, pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(route.owner_node, target);
    let after = service
        .metadata
        .as_ref()
        .unwrap()
        .state
        .try_with_application(|state| state.diagnose_ownership(partition))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.phase, crate::ownership::OwnershipPhase::Active);
    assert_eq!(after.epoch, diagnostic.epoch + 1);
    assert_eq!(after.route_generation, diagnostic.route_generation + 1);
    let attempts = pause.attempts.lock().unwrap().clone();
    assert!(attempts.len() >= 2);
    let operation = attempts.first().unwrap().0;
    assert!(attempts.iter().all(|(id, _)| *id == operation));
    assert!(
        attempts
            .iter()
            .skip(1)
            .all(|(_, revision)| *revision > expected)
    );
    assert_eq!(
        service
            .execute_takeover(
                partition,
                target,
                diagnostic.epoch,
                diagnostic.route_generation,
                until
            )
            .await
            .unwrap(),
        route
    );
    storage
        .resource_lease(LeaseRequest::Release { id: lease_id }, until)
        .await
        .unwrap();
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn granting_ownership_retries_only_a_rejected_cas_with_the_original_identity() {
    takeover_survives_lease_cas(Step::Grant).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observing_the_fence_retries_with_fresh_proof_after_a_lease_cas() {
    takeover_survives_lease_cas(Step::Fence).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observing_recovery_retries_with_fresh_proof_after_a_lease_cas() {
    takeover_survives_lease_cas(Step::Recovery).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activating_the_original_route_recovers_the_replayed_resource_lease_interleaving() {
    takeover_survives_lease_cas(Step::Activate).await;
}

async fn local_command() -> (Fixture, tidemq_sdk::Client, Arc<Service>, Command) {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("local-cas").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = client
        .admin()
        .create_topic("local-cas/app/orders")
        .await
        .unwrap();
    let producer = client
        .new_producer("local-cas/app/orders")
        .build()
        .await
        .unwrap();
    producer
        .send(tidemq_sdk::Message::new("before-cas"))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let partition = PartitionKey {
        topic_id: topic.id().into(),
        partition: 0,
    };
    let service = fixture.running.get(&1).unwrap().service.clone();
    let diagnostic = service
        .metadata
        .as_ref()
        .unwrap()
        .state
        .try_with_application(|state| state.diagnose_ownership(partition))
        .await
        .unwrap()
        .unwrap();
    let command = Command::grant_ownership(
        service.meta_revision().await.unwrap(),
        service.cluster,
        partition,
        1,
        diagnostic.epoch,
        diagnostic.route_generation,
    )
    .unwrap();
    (fixture, client, service, command)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_ownership_commit_is_not_automatically_resubmitted() {
    let (fixture, client, service, command) = local_command().await;
    let _guard = service.metadata_mutation.lock().await;
    let hook = crate::execution::LocalExecutionHook::new(
        &service.config.data_dir.join("group-1"),
        "before_durable",
        false,
    );
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = attempts.clone();
    let original = command.operation_id();
    let coordinator = service.clone();
    let pending = tokio::spawn(async move {
        coordinator
            .ownership_meta_write(
                crate::ownership::Transition::Grant,
                2,
                || {
                    observed.fetch_add(1, Ordering::AcqRel);
                    let command = command.clone();
                    async move { Ok(command) }
                },
                Instant::now() + Duration::from_millis(200),
            )
            .await
    });
    hook.entered().await;
    let error = pending.await.unwrap().unwrap_err();
    assert_eq!(error.failure().outcome, i32::from(Outcome::Unknown));
    assert_eq!(attempts.load(Ordering::Acquire), 1);
    hook.release();
    tokio::time::timeout_at(deadline(), async {
        loop {
            let saved = service
                .metadata
                .as_ref()
                .unwrap()
                .state
                .try_with_application(|state| state.lookup_operation(original))
                .await
                .unwrap();
            if let metadata::OperationResult::Known(Response::Applied {
                outcome: metadata::Outcome::OwnershipGranted(grant),
                ..
            }) = saved
            {
                assert_eq!(grant.operation_id, original);
                assert_eq!(grant.epoch, 2);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(attempts.load(Ordering::Acquire), 1);
    drop(_guard);
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_ownership_deadline_never_prepares_a_new_attempt() {
    let (fixture, client, service, command) = local_command().await;
    let attempts = AtomicUsize::new(0);
    let error = service
        .ownership_meta_write(
            crate::ownership::Transition::Grant,
            2,
            || {
                attempts.fetch_add(1, Ordering::AcqRel);
                let command = command.clone();
                async move { Ok(command) }
            },
            Instant::now() - Duration::from_millis(1),
        )
        .await
        .unwrap_err();
    assert_eq!(error.failure().outcome, i32::from(Outcome::Rejected));
    assert_eq!(attempts.load(Ordering::Acquire), 0);
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_permanent_ownership_refusal_is_returned_after_one_attempt() {
    let (fixture, client, service, command) = local_command().await;
    let metadata::Action::GrantOwnership {
        partition,
        expected_epoch,
        expected_route_generation,
        ..
    } = command.action
    else {
        panic!("Expected an ownership grant");
    };
    let attempts = AtomicUsize::new(0);
    let error = service
        .ownership_meta_write(
            crate::ownership::Transition::Grant,
            2,
            || async {
                attempts.fetch_add(1, Ordering::AcqRel);
                Command::grant_ownership(
                    service.meta_revision().await?,
                    service.cluster,
                    partition,
                    999,
                    expected_epoch,
                    expected_route_generation,
                )
                .map_err(metadata_rejection)
            },
            deadline(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.failure().outcome, i32::from(Outcome::Rejected));
    assert_eq!(attempts.load(Ordering::Acquire), 1);
    client.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_proof_preparation_is_bounded_by_the_original_deadline() {
    let (fixture, client, service, command) = local_command().await;
    let metadata::Action::GrantOwnership { partition, .. } = command.action else {
        panic!("Expected an ownership grant");
    };
    let grant = service
        .metadata
        .as_ref()
        .unwrap()
        .state
        .try_with_application(|state| state.grant(partition).cloned())
        .await
        .unwrap()
        .unwrap();
    let replica = service.data.get(&grant.group_id).unwrap();
    let authority = DataAuthority::new(
        replica.group.clone(),
        service.cluster,
        grant.group_id,
        replica.current_placement().unwrap(),
        ControlPolicy::default(),
    )
    .unwrap();
    let hook = crate::execution::LocalExecutionHook::new(
        &service
            .config
            .data_dir
            .join(format!("group-{}", grant.group_id)),
        "before_apply",
        false,
    );
    let group = replica.group.clone();
    let write = DataCommand::Recover {
        partition,
        writer: WriterFence {
            node_id: grant.owner_node,
            owner_epoch: grant.epoch,
        },
    };
    let held_writer = tokio::spawn(async move { group.commit(write, deadline()).await });
    hook.entered().await;
    let until = Instant::now() + Duration::from_millis(50);
    let mut pending = Box::pin(service.ownership_meta_write(
        crate::ownership::Transition::Activate,
        grant.epoch,
        || async {
            let revision = service.meta_revision().await?;
            authority
                .verify_recovery(&grant)
                .await
                .and_then(|proof| proof.into_activate_command(revision))
                .map_err(|source| Error::operation("verify fresh local route recovery", source))
        },
        until,
    ));
    let observation =
        tokio::time::timeout_at(until + Duration::from_millis(200), pending.as_mut()).await;
    let respected_deadline = observation.is_ok();
    eprintln!(
        "Independent prepare deadline ledger: completed_within_deadline_plus_200ms={respected_deadline}"
    );
    hook.release();
    held_writer.await.unwrap().unwrap();
    match observation {
        Ok(result) => assert!(result.is_err()),
        Err(_) => assert!(pending.await.is_err()),
    }
    client.close().await.unwrap();
    fixture.close().await;
    assert!(
        respected_deadline,
        "A real local proof preparation outlived the original ownership deadline by more than 200ms"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revision_retry_preparation_is_bounded_by_the_original_deadline() {
    let (fixture, client, service, mut command) = local_command().await;
    let guard = service.metadata_mutation.lock().await;
    command.expected_revision = command.expected_revision.checked_sub(1).unwrap();
    let attempts = AtomicUsize::new(0);
    let until = Instant::now() + Duration::from_millis(300);
    let mut pending = Box::pin(service.ownership_meta_write(
        crate::ownership::Transition::Grant,
        2,
        || {
            let attempt = attempts.fetch_add(1, Ordering::AcqRel);
            let command = command.clone();
            async move {
                if attempt > 0 {
                    std::future::pending::<()>().await;
                }
                Ok(command)
            }
        },
        until,
    ));
    let observation =
        tokio::time::timeout_at(until + Duration::from_millis(200), pending.as_mut()).await;
    let respected_deadline = observation.is_ok();
    if let Ok(result) = observation {
        assert_eq!(
            result.unwrap_err().failure().outcome,
            i32::from(Outcome::Unknown)
        );
    }
    drop(pending);
    assert_eq!(attempts.load(Ordering::Acquire), 2);
    drop(guard);
    client.close().await.unwrap();
    fixture.close().await;
    assert!(
        respected_deadline,
        "A rejected ownership CAS retry preparation outlived its original deadline"
    );
}
