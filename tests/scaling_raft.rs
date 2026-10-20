#[path = "replication_support/quic.rs"]
mod quic;
#[path = "replication_support/scaling.rs"]
mod scaling_support;
use scaling_support::metadata;
#[path = "replication_support/mod.rs"]
mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use prometheus_client::registry::Registry;
use quic::{Cluster, GROUP, WAIT};
use support::{Command, Response, State};
use tidemq::metadata::MetadataState;
use tidemq::replication::rpc::GroupRole;
use tidemq::scaling::{
    Capacity, Error, MembershipCoordinator, MembershipPlan, RepairBudget, RepairLimits,
    ScalingMetrics,
};
use tidemq_protocol::Id;
use tokio::time::Instant;

fn capacity() -> [Capacity; 1] {
    [Capacity {
        node_id: 4,
        observed_at: std::time::Instant::now(),
        free_bytes: 4 * 1024 * 1024 * 1024,
        reserved_bytes: 0,
        bytes_per_second: 0,
        cpu_per_mille: 1,
        connections: 0,
        ack_bytes: 0,
    }]
}

async fn coordinator_leaders(
    metadata: &Cluster<MetadataState>,
    data: &Cluster<State>,
    eligible: &[u64],
    deadline: Instant,
    stage: &str,
) -> (u64, u64) {
    let ready = tokio::time::timeout_at(deadline, async {
        loop {
            let current = data.node(2).raft.metrics().borrow().current_leader;
            let observed = data.node(4).raft.metrics().borrow().current_leader;
            // Published leader IDs do not prove either group's quorum is ready.
            if current == observed
                && let Some(leader) = current.filter(|leader| eligible.contains(leader))
                && let Some(metadata_leader) = fresh_metadata_leader(metadata, &[1, 2, 3]).await
                && data.node(leader).raft.ensure_linearizable().await.is_ok()
            {
                return (metadata_leader, leader);
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    match ready {
        Ok(leader) => leader,
        Err(error) => {
            panic!("The replacement coordinator requires a ready leader during {stage}: {error}")
        }
    }
}

async fn fresh_metadata_leader(metadata: &Cluster<MetadataState>, eligible: &[u64]) -> Option<u64> {
    for &leader in eligible {
        let raft = &metadata.node(leader).raft;
        let current = raft.metrics().borrow().current_leader;
        if current == Some(leader) && raft.ensure_linearizable().await.is_ok() {
            return Some(leader);
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_quic_membership_reconciles_unknown_learner_then_replaces_voter_without_data_loss() {
    let cluster_id = Id::random().unwrap();
    let budget = RepairBudget::new(RepairLimits {
        concurrent: 1,
        bytes_per_second: 1024 * 1024,
        burst_bytes: 64 * 1024,
        queued_bytes: 128 * 1024,
    })
    .unwrap();
    let data = Cluster::<State>::with_budget(
        &[1, 2, 3, 4],
        cluster_id,
        GROUP,
        GroupRole::Data,
        Some(budget.clone()),
    )
    .await;
    data.initialize().await;
    data.node(1)
        .raft
        .client_write(Command::Initialize {
            cluster: cluster_id,
            group: GROUP,
        })
        .await
        .unwrap();
    for request in 0..20 {
        assert_eq!(
            data.node(1)
                .raft
                .client_write(Command::Append {
                    epoch: 0,
                    request,
                    value: request * 7
                })
                .await
                .unwrap()
                .data,
            Response::Appended { offset: request }
        );
    }
    let meta = metadata(cluster_id).await;
    let group = meta
        .node(1)
        .state
        .application()
        .await
        .group(GROUP)
        .unwrap()
        .clone();
    let plan = MembershipPlan::new(cluster_id, &group, BTreeSet::from([1, 2, 4])).unwrap();
    let mut registry = Registry::default();
    let metrics = Arc::new(ScalingMetrics::register(&mut registry));
    let coordinator = MembershipCoordinator::new(
        meta.node(1).raft.clone(),
        meta.node(1).state.clone(),
        data.node(1).raft.clone(),
        data.node(1).state.clone(),
        budget.clone(),
        metrics,
    );
    data.isolated.write().await.insert(4);
    let result = coordinator
        .execute(
            &plan,
            4096,
            &capacity(),
            Instant::now() + Duration::from_millis(250),
        )
        .await;
    assert!(matches!(result, Err(Error::Unknown)));
    assert!(
        budget.is_repair(4, GROUP).unwrap(),
        "Unknown retains the repair lease"
    );
    assert_eq!(
        meta.node(1)
            .state
            .application()
            .await
            .group(GROUP)
            .unwrap()
            .voters,
        BTreeSet::from([1, 2, 3])
    );
    // A healthy voter quorum still commits while the learner cannot make progress.
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(2),
            data.node(1).raft.client_write(Command::Append {
                epoch: 0,
                request: 100,
                value: 700
            })
        )
        .await
        .unwrap()
        .unwrap()
        .data,
        Response::Appended { offset: 20 }
    );
    data.isolated.write().await.remove(&4);
    coordinator
        .execute(&plan, 4096, &capacity(), Instant::now() + WAIT)
        .await
        .unwrap();
    assert!(!budget.is_repair(4, GROUP).unwrap());
    let current = meta.node(1).state.application().await;
    assert_eq!(
        current.group(GROUP).unwrap().generation,
        group.generation + 1
    );
    assert_eq!(
        current.group(GROUP).unwrap().voters,
        BTreeSet::from([1, 2, 4])
    );
    assert!(current.membership_change(GROUP).unwrap().is_complete());
    let position = data.node(1).raft.metrics().borrow().last_applied.unwrap();
    data.applied(&[1, 2, 4], position.index).await;
    assert_eq!(
        data.node(4).state.application().await,
        data.node(1).state.application().await
    );
    let proof = coordinator
        .verify(&plan, Instant::now() + WAIT)
        .await
        .unwrap();
    let original = meta
        .node(1)
        .raft
        .client_write(proof.into_command(current.revision()).unwrap())
        .await
        .unwrap()
        .data;
    assert!(
        original.is_applied(),
        "The same completed plan returns its original result"
    );
    current.validate_snapshot().unwrap();
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
    assert!(text.contains("tidemq_scaling_unknown_outcomes_total 1"));
    assert!(!text.contains("group_id="));
    drop(coordinator);
    meta.shutdown().await;
    data.shutdown().await;
}

#[tokio::test]
async fn repair_budget_is_finite_cancellable_and_classification_does_not_limit_control() {
    let budget = RepairBudget::new(RepairLimits {
        concurrent: 1,
        bytes_per_second: 1000,
        burst_bytes: 100,
        queued_bytes: 200,
    })
    .unwrap();
    let lease = budget
        .register(4, GROUP, Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(budget.is_repair(4, GROUP).unwrap());
    assert!(!budget.is_repair(2, GROUP).unwrap());
    assert!(!budget.is_repair(4, GROUP + 1).unwrap());
    assert!(matches!(
        budget
            .register(5, GROUP, Instant::now() + Duration::from_millis(10))
            .await,
        Err(Error::Deadline)
    ));
    assert!(matches!(
        budget
            .pace(101, Instant::now() + Duration::from_secs(1))
            .await,
        Err(Error::Overloaded)
    ));
    assert!(matches!(
        budget
            .pace(100, Instant::now() + Duration::from_millis(10))
            .await,
        Err(Error::Deadline)
    ));
    drop(lease);
    assert!(!budget.is_repair(4, GROUP).unwrap());
    budget
        .register(5, GROUP, Instant::now() + Duration::from_millis(100))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_joint_survives_leader_loss_and_finishes_only_the_original_plan() {
    use std::sync::atomic::Ordering;
    let cluster_id = Id::random().unwrap();
    let budget = RepairBudget::new(RepairLimits::default()).unwrap();
    let mut data = Cluster::<State>::with_budget(
        &[1, 2, 3, 4],
        cluster_id,
        GROUP,
        GroupRole::Data,
        Some(budget.clone()),
    )
    .await;
    data.initialize().await;
    data.node(1)
        .raft
        .client_write(Command::Initialize {
            cluster: cluster_id,
            group: GROUP,
        })
        .await
        .unwrap();
    let meta = metadata(cluster_id).await;
    let group = meta
        .node(1)
        .state
        .application()
        .await
        .group(GROUP)
        .unwrap()
        .clone();
    let plan = MembershipPlan::new(cluster_id, &group, BTreeSet::from([1, 2, 4])).unwrap();
    let mut registry = Registry::default();
    let metrics = Arc::new(ScalingMetrics::register(&mut registry));
    let coordinator = MembershipCoordinator::new(
        meta.node(1).raft.clone(),
        meta.node(1).state.clone(),
        data.node(1).raft.clone(),
        data.node(1).state.clone(),
        budget.clone(),
        metrics.clone(),
    );
    data.pause_final_membership.store(true, Ordering::Release);
    let result = coordinator
        .execute(
            &plan,
            4096,
            &capacity(),
            Instant::now() + Duration::from_millis(500),
        )
        .await;
    assert!(matches!(
        result,
        Err(Error::Unknown | Error::Consensus { .. })
    ));
    let joint = data
        .node(1)
        .state
        .try_with_membership(|_, _, membership| membership.clone())
        .await
        .unwrap();
    assert_eq!(
        joint.membership().get_joint_config().len(),
        2,
        "The first joint configuration committed"
    );
    assert!(
        coordinator
            .verify(&plan, Instant::now() + Duration::from_millis(100))
            .await
            .is_err(),
        "A joint membership never becomes a stable proof"
    );
    assert!(
        !meta
            .node(1)
            .state
            .application()
            .await
            .membership_change(GROUP)
            .unwrap()
            .is_complete()
    );
    data.node(1).raft.shutdown().await.unwrap();
    drop(coordinator);
    data.stop(1).await;
    data.pause_final_membership.store(false, Ordering::Release);
    let resume_deadline = Instant::now() + WAIT;
    let (previous_metadata, _) = coordinator_leaders(
        &meta,
        &data,
        &[2, 3, 4],
        resume_deadline,
        "after_data_leader_loss",
    )
    .await;
    // Keep the data leader visible while the coordinator's metadata authority is unavailable.
    meta.isolated.write().await.insert(previous_metadata);
    let barrier = meta
        .node(previous_metadata)
        .raft
        .ensure_linearizable()
        .await;
    assert!(matches!(
        barrier,
        Err(openraft::error::RaftError::APIError(
            openraft::error::CheckIsLeaderError::QuorumNotEnough(_)
        ))
    ));
    let eligible_metadata: Vec<_> = meta
        .nodes
        .keys()
        .copied()
        .filter(|node| *node != previous_metadata)
        .collect();
    let replacement_metadata = tokio::time::timeout_at(resume_deadline, async {
        loop {
            if let Some(leader) = fresh_metadata_leader(&meta, &eligible_metadata).await {
                break leader;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("A real metadata quorum elects a replacement while the former leader is isolated");
    assert_ne!(replacement_metadata, previous_metadata);
    // A genuine metadata leader election still cannot bypass a subsequent quorum loss.
    meta.isolated.write().await.insert(replacement_metadata);
    let (mut metadata_leader, leader) = {
        let ready = coordinator_leaders(
            &meta,
            &data,
            &[2, 3, 4],
            resume_deadline,
            "after_metadata_isolation",
        );
        tokio::pin!(ready);
        tokio::select! {
            ready = &mut ready => panic!("Leader metrics cannot prove metadata quorum readiness: {ready:?}"),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        {
            let mut isolated = meta.isolated.write().await;
            isolated.remove(&previous_metadata);
            isolated.remove(&replacement_metadata);
        }
        ready.await
    };
    let resumed = MembershipCoordinator::new(
        meta.node(metadata_leader).raft.clone(),
        meta.node(metadata_leader).state.clone(),
        data.node(leader).raft.clone(),
        data.node(leader).state.clone(),
        budget,
        metrics,
    );
    let result = resumed
        .execute(&plan, 4096, &capacity(), resume_deadline)
        .await;
    if result.is_err() {
        assert_eq!(
            leader, 3,
            "Only retirement of the current leader needs a new authority adapter: {result:?}"
        );
        drop(resumed);
        let resume_deadline = Instant::now() + WAIT;
        let (next_metadata, next) = coordinator_leaders(
            &meta,
            &data,
            &[2, 4],
            resume_deadline,
            "after_data_leader_retirement",
        )
        .await;
        metadata_leader = next_metadata;
        let resumed = MembershipCoordinator::new(
            meta.node(next_metadata).raft.clone(),
            meta.node(next_metadata).state.clone(),
            data.node(next).raft.clone(),
            data.node(next).state.clone(),
            RepairBudget::new(RepairLimits::default()).unwrap(),
            Arc::new(ScalingMetrics::register(&mut Registry::default())),
        );
        resumed
            .execute(&plan, 4096, &capacity(), resume_deadline)
            .await
            .unwrap();
    } else {
        drop(resumed);
    }
    let completed = meta
        .node(metadata_leader)
        .raft
        .metrics()
        .borrow()
        .last_applied
        .unwrap();
    meta.applied(&[1, 2, 3], completed.index).await;
    assert!(
        meta.node(1)
            .state
            .application()
            .await
            .membership_change(GROUP)
            .unwrap()
            .is_complete()
    );
    assert_eq!(
        data.node(leader)
            .state
            .try_with_membership(|_, _, m| m.membership().get_joint_config().clone())
            .await
            .unwrap(),
        vec![BTreeSet::from([1, 2, 4])]
    );
    meta.shutdown().await;
    data.shutdown().await;
}
