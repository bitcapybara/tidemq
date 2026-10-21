#[path = "replication_support/quic.rs"]
mod quic;
#[path = "replication_support/scaling.rs"]
mod scaling_support;
#[path = "replication_support/mod.rs"]
mod support;

use prometheus_client::registry::Registry;
use quic::{Cluster, GROUP, WAIT};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use support::{Command, State};
use tidemq::{
    metadata::MetadataState,
    replication::{Raft, rpc::GroupRole},
    scaling::{
        BlockReason, Capacity, Error, MembershipPlan, Placement, PlacementPolicy, RepairBudget,
        RepairLimits, Result, ScalingMetrics,
        remote::{self, CapacitySource, LocalReplicas, Query, Reply, Rpc, Service, ServiceConfig},
    },
    transport::NodeGrant,
};
use tidemq_protocol::{Id, common::TrafficClass};
use tokio::{
    sync::{Notify, RwLock},
    time::Instant,
};

const HEALTHY_GROUP: u64 = 3;

struct Sample(u64, Arc<AtomicBool>, Arc<AtomicBool>);
impl CapacitySource for Sample {
    fn snapshot(&self) -> Option<Capacity> {
        if self.0 == 4 && !self.1.load(Ordering::Acquire) {
            return None;
        }
        // Replay the exact free/reserved bytes captured from the constrained Linux CI runner.
        let constrained = self.0 == 4 && self.2.load(Ordering::Acquire);
        Some(Capacity {
            node_id: self.0,
            observed_at: std::time::Instant::now(),
            free_bytes: if constrained {
                1_352_884_224
            } else {
                100 * 1024 * 1024 * 1024
            },
            reserved_bytes: if constrained { 402_647_713 } else { 0 },
            bytes_per_second: 0,
            cpu_per_mille: if self.0 == 3 { 800 } else { 1 },
            connections: 0,
            ack_bytes: 0,
        })
    }
}

type TestService = Service<Client, Sample, State>;

#[derive(Default)]
enum GroupFault {
    #[default]
    None,
    Advanced,
    TargetMismatch,
    Retired,
}

#[derive(Default)]
struct Router {
    nodes: RwLock<BTreeMap<u64, Arc<TestService>>>,
    data: RwLock<BTreeMap<u64, Raft<State>>>,
    metadata: RwLock<BTreeMap<u64, Raft<MetadataState>>>,
    hold_view: AtomicBool,
    view_entered: Notify,
    resume_view: Notify,
    drop_proof_reply: AtomicBool,
    advance_after_proof: AtomicBool,
    group_fault: StdMutex<GroupFault>,
}
struct Client {
    router: Arc<Router>,
    cluster: Id,
    sender: u64,
}
impl Rpc for Client {
    async fn leader(&self, group: u64, deadline: Instant) -> Result<u64> {
        loop {
            if group == 77 {
                let nodes = self.router.metadata.read().await;
                for (id, raft) in nodes.iter() {
                    let attempt = (Instant::now() + Duration::from_millis(100)).min(deadline);
                    if matches!(
                        tokio::time::timeout_at(attempt, raft.ensure_linearizable()).await,
                        Ok(Ok(_))
                    ) {
                        return Ok(*id);
                    }
                }
            } else {
                let nodes = self.router.data.read().await;
                for (id, raft) in nodes.iter() {
                    let attempt = (Instant::now() + Duration::from_millis(100)).min(deadline);
                    if matches!(
                        tokio::time::timeout_at(attempt, raft.ensure_linearizable()).await,
                        Ok(Ok(_))
                    ) {
                        return Ok(*id);
                    }
                }
            }
            if Instant::now() >= deadline {
                return Err(Error::Authority);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    fn call(
        &self,
        node: u64,
        query: Query,
        deadline: Instant,
    ) -> impl std::future::Future<Output = Result<Reply>> + Send {
        let result: std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Reply>> + Send + '_>,
        > = Box::pin(async move {
            let service = self
                .router
                .nodes
                .read()
                .await
                .get(&node)
                .cloned()
                .ok_or(Error::Authority)?;
            let query = remote::decode_query(&remote::encode_query(&query)?)?;
            if matches!(query, Query::View { .. })
                && self.router.hold_view.swap(false, Ordering::AcqRel)
            {
                self.router.view_entered.notify_one();
                tokio::time::timeout_at(deadline, self.router.resume_view.notified())
                    .await
                    .map_err(|_| Error::Unknown)?;
            }
            let peer = NodeGrant {
                certificate_pin: [1; 32],
                certificate_name: format!("node-{}.local", self.sender),
                cluster_id: self.cluster,
                node_id: self.sender,
                role: if self.sender < 4 {
                    "metadata"
                } else {
                    "storage"
                }
                .into(),
                allowed_groups: vec![77, GROUP, HEALTHY_GROUP],
                allowed_classes: vec![TrafficClass::Control],
            };
            let mut response =
                tokio::time::timeout_at(deadline, service.handle(&peer, query, deadline))
                    .await
                    .map_err(|_| Error::Unknown)??;
            match &mut response {
                Reply::Group { group, migration } => {
                    let fault = std::mem::take(&mut *self.router.group_fault.lock().unwrap());
                    match fault {
                        GroupFault::None => {}
                        GroupFault::Advanced => group.generation += 1,
                        GroupFault::TargetMismatch => group.voters = [1, 2, 3].into(),
                        GroupFault::Retired => *migration = None,
                    }
                }
                Reply::Stable(_) => {
                    if self.router.drop_proof_reply.swap(false, Ordering::AcqRel) {
                        return Err(Error::Unknown);
                    }
                    if self
                        .router
                        .advance_after_proof
                        .swap(false, Ordering::AcqRel)
                    {
                        *self.router.group_fault.lock().unwrap() = GroupFault::Advanced;
                    }
                }
                _ => {}
            }
            remote::decode_reply(&remote::encode_reply(&response)?)
        });
        result
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_semantics_reconcile_native_quic_groups_with_distinct_leaders_and_measured_copy_bounds()
 {
    exercise(Scenario::Explicit).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_tick_waits_for_capacity_then_moves_one_native_replica_and_reconciles_original_plan()
 {
    exercise(Scenario::Automatic).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_plan_resumes_after_stable_proof_reply_is_lost_before_metadata_completion() {
    exercise(Scenario::LostPendingProof).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Explicit,
    Automatic,
    LostPendingProof,
}

async fn exercise(scenario: Scenario) {
    let automatic = scenario == Scenario::Automatic;
    let cluster = Id::random().unwrap();
    let data =
        Cluster::<State>::with_identity(&[1, 2, 3, 4], cluster, GROUP, GroupRole::Data).await;
    data.initialize().await;
    data.node(1)
        .raft
        .client_write(Command::Initialize {
            cluster,
            group: GROUP,
        })
        .await
        .unwrap();
    data.node(1)
        .raft
        .client_write(Command::Append {
            epoch: 0,
            request: 1,
            value: 7,
        })
        .await
        .unwrap();
    let healthy =
        Cluster::<State>::with_identity(&[1, 2, 3], cluster, HEALTHY_GROUP, GroupRole::Data).await;
    healthy.initialize().await;
    healthy
        .node(1)
        .raft
        .client_write(Command::Initialize {
            cluster,
            group: HEALTHY_GROUP,
        })
        .await
        .unwrap();
    let meta = scaling_support::metadata(cluster).await;
    if scenario != Scenario::LostPendingProof {
        // Only the data group loses node 1; metadata still has its real leader on node 1.
        data.isolated.write().await.insert(1);
    }
    let router = Arc::new(Router::default());
    let available = Arc::new(AtomicBool::new(!automatic));
    let constrained = Arc::new(AtomicBool::new(true));
    for id in 1..=4 {
        router
            .data
            .write()
            .await
            .insert(id, data.node(id).raft.clone());
        if id < 4 {
            router
                .metadata
                .write()
                .await
                .insert(id, meta.node(id).raft.clone());
        }
        let mut local = vec![(
            data.node(id).raft.clone(),
            data.node(id).log.clone(),
            data.node(id).state.clone(),
        )];
        if id < 4 {
            local.push((
                healthy.node(id).raft.clone(),
                healthy.node(id).log.clone(),
                healthy.node(id).state.clone(),
            ));
        }
        let replicas = LocalReplicas::new(
            if id < 4 {
                Some((meta.node(id).raft.clone(), meta.node(id).state.clone()))
            } else {
                None
            },
            local,
        )
        .unwrap();
        let client = Arc::new(Client {
            router: router.clone(),
            cluster,
            sender: id,
        });
        let config = ServiceConfig {
            cluster_id: cluster,
            node_id: id,
            metadata_group: 77,
            mode: tidemq::metadata::DeploymentMode::ThreeReplica,
            metadata_voters: [1, 2, 3].into(),
        };
        let service = Service::new(
            config,
            replicas,
            client,
            Arc::new(Sample(id, available.clone(), constrained.clone())),
            RepairBudget::new(RepairLimits::default()).unwrap(),
            Arc::new(ScalingMetrics::register(&mut Registry::default())),
        )
        .unwrap();
        router.nodes.write().await.insert(id, Arc::new(service));
    }
    let client = Arc::new(Client {
        router: router.clone(),
        cluster,
        sender: 1,
    });
    let leader = client.leader(GROUP, Instant::now() + WAIT).await.unwrap();
    if scenario == Scenario::LostPendingProof {
        assert_eq!(leader, 1, "The proof-loss scenario keeps its data leader");
    } else {
        assert_ne!(leader, 1);
    }
    let group = meta
        .node(1)
        .state
        .application()
        .await
        .group(GROUP)
        .unwrap()
        .clone();
    let plan = MembershipPlan::new(cluster, &group, [1, 2, 4].into()).unwrap();
    let incompatible = tidemq::metadata::DataGroup {
        group_id: GROUP,
        generation: 2,
        voters: [1, 2, 4].into(),
    };
    let incompatible = MembershipPlan::new(cluster, &incompatible, [1, 2, 5].into()).unwrap();
    assert!(matches!(
        client
            .call(
                leader,
                Query::Estimate { plan: incompatible },
                Instant::now() + WAIT
            )
            .await,
        Err(Error::Authority)
    ));
    let driver = remote::Coordinator::new(client.clone(), 77).unwrap();
    assert_eq!(
        driver
            .plan(cluster, GROUP, 3, 4, Instant::now() + WAIT)
            .await
            .unwrap(),
        plan
    );
    if automatic {
        let service = router.nodes.read().await.get(&1).unwrap().clone();
        let mut placement = Placement::new(PlacementPolicy::default()).unwrap();
        let groups = BTreeMap::from([(GROUP, [4].into())]);
        let blocked = service
            .tick(&mut placement, &groups, Instant::now() + WAIT)
            .await
            .unwrap();
        assert_eq!(blocked.blocked, Some(BlockReason::MissingObservation));
        assert!(
            meta.node(1)
                .state
                .application()
                .await
                .membership_change(GROUP)
                .is_none()
        );
        available.store(true, Ordering::Release);
        let constrained_tick = service
            .tick(&mut placement, &groups, Instant::now() + WAIT)
            .await;
        assert!(matches!(
            constrained_tick,
            Err(Error::Capacity {
                reason: BlockReason::FreeCapacity
            })
        ));
        assert_eq!(
            placement.diagnose().blocked,
            Some(BlockReason::FreeCapacity)
        );
        assert!(
            meta.node(1)
                .state
                .application()
                .await
                .membership_change(GROUP)
                .is_none()
        );
        constrained.store(false, Ordering::Release);
        let mut completed = None;
        for _ in 0..3 {
            if let Ok(result) = service
                .tick(&mut placement, &groups, Instant::now() + WAIT)
                .await
            {
                completed = result.completed;
                if completed.is_some() {
                    break;
                }
            }
        }
        assert_eq!(completed, Some(plan.clone()));
        let repeated = service
            .tick(&mut placement, &groups, Instant::now() + WAIT)
            .await
            .unwrap();
        assert!(repeated.completed.is_none());
        assert_eq!(repeated.blocked, Some(BlockReason::NoStandby));
        drop(service);
    } else {
        let constrained_attempt = driver.execute(&plan, Instant::now() + WAIT).await;
        assert!(matches!(
            constrained_attempt,
            Err(Error::Capacity {
                reason: BlockReason::FreeCapacity
            })
        ));
        assert!(
            meta.node(1)
                .state
                .application()
                .await
                .membership_change(GROUP)
                .is_none()
        );
        assert_eq!(
            data.node(leader)
                .state
                .try_with_membership(|_, _, membership| {
                    membership.membership().get_joint_config().clone()
                })
                .await
                .unwrap(),
            vec![[1, 2, 3].into()],
            "Insufficient disk headroom must not begin a native membership change"
        );
        constrained.store(false, Ordering::Release);
        assert_eq!(
            driver
                .plan(cluster, GROUP, 3, 4, Instant::now() + WAIT)
                .await
                .unwrap(),
            plan,
            "Restoring capacity must retain the original canonical plan"
        );
        if scenario == Scenario::LostPendingProof {
            let deadline = Instant::now() + WAIT;
            router.drop_proof_reply.store(true, Ordering::Release);
            assert!(matches!(
                driver.execute(&plan, deadline).await,
                Err(Error::Unknown)
            ));
            assert!(!router.drop_proof_reply.load(Ordering::Acquire));
            let pending = meta.node(1).state.application().await;
            let record = pending.membership_change(GROUP).unwrap();
            assert_eq!(record.plan, plan);
            assert!(!record.is_complete());
            assert_eq!(pending.group(GROUP).unwrap().voters, *plan.previous());
            let leader = client.leader(GROUP, deadline).await.unwrap();
            assert_eq!(
                data.node(leader)
                    .state
                    .try_with_membership(|_, applied, membership| {
                        let applied = applied.expect("The stable native change must be applied");
                        let membership_log = membership
                            .log_id()
                            .expect("The stable membership must have a real log position");
                        assert!(applied.index >= membership_log.index);
                        membership.membership().get_joint_config().clone()
                    })
                    .await
                    .unwrap(),
                vec![plan.target().clone()],
                "The native change is stable while metadata still retains the pending plan"
            );
            let recovered = driver.plan(cluster, GROUP, 3, 4, deadline).await.unwrap();
            assert_eq!(recovered, plan);
            driver.execute(&recovered, deadline).await.unwrap();
        } else {
            let first = driver.execute(&plan, Instant::now() + WAIT).await;
            if first.is_err() {
                // Retiring the joint leader requires discovering its successor before final proof.
                driver.execute(&plan, Instant::now() + WAIT).await.unwrap();
            }
        }
    }
    assert_eq!(
        driver
            .plan(cluster, GROUP, 3, 4, Instant::now() + WAIT)
            .await
            .unwrap(),
        plan,
        "A lost completion response must return the latest frozen original plan"
    );
    let applied = meta.node(1).state.application().await;
    let record = applied.membership_change(GROUP).unwrap();
    assert!(record.is_complete());
    assert!(
        record.copy_bytes > 1024 * 1024,
        "The source reserves actual snapshot and bounded journal/tail growth"
    );
    assert_eq!(applied.group(GROUP).unwrap().voters, [1, 2, 4].into());
    let new_leader = client.leader(GROUP, Instant::now() + WAIT).await.unwrap();
    assert_eq!(
        data.node(new_leader)
            .state
            .application()
            .await
            .values
            .get(&0),
        Some(&7)
    );
    let original_group = meta
        .node(1)
        .state
        .application()
        .await
        .group(GROUP)
        .unwrap()
        .clone();
    let original_revision = meta.node(1).state.application().await.revision();
    let membership_log = data
        .node(new_leader)
        .state
        .try_with_membership(|_, _, membership| *membership.log_id())
        .await
        .unwrap();
    for fault in [
        GroupFault::Advanced,
        GroupFault::TargetMismatch,
        GroupFault::Retired,
    ] {
        *router.group_fault.lock().unwrap() = fault;
        assert!(
            driver.execute(&plan, Instant::now() + WAIT).await.is_err(),
            "A mismatched or retired completed record cannot authorize replay"
        );
    }
    router.advance_after_proof.store(true, Ordering::Release);
    assert!(
        driver.execute(&plan, Instant::now() + WAIT).await.is_err(),
        "Metadata must still confirm the original completed generation after the data proof"
    );
    router.drop_proof_reply.store(true, Ordering::Release);
    assert!(
        matches!(
            driver.execute(&plan, Instant::now() + WAIT).await,
            Err(Error::Unknown)
        ),
        "Losing the actual stable proof must remain unknown"
    );
    let recovered = driver
        .plan(cluster, GROUP, 3, 4, Instant::now() + WAIT)
        .await
        .unwrap();
    assert_eq!(
        recovered, plan,
        "A lost completed proof must retain the exact original canonical identity"
    );
    driver
        .execute(&recovered, Instant::now() + WAIT)
        .await
        .unwrap();
    router.hold_view.store(true, Ordering::Release);
    let in_progress = client.call(
        new_leader,
        Query::Change { plan: plan.clone() },
        Instant::now() + WAIT,
    );
    let repeated = async {
        tokio::time::timeout(WAIT, router.view_entered.notified())
            .await
            .unwrap();
        let result = driver.execute(&plan, Instant::now() + WAIT).await;
        router.resume_view.notify_one();
        result
    };
    let (in_progress, repeated) = tokio::join!(in_progress, repeated);
    assert!(matches!(in_progress.unwrap(), Reply::Done));
    repeated.expect("A completed plan replay verifies stable membership while another executor holds its mutation slot");
    assert_eq!(
        meta.node(1).state.application().await.revision(),
        original_revision
    );
    assert_eq!(
        meta.node(1).state.application().await.group(GROUP),
        Some(&original_group)
    );
    assert_eq!(
        data.node(new_leader)
            .state
            .try_with_membership(|_, _, membership| *membership.log_id())
            .await
            .unwrap(),
        membership_log,
        "Completed verification must not submit another membership change"
    );
    data.node(1).raft.shutdown().await.unwrap();
    assert!(matches!(
        client
            .call(
                1,
                Query::Capacity {
                    group: GROUP,
                    node: 1
                },
                Instant::now() + WAIT
            )
            .await
            .unwrap(),
        Reply::ReplicaFailed {
            group: GROUP,
            node: 1,
            failure: remote::ReplicaFailure::RaftStopped
        }
    ));
    healthy.node(1).raft.ensure_linearizable().await.unwrap();
    assert!(
        matches!(
            client
                .call(
                    1,
                    Query::Capacity {
                        group: HEALTHY_GROUP,
                        node: 1,
                    },
                    Instant::now() + WAIT,
                )
                .await
                .unwrap(),
            Reply::Capacity(_)
        ),
        "A stopped Raft must not invalidate a different healthy data group on the same node"
    );
    assert!(
        matches!(
            client
                .call(1, Query::Group { group: GROUP }, Instant::now() + WAIT)
                .await
                .unwrap(),
            Reply::Group { .. }
        ),
        "A stopped data Raft must not invalidate the same node's healthy metadata group"
    );
    let forbidden = NodeGrant {
        certificate_pin: [1; 32],
        certificate_name: "gateway.local".into(),
        cluster_id: cluster,
        node_id: 9,
        role: "gateway".into(),
        allowed_groups: vec![77, GROUP],
        allowed_classes: vec![TrafficClass::Control],
    };
    let service = router.nodes.read().await.get(&1).unwrap().clone();
    assert!(matches!(
        service
            .handle(
                &forbidden,
                Query::Prepare {
                    plan: plan.clone(),
                    copy_bytes: 0
                },
                Instant::now() + WAIT
            )
            .await,
        Err(Error::Authority)
    ));
    drop(service);
    drop(driver);
    drop(client);
    router.nodes.write().await.clear();
    router.data.write().await.clear();
    router.metadata.write().await.clear();
    meta.shutdown().await;
    data.shutdown().await;
    healthy.shutdown().await;
}
