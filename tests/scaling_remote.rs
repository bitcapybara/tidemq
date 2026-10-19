#[path = "replication_support/quic.rs"]
mod quic;
#[path = "replication_support/scaling.rs"]
mod scaling_support;
#[path = "replication_support/mod.rs"]
mod support;

use prometheus_client::registry::Registry;
use quic::{Cluster, GROUP, WAIT};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use support::{Command, State};
use tidemq::{
    metadata::MetadataState,
    replication::{Raft, rpc::GroupRole},
    scaling::{
        Capacity, Error, MembershipPlan, RepairBudget, RepairLimits, Result, ScalingMetrics,
        remote::{self, CapacitySource, LocalReplicas, Query, Reply, Rpc, Service, ServiceConfig},
    },
    transport::NodeGrant,
};
use tidemq_protocol::{Id, common::TrafficClass};
use tokio::{sync::RwLock, time::Instant};

struct Sample(u64);
impl CapacitySource for Sample {
    fn snapshot(&self) -> Option<Capacity> {
        Some(Capacity {
            node_id: self.0,
            observed_at: std::time::Instant::now(),
            free_bytes: 100 * 1024 * 1024 * 1024,
            reserved_bytes: 0,
            bytes_per_second: 0,
            cpu_per_mille: 1,
            connections: 0,
            ack_bytes: 0,
        })
    }
}

type TestService = Service<Client, Sample, State>;

#[derive(Default)]
struct Router {
    nodes: RwLock<BTreeMap<u64, Arc<TestService>>>,
    data: RwLock<BTreeMap<u64, Raft<State>>>,
    metadata: RwLock<BTreeMap<u64, Raft<MetadataState>>>,
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
                allowed_groups: vec![77, GROUP],
                allowed_classes: vec![TrafficClass::Control],
            };
            let response =
                tokio::time::timeout_at(deadline, service.handle(&peer, query, deadline))
                    .await
                    .map_err(|_| Error::Unknown)??;
            remote::decode_reply(&remote::encode_reply(&response)?)
        });
        result
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_semantics_reconcile_native_quic_groups_with_distinct_leaders_and_measured_copy_bounds()
 {
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
    let meta = scaling_support::metadata(cluster).await;
    // Only the data group loses node 1; metadata still has its real leader on node 1.
    data.isolated.write().await.insert(1);
    let router = Arc::new(Router::default());
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
        let replicas = LocalReplicas::new(
            if id < 4 {
                Some((meta.node(id).raft.clone(), meta.node(id).state.clone()))
            } else {
                None
            },
            vec![(
                data.node(id).raft.clone(),
                data.node(id).log.clone(),
                data.node(id).state.clone(),
            )],
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
            Arc::new(Sample(id)),
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
    assert_ne!(leader, 1);
    let group = meta
        .node(1)
        .state
        .application()
        .await
        .group(GROUP)
        .unwrap()
        .clone();
    let plan = MembershipPlan::new(cluster, &group, [1, 2, 4].into()).unwrap();
    let driver = remote::Coordinator::new(client.clone(), 77).unwrap();
    assert_eq!(
        driver
            .plan(cluster, GROUP, 3, 4, Instant::now() + WAIT)
            .await
            .unwrap(),
        plan
    );
    let first = driver.execute(&plan, Instant::now() + WAIT).await;
    if first.is_err() {
        // Retiring the joint leader requires discovering its successor before final proof.
        driver.execute(&plan, Instant::now() + WAIT).await.unwrap();
    }
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
}
