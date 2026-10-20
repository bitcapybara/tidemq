use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{Backoff, RPCOption};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory, SnapshotPolicy};
use tidemq_protocol::Id;
use tokio::sync::RwLock;

use crate::budget::MemoryBudget;
use crate::config::MemoryConfig;
use crate::metadata::PartitionKey;
use crate::ownership::{ControlLease, ControlPolicy, FenceCommand, Grant};
use crate::producer::{AppendBatch, MessageData, ProducerLimits, SessionInfo};
use crate::replication::{
    DiskLog, DiskStateMachine, DiskStore, Raft, StorageIdentity, StorageOptions, TypeConfig,
};

use super::super::{
    Broker, BrokerLimits, DataApplication, DataCommand, DataGroupConfig, DataResponse,
    ProducerMetrics, RequestContext, WriterFence,
};

type Config = TypeConfig<DataApplication>;

#[derive(Clone, Default)]
pub(super) struct Router {
    nodes: Arc<RwLock<BTreeMap<u64, Raft<DataApplication>>>>,
    pub isolated: Arc<RwLock<BTreeSet<u64>>>,
}

#[derive(Clone)]
struct Factory {
    source: u64,
    router: Router,
}
struct Network {
    source: u64,
    target: u64,
    router: Router,
}

impl RaftNetworkFactory<Config> for Factory {
    type Network = Network;
    async fn new_client(&mut self, target: u64, _: &BasicNode) -> Network {
        Network {
            source: self.source,
            target,
            router: self.router.clone(),
        }
    }
}

impl Network {
    async fn target(&self) -> Result<Raft<DataApplication>, Unreachable> {
        let isolated = self.router.isolated.read().await;
        if isolated.contains(&self.source) || isolated.contains(&self.target) {
            return Err(Unreachable::new(&std::io::Error::other(
                "Injected data replica isolation",
            )));
        }
        self.router
            .nodes
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| Unreachable::new(&std::io::Error::other("Data replica is stopped")))
    }
}

impl RaftNetwork<Config> for Network {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<Config>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.target()
            .await?
            .append_entries(request)
            .await
            .map_err(|error| RemoteError::new(self.target, error).into())
    }
    async fn vote(
        &mut self,
        request: VoteRequest<u64>,
        _: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.target()
            .await?
            .vote(request)
            .await
            .map_err(|error| RemoteError::new(self.target, error).into())
    }
    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<Config>,
        _: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        self.target()
            .await?
            .install_snapshot(request)
            .await
            .map_err(|error| RemoteError::new(self.target, error).into())
    }
    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(Duration::from_millis(10)))
    }
}

pub(super) struct Node {
    pub raft: Raft<DataApplication>,
    pub log: DiskLog<DataApplication>,
    pub state: DiskStateMachine<DataApplication>,
}

pub(super) struct Cluster {
    directory: PathBuf,
    pub config: DataGroupConfig,
    pub router: Router,
    nodes: BTreeMap<u64, Node>,
    raft_config: Arc<openraft::Config>,
}

impl Cluster {
    pub fn directory(&self) -> &std::path::Path {
        &self.directory
    }

    pub async fn new(ids: &[u64]) -> Self {
        let cluster_id = Id::random().unwrap();
        let directory =
            std::env::temp_dir().join(format!("tidemq-producer-{}", Id::random().unwrap()));
        tokio::fs::create_dir(&directory).await.unwrap();
        let mut cluster = Self {
            directory,
            config: DataGroupConfig {
                cluster_id,
                group_id: 41,
                max_partitions: 8,
                max_hot_bytes: 32 * 1024 * 1024,
                max_hot_messages: 65_536,
                producer: ProducerLimits::default(),
                subscription: crate::subscription::Limits::default(),
                retention: crate::retention::Policy::default(),
            },
            router: Router::default(),
            nodes: BTreeMap::new(),
            raft_config: Arc::new(
                openraft::Config {
                    cluster_name: "tidemq-producer-durable-test".to_owned(),
                    heartbeat_interval: 20,
                    election_timeout_min: 100,
                    election_timeout_max: 200,
                    enable_elect: false,
                    snapshot_policy: SnapshotPolicy::Never,
                    snapshot_max_chunk_size: 1024,
                    max_in_snapshot_log_to_keep: 0,
                    ..openraft::Config::default()
                }
                .validate()
                .unwrap(),
            ),
        };
        for id in ids {
            cluster.start(*id).await;
        }
        let members: BTreeMap<_, _> = ids
            .iter()
            .map(|id| (*id, BasicNode::new(format!("harness://{id}"))))
            .collect();
        cluster.node(1).raft.initialize(members).await.unwrap();
        cluster.wait_leader().await;
        assert_eq!(
            cluster
                .write(DataCommand::Initialize(cluster.config.clone()))
                .await,
            DataResponse::Initialized
        );
        cluster
    }

    pub fn node(&self, id: u64) -> &Node {
        self.nodes.get(&id).expect("test data node exists")
    }

    async fn start(&mut self, id: u64) {
        let storage = DiskStore::<DataApplication>::open(
            self.directory.join(format!("node-{id}")),
            StorageIdentity::new::<DataApplication>(
                self.config.cluster_id,
                self.config.group_id,
                id,
            ),
            StorageOptions::default(),
            None,
        )
        .await
        .unwrap();
        let raft = Raft::new(
            id,
            self.raft_config.clone(),
            Factory {
                source: id,
                router: self.router.clone(),
            },
            storage.log.clone(),
            storage.state_machine.clone(),
        )
        .await
        .unwrap();
        self.router.nodes.write().await.insert(id, raft.clone());
        self.nodes.insert(
            id,
            Node {
                raft,
                log: storage.log,
                state: storage.state_machine,
            },
        );
    }

    pub async fn wait_leader(&self) {
        self.node(1)
            .raft
            .wait(Some(Duration::from_secs(5)))
            .current_leader(1, "Data leader is ready")
            .await
            .unwrap();
    }

    pub async fn write(&self, command: DataCommand) -> DataResponse {
        self.node(1).raft.client_write(command).await.unwrap().data
    }

    /// This fixture supplies a trusted grant; metadata authority itself is tested separately.
    pub fn grant(&self, partition: PartitionKey, epoch: u64) -> Grant {
        Grant {
            cluster_id: self.config.cluster_id,
            partition,
            group_id: self.config.group_id,
            operation_id: crate::metadata::grant_operation_id(
                self.config.cluster_id,
                partition,
                epoch,
                1,
                epoch - 1,
            )
            .unwrap(),
            epoch,
            owner_node: 1,
            expected_route_generation: epoch - 1,
            metadata_revision: epoch,
        }
    }

    pub async fn fence_and_recover(&self, grant: &Grant) {
        assert!(matches!(
            self.write(DataCommand::InstallFence(FenceCommand {
                grant: grant.clone()
            }))
            .await,
            DataResponse::Fenced(_)
        ));
        assert!(matches!(
            self.write(DataCommand::Recover {
                partition: grant.partition,
                writer: writer(grant)
            })
            .await,
            DataResponse::Recovered(_)
        ));
    }

    pub fn broker(&self, limits: BrokerLimits) -> Broker {
        let node = self.node(1);
        Broker::new(
            1,
            node.raft.clone(),
            node.state.clone(),
            self.config.clone(),
            MemoryBudget::new(&MemoryConfig::default()).unwrap(),
            limits,
            ProducerMetrics::default(),
        )
        .unwrap()
    }

    pub async fn restart(&mut self, id: u64) {
        self.stop(id).await;
        self.start(id).await;
        self.node(id).raft.trigger().elect().await.unwrap();
        self.wait_leader().await;
    }

    async fn stop(&mut self, id: u64) {
        self.router.nodes.write().await.remove(&id);
        let node = self.nodes.remove(&id).unwrap();
        node.raft.shutdown().await.unwrap();
        node.log.close().await.unwrap();
    }

    pub async fn shutdown(mut self) {
        let ids: Vec<_> = self.nodes.keys().copied().collect();
        for id in ids {
            self.stop(id).await;
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

pub(super) fn writer(grant: &Grant) -> WriterFence {
    WriterFence {
        node_id: grant.owner_node,
        owner_epoch: grant.epoch,
    }
}

pub(super) fn lease(grant: &Grant) -> ControlLease {
    let policy = ControlPolicy {
        ttl: Duration::from_secs(10),
        ..ControlPolicy::default()
    };
    crate::ownership::test_lease(grant.clone(), policy).unwrap()
}

pub(super) fn context() -> RequestContext {
    RequestContext {
        operation_id: Id::random().unwrap(),
        request_id: Id::random().unwrap(),
        attempt: 1,
        deadline: Instant::now() + Duration::from_secs(5),
    }
}

pub(super) fn batch(
    partition: PartitionKey,
    session: &SessionInfo,
    sequence: u64,
    body: &'static [u8],
) -> AppendBatch {
    let messages = vec![MessageData {
        key: Bytes::from_static(b"private-key"),
        body: Bytes::from_static(body),
    }];
    let digest = tidemq_protocol::producer::canonical_produce_digest_parts(
        partition.topic_id,
        partition.partition,
        session.producer_id,
        session.producer_epoch,
        sequence,
        messages
            .iter()
            .map(|message| (message.key.as_ref(), message.body.as_ref())),
    )
    .unwrap();
    AppendBatch {
        topic_id: partition.topic_id,
        partition_id: partition.partition,
        producer_id: session.producer_id,
        producer_epoch: session.producer_epoch,
        first_sequence: sequence,
        messages,
        content_digest: digest,
    }
}
