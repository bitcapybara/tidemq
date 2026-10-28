#![allow(dead_code)] // Individual lifecycle targets exercise different fixture actions.

pub mod rustfs;
#[path = "../s3_support/mod.rs"]
pub mod s3;

use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use openraft::{
    BasicNode, RaftNetwork, RaftNetworkFactory,
    error::{InstallSnapshotError, RPCError, RaftError, Unreachable},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::RaftSnapshotBuilder,
};
use tidemq::{
    broker::{
        DataApplication, DataCommand, DataGroupConfig, DataResponse, ProducerCommand, WriterFence,
    },
    lifecycle::{Coordinator, DataReplica, MetadataReplica},
    metadata::{
        self, Command, DeploymentMode, MetadataLimits, MetadataState, NodeRole, NodeSpec,
        PartitionKey,
    },
    object_store::S3Store,
    ownership::{ControlPolicy, DataAuthority, MetadataAuthority},
    producer::{AppendBatch, MessageData, ProducerLimits, RegisterProducer},
    replication::{
        Application, DiskLog, DiskStateMachine, DiskStore, Raft, StorageIdentity, StorageOptions,
        TypeConfig,
    },
};
use tidemq_protocol::Id;

struct Local<A>(PhantomData<A>);
impl<A: Application> RaftNetworkFactory<TypeConfig<A>> for Local<A> {
    type Network = Self;
    async fn new_client(&mut self, _: u64, _: &BasicNode) -> Self {
        Self(PhantomData)
    }
}
fn unavailable<E: std::error::Error>() -> RPCError<u64, BasicNode, E> {
    Unreachable::new(&std::io::Error::other("RF1 fixture has no remote voter")).into()
}
impl<A: Application> RaftNetwork<TypeConfig<A>> for Local<A> {
    async fn append_entries(
        &mut self,
        _: AppendEntriesRequest<TypeConfig<A>>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(unavailable())
    }
    async fn vote(
        &mut self,
        _: VoteRequest<u64>,
        _: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(unavailable())
    }
    async fn install_snapshot(
        &mut self,
        _: InstallSnapshotRequest<TypeConfig<A>>,
        _: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        Err(unavailable())
    }
}

pub struct Node<A: Application> {
    pub raft: Raft<A>,
    pub state: DiskStateMachine<A>,
    pub log: DiskLog<A>,
}
impl<A: Application> Node<A> {
    pub async fn open(path: &Path, cluster: Id, group: u64, initialize: bool) -> Self {
        let store = DiskStore::<A>::open(
            path,
            StorageIdentity::new::<A>(cluster, group, 1),
            StorageOptions::default(),
            None,
        )
        .await
        .unwrap();
        let config = Arc::new(
            openraft::Config {
                cluster_name: format!("lifecycle-{group}"),
                enable_elect: false,
                heartbeat_interval: 20,
                election_timeout_min: 100,
                election_timeout_max: 200,
                ..openraft::Config::default()
            }
            .validate()
            .unwrap(),
        );
        let raft = Raft::new(
            1,
            config,
            Local(PhantomData),
            store.log.clone(),
            store.state_machine.clone(),
        )
        .await
        .unwrap();
        if initialize {
            raft.initialize(BTreeMap::from([(1, BasicNode::new("fixture://local"))]))
                .await
                .unwrap();
        } else {
            raft.trigger().elect().await.unwrap();
        }
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "RF1 lifecycle leader ready")
            .await
            .unwrap();
        Self {
            raft,
            state: store.state_machine,
            log: store.log,
        }
    }
    pub async fn snapshot_and_close(self) {
        let mut state = self.state.clone();
        state.build_snapshot().await.unwrap();
        self.raft.shutdown().await.unwrap();
        self.log.close().await.unwrap();
    }
}

pub struct Fixture {
    pub directory: PathBuf,
    pub cluster: Id,
    pub partition: PartitionKey,
    pub metadata: Node<MetadataState>,
    pub data: Node<DataApplication>,
}
impl Fixture {
    pub async fn new() -> Self {
        Self::with_partitions(1).await
    }
    pub async fn with_partitions(partitions: usize) -> Self {
        let directory =
            std::env::temp_dir().join(format!("tidemq-lifecycle-{}", Id::new_v7().unwrap()));
        tokio::fs::create_dir(&directory).await.unwrap();
        let cluster = Id::new_v7().unwrap();
        let partition = PartitionKey {
            topic_id: Id::new_v7().unwrap(),
            partition: 0,
        };
        let metadata = Node::open(&directory.join("meta"), cluster, 77, true).await;
        let data = Node::open(&directory.join("data"), cluster, 9, true).await;
        let fixture = Self {
            directory,
            cluster,
            partition,
            metadata,
            data,
        };
        fixture.bootstrap(partitions).await;
        fixture
    }
    async fn meta_write(&self, command: Command) -> metadata::Response {
        self.metadata.raft.client_write(command).await.unwrap().data
    }
    pub async fn revision(&self) -> u64 {
        self.metadata
            .state
            .with_application(MetadataState::revision)
            .await
    }
    async fn bootstrap(&self, partitions: usize) {
        assert!(
            self.meta_write(Command::bootstrap(
                Id::new_v7().unwrap(),
                self.cluster,
                77,
                DeploymentMode::SeparateRf1,
                MetadataLimits::default()
            ))
            .await
            .is_applied()
        );
        assert!(
            self.meta_write(Command::put_node(
                Id::new_v7().unwrap(),
                self.revision().await,
                NodeSpec {
                    node_id: 1,
                    zone: "local".into(),
                    endpoint: "127.0.0.1:19009".parse().unwrap(),
                    roles: BTreeSet::from([
                        NodeRole::Metadata,
                        NodeRole::Storage,
                        NodeRole::Broker
                    ]),
                    schedulable: true
                },
                0
            ))
            .await
            .is_applied()
        );
        assert!(
            self.meta_write(Command::create_group(
                Id::new_v7().unwrap(),
                self.revision().await,
                9,
                tidemq::metadata::GroupPlacement::Raft {
                    voters: BTreeSet::from([1])
                }
            ))
            .await
            .is_applied()
        );
        assert!(
            self.meta_write(Command::create_topic(
                Id::new_v7().unwrap(),
                self.revision().await,
                self.partition.topic_id,
                "events".into(),
                vec![9; partitions]
            ))
            .await
            .is_applied()
        );
        let config = DataGroupConfig {
            cluster_id: self.cluster,
            group_id: 9,
            max_partitions: 8,
            max_hot_bytes: 32 * 1024 * 1024,
            max_hot_messages: 65_536,
            producer: ProducerLimits::default(),
            subscription: tidemq::subscription::Limits::default(),
            retention: tidemq::retention::Policy::default(),
        };
        assert!(matches!(
            self.data
                .raft
                .client_write(DataCommand::Initialize(config))
                .await
                .unwrap()
                .data,
            DataResponse::Initialized
        ));
        for partition in 0..partitions {
            self.activate(PartitionKey {
                topic_id: self.partition.topic_id,
                partition: u32::try_from(partition).unwrap(),
            })
            .await;
        }
    }
    async fn activate(&self, partition: PartitionKey) {
        let grant = match self
            .meta_write(
                Command::grant_ownership(self.revision().await, self.cluster, partition, 1, 0, 0)
                    .unwrap(),
            )
            .await
        {
            metadata::Response::Applied {
                outcome: metadata::Outcome::OwnershipGranted(grant),
                ..
            } => grant,
            value => panic!("Unexpected grant response: {value:?}"),
        };
        let meta = MetadataAuthority::new(
            tidemq::execution::GroupHandle::from_raft(
                self.metadata.raft.clone(),
                self.metadata.state.clone(),
                self.metadata.log.clone(),
            ),
            self.cluster,
            77,
            ControlPolicy::default(),
        )
        .unwrap();
        let authority = DataAuthority::new(
            tidemq::execution::GroupHandle::from_raft(
                self.data.raft.clone(),
                self.data.state.clone(),
                self.data.log.clone(),
            ),
            self.cluster,
            9,
            tidemq::metadata::GroupPlacement::Raft {
                voters: BTreeSet::from([1]),
            },
            ControlPolicy::default(),
        )
        .unwrap();
        let fence = meta
            .verify_grant(&grant)
            .await
            .unwrap()
            .into_fence_command()
            .unwrap();
        assert!(matches!(
            self.data
                .raft
                .client_write(DataCommand::InstallFence(fence))
                .await
                .unwrap()
                .data,
            DataResponse::Fenced(_)
        ));
        assert!(
            self.meta_write(
                authority
                    .verify_fence(&grant)
                    .await
                    .unwrap()
                    .into_observe_command(self.revision().await)
                    .unwrap()
            )
            .await
            .is_applied()
        );
        assert!(matches!(
            self.data
                .raft
                .client_write(DataCommand::Recover {
                    partition,
                    writer: WriterFence {
                        node_id: 1,
                        owner_epoch: 1
                    }
                })
                .await
                .unwrap()
                .data,
            DataResponse::Recovered(_)
        ));
        assert!(
            self.meta_write(
                authority
                    .verify_recovery(&grant)
                    .await
                    .unwrap()
                    .into_observe_command(self.revision().await)
                    .unwrap()
            )
            .await
            .is_applied()
        );
        assert!(
            self.meta_write(
                authority
                    .verify_recovery(&grant)
                    .await
                    .unwrap()
                    .into_activate_command(self.revision().await)
                    .unwrap()
            )
            .await
            .is_applied()
        );
    }
    pub fn coordinator(&self, store: S3Store) -> Coordinator {
        self.coordinator_with_budget(store, s3::budget())
    }
    pub fn coordinator_with_budget(
        &self,
        store: S3Store,
        budget: tidemq::budget::MemoryBudget,
    ) -> Coordinator {
        let data = DataReplica::local(tidemq::execution::GroupHandle::from_raft(
            self.data.raft.clone(),
            self.data.state.clone(),
            self.data.log.clone(),
        ));
        Coordinator::new(
            MetadataReplica::local(tidemq::execution::GroupHandle::from_raft(
                self.metadata.raft.clone(),
                self.metadata.state.clone(),
                self.metadata.log.clone(),
            )),
            data.clone(),
            vec![data.clone()],
            store,
            budget,
        )
        .unwrap()
        .with_checkpoint_candidates(vec![tidemq::lifecycle::ReplicaCheckpoint::local(data)])
        .unwrap()
    }
    pub async fn produce(&self, count: u64) -> AppendBatch {
        self.produce_partition(self.partition, count).await
    }
    pub async fn produce_partition(&self, partition: PartitionKey, count: u64) -> AppendBatch {
        let producer_id = Id::new_v7().unwrap();
        let epoch = self
            .data
            .state
            .with_application(|state| state.producer(partition).unwrap().next_registration_epoch())
            .await;
        let writer = WriterFence {
            node_id: 1,
            owner_epoch: 1,
        };
        let registration = DataCommand::Producer {
            partition,
            writer,
            now_ms: 1,
            command: ProducerCommand::Register(RegisterProducer {
                producer_id,
                expected_registration_epoch: epoch,
            }),
        };
        assert!(matches!(
            self.data
                .raft
                .client_write(registration)
                .await
                .unwrap()
                .data,
            DataResponse::Registered(_)
        ));
        let messages = (0..count)
            .map(|offset| MessageData {
                context: Bytes::new(),
                key: Bytes::from(format!("key-{offset}")),
                body: Bytes::from(format!("body-{offset}")),
            })
            .collect::<Vec<_>>();
        let digest = tidemq_protocol::producer::canonical_produce_digest_parts(
            partition.topic_id,
            partition.partition,
            producer_id,
            epoch,
            0,
            messages.iter().map(|m| (m.key.as_ref(), m.body.as_ref())),
        )
        .unwrap();
        let batch = AppendBatch {
            topic_id: partition.topic_id,
            partition_id: partition.partition,
            producer_id,
            producer_epoch: epoch,
            first_sequence: 0,
            messages,
            content_digest: digest,
        };
        assert!(matches!(
            self.data
                .raft
                .client_write(DataCommand::Producer {
                    partition,
                    writer,
                    now_ms: 1,
                    command: ProducerCommand::Append(batch.clone())
                })
                .await
                .unwrap()
                .data,
            DataResponse::Produced(_)
        ));
        batch
    }
    pub async fn restart(self) -> Self {
        let Self {
            directory,
            cluster,
            partition,
            metadata,
            data,
        } = self;
        metadata.snapshot_and_close().await;
        data.snapshot_and_close().await;
        let metadata = Node::open(&directory.join("meta"), cluster, 77, false).await;
        let data = Node::open(&directory.join("data"), cluster, 9, false).await;
        Self {
            directory,
            cluster,
            partition,
            metadata,
            data,
        }
    }
    pub async fn close(self) {
        let Self {
            directory,
            metadata,
            data,
            ..
        } = self;
        metadata.snapshot_and_close().await;
        data.snapshot_and_close().await;
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}

pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(20)
}
