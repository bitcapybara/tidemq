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
        let directory =
            std::env::temp_dir().join(format!("tidemq-lifecycle-{}", Id::random().unwrap()));
        tokio::fs::create_dir(&directory).await.unwrap();
        let cluster = Id::random().unwrap();
        let partition = PartitionKey {
            topic_id: Id::random().unwrap(),
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
        fixture.bootstrap().await;
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
    async fn bootstrap(&self) {
        assert!(
            self.meta_write(Command::bootstrap(
                Id::random().unwrap(),
                self.cluster,
                77,
                DeploymentMode::LocalRf1,
                MetadataLimits::default()
            ))
            .await
            .is_applied()
        );
        assert!(
            self.meta_write(Command::put_node(
                Id::random().unwrap(),
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
                Id::random().unwrap(),
                self.revision().await,
                9,
                BTreeSet::from([1])
            ))
            .await
            .is_applied()
        );
        assert!(
            self.meta_write(Command::create_topic(
                Id::random().unwrap(),
                self.revision().await,
                self.partition.topic_id,
                "events".into(),
                vec![9]
            ))
            .await
            .is_applied()
        );
        let grant = match self
            .meta_write(
                Command::grant_ownership(
                    self.revision().await,
                    self.cluster,
                    self.partition,
                    1,
                    0,
                    0,
                )
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
        let meta = MetadataAuthority::new(
            self.metadata.raft.clone(),
            self.metadata.state.clone(),
            self.cluster,
            77,
            ControlPolicy::default(),
        )
        .unwrap();
        let authority = DataAuthority::new(
            self.data.raft.clone(),
            self.data.state.clone(),
            self.cluster,
            9,
            BTreeSet::from([1]),
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
                    partition: self.partition,
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
        let data = DataReplica::local(self.data.raft.clone(), self.data.state.clone());
        Coordinator::new(
            MetadataReplica::local(self.metadata.raft.clone(), self.metadata.state.clone()),
            data.clone(),
            vec![data],
            store,
            s3::budget(),
        )
        .unwrap()
    }
    pub async fn produce(&self, count: u64) -> AppendBatch {
        let producer_id = Id::random().unwrap();
        let writer = WriterFence {
            node_id: 1,
            owner_epoch: 1,
        };
        let registration = DataCommand::Producer {
            partition: self.partition,
            writer,
            now_ms: 1,
            command: ProducerCommand::Register(RegisterProducer {
                producer_id,
                expected_registration_epoch: 1,
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
                key: Bytes::from(format!("key-{offset}")),
                body: Bytes::from(format!("body-{offset}")),
            })
            .collect::<Vec<_>>();
        let digest = tidemq_protocol::producer::canonical_produce_digest_parts(
            self.partition.topic_id,
            0,
            producer_id,
            1,
            0,
            messages.iter().map(|m| (m.key.as_ref(), m.body.as_ref())),
        )
        .unwrap();
        let batch = AppendBatch {
            topic_id: self.partition.topic_id,
            partition_id: 0,
            producer_id,
            producer_epoch: 1,
            first_sequence: 0,
            messages,
            content_digest: digest,
        };
        assert!(matches!(
            self.data
                .raft
                .client_write(DataCommand::Producer {
                    partition: self.partition,
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
