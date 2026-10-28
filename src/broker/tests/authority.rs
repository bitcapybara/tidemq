use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use tidemq_protocol::Id;

use crate::metadata::{
    self, Command, DeploymentMode, MetadataLimits, MetadataState, NodeRole, NodeSpec, PartitionKey,
};
use crate::ownership::{
    AuthorityError, ControlLease, ControlPolicy, DataAuthority, Grant, MetadataAuthority,
};
use crate::producer::RegisterProducer;
use crate::replication::{
    DiskLog, DiskStateMachine, DiskStore, Raft, StorageIdentity, StorageOptions, TypeConfig,
};
use crate::subscription;

use super::super::{BrokerLimits, DataCommand, DataResponse};
use super::support::{Cluster, batch, context, writer};

type Config = TypeConfig<MetadataState>;

struct LocalMetadata;

impl RaftNetworkFactory<Config> for LocalMetadata {
    type Network = Self;
    async fn new_client(&mut self, _: u64, _: &BasicNode) -> Self {
        Self
    }
}

fn no_remote<E: std::error::Error>() -> RPCError<u64, BasicNode, E> {
    Unreachable::new(&std::io::Error::other(
        "The RF1 metadata fixture has no remote voter",
    ))
    .into()
}

impl RaftNetwork<Config> for LocalMetadata {
    async fn append_entries(
        &mut self,
        _: AppendEntriesRequest<Config>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(no_remote())
    }
    async fn vote(
        &mut self,
        _: VoteRequest<u64>,
        _: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(no_remote())
    }
    async fn install_snapshot(
        &mut self,
        _: InstallSnapshotRequest<Config>,
        _: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        Err(no_remote())
    }
}

struct Metadata {
    raft: Raft<MetadataState>,
    state: DiskStateMachine<MetadataState>,
    log: DiskLog<MetadataState>,
    cluster_id: Id,
    group_id: u64,
}

impl Metadata {
    async fn new(cluster: &Cluster) -> Self {
        let group_id = 77;
        let store = DiskStore::<MetadataState>::open(
            cluster.directory().join("metadata"),
            StorageIdentity::new::<MetadataState>(cluster.config.cluster_id, group_id, 1),
            StorageOptions::default(),
            None,
        )
        .await
        .unwrap();
        let config = Arc::new(
            openraft::Config {
                cluster_name: "tidemq-business-authority-rf1".to_owned(),
                enable_elect: false,
                ..openraft::Config::default()
            }
            .validate()
            .unwrap(),
        );
        let raft = Raft::new(
            1,
            config,
            LocalMetadata,
            store.log.clone(),
            store.state_machine.clone(),
        )
        .await
        .unwrap();
        raft.initialize(BTreeMap::from([(1, BasicNode::new("harness://metadata"))]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "Metadata leader is ready")
            .await
            .unwrap();
        let meta = Self {
            raft,
            state: store.state_machine,
            log: store.log,
            cluster_id: cluster.config.cluster_id,
            group_id,
        };
        assert!(
            meta.write(Command::bootstrap(
                Id::new_v7().unwrap(),
                meta.cluster_id,
                group_id,
                DeploymentMode::SeparateRf1,
                MetadataLimits::default()
            ))
            .await
            .is_applied()
        );
        assert!(
            meta.write(Command::put_node(
                Id::new_v7().unwrap(),
                meta.revision().await,
                NodeSpec {
                    node_id: 1,
                    zone: "local-zone".to_owned(),
                    endpoint: "127.0.0.1:17001".parse().unwrap(),
                    roles: BTreeSet::from([
                        NodeRole::Metadata,
                        NodeRole::Storage,
                        NodeRole::Broker
                    ]),
                    schedulable: true,
                },
                0
            ))
            .await
            .is_applied()
        );
        assert!(
            meta.write(Command::create_group(
                Id::new_v7().unwrap(),
                meta.revision().await,
                cluster.config.group_id,
                crate::metadata::GroupPlacement::Raft {
                    voters: BTreeSet::from([1])
                }
            ))
            .await
            .is_applied()
        );
        meta
    }

    async fn write(&self, command: Command) -> metadata::Response {
        self.raft.client_write(command).await.unwrap().data
    }

    async fn revision(&self) -> u64 {
        self.state.with_application(MetadataState::revision).await
    }

    async fn grant(&self, cluster: &Cluster, partition: PartitionKey) -> Grant {
        assert!(
            self.write(Command::create_topic(
                Id::new_v7().unwrap(),
                self.revision().await,
                partition.topic_id,
                "orders".to_owned(),
                vec![cluster.config.group_id]
            ))
            .await
            .is_applied()
        );
        let command =
            Command::grant_ownership(self.revision().await, self.cluster_id, partition, 1, 0, 0)
                .unwrap();
        match self.write(command).await {
            metadata::Response::Applied {
                outcome: metadata::Outcome::OwnershipGranted(grant),
                ..
            } => grant,
            response => panic!("Unexpected metadata grant response: {response:?}"),
        }
    }

    fn authority(&self) -> MetadataAuthority {
        MetadataAuthority::new(
            crate::execution::GroupHandle::from_raft(
                self.raft.clone(),
                self.state.clone(),
                self.log.clone(),
            ),
            self.cluster_id,
            self.group_id,
            policy(),
        )
        .unwrap()
    }

    async fn close(self) {
        self.raft.shutdown().await.unwrap();
        self.log.close().await.unwrap();
    }
}

fn policy() -> ControlPolicy {
    ControlPolicy {
        ttl: Duration::from_secs(10),
        ..ControlPolicy::default()
    }
}

async fn activate(metadata: &Metadata, cluster: &Cluster, grant: &Grant) -> ControlLease {
    let authority = metadata.authority();
    let data = DataAuthority::new(
        crate::execution::GroupHandle::from_raft(
            cluster.node(1).raft.clone(),
            cluster.node(1).state.clone(),
            cluster.node(1).log.clone(),
        ),
        cluster.config.cluster_id,
        cluster.config.group_id,
        crate::metadata::GroupPlacement::Raft {
            voters: BTreeSet::from([1]),
        },
        policy(),
    )
    .unwrap();
    let command = authority
        .verify_grant(grant)
        .await
        .unwrap()
        .into_fence_command()
        .unwrap();
    assert!(matches!(
        cluster.write(DataCommand::InstallFence(command)).await,
        DataResponse::Fenced(_)
    ));
    let observed = data
        .verify_fence(grant)
        .await
        .unwrap()
        .into_observe_command(metadata.revision().await)
        .unwrap();
    assert!(metadata.write(observed).await.is_applied());
    let fence = cluster
        .node(1)
        .state
        .with_application(|state| {
            state
                .ownership()
                .unwrap()
                .fence(grant.partition)
                .unwrap()
                .clone()
        })
        .await;
    assert!(matches!(
        authority.active_lease(&fence).await,
        Err(AuthorityError::RouteRequired)
    ));
    assert!(matches!(
        data.verify_recovery(grant).await,
        Err(AuthorityError::RecoveryRequired)
    ));
    assert!(matches!(
        cluster
            .write(DataCommand::Recover {
                partition: grant.partition,
                writer: writer(grant)
            })
            .await,
        DataResponse::Recovered(_)
    ));
    let recovered = data
        .verify_recovery(grant)
        .await
        .unwrap()
        .into_observe_command(metadata.revision().await)
        .unwrap();
    assert!(metadata.write(recovered).await.is_applied());
    let activate = data
        .verify_recovery(grant)
        .await
        .unwrap()
        .into_activate_command(metadata.revision().await)
        .unwrap();
    assert!(metadata.write(activate).await.is_applied());
    authority.active_lease(&fence).await.unwrap()
}

#[tokio::test]
async fn real_metadata_authority_activates_actual_producer_and_ack_state_through_snapshot_reopen() {
    let mut cluster = Cluster::new(&[1]).await;
    let metadata = Metadata::new(&cluster).await;
    let partition = PartitionKey {
        topic_id: Id::new_v7().unwrap(),
        partition: 0,
    };
    let grant = metadata.grant(&cluster, partition).await;
    let authorization = activate(&metadata, &cluster, &grant).await;
    let broker = cluster.broker(BrokerLimits::default());
    let session = broker
        .register(
            &authorization,
            context(),
            partition,
            RegisterProducer {
                producer_id: Id::new_v7().unwrap(),
                expected_registration_epoch: 1,
            },
        )
        .await
        .unwrap();
    let original = batch(partition, &session, 0, b"authorized-payload");
    broker
        .produce(&authorization, context(), original.clone())
        .await
        .unwrap();
    let bytes = original.messages.first().unwrap().logical_bytes().unwrap() as u32;
    let ack = super::ack::commit_first_ack(&cluster, &grant, &authorization, bytes).await;
    let last = cluster.node(1).log.diagnostics().await.applied_log.unwrap();
    cluster.node(1).raft.trigger().snapshot().await.unwrap();
    cluster
        .node(1)
        .raft
        .wait(Some(Duration::from_secs(5)))
        .snapshot(last, "Business authority snapshot is durable")
        .await
        .unwrap();
    cluster
        .node(1)
        .raft
        .trigger()
        .purge_log(last.index)
        .await
        .unwrap();
    cluster
        .node(1)
        .raft
        .wait(Some(Duration::from_secs(5)))
        .purged(Some(last), "Business state protects purged commands")
        .await
        .unwrap();
    broker
        .drain(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    drop(broker);
    drop(authorization);
    cluster.restart(1).await;
    let fence = cluster
        .node(1)
        .state
        .with_application(|application| {
            application
                .ownership()
                .unwrap()
                .fence(partition)
                .unwrap()
                .clone()
        })
        .await;
    let refreshed = metadata.authority().active_lease(&fence).await.unwrap();
    let broker = cluster.broker(BrokerLimits::default());
    assert!(
        broker
            .produce(&refreshed, context(), original)
            .await
            .unwrap()
            .duplicate
    );
    refreshed
        .check(&fence, crate::ownership::Work::ControlMutation)
        .unwrap();
    let retry = cluster
        .write(DataCommand::Subscription {
            partition,
            writer: writer(&grant),
            command: ack,
        })
        .await;
    assert!(
        matches!(retry, DataResponse::Subscription(subscription::Outcome::Acknowledged(receipt))
        if receipt.acknowledged_before == 1 && receipt.already_acknowledged == 1)
    );
    drop(broker);
    metadata.close().await;
    cluster.shutdown().await;
}

#[test]
fn new_consumer_work_cannot_consume_the_ack_completion_reserve() {
    use crate::subscription::{GroupAck, GroupCommand, GroupRead};
    let id = Id::new_v7().unwrap();
    let wrap = |command| DataCommand::ConsumerGroup {
        partition: PartitionKey {
            topic_id: id,
            partition: 0,
        },
        writer: crate::broker::WriterFence {
            node_id: 1,
            owner_epoch: 1,
        },
        command,
    };
    assert!(
        !wrap(GroupCommand::Read {
            subscription: id,
            request: GroupRead {
                member: id,
                epoch: 1,
                sequence: 1,
                now_ms: 0,
                max_messages: 1,
                max_bytes: 1,
            },
            cold: Vec::new()
        })
        .is_completion()
    );
    assert!(
        !wrap(GroupCommand::InstallMembers {
            subscription: id,
            version: 1,
            grants: Vec::new(),
            now_ms: 0
        })
        .is_completion()
    );
    assert!(
        wrap(GroupCommand::Acknowledge {
            subscription: id,
            request: GroupAck {
                member: id,
                epoch: 1,
                sequence: 1,
                now_ms: 0,
                model: tidemq_protocol::topic::SubscriptionKind::Queue,
                proofs: Vec::new(),
                nack_delay_ms: None,
            }
        })
        .is_completion()
    );
}
