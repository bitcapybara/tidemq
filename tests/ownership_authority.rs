use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory, SnapshotPolicy};
use serde::{Deserialize, Serialize};
use tidemq::metadata::{
    Command, DeploymentMode, MetadataLimits, MetadataState, NodeRole, NodeSpec, Outcome,
    PartitionKey, Response,
};
use tidemq::ownership::{
    AuthorityError, ControlPolicy, DataAuthority, FenceCommand, FenceResponse, FenceState,
    FinalizationKind, MetadataAuthority, OwnershipApplication, RecoveryCheckpoint, Work,
};
use tidemq::replication::{
    Application, DiskLog, DiskStateMachine, DiskStore, Raft, StorageIdentity, StorageOptions,
    TypeConfig,
};
use tidemq_protocol::Id;

const WAIT: Duration = Duration::from_secs(10);

struct NoNetwork<A>(PhantomData<fn() -> A>);

impl<A> Default for NoNetwork<A> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<A: Application> RaftNetworkFactory<TypeConfig<A>> for NoNetwork<A> {
    type Network = Self;
    async fn new_client(&mut self, _: u64, _: &BasicNode) -> Self::Network {
        Self::default()
    }
}

fn unavailable<E: std::error::Error>() -> RPCError<u64, BasicNode, E> {
    Unreachable::new(&std::io::Error::other(
        "RF1 ownership test has no remote voter",
    ))
    .into()
}

impl<A: Application> RaftNetwork<TypeConfig<A>> for NoNetwork<A> {
    async fn append_entries(
        &mut self,
        _: AppendEntriesRequest<TypeConfig<A>>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
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
    async fn vote(
        &mut self,
        _: VoteRequest<u64>,
        _: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(unavailable())
    }
}

struct Directory(PathBuf);

impl Directory {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!("tidemq-ownership-{}", Id::new_v7().unwrap()));
        tokio::fs::create_dir(&path).await.unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Rf1<A: Application> {
    raft: Raft<A>,
    log: DiskLog<A>,
    state: DiskStateMachine<A>,
}

impl<A: Application> Rf1<A> {
    fn group(&self) -> tidemq::execution::GroupHandle<A> {
        tidemq::execution::GroupHandle::from_raft(
            self.raft.clone(),
            self.state.clone(),
            self.log.clone(),
        )
    }

    async fn open(path: &Path, cluster: Id, group: u64, initialize: bool) -> Self {
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
                cluster_name: "tidemq-ownership-rf1".into(),
                heartbeat_interval: 20,
                election_timeout_min: 100,
                election_timeout_max: 200,
                enable_elect: false,
                snapshot_policy: SnapshotPolicy::Never,
                max_in_snapshot_log_to_keep: 0,
                ..openraft::Config::default()
            }
            .validate()
            .unwrap(),
        );
        let raft = Raft::<A>::new(
            1,
            config,
            NoNetwork::default(),
            store.log.clone(),
            store.state_machine.clone(),
        )
        .await
        .unwrap();
        if initialize {
            raft.initialize(BTreeMap::from([(1, BasicNode::new("in-process-rf1"))]))
                .await
                .unwrap();
        } else {
            raft.trigger().elect().await.unwrap();
        }
        raft.wait(Some(WAIT))
            .current_leader(1, "RF1 ownership fixture elects its leader")
            .await
            .unwrap();
        Self {
            raft,
            log: store.log,
            state: store.state_machine,
        }
    }

    async fn snapshot_and_purge(&self) {
        let last = self.log.diagnostics().await.applied_log.unwrap();
        self.raft.trigger().snapshot().await.unwrap();
        self.raft
            .wait(Some(WAIT))
            .snapshot(last, "Ownership state snapshot is durable")
            .await
            .unwrap();
        self.raft.trigger().purge_log(last.index).await.unwrap();
        self.raft
            .wait(Some(WAIT))
            .purged(Some(last), "Ownership state precedes WAL purge")
            .await
            .unwrap();
    }

    async fn close(self) {
        self.raft.shutdown().await.unwrap();
        self.log.close().await.unwrap();
    }
}

fn id(value: u64) -> Id {
    format!("00000000-0000-4000-8000-{value:012x}")
        .parse()
        .unwrap()
}

async fn revision(meta: &Rf1<MetadataState>) -> u64 {
    meta.state.with_application(MetadataState::revision).await
}

async fn bootstrap(
    meta: &tidemq::execution::GroupHandle<MetadataState>,
    mode: DeploymentMode,
) -> tidemq::ownership::Grant {
    assert!(
        meta.commit(
            Command::bootstrap(id(10), id(2), 1, mode, MetadataLimits::default()),
            tokio::time::Instant::now() + WAIT
        )
        .await
        .unwrap()
        .response
        .is_applied()
    );
    let count = mode.replication_factor();
    for node in 1..=count {
        let spec = NodeSpec {
            node_id: node as u64,
            zone: format!("zone-{node}"),
            endpoint: format!("127.0.0.1:{}", 20_000 + node).parse().unwrap(),
            roles: BTreeSet::from([NodeRole::Metadata, NodeRole::Storage, NodeRole::Broker]),
            schedulable: true,
        };
        assert!(
            meta.commit(
                Command::put_node(
                    id(20 + node as u64),
                    meta.state().with_application(MetadataState::revision).await,
                    spec,
                    0
                ),
                tokio::time::Instant::now() + WAIT
            )
            .await
            .unwrap()
            .response
            .is_applied()
        );
    }
    let voters = (1..=count as u64).collect();
    assert!(
        meta.commit(
            Command::create_group(
                id(30),
                meta.state().with_application(MetadataState::revision).await,
                2,
                if mode == DeploymentMode::Local {
                    tidemq::metadata::GroupPlacement::Local { node_id: 1 }
                } else {
                    tidemq::metadata::GroupPlacement::Raft { voters }
                }
            ),
            tokio::time::Instant::now() + WAIT
        )
        .await
        .unwrap()
        .response
        .is_applied()
    );
    assert!(
        meta.commit(
            Command::create_topic(
                id(31),
                meta.state().with_application(MetadataState::revision).await,
                id(3),
                "orders".into(),
                vec![2]
            ),
            tokio::time::Instant::now() + WAIT
        )
        .await
        .unwrap()
        .response
        .is_applied()
    );
    let command = Command::grant_ownership(
        meta.state().with_application(MetadataState::revision).await,
        id(2),
        PartitionKey {
            topic_id: id(3),
            partition: 0,
        },
        1,
        0,
        0,
    )
    .unwrap();
    let result = meta
        .commit(command, tokio::time::Instant::now() + WAIT)
        .await
        .unwrap()
        .response;
    match result {
        Response::Applied {
            outcome: Outcome::OwnershipGranted(grant),
            ..
        } => grant,
        _ => panic!("expected fixture ownership grant"),
    }
}

/// Empty producer/ACK maps are an explicit authorization-seam fixture, not T04/T05.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct DataFixture {
    ownership: Option<FenceState>,
    revision: u64,
    producer_state: BTreeMap<u64, u64>,
    acknowledged: BTreeSet<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum DataCommand {
    Initialize,
    Fence(FenceCommand),
    RebuildFixtureCheckpoint(PartitionKey),
    Noop,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
enum DataResponse {
    #[default]
    Rejected,
    Initialized,
    Fence(FenceResponse),
    Rebuilt,
    Noop,
}

impl Application for DataFixture {
    const NAME: &'static str = "ownership-authority-test-fixture";
    const VERSION: u32 = 1;
    type Command = DataCommand;
    type Response = DataResponse;

    fn apply(&mut self, command: Self::Command) -> Self::Response {
        self.revision += 1;
        match command {
            DataCommand::Initialize if self.ownership.is_none() => {
                self.ownership = Some(FenceState::new(id(2), 2, 16).unwrap());
                DataResponse::Initialized
            }
            DataCommand::Fence(command) => match self.ownership.as_mut() {
                Some(state) => DataResponse::Fence(state.apply(command)),
                None => DataResponse::Rejected,
            },
            DataCommand::RebuildFixtureCheckpoint(partition) => {
                let Some(state) = self.ownership.as_mut() else {
                    return DataResponse::Rejected;
                };
                let Some(fence) = state.fence(partition) else {
                    return DataResponse::Rejected;
                };
                let bytes =
                    serde_json::to_vec(&(&self.producer_state, &self.acknowledged)).unwrap();
                let checkpoint = RecoveryCheckpoint {
                    fence_generation: fence.fence_generation,
                    producer_state_revision: self.revision,
                    ack_state_revision: self.revision,
                    committed_end: 0,
                    state_digest: *blake3::hash(&bytes).as_bytes(),
                };
                match state.complete_recovery(partition, checkpoint) {
                    Ok(_) => DataResponse::Rebuilt,
                    Err(_) => DataResponse::Rejected,
                }
            }
            DataCommand::Noop => DataResponse::Noop,
            DataCommand::Initialize => DataResponse::Rejected,
        }
    }

    fn validate(&self) -> std::io::Result<()> {
        if !self.producer_state.is_empty() || !self.acknowledged.is_empty() {
            return Err(std::io::Error::other(
                "The authorization fixture has no business data",
            ));
        }
        if let Some(state) = &self.ownership {
            state.validate_snapshot().map_err(|reason| {
                std::io::Error::other(format!("Invalid fixture fence snapshot: {reason:?}"))
            })?;
        }
        Ok(())
    }
}

impl OwnershipApplication for DataFixture {
    fn ownership_state(&self) -> Option<&FenceState> {
        self.ownership.as_ref()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_rf1_barriers_gate_fences_recovery_activation_and_snapshot_reopen() {
    let directory = Directory::new().await;
    let meta_path = directory.0.join("metadata");
    let data_path = directory.0.join("data");
    let meta = Rf1::<MetadataState>::open(&meta_path, id(2), 1, true).await;
    let data = Rf1::<DataFixture>::open(&data_path, id(2), 2, true).await;
    let grant = bootstrap(&meta.group(), DeploymentMode::SeparateRf1).await;
    data.raft
        .client_write(DataCommand::Initialize)
        .await
        .unwrap();
    let authority =
        MetadataAuthority::new(meta.group(), id(2), 1, ControlPolicy::default()).unwrap();
    let data_authority = DataAuthority::new(
        data.group(),
        id(2),
        2,
        tidemq::metadata::GroupPlacement::Raft {
            voters: BTreeSet::from([1]),
        },
        ControlPolicy::default(),
    )
    .unwrap();
    let mut forged = grant.clone();
    forged.epoch = 100;
    assert!(matches!(
        authority.verify_grant(&forged).await,
        Err(AuthorityError::GrantMismatch)
    ));
    let command = authority
        .verify_grant(&grant)
        .await
        .unwrap()
        .into_fence_command()
        .unwrap();
    assert!(matches!(
        data.raft
            .client_write(DataCommand::Fence(command))
            .await
            .unwrap()
            .data,
        DataResponse::Fence(FenceResponse::Installed(_))
    ));
    assert!(matches!(
        data_authority.verify_recovery(&grant).await,
        Err(AuthorityError::RecoveryRequired)
    ));
    let command = data_authority
        .verify_fence(&grant)
        .await
        .unwrap()
        .into_observe_command(revision(&meta).await)
        .unwrap();
    assert!(
        meta.raft
            .client_write(command)
            .await
            .unwrap()
            .data
            .is_applied()
    );
    let fence = data
        .state
        .with_application(|state| {
            state
                .ownership
                .as_ref()
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
        data.raft
            .client_write(DataCommand::RebuildFixtureCheckpoint(grant.partition))
            .await
            .unwrap()
            .data,
        DataResponse::Rebuilt
    ));
    let command = data_authority
        .verify_recovery(&grant)
        .await
        .unwrap()
        .into_observe_command(revision(&meta).await)
        .unwrap();
    assert!(
        meta.raft
            .client_write(command)
            .await
            .unwrap()
            .data
            .is_applied()
    );
    let command = data_authority
        .verify_recovery(&grant)
        .await
        .unwrap()
        .into_activate_command(revision(&meta).await)
        .unwrap();
    let original = meta.raft.client_write(command).await.unwrap().data;
    assert!(original.is_applied());
    let lease = authority.active_lease(&fence).await.unwrap();
    lease.check(&fence, Work::Produce).unwrap();
    let finalization = lease
        .begin_finalization(
            &fence,
            FinalizationKind::Ack,
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    finalization.check(&fence).unwrap();
    let refreshed = authority.active_lease(&fence).await.unwrap();
    assert_eq!(refreshed.diagnose().available_finalizations, 255);
    drop(finalization);
    assert_eq!(refreshed.diagnose().available_finalizations, 256);
    drop(refreshed);
    data.raft.client_write(DataCommand::Noop).await.unwrap();
    let command = data_authority
        .verify_recovery(&grant)
        .await
        .unwrap()
        .into_activate_command(revision(&meta).await)
        .unwrap();
    assert_eq!(
        meta.raft.client_write(command).await.unwrap().data,
        original
    );
    meta.snapshot_and_purge().await;
    data.snapshot_and_purge().await;
    drop(lease);
    drop(authority);
    drop(data_authority);
    meta.close().await;
    data.close().await;
    let meta = Rf1::<MetadataState>::open(&meta_path, id(2), 1, false).await;
    let data = Rf1::<DataFixture>::open(&data_path, id(2), 2, false).await;
    let authority =
        MetadataAuthority::new(meta.group(), id(2), 1, ControlPolicy::default()).unwrap();
    let data_authority = DataAuthority::new(
        data.group(),
        id(2),
        2,
        tidemq::metadata::GroupPlacement::Raft {
            voters: BTreeSet::from([1]),
        },
        ControlPolicy::default(),
    )
    .unwrap();
    assert!(authority.verify_grant(&grant).await.is_ok());
    assert!(data_authority.verify_recovery(&grant).await.is_ok());
    assert_eq!(
        meta.state
            .with_application(|state| state.route(grant.partition).unwrap().epoch)
            .await,
        1
    );
    drop(authority);
    drop(data_authority);
    meta.close().await;
    data.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_rf3_cannot_mint_authority_from_an_actual_rf1_group() {
    let directory = Directory::new().await;
    let meta = Rf1::<MetadataState>::open(&directory.0.join("metadata"), id(2), 1, true).await;
    let grant = bootstrap(&meta.group(), DeploymentMode::SeparateRf3).await;
    let authority =
        MetadataAuthority::new(meta.group(), id(2), 1, ControlPolicy::default()).unwrap();
    assert!(matches!(
        authority.verify_grant(&grant).await,
        Err(AuthorityError::MembershipMismatch)
    ));
    drop(authority);
    meta.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopped_authority_cannot_refresh_a_grant_or_reuse_expired_verification() {
    let directory = Directory::new().await;
    let meta = Rf1::<MetadataState>::open(&directory.0.join("metadata"), id(2), 1, true).await;
    let grant = bootstrap(&meta.group(), DeploymentMode::SeparateRf1).await;
    let policy = ControlPolicy {
        ttl: Duration::from_millis(200),
        ..ControlPolicy::default()
    };
    let authority = MetadataAuthority::new(meta.group(), id(2), 1, policy).unwrap();
    let verified = authority.verify_grant(&grant).await.unwrap();
    meta.raft.shutdown().await.unwrap();
    assert!(authority.verify_grant(&grant).await.is_err());
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(matches!(
        verified.into_fence_command(),
        Err(AuthorityError::Validity { .. })
    ));
    drop(authority);
    meta.log.close().await.unwrap();
}

async fn local_group<A: Application + Default>(
    path: &Path,
    group: u64,
) -> tidemq::execution::GroupHandle<A> {
    tidemq::execution::GroupHandle::Local(
        tidemq::execution::LocalGroup::open(
            path,
            StorageIdentity::new::<A>(id(2), group, 1),
            Default::default(),
            A::default(),
        )
        .await
        .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_authority_preserves_proofs_expiry_and_snapshot_recovery_without_raft() {
    let directory = Directory::new().await;
    let meta_path = directory.0.join("metadata");
    let data_path = directory.0.join("data");
    let meta = local_group::<MetadataState>(&meta_path, 1).await;
    let data = local_group::<DataFixture>(&data_path, 2).await;
    let grant = bootstrap(&meta, DeploymentMode::Local).await;
    assert!(meta.raft().is_none());
    assert!(data.raft().is_none());
    let deadline = || tokio::time::Instant::now() + WAIT;
    data.commit(DataCommand::Initialize, deadline())
        .await
        .unwrap();
    let authority =
        MetadataAuthority::new(meta.clone(), id(2), 1, ControlPolicy::default()).unwrap();
    let data_authority = DataAuthority::new(
        data.clone(),
        id(2),
        2,
        tidemq::metadata::GroupPlacement::Local { node_id: 1 },
        ControlPolicy::default(),
    )
    .unwrap();
    let fence = authority
        .verify_grant(&grant)
        .await
        .unwrap()
        .into_fence_command()
        .unwrap();
    let committed = data
        .commit(DataCommand::Fence(fence), deadline())
        .await
        .unwrap();
    assert!(
        matches!(committed.position, tidemq::execution::ExecutionPosition::Local { sequence } if sequence > 0)
    );
    let observed = data_authority
        .verify_fence(&grant)
        .await
        .unwrap()
        .into_observe_command(meta.state().with_application(MetadataState::revision).await)
        .unwrap();
    assert!(
        meta.commit(observed, deadline())
            .await
            .unwrap()
            .response
            .is_applied()
    );
    assert!(matches!(
        data_authority.verify_recovery(&grant).await,
        Err(AuthorityError::RecoveryRequired)
    ));
    data.commit_completion(
        DataCommand::RebuildFixtureCheckpoint(grant.partition),
        deadline(),
    )
    .await
    .unwrap();
    let observed = data_authority
        .verify_recovery(&grant)
        .await
        .unwrap()
        .into_observe_command(meta.state().with_application(MetadataState::revision).await)
        .unwrap();
    assert!(
        meta.commit(observed, deadline())
            .await
            .unwrap()
            .response
            .is_applied()
    );
    let activated = data_authority
        .verify_recovery(&grant)
        .await
        .unwrap()
        .into_activate_command(meta.state().with_application(MetadataState::revision).await)
        .unwrap();
    assert!(
        meta.commit(activated, deadline())
            .await
            .unwrap()
            .response
            .is_applied()
    );
    assert!(meta.snapshot(deadline()).await.unwrap().is_some());
    assert!(data.snapshot(deadline()).await.unwrap().is_some());
    meta.close(deadline()).await.unwrap();
    assert!(authority.verify_grant(&grant).await.is_err());
    data.close(deadline()).await.unwrap();
    assert!(data_authority.verify_fence(&grant).await.is_err());
    drop((authority, data_authority, meta, data));
    let meta = local_group::<MetadataState>(&meta_path, 1).await;
    let data = local_group::<DataFixture>(&data_path, 2).await;
    let authority =
        MetadataAuthority::new(meta.clone(), id(2), 1, ControlPolicy::default()).unwrap();
    let data_authority = DataAuthority::new(
        data.clone(),
        id(2),
        2,
        tidemq::metadata::GroupPlacement::Local { node_id: 1 },
        ControlPolicy::default(),
    )
    .unwrap();
    let recovered = data_authority
        .verify_recovery(&grant)
        .await
        .unwrap()
        .into_activate_command(meta.state().with_application(MetadataState::revision).await)
        .unwrap();
    assert!(
        meta.commit(recovered, deadline())
            .await
            .unwrap()
            .response
            .is_applied()
    );
    assert!(authority.verify_grant(&grant).await.is_ok());
    let fence = data
        .state()
        .try_with_application(|state| {
            state
                .ownership
                .as_ref()
                .unwrap()
                .fence(grant.partition)
                .unwrap()
                .clone()
        })
        .await
        .unwrap();
    assert!(authority.active_lease(&fence).await.is_ok());
    meta.close(deadline()).await.unwrap();
    data.close(deadline()).await.unwrap();
}
