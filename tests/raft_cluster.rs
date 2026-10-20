#[path = "replication_support/mod.rs"]
mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{Backoff, RPCOption};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory, SnapshotPolicy};
use tidemq::replication::{DiskLog, DiskStateMachine};
use tidemq_protocol::Id;
use tokio::sync::RwLock;

use support::{Command, Config, Directory, Raft, Response, State, identity, store};

const WAIT: Duration = Duration::from_secs(10);

/// A deterministic fault-control seam around real OpenRaft RPC handlers. No QUIC is exercised.
#[derive(Clone, Default)]
struct Router {
    nodes: Arc<RwLock<BTreeMap<u64, Raft>>>,
    isolated: Arc<RwLock<BTreeSet<u64>>>,
    lose_append_responses: Arc<AtomicBool>,
    control_calls: Arc<AtomicU64>,
    replication_calls: Arc<AtomicU64>,
    snapshot_chunks: Arc<AtomicU64>,
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
    async fn new_client(&mut self, target: u64, _: &BasicNode) -> Self::Network {
        Network {
            source: self.source,
            target,
            router: self.router.clone(),
        }
    }
}

impl Network {
    async fn target(&self) -> Result<Raft, Unreachable> {
        let isolated = self.router.isolated.read().await;
        if isolated.contains(&self.source) || isolated.contains(&self.target) {
            return Err(Unreachable::new(&std::io::Error::other(
                "Injected replica isolation",
            )));
        }
        self.router
            .nodes
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| Unreachable::new(&std::io::Error::other("Replica is stopped")))
    }
}

impl RaftNetwork<Config> for Network {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<Config>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if rpc.entries.is_empty() {
            self.router.control_calls.fetch_add(1, Ordering::Relaxed);
        } else {
            self.router
                .replication_calls
                .fetch_add(1, Ordering::Relaxed);
        }
        let target = self.target().await?;
        let result = target
            .append_entries(rpc)
            .await
            .map_err(|error| RemoteError::new(self.target, error).into());
        if self.router.lose_append_responses.load(Ordering::Relaxed) {
            return Err(Unreachable::new(&std::io::Error::other(
                "Injected loss after remote append execution",
            ))
            .into());
        }
        result
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<Config>,
        _: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        assert!(
            rpc.data.len() <= 256,
            "Snapshot chunks obey the configured transfer budget"
        );
        self.router.snapshot_chunks.fetch_add(1, Ordering::Relaxed);
        self.target()
            .await?
            .install_snapshot(rpc)
            .await
            .map_err(|error| RemoteError::new(self.target, error).into())
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.router.control_calls.fetch_add(1, Ordering::Relaxed);
        self.target()
            .await?
            .vote(rpc)
            .await
            .map_err(|error| RemoteError::new(self.target, error).into())
    }

    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(Duration::from_millis(20)))
    }
}

struct Node {
    raft: Raft,
    log: DiskLog<State>,
    state: DiskStateMachine<State>,
}

struct Cluster {
    directory: Directory,
    cluster_id: Id,
    group_id: u64,
    router: Router,
    nodes: BTreeMap<u64, Node>,
    config: Arc<openraft::Config>,
}

impl Cluster {
    async fn new(ids: &[u64]) -> Self {
        let mut cluster = Self {
            directory: Directory::new().await,
            cluster_id: Id::random().unwrap(),
            group_id: 1,
            router: Router::default(),
            nodes: BTreeMap::new(),
            config: Arc::new(
                openraft::Config {
                    cluster_name: "tidemq-real-raft-test".to_owned(),
                    heartbeat_interval: 30,
                    election_timeout_min: 100,
                    election_timeout_max: 200,
                    enable_elect: false,
                    snapshot_policy: SnapshotPolicy::Never,
                    snapshot_max_chunk_size: 256,
                    max_in_snapshot_log_to_keep: 0,
                    replication_lag_threshold: 8,
                    ..openraft::Config::default()
                }
                .validate()
                .unwrap(),
            ),
        };
        for id in ids {
            cluster.start(*id).await;
        }
        cluster
    }

    async fn start(&mut self, id: u64) {
        let storage = store(
            &self.directory.path().join(format!("node-{id}")),
            identity(self.cluster_id, self.group_id, id),
        )
        .await;
        let raft = Raft::new(
            id,
            self.config.clone(),
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

    fn node(&self, id: u64) -> &Node {
        self.nodes.get(&id).expect("test node exists")
    }

    async fn initialize(&self, voters: &[u64]) {
        let first = voters.first().copied().unwrap();
        let members: BTreeMap<_, _> = voters
            .iter()
            .map(|id| (*id, BasicNode::new(format!("harness://node-{id}"))))
            .collect();
        self.node(first).raft.initialize(members).await.unwrap();
        self.node(first)
            .raft
            .wait(Some(WAIT))
            .current_leader(first, "Initial leader is elected")
            .await
            .unwrap();
    }

    async fn wait_applied(&self, ids: &[u64], index: u64) {
        for id in ids {
            self.node(*id)
                .raft
                .wait(Some(WAIT))
                .applied_index_at_least(Some(index), "Committed state reaches the replica")
                .await
                .unwrap();
        }
    }

    async fn stop(&mut self, id: u64) {
        self.router.nodes.write().await.remove(&id);
        let node = self.nodes.remove(&id).expect("test node exists");
        node.raft.shutdown().await.unwrap();
        node.log.close().await.unwrap();
    }

    async fn shutdown(mut self) {
        let ids: Vec<_> = self.nodes.keys().copied().collect();
        for id in ids {
            self.stop(id).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rf1_commit_response_loss_and_disk_reopen_preserve_original_offsets() {
    let mut cluster = Cluster::new(&[1]).await;
    cluster.initialize(&[1]).await;
    cluster
        .node(1)
        .raft
        .client_write(Command::Fence(3))
        .await
        .unwrap();
    let command = Command::Append {
        epoch: 3,
        request: 77,
        value: 8,
    };
    let lost_response = cluster
        .node(1)
        .raft
        .client_write(command.clone())
        .await
        .unwrap();
    assert_eq!(lost_response.data, Response::Appended { offset: 0 });
    assert!(
        lost_response.log_id.index > 0,
        "Consensus membership and fence logs do not allocate message offsets"
    );
    cluster.stop(1).await;
    cluster.start(1).await;
    cluster.node(1).raft.trigger().elect().await.unwrap();
    cluster
        .node(1)
        .raft
        .wait(Some(WAIT))
        .current_leader(1, "Restarted RF1 elects itself")
        .await
        .unwrap();
    let retry = cluster.node(1).raft.client_write(command).await.unwrap();
    assert_eq!(retry.data, Response::Appended { offset: 0 });
    assert_eq!(cluster.node(1).state.application().await.next_offset, 1);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf3_election_quorum_loss_and_disk_restart_never_acknowledge_an_isolated_write() {
    let mut cluster = Cluster::new(&[1, 2, 3]).await;
    cluster.initialize(&[1, 2, 3]).await;
    let result = cluster
        .node(1)
        .raft
        .client_write(Command::Append {
            epoch: 0,
            request: 1,
            value: 5,
        })
        .await
        .unwrap();
    cluster.wait_applied(&[1, 2, 3], result.log_id.index).await;
    cluster.router.isolated.write().await.insert(1);
    let unavailable = tokio::time::timeout(
        Duration::from_millis(250),
        cluster.node(1).raft.client_write(Command::Append {
            epoch: 0,
            request: 2,
            value: 6,
        }),
    )
    .await;
    assert!(
        !matches!(unavailable, Ok(Ok(_))),
        "An isolated leader cannot report a new committed write"
    );
    assert_eq!(cluster.node(1).state.application().await.next_offset, 1);
    cluster.node(2).raft.trigger().elect().await.unwrap();
    cluster
        .node(2)
        .raft
        .wait(Some(WAIT))
        .current_leader(2, "Remaining quorum elects a new leader")
        .await
        .unwrap();
    let next = cluster
        .node(2)
        .raft
        .client_write(Command::Append {
            epoch: 0,
            request: 3,
            value: 7,
        })
        .await
        .unwrap();
    assert_eq!(next.data, Response::Appended { offset: 1 });
    cluster.wait_applied(&[2, 3], next.log_id.index).await;
    cluster.stop(1).await;
    cluster.router.isolated.write().await.remove(&1);
    cluster.start(1).await;
    cluster.wait_applied(&[1], next.log_id.index).await;
    assert_eq!(
        cluster.node(1).state.application().await,
        cluster.node(2).state.application().await
    );
    assert!(
        !cluster
            .node(1)
            .state
            .application()
            .await
            .results
            .contains_key(&2)
    );
    assert!(cluster.router.replication_calls.load(Ordering::Relaxed) > 0);
    assert!(cluster.router.control_calls.load(Ordering::Relaxed) > 0);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_membership_change_and_chunked_snapshot_install_recover_a_new_replica() {
    let mut cluster = Cluster::new(&[1, 2, 3, 4]).await;
    cluster.initialize(&[1, 2, 3]).await;
    let mut last = None;
    for request in 0..24 {
        last = Some(
            cluster
                .node(1)
                .raft
                .client_write(Command::Append {
                    epoch: 0,
                    request,
                    value: request * 2,
                })
                .await
                .unwrap()
                .log_id,
        );
    }
    let last = last.unwrap();
    cluster.wait_applied(&[1, 2, 3], last.index).await;
    cluster.node(1).raft.trigger().snapshot().await.unwrap();
    cluster
        .node(1)
        .raft
        .wait(Some(WAIT))
        .snapshot(last, "Snapshot becomes durable before purge")
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
        .wait(Some(WAIT))
        .purged(Some(last), "Only snapshotted logs are purged")
        .await
        .unwrap();
    assert!(
        cluster.node(1).log.diagnostics().await.snapshot_log
            >= cluster.node(1).log.diagnostics().await.purged_log
    );
    cluster
        .node(1)
        .raft
        .add_learner(4, BasicNode::new("harness://node-4"), true)
        .await
        .unwrap();
    let changed = cluster
        .node(1)
        .raft
        .change_membership(BTreeSet::from([1, 2, 4]), false)
        .await
        .unwrap();
    cluster.wait_applied(&[1, 2, 4], changed.log_id.index).await;
    assert!(cluster.router.snapshot_chunks.load(Ordering::Relaxed) > 1);
    assert_eq!(
        cluster.node(4).state.application().await,
        cluster.node(1).state.application().await
    );
    let voters = cluster
        .node(1)
        .raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .voter_ids()
        .collect::<BTreeSet<_>>();
    assert_eq!(voters, BTreeSet::from([1, 2, 4]));
    cluster.stop(4).await;
    cluster.start(4).await;
    cluster.wait_applied(&[4], changed.log_id.index).await;
    assert_eq!(cluster.node(4).state.application().await.next_offset, 24);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_replication_responses_do_not_become_false_commit_acknowledgements() {
    let cluster = Cluster::new(&[1, 2, 3]).await;
    cluster.initialize(&[1, 2, 3]).await;
    cluster
        .node(1)
        .raft
        .client_write(Command::Fence(2))
        .await
        .unwrap();
    cluster
        .router
        .lose_append_responses
        .store(true, Ordering::Relaxed);
    let command = Command::Append {
        epoch: 2,
        request: 9,
        value: 99,
    };
    let unknown = tokio::time::timeout(
        Duration::from_millis(250),
        cluster.node(1).raft.client_write(command.clone()),
    )
    .await;
    assert!(
        !matches!(unknown, Ok(Ok(_))),
        "Remote execution without quorum responses cannot produce a durable success reply"
    );
    cluster
        .router
        .lose_append_responses
        .store(false, Ordering::Relaxed);
    let retry = tokio::time::timeout(WAIT, cluster.node(1).raft.client_write(command))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retry.data, Response::Appended { offset: 0 });
    cluster.wait_applied(&[1, 2, 3], retry.log_id.index).await;
    assert_eq!(cluster.node(1).state.application().await.next_offset, 1);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_early_heartbeat_cannot_apply_a_missing_replication_body_or_override_a_newer_vote() {
    let cluster = Cluster::new(&[2]).await;
    let raft = &cluster.node(2).raft;
    let log_id = openraft::LogId::new(openraft::CommittedLeaderId::new(5, 1), 0);
    let vote = openraft::Vote::new_committed(5, 1);
    let early = raft
        .append_entries(AppendEntriesRequest {
            vote,
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: Some(log_id),
        })
        .await
        .unwrap();
    assert!(matches!(early, AppendEntriesResponse::Success));
    assert_eq!(cluster.node(2).state.application().await.next_offset, 0);
    assert_eq!(cluster.node(2).log.diagnostics().await.applied_log, None);
    let missing_previous = raft
        .append_entries(AppendEntriesRequest {
            vote,
            prev_log_id: Some(log_id),
            entries: Vec::new(),
            leader_commit: Some(log_id),
        })
        .await
        .unwrap();
    assert!(matches!(missing_previous, AppendEntriesResponse::Conflict));
    let body = openraft::Entry {
        log_id,
        payload: openraft::EntryPayload::Normal(Command::Append {
            epoch: 0,
            request: 1,
            value: 42,
        }),
    };
    let appended = raft
        .append_entries(AppendEntriesRequest {
            vote,
            prev_log_id: None,
            entries: vec![body],
            leader_commit: Some(log_id),
        })
        .await
        .unwrap();
    assert!(matches!(appended, AppendEntriesResponse::Success));
    raft.wait(Some(WAIT))
        .applied_index_at_least(
            Some(0),
            "Only the durable matching log body becomes applied",
        )
        .await
        .unwrap();
    assert_eq!(
        cluster
            .node(2)
            .state
            .with_application(|state| state.values.get(&0).copied())
            .await,
        Some(42)
    );
    let stale = raft
        .append_entries(AppendEntriesRequest {
            vote: openraft::Vote::new_committed(4, 1),
            prev_log_id: Some(log_id),
            entries: Vec::new(),
            leader_commit: Some(log_id),
        })
        .await
        .unwrap();
    assert!(matches!(stale, AppendEntriesResponse::HigherVote(_)));
    cluster.shutdown().await;
}
