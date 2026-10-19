#[path = "replication_support/mod.rs"]
mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use openraft::{BasicNode, SnapshotPolicy};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use tidemq::budget::MemoryBudget;
use tidemq::config::MemoryConfig;
use tidemq::replication::rpc::{
    Error, GroupRole, NetworkConfig, PeerPool, QuicNetworkFactory, RaftService,
};
use tidemq::replication::{DiskLog, DiskStateMachine, DiskStore, StorageOptions};
use tidemq::server::{DispatchConfig, SubmissionOrder, WireService};
use tidemq::transport::{
    Authorization, Client, ClientCredentials, ClientTls, NodeGrant, PeerTarget, Server,
    ServerAuthorization, ServerTls, TlsIdentity, TransportConfig,
};
use tidemq_protocol::{
    Id, ProtocolLimits,
    common::{Frame, FrameKind, TrafficClass},
    framing::new_frame,
    internal::RaftRequest,
};
use tokio::sync::{RwLock, watch};
use tokio::time::Instant;

use support::{Command, Directory, Raft, Response, State, identity};

const WAIT: Duration = Duration::from_secs(12);
const GROUP: u64 = 2;

struct Leaf {
    cert: Certificate,
    key: KeyPair,
    name: String,
}
struct Pki {
    ca: Certificate,
    leaves: BTreeMap<u64, Leaf>,
}

impl Pki {
    fn new(ids: &[u64]) -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
        ];
        let ca = params.self_signed(&key).unwrap();
        let issuer = Issuer::new(params, key);
        let leaves = ids
            .iter()
            .map(|id| {
                let name = format!("node-{id}.local");
                let key = KeyPair::generate().unwrap();
                let mut params = CertificateParams::new(vec![name.clone()]).unwrap();
                params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
                params.extended_key_usages = vec![
                    ExtendedKeyUsagePurpose::ClientAuth,
                    ExtendedKeyUsagePurpose::ServerAuth,
                ];
                let cert = params.signed_by(&key, &issuer).unwrap();
                (*id, Leaf { cert, key, name })
            })
            .collect();
        Self { ca, leaves }
    }
    fn identity(&self, id: u64) -> TlsIdentity {
        let leaf = self.leaves.get(&id).unwrap();
        TlsIdentity::new(
            Bytes::from(leaf.cert.pem()),
            Bytes::from(leaf.key.serialize_pem()),
        )
        .unwrap()
    }
    fn grants(&self, cluster: Id) -> Vec<NodeGrant> {
        self.leaves
            .iter()
            .map(|(id, leaf)| NodeGrant {
                certificate_pin: NodeGrant::certificate_pin(leaf.cert.der()),
                certificate_name: leaf.name.clone(),
                cluster_id: cluster,
                node_id: *id,
                role: "storage".into(),
                allowed_groups: vec![GROUP],
                allowed_classes: vec![
                    TrafficClass::Control,
                    TrafficClass::Replication,
                    TrafficClass::Bulk,
                ],
            })
            .collect()
    }
}

struct Service {
    node: u64,
    current: RwLock<Option<Arc<RaftService<State>>>>,
    isolated: Arc<RwLock<BTreeSet<u64>>>,
    lose_append_reply: Arc<AtomicBool>,
}

impl WireService for Service {
    type Error = Error;
    async fn handle(
        &self,
        peer: Authorization,
        frame: &Frame,
        _deadline: Instant,
        order: Option<SubmissionOrder>,
    ) -> Result<Option<Frame>, Error> {
        drop(order);
        let Authorization::Internal(grant) = &peer else {
            return Err(Error::Invalid {
                reason: "test requires internal authentication",
            });
        };
        let isolated = self.isolated.read().await;
        if isolated.contains(&self.node) || isolated.contains(&grant.node_id) {
            return Err(Error::Invalid {
                reason: "injected RPC isolation",
            });
        }
        drop(isolated);
        let service = self.current.read().await.clone().ok_or(Error::Invalid {
            reason: "replica is stopped",
        })?;
        let response = service.handle(&peer, frame).await?;
        if frame.kind == i32::from(FrameKind::RaftAppend)
            && self.lose_append_reply.load(Ordering::Acquire)
        {
            return Ok(None);
        }
        Ok(Some(response))
    }
}

struct Node {
    raft: Raft,
    log: DiskLog<State>,
    state: DiskStateMachine<State>,
}
struct Cluster {
    directory: Directory,
    cluster: Id,
    nodes: BTreeMap<u64, Node>,
    pools: BTreeMap<u64, Arc<PeerPool>>,
    services: BTreeMap<u64, Arc<Service>>,
    servers: Vec<tokio::task::JoinHandle<tidemq::Result<()>>>,
    isolated: Arc<RwLock<BTreeSet<u64>>>,
    lose_append_reply: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
    config: Arc<openraft::Config>,
}

impl Cluster {
    async fn new(ids: &[u64]) -> Self {
        let pki = Pki::new(ids);
        let cluster = Id::random().unwrap();
        let grants = pki.grants(cluster);
        let mut listeners = BTreeMap::new();
        let mut budgets = BTreeMap::new();
        for id in ids {
            let budget = MemoryBudget::new(&MemoryConfig::default()).unwrap();
            let server = Server::bind(
                "127.0.0.1:0".parse().unwrap(),
                ServerTls {
                    identity: pki.identity(*id),
                    client_trust_roots: Some(Bytes::from(pki.ca.pem())),
                },
                ServerAuthorization::Internal(grants.clone()),
                TransportConfig::default(),
                budget.clone(),
            )
            .unwrap();
            listeners.insert(*id, server);
            budgets.insert(*id, budget);
        }
        let addresses: BTreeMap<u64, SocketAddr> = listeners
            .iter()
            .map(|(id, server)| (*id, server.local_addr()))
            .collect();
        let isolated = Arc::new(RwLock::new(BTreeSet::new()));
        let lose_append_reply = Arc::new(AtomicBool::new(false));
        let (shutdown, _) = watch::channel(false);
        let mut this = Self {
            directory: Directory::new().await,
            cluster,
            nodes: BTreeMap::new(),
            pools: BTreeMap::new(),
            services: BTreeMap::new(),
            servers: Vec::new(),
            isolated: isolated.clone(),
            lose_append_reply: lose_append_reply.clone(),
            shutdown,
            config: Arc::new(
                openraft::Config {
                    cluster_name: "tidemq-quic-rf3".into(),
                    heartbeat_interval: 50,
                    election_timeout_min: 250,
                    election_timeout_max: 500,
                    enable_elect: true,
                    snapshot_policy: SnapshotPolicy::Never,
                    snapshot_max_chunk_size: 512,
                    max_payload_entries: 128,
                    max_in_snapshot_log_to_keep: 0,
                    replication_lag_threshold: 8,
                    ..Default::default()
                }
                .validate()
                .unwrap(),
            ),
        };
        for (id, server) in listeners {
            let budget = budgets.get(&id).unwrap().clone();
            let client = Arc::new(
                Client::bind(
                    "127.0.0.1:0".parse().unwrap(),
                    ClientTls {
                        trust_roots: Bytes::from(pki.ca.pem()),
                        identity: Some(pki.identity(id)),
                        expected_nodes: grants.clone(),
                    },
                    TransportConfig::default(),
                    budget.clone(),
                )
                .unwrap(),
            );
            let peers = addresses
                .iter()
                .map(|(peer, address)| PeerTarget {
                    address: *address,
                    server_name: format!("node-{peer}.local"),
                    local_role: "storage".into(),
                    remote_role: "storage".into(),
                    cluster_id: Some(cluster),
                    node_id: Some(*peer),
                })
                .collect();
            let pool = Arc::new(
                PeerPool::new(
                    client,
                    ClientCredentials::Internal {
                        session_id: Id::random().unwrap(),
                        cluster_id: cluster,
                        node_id: id,
                        role: "storage".into(),
                    },
                    peers,
                    NetworkConfig::default(),
                    budget.clone(),
                    None,
                )
                .unwrap(),
            );
            this.pools.insert(id, pool);
            let service = Arc::new(Service {
                node: id,
                current: RwLock::new(None),
                isolated: isolated.clone(),
                lose_append_reply: lose_append_reply.clone(),
            });
            this.services.insert(id, service.clone());
            this.servers.push(tokio::spawn(tidemq::server::run(
                server,
                service,
                DispatchConfig::default(),
                budget,
                this.shutdown.subscribe(),
            )));
            this.start(id).await;
        }
        this
    }

    async fn start(&mut self, id: u64) {
        let disk = DiskStore::<State>::open(
            &self.directory.path().join(format!("node-{id}")),
            identity(self.cluster, GROUP, id),
            StorageOptions::default(),
            None,
        )
        .await
        .unwrap();
        let factory = QuicNetworkFactory::new(self.pools.get(&id).unwrap().clone(), GROUP).unwrap();
        let raft = Raft::new(
            id,
            self.config.clone(),
            factory,
            disk.log.clone(),
            disk.state_machine.clone(),
        )
        .await
        .unwrap();
        let service = RaftService::new(
            raft.clone(),
            self.cluster,
            GROUP,
            GroupRole::Data,
            BTreeSet::from([1, 2, 3]),
            ProtocolLimits::default(),
        )
        .unwrap();
        *self.services.get(&id).unwrap().current.write().await = Some(Arc::new(service));
        self.nodes.insert(
            id,
            Node {
                raft,
                log: disk.log,
                state: disk.state_machine,
            },
        );
    }
    fn node(&self, id: u64) -> &Node {
        self.nodes.get(&id).unwrap()
    }
    async fn initialize(&self) {
        self.node(1)
            .raft
            .initialize(BTreeMap::from([
                (1, BasicNode::new("configured-node-1")),
                (2, BasicNode::new("configured-node-2")),
                (3, BasicNode::new("configured-node-3")),
            ]))
            .await
            .unwrap();
        self.node(1)
            .raft
            .wait(Some(WAIT))
            .current_leader(1, "QUIC RF3 elects the initial leader")
            .await
            .unwrap();
    }
    async fn applied(&self, ids: &[u64], index: u64) {
        for id in ids {
            self.node(*id)
                .raft
                .wait(Some(WAIT))
                .applied_index_at_least(Some(index), "QUIC replica applies committed state")
                .await
                .unwrap();
        }
    }
    async fn stop(&mut self, id: u64) {
        *self.services.get(&id).unwrap().current.write().await = None;
        let node = self.nodes.remove(&id).unwrap();
        node.raft.shutdown().await.unwrap();
        node.log.close().await.unwrap();
    }
    async fn shutdown(mut self) {
        self.shutdown.send(true).unwrap();
        for server in self.servers.drain(..) {
            server.await.unwrap().unwrap();
        }
        let ids: Vec<_> = self.nodes.keys().copied().collect();
        for id in ids {
            self.stop(id).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_quic_rf3_rejects_isolated_writes_and_recovers_from_disk() {
    if std::env::var_os("TIDEMQ_TEST_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("tidemq=debug,openraft=warn")
            .with_test_writer()
            .try_init();
    }
    let mut cluster = Cluster::new(&[1, 2, 3]).await;
    cluster.initialize().await;
    let first = cluster
        .node(1)
        .raft
        .client_write(Command::Append {
            epoch: 0,
            request: 1,
            value: 11,
        })
        .await
        .unwrap();
    assert_eq!(first.data, Response::Appended { offset: 0 });
    cluster.applied(&[1, 2, 3], first.log_id.index).await;
    cluster.isolated.write().await.insert(1);
    let unknown = tokio::time::timeout(
        Duration::from_millis(300),
        cluster.node(1).raft.client_write(Command::Append {
            epoch: 0,
            request: 2,
            value: 22,
        }),
    )
    .await;
    assert!(!matches!(unknown, Ok(Ok(_))));
    assert_eq!(cluster.node(1).state.application().await.next_offset, 1);
    let leader = tokio::time::timeout(WAIT, async {
        loop {
            let left = cluster.node(2).raft.metrics().borrow().current_leader;
            let right = cluster.node(3).raft.metrics().borrow().current_leader;
            if left == right && matches!(left, Some(2 | 3)) {
                return left.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let second = cluster
        .node(leader)
        .raft
        .client_write(Command::Append {
            epoch: 0,
            request: 3,
            value: 33,
        })
        .await
        .unwrap();
    assert_eq!(second.data, Response::Appended { offset: 1 });
    cluster.applied(&[2, 3], second.log_id.index).await;
    cluster.stop(1).await;
    cluster.isolated.write().await.remove(&1);
    cluster.start(1).await;
    cluster.applied(&[1], second.log_id.index).await;
    assert_eq!(
        cluster.node(1).state.application().await,
        cluster.node(2).state.application().await
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_quic_bulk_snapshot_and_lost_replies_preserve_original_offsets() {
    let cluster = Cluster::new(&[1, 2, 3, 4]).await;
    cluster.initialize().await;
    cluster.lose_append_reply.store(true, Ordering::Release);
    let command = Command::Append {
        epoch: 0,
        request: 7,
        value: 77,
    };
    let unknown = tokio::time::timeout(
        Duration::from_millis(250),
        cluster.node(1).raft.client_write(command.clone()),
    )
    .await;
    assert!(!matches!(unknown, Ok(Ok(_))));
    cluster.lose_append_reply.store(false, Ordering::Release);
    let retry = tokio::time::timeout(WAIT, cluster.node(1).raft.client_write(command))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retry.data, Response::Appended { offset: 0 });
    cluster.applied(&[1, 2, 3], retry.log_id.index).await;
    cluster.node(1).raft.trigger().snapshot().await.unwrap();
    cluster
        .node(1)
        .raft
        .wait(Some(WAIT))
        .snapshot(retry.log_id, "Snapshot is durable before QUIC transfer")
        .await
        .unwrap();
    cluster
        .node(1)
        .raft
        .trigger()
        .purge_log(retry.log_id.index)
        .await
        .unwrap();
    cluster
        .node(1)
        .raft
        .wait(Some(WAIT))
        .purged(Some(retry.log_id), "Only snapshotted logs are purged")
        .await
        .unwrap();
    cluster
        .node(1)
        .raft
        .add_learner(4, BasicNode::new("configured-node-4"), true)
        .await
        .unwrap();
    let changed = cluster
        .node(1)
        .raft
        .change_membership(BTreeSet::from([1, 2, 4]), false)
        .await
        .unwrap();
    cluster.applied(&[1, 2, 4], changed.log_id.index).await;
    assert_eq!(
        cluster.node(4).state.application().await,
        cluster.node(1).state.application().await
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_raft_rpc_rejects_forged_sender_group_and_nonempty_control_body() {
    let cluster = Cluster::new(&[1, 2, 3]).await;
    let service = cluster
        .services
        .get(&2)
        .unwrap()
        .current
        .read()
        .await
        .clone()
        .unwrap();
    let id = Id::random().unwrap();
    let peer = Authorization::Internal(NodeGrant {
        certificate_pin: [1; 32],
        certificate_name: "node-1.local".into(),
        cluster_id: cluster.cluster,
        node_id: 1,
        role: "storage".into(),
        allowed_groups: vec![GROUP],
        allowed_classes: vec![TrafficClass::Control],
    });
    let make = |group, sender, entries| {
        let rpc = openraft::raft::AppendEntriesRequest::<support::Config> {
            vote: openraft::Vote::new_committed(5, sender),
            prev_log_id: None,
            entries,
            leader_commit: None,
        };
        let mut encoded = Vec::new();
        postcard::to_io(&rpc, &mut encoded).unwrap();
        new_frame(
            FrameKind::RaftAppend,
            TrafficClass::Control,
            id,
            id,
            &RaftRequest {
                group_id: group,
                rpc_id: Bytes::copy_from_slice(id.as_bytes()),
                encoded_rpc: Bytes::from(encoded),
            },
            1000,
            &ProtocolLimits::default(),
        )
        .unwrap()
    };
    assert!(
        service
            .handle(&peer, &make(GROUP + 1, 1, vec![]))
            .await
            .is_err()
    );
    assert!(
        service
            .handle(&peer, &make(GROUP, 3, vec![]))
            .await
            .is_err()
    );
    assert!(
        service
            .handle(
                &peer,
                &make(GROUP, 1, vec![support::entry(5, 0, Command::Fence(1))])
            )
            .await
            .is_err()
    );
    assert!(
        service
            .handle(&Authorization::Server, &make(GROUP, 1, vec![]))
            .await
            .is_err()
    );
    assert_eq!(cluster.node(2).state.application().await.next_offset, 0);
    cluster.shutdown().await;
}
