#![allow(dead_code, unused_imports)] // Shared real-QUIC fixtures serve separate integration-test binaries.

use crate::support;

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
use tidemq::replication::{
    Application, DiskLog, DiskStateMachine, DiskStore, Raft as GenericRaft, StorageIdentity,
    StorageOptions,
};
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

pub(crate) const WAIT: Duration = Duration::from_secs(12);
pub(crate) const GROUP: u64 = 2;

pub(crate) struct Leaf {
    cert: Certificate,
    key: KeyPair,
    name: String,
}
pub(crate) struct Pki {
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
    fn grants(&self, cluster: Id, group: u64, role: &str) -> Vec<NodeGrant> {
        self.leaves
            .iter()
            .map(|(id, leaf)| NodeGrant {
                certificate_pin: NodeGrant::certificate_pin(leaf.cert.der()),
                certificate_name: leaf.name.clone(),
                cluster_id: cluster,
                node_id: *id,
                role: role.into(),
                allowed_groups: vec![group],
                allowed_classes: vec![
                    TrafficClass::Control,
                    TrafficClass::Replication,
                    TrafficClass::Bulk,
                ],
            })
            .collect()
    }
}

pub(crate) struct Service<A: Application> {
    node: u64,
    pub(crate) current: RwLock<Option<Arc<RaftService<A>>>>,
    pub(crate) isolated: Arc<RwLock<BTreeSet<u64>>>,
    pub(crate) lose_append_reply: Arc<AtomicBool>,
    pub(crate) pause_final_membership: Arc<AtomicBool>,
}

impl<A: Application> WireService for Service<A> {
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
        if frame.kind == i32::from(FrameKind::RaftAppend)
            && self.pause_final_membership.load(Ordering::Acquire)
        {
            let envelope: RaftRequest = tidemq_protocol::framing::decode_payload(
                frame.payload.clone(),
                &ProtocolLimits::default(),
            )
            .unwrap();
            let request: openraft::raft::AppendEntriesRequest<tidemq::replication::TypeConfig<A>> =
                postcard::from_bytes(&envelope.encoded_rpc).unwrap();
            if request.entries.iter().any(|entry| matches!(&entry.payload, openraft::EntryPayload::Membership(membership)
                if membership.get_joint_config().len() == 1 && membership.voter_ids().collect::<BTreeSet<_>>() == BTreeSet::from([1, 2, 4]))) {
                return Err(Error::Invalid { reason: "injected final membership replication failure" });
            }
        }
        let current = self.current.read().await;
        let service = current.as_ref().ok_or(Error::Invalid {
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

pub(crate) struct Node<A: Application> {
    pub(crate) raft: GenericRaft<A>,
    pub(crate) log: DiskLog<A>,
    pub(crate) state: DiskStateMachine<A>,
}
pub(crate) struct Cluster<A: Application = State> {
    directory: Directory,
    pub(crate) cluster: Id,
    pub(crate) group: u64,
    role: GroupRole,
    pub(crate) nodes: BTreeMap<u64, Node<A>>,
    pub(crate) pools: BTreeMap<u64, Arc<PeerPool>>,
    pub(crate) services: BTreeMap<u64, Arc<Service<A>>>,
    servers: Vec<tokio::task::JoinHandle<tidemq::Result<()>>>,
    pub(crate) isolated: Arc<RwLock<BTreeSet<u64>>>,
    pub(crate) lose_append_reply: Arc<AtomicBool>,
    pub(crate) pause_final_membership: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
    config: Arc<openraft::Config>,
}

impl Cluster<State> {
    pub(crate) async fn new(ids: &[u64]) -> Self {
        Self::with_identity(ids, Id::random().unwrap(), GROUP, GroupRole::Data).await
    }
}

impl<A: Application> Cluster<A> {
    pub(crate) async fn with_identity(
        ids: &[u64],
        cluster: Id,
        group: u64,
        role: GroupRole,
    ) -> Self {
        Self::with_budget(ids, cluster, group, role, None).await
    }
    pub(crate) async fn with_budget(
        ids: &[u64],
        cluster: Id,
        group: u64,
        role: GroupRole,
        repair: Option<tidemq::scaling::RepairBudget>,
    ) -> Self {
        let pki = Pki::new(ids);
        let role_name = match role {
            GroupRole::Data => "storage",
            GroupRole::Metadata => "metadata",
        };
        let grants = pki.grants(cluster, group, role_name);
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
        let pause_final_membership = Arc::new(AtomicBool::new(false));
        let (shutdown, _) = watch::channel(false);
        let mut this = Self {
            directory: Directory::new().await,
            cluster,
            group,
            role,
            nodes: BTreeMap::new(),
            pools: BTreeMap::new(),
            services: BTreeMap::new(),
            servers: Vec::new(),
            isolated: isolated.clone(),
            lose_append_reply: lose_append_reply.clone(),
            pause_final_membership: pause_final_membership.clone(),
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
                    local_role: role_name.into(),
                    remote_role: role_name.into(),
                    cluster_id: Some(cluster),
                    node_id: Some(*peer),
                })
                .collect();
            let pool = PeerPool::new(
                client,
                ClientCredentials::Internal {
                    session_id: Id::random().unwrap(),
                    cluster_id: cluster,
                    node_id: id,
                    role: role_name.into(),
                },
                peers,
                NetworkConfig::default(),
                budget.clone(),
                None,
            )
            .unwrap();
            let pool = if id == 1 {
                if let Some(repair) = &repair {
                    pool.with_repair_budget(repair.clone())
                } else {
                    pool
                }
            } else {
                pool
            };
            this.pools.insert(id, Arc::new(pool));
            let service = Arc::new(Service {
                node: id,
                current: RwLock::new(None),
                isolated: isolated.clone(),
                lose_append_reply: lose_append_reply.clone(),
                pause_final_membership: pause_final_membership.clone(),
            });
            this.services.insert(id, service.clone());
            this.servers.push(tokio::spawn(tidemq::server::run(
                server,
                service,
                DispatchConfig::default(),
                budget,
                Arc::new(tidemq::server::DispatchMetrics::default()),
                this.shutdown.subscribe(),
            )));
            this.start(id).await;
        }
        this
    }

    pub(crate) async fn start(&mut self, id: u64) {
        let disk = DiskStore::<A>::open(
            &self.directory.path().join(format!("node-{id}")),
            StorageIdentity::new::<A>(self.cluster, self.group, id),
            StorageOptions::default(),
            None,
        )
        .await
        .unwrap();
        let factory =
            QuicNetworkFactory::new(self.pools.get(&id).unwrap().clone(), self.group).unwrap();
        let raft = GenericRaft::<A>::new(
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
            self.group,
            self.role,
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
    pub(crate) fn node(&self, id: u64) -> &Node<A> {
        self.nodes.get(&id).unwrap()
    }
    pub(crate) async fn initialize(&self) {
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
    pub(crate) async fn applied(&self, ids: &[u64], index: u64) {
        for id in ids {
            self.node(*id)
                .raft
                .wait(Some(WAIT))
                .applied_index_at_least(Some(index), "QUIC replica applies committed state")
                .await
                .unwrap();
        }
    }
    pub(crate) async fn stop(&mut self, id: u64) {
        let node = self.nodes.remove(&id).unwrap();
        node.raft.shutdown().await.unwrap();
        // A write guard waits for every in-flight RPC to release the old replica.
        *self.services.get(&id).unwrap().current.write().await = None;
        node.log.close().await.unwrap();
        drop(node);
        let probe = tokio::fs::OpenOptions::new()
            .write(true)
            .open(
                self.directory
                    .path()
                    .join(format!("node-{id}/.writer-lock")),
            )
            .await
            .unwrap()
            .into_std()
            .await;
        tokio::time::timeout(WAIT, async {
            loop {
                match probe.try_lock() {
                    Ok(()) => {
                        probe.unlock().unwrap();
                        break;
                    }
                    Err(std::fs::TryLockError::WouldBlock) => {
                        tokio::time::sleep(Duration::from_millis(2)).await
                    }
                    Err(error) => panic!("Replica ownership probe failed: {error}"),
                }
            }
        })
        .await
        .expect("Detached replication tasks release the directory ownership lock");
    }
    pub(crate) async fn shutdown(mut self) {
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
