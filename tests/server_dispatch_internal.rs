use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tidemq::budget::MemoryBudget;
use tidemq::config::MemoryConfig;
use tidemq::server::{DispatchConfig, SubmissionOrder, WireService};
use tidemq::transport::rpc::{RpcClient, RpcConfig, RpcResponse};
use tidemq::transport::{
    Authorization, Client, ClientCredentials, ClientTls, PeerTarget, Server, ServerAuthorization,
    ServerTls, TransportConfig, TransportMetrics,
};
use tidemq_protocol::{
    Id, ProtocolLimits,
    common::{CancelRequest, Frame, FrameKind, TrafficClass},
    framing::{decode_payload, new_frame},
    internal::{RaftRequest, RaftResult},
};
use tokio::sync::{Semaphore, watch};
use tokio::time::Instant;

#[path = "replication_support/quic.rs"]
mod quic;
#[path = "replication_support/mod.rs"]
mod support;

struct HeldRaft {
    started: Semaphore,
    release: Semaphore,
    cancelled: Semaphore,
    active: AtomicUsize,
    maximum: AtomicUsize,
}

impl Default for HeldRaft {
    fn default() -> Self {
        Self {
            started: Semaphore::new(0),
            release: Semaphore::new(0),
            cancelled: Semaphore::new(0),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        }
    }
}

struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl WireService for HeldRaft {
    type Error = tidemq_protocol::Error;

    async fn handle(
        &self,
        peer: Authorization,
        frame: &Frame,
        _deadline: Instant,
        order: Option<SubmissionOrder>,
    ) -> Result<Option<Frame>, Self::Error> {
        assert!(matches!(peer, Authorization::Internal(_)));
        drop(order);
        if frame.kind == i32::from(FrameKind::Cancel) {
            let _: CancelRequest =
                decode_payload(frame.payload.clone(), &ProtocolLimits::default())?;
            self.cancelled.add_permits(1);
            return Ok(None);
        }
        assert_eq!(frame.kind, i32::from(FrameKind::RaftAppend));
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        let _active = Active(&self.active);
        let request: RaftRequest =
            decode_payload(frame.payload.clone(), &ProtocolLimits::default())?;
        if request.encoded_rpc == Bytes::from_static(b"held") {
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        Ok(Some(new_frame(
            FrameKind::RaftResult,
            TrafficClass::Control,
            Id::from_slice(&frame.request_id)?,
            Id::from_slice(&frame.operation_id)?,
            &RaftResult {
                rpc_id: frame.request_id.clone(),
                encoded_result: Bytes::from(postcard::to_allocvec(&true).unwrap()),
            },
            1000,
            &ProtocolLimits::default(),
        )?))
    }
}

struct Fixture {
    service: Arc<HeldRaft>,
    rpc: RpcClient,
    client: Client,
    transport: Arc<TransportMetrics>,
    budget: MemoryBudget,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<tidemq::Result<()>>,
}

impl Fixture {
    async fn new() -> Self {
        let pki = quic::Pki::new(&[1, 2]);
        let cluster = Id::new_v7().unwrap();
        let mut grants = pki.grants(cluster, 1, "combined");
        for grant in &mut grants {
            grant.allowed_groups.push(2);
        }
        let budget = MemoryBudget::new(&MemoryConfig::default()).unwrap();
        let server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            ServerTls {
                identity: pki.identity(2),
                client_trust_roots: Some(pki.roots()),
            },
            ServerAuthorization::Internal(grants.clone()),
            TransportConfig::default(),
            budget.clone(),
        )
        .unwrap();
        let address = server.local_addr();
        let transport = server.metrics.clone();
        let client = Client::bind(
            "127.0.0.1:0".parse().unwrap(),
            ClientTls {
                trust_roots: pki.roots(),
                identity: Some(pki.identity(1)),
                expected_nodes: grants,
            },
            TransportConfig::default(),
            MemoryBudget::new(&MemoryConfig::default()).unwrap(),
        )
        .unwrap();
        let service = Arc::new(HeldRaft::default());
        let (stop, receiver) = watch::channel(false);
        let task = tokio::spawn(tidemq::server::run(
            server,
            service.clone(),
            DispatchConfig::default(),
            budget.clone(),
            Arc::default(),
            receiver,
        ));
        let session = client
            .connect(
                PeerTarget {
                    address,
                    server_name: "node-2.local".into(),
                    local_role: "combined".into(),
                    remote_role: "combined".into(),
                    cluster_id: Some(cluster),
                    node_id: Some(2),
                },
                &ClientCredentials::Internal {
                    session_id: Id::new_v7().unwrap(),
                    cluster_id: cluster,
                    node_id: 1,
                    role: "combined".into(),
                },
                TrafficClass::Control,
                Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap();
        let rpc = RpcClient::from_session(session, RpcConfig::default())
            .await
            .unwrap();
        assert_eq!(rpc.lane_count(), 2);
        Self {
            service,
            rpc,
            client,
            transport,
            budget,
            stop,
            task,
        }
    }

    async fn close(self) {
        self.service.release.add_permits(2);
        self.rpc
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await;
        self.client
            .close(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        let _ = self.stop.send(true);
        self.task.await.unwrap().unwrap();
        assert_eq!(self.service.active.load(Ordering::SeqCst), 0);
        assert!(self.service.maximum.load(Ordering::SeqCst) <= 2);
        assert_eq!(
            self.budget.available_bytes(),
            usize::try_from(MemoryConfig::default().total_bytes).unwrap()
        );
    }
}

fn frame(group: u64, held: bool, milliseconds: u64) -> Frame {
    let id = Id::new_v7().unwrap();
    new_frame(
        FrameKind::RaftAppend,
        TrafficClass::Control,
        id,
        id,
        &RaftRequest {
            group_id: group,
            rpc_id: Bytes::copy_from_slice(id.as_bytes()),
            encoded_rpc: Bytes::from_static(if held { b"held" } else { b"live" }),
        },
        milliseconds,
        &ProtocolLimits::default(),
    )
    .unwrap()
}

async fn wait_received(fixture: &Fixture, previous: u64) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while fixture.transport.snapshot().frames_received_total <= previous {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn occupy_slots(fixture: &Fixture) -> [RpcResponse; 2] {
    let until = Instant::now() + Duration::from_secs(3);
    let first = fixture
        .rpc
        .submit_affinity(frame(1, true, 3000), until, 0)
        .await
        .unwrap();
    let second = fixture
        .rpc
        .submit_affinity(frame(2, true, 3000), until, 1)
        .await
        .unwrap();
    tokio::time::timeout_at(until, fixture.service.started.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    [first, second]
}

async fn wait_core_budget(fixture: &Fixture, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while fixture.budget.available_core_bytes() != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_core_waits_for_existing_slots_within_the_original_deadline() {
    let fixture = Fixture::new().await;
    let until = Instant::now() + Duration::from_secs(2);
    let first = fixture
        .rpc
        .submit(frame(1, true, 2000), until)
        .await
        .unwrap();
    let second = fixture
        .rpc
        .submit(frame(2, true, 2000), until)
        .await
        .unwrap();
    fixture
        .service
        .started
        .acquire_many(2)
        .await
        .unwrap()
        .forget();
    let before = fixture.transport.snapshot().frames_received_total;
    let deadline = Instant::now() + Duration::from_millis(100);
    let request = frame(2, false, 100);
    let request_id = request.request_id.clone();
    let third = fixture.rpc.submit(request, deadline).await.unwrap();
    wait_received(&fixture, before).await;
    let response = third.receive();
    tokio::pin!(response);
    let early = tokio::select! {
        result = &mut response => Some(result),
        _ = tokio::time::sleep(Duration::from_millis(5)) => None,
    };
    fixture.service.release.add_permits(1);
    let result = match early {
        Some(result) => result,
        None => response.await,
    }
    .unwrap();
    let kind = result.frame().kind;
    let echoed = result.frame().request_id.clone();
    let rejection = (kind == i32::from(FrameKind::Error)).then(|| {
        decode_payload::<tidemq_protocol::common::ErrorReply>(
            result.frame().payload.clone(),
            &ProtocolLimits::default(),
        )
        .unwrap()
    });
    fixture.service.release.add_permits(1);
    first.receive().await.unwrap();
    second.receive().await.unwrap();
    drop(result);
    fixture.close().await;
    assert_eq!(
        kind,
        i32::from(FrameKind::RaftResult),
        "A free slot within the original deadline must satisfy the original Raft request: {rejection:?}"
    );
    assert_eq!(echoed, request_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_core_lanes_release_expired_waiters_and_deliver_cancel() {
    let fixture = Fixture::new().await;
    let owned = occupy_slots(&fixture).await;
    let before_budget = fixture.budget.available_core_bytes();
    let before_frames = fixture.transport.snapshot().frames_received_total;
    let until = Instant::now() + Duration::from_millis(100);
    let first = fixture
        .rpc
        .submit_affinity(frame(1, false, 100), until, 0)
        .await
        .unwrap();
    let second = fixture
        .rpc
        .submit_affinity(frame(2, false, 100), until, 1)
        .await
        .unwrap();
    wait_received(&fixture, before_frames + 1).await;
    assert!(fixture.budget.available_core_bytes() < before_budget);
    let id = Id::new_v7().unwrap();
    fixture
        .rpc
        .notify(
            new_frame(
                FrameKind::Cancel,
                TrafficClass::Control,
                id,
                id,
                &CancelRequest {
                    request_id: Bytes::copy_from_slice(id.as_bytes()),
                },
                1000,
                &ProtocolLimits::default(),
            )
            .unwrap(),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert!(matches!(
        first.receive().await,
        Err(tidemq::transport::rpc::Error::Deadline { .. })
    ));
    assert!(matches!(
        second.receive().await,
        Err(tidemq::transport::rpc::Error::Deadline { .. })
    ));
    tokio::time::timeout(Duration::from_secs(1), fixture.service.cancelled.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    wait_core_budget(&fixture, before_budget).await;
    assert_eq!(fixture.service.active.load(Ordering::SeqCst), 2);
    fixture.service.release.add_permits(2);
    for response in owned {
        assert_eq!(
            response.receive().await.unwrap().frame().kind,
            i32::from(FrameKind::RaftResult)
        );
    }
    fixture.close().await;
}

async fn close_releases_waiter(server_shutdown: bool) {
    let fixture = Fixture::new().await;
    let owned = occupy_slots(&fixture).await;
    let before_budget = fixture.budget.available_core_bytes();
    let before_frames = fixture.transport.snapshot().frames_received_total;
    let waiting = fixture
        .rpc
        .submit_affinity(
            frame(2, false, 3000),
            Instant::now() + Duration::from_secs(3),
            0,
        )
        .await
        .unwrap();
    wait_received(&fixture, before_frames).await;
    assert!(fixture.budget.available_core_bytes() < before_budget);
    if server_shutdown {
        fixture.stop.send(true).unwrap();
    } else {
        fixture
            .rpc
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
    }
    // The waiter releases before its three-second deadline; already owned handlers still run.
    wait_core_budget(&fixture, before_budget).await;
    assert_eq!(fixture.service.active.load(Ordering::SeqCst), 2);
    drop(waiting);
    drop(owned);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_releases_waiting_frames_without_canceling_owned_handlers() {
    close_releases_waiter(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_close_releases_waiting_frames_without_canceling_owned_handlers() {
    close_releases_waiter(false).await;
}
