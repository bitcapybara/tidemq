use super::*;

use crate::config::MemoryConfig;
use crate::transport::{
    ClientTls, FramedStream, Server, ServerAuthorization, ServerTls, TlsIdentity, TokenGrant,
    TransportConfig,
};
use std::collections::VecDeque;

struct Peer {
    pool: PeerPool,
    replies: Arc<Mutex<VecDeque<ErrorReply>>>,
    received: Arc<Mutex<Vec<(Bytes, u64)>>>,
    stop: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl Peer {
    async fn new() -> Self {
        let key = rcgen::KeyPair::generate().unwrap();
        let certificate = rcgen::CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let budget = || MemoryBudget::new(&MemoryConfig::default()).unwrap();
        let token = "raft-rpc-rejection-test-token-at-least-32-characters";
        let mut server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            ServerTls {
                identity: TlsIdentity::new(
                    Bytes::from(certificate.pem()),
                    Bytes::from(key.serialize_pem()),
                )
                .unwrap(),
                client_trust_roots: None,
            },
            ServerAuthorization::External(vec![
                TokenGrant::new(
                    token,
                    "test".into(),
                    "test".into(),
                    vec![FrameKind::Diagnose],
                )
                .unwrap(),
            ]),
            TransportConfig::default(),
            budget(),
        )
        .unwrap();
        let target = PeerTarget {
            address: server.local_addr(),
            server_name: "localhost".into(),
            local_role: "sdk".into(),
            remote_role: "gateway".into(),
            cluster_id: None,
            node_id: None,
        };
        let client = Arc::new(
            Client::bind(
                "127.0.0.1:0".parse().unwrap(),
                ClientTls {
                    trust_roots: Bytes::from(certificate.pem()),
                    identity: None,
                    expected_nodes: Vec::new(),
                },
                TransportConfig::default(),
                budget(),
            )
            .unwrap(),
        );
        let replies = Arc::new(Mutex::new(VecDeque::<ErrorReply>::new()));
        let received = Arc::new(Mutex::new(Vec::new()));
        let reply_plan = replies.clone();
        let observed = received.clone();
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            let session = server.accept().await.unwrap();
            let mut handlers = tokio::task::JoinSet::new();
            for stream in session.take_initial_streams().await {
                handlers.spawn(serve_replies(
                    stream,
                    reply_plan.clone(),
                    observed.clone(),
                    stopped.clone(),
                ));
            }
            loop {
                tokio::select! {
                    _ = stopped.changed() => break,
                    stream = session.accept() => {
                        let Some(stream) = stream else { break; };
                        handlers.spawn(serve_replies(
                            stream,
                            reply_plan.clone(),
                            observed.clone(),
                            stopped.clone(),
                        ));
                    }
                    _ = handlers.join_next(), if !handlers.is_empty() => {}
                }
            }
            handlers.abort_all();
            while handlers.join_next().await.is_some() {}
            session.close();
            server
                .shutdown_by(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
        });
        // This authenticated transport peer injects envelopes; runtime regressions cover internal mTLS.
        let pool = PeerPool {
            client,
            peers: BTreeMap::from([(2, target)]),
            credentials: ClientCredentials::External {
                session_id: Id::new_v7().unwrap(),
                access_token: token.into(),
            },
            slots: Mutex::new(BTreeMap::new()),
            config: NetworkConfig::default(),
            budget: budget(),
            metrics: None,
            repair: None,
        };
        Self {
            pool,
            replies,
            received,
            stop,
            task,
        }
    }

    fn frame(&self, deadline: Instant) -> Frame {
        let id = Id::new_v7().unwrap();
        self.pool
            .frame(
                FrameKind::Diagnose,
                TrafficClass::Control,
                id,
                &tidemq_protocol::internal::RaftRequest {
                    group_id: 1,
                    rpc_id: Bytes::copy_from_slice(id.as_bytes()),
                    encoded_rpc: codec::encode(&true, 64).unwrap(),
                },
                deadline,
                self.pool.reserve(TrafficClass::Control).unwrap(),
            )
            .unwrap()
    }

    async fn close(mut self) {
        let _ = self.stop.send(true);
        self.pool
            .close(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        (&mut self.task).await.unwrap();
    }
}

async fn serve_replies(
    stream: FramedStream,
    replies: Arc<Mutex<VecDeque<ErrorReply>>>,
    observed: Arc<Mutex<Vec<(Bytes, u64)>>>,
    mut stopped: tokio::sync::watch::Receiver<bool>,
) {
    let (mut sender, mut receiver) = stream.split();
    loop {
        let received = tokio::select! {
            _ = stopped.changed() => break,
            received = receiver.receive_next(Duration::from_secs(3)) => received,
        };
        let Ok(Some(received)) = received else {
            break;
        };
        let request = received.frame();
        observed
            .lock()
            .unwrap()
            .push((request.request_id.clone(), request.remaining_timeout_ms));
        let id = Id::from_slice(&request.request_id).unwrap();
        let error = replies.lock().unwrap().pop_front();
        let response = match error {
            Some(error) => new_frame(
                FrameKind::Error,
                TrafficClass::Control,
                id,
                id,
                &error,
                1000,
                &ProtocolLimits::default(),
            )
            .unwrap(),
            None => new_frame(
                FrameKind::RaftResult,
                TrafficClass::Control,
                id,
                id,
                &RaftResult {
                    rpc_id: request.request_id.clone(),
                    encoded_result: codec::encode(&true, 64).unwrap(),
                },
                1000,
                &ProtocolLimits::default(),
            )
            .unwrap(),
        };
        sender
            .send(&response, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn rejection(code: ErrorCode, outcome: Outcome, retryable: bool) -> ErrorReply {
    ErrorReply {
        code: code.into(),
        outcome: outcome.into(),
        retryable,
        ..Default::default()
    }
}

#[tokio::test]
async fn quota_rejection_keeps_shared_rpc_open_but_identity_corruption_closes_it() {
    let peer = Peer::new().await;
    let rpc = peer
        .pool
        .connection(
            2,
            TrafficClass::Control,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    let id = Id::new_v7().unwrap();
    let frame = new_frame(
        FrameKind::Error,
        TrafficClass::Control,
        id,
        id,
        &rejection(ErrorCode::QuotaExceeded, Outcome::Rejected, true),
        1000,
        &ProtocolLimits::default(),
    )
    .unwrap();
    let bytes = Bytes::copy_from_slice(id.as_bytes());
    let result = receive_response::<bool>(
        &rpc,
        &frame,
        TrafficClass::Control,
        &bytes,
        &ProtocolLimits::default(),
    )
    .await;
    assert!(matches!(
        result,
        Err(Error::PeerRejected {
            code: ErrorCode::QuotaExceeded,
            outcome: Outcome::Rejected,
            retryable: true
        })
    ));
    assert!(
        !rpc.is_closed(),
        "Valid backpressure must preserve the shared RPC"
    );
    let mut corrupted = frame;
    corrupted.operation_id = Bytes::copy_from_slice(Id::new_v7().unwrap().as_bytes());
    let result = receive_response::<bool>(
        &rpc,
        &corrupted,
        TrafficClass::Control,
        &bytes,
        &ProtocolLimits::default(),
    )
    .await;
    assert!(matches!(result, Err(Error::Invalid { .. })));
    assert!(
        rpc.is_closed(),
        "Identity corruption must still close the shared RPC"
    );
    peer.close().await;
}

#[tokio::test]
async fn quota_retries_preserve_identity_and_never_refresh_the_original_deadline() {
    let peer = Peer::new().await;
    peer.replies.lock().unwrap().extend(std::iter::repeat_n(
        rejection(ErrorCode::QuotaExceeded, Outcome::Rejected, true),
        2,
    ));
    let deadline = Instant::now() + Duration::from_secs(2);
    let frame = peer.frame(deadline);
    let id = frame.request_id.clone();
    assert!(peer.pool.call::<bool>(2, frame, deadline, 1).await.unwrap());
    {
        let received = peer.received.lock().unwrap();
        assert_eq!(received.len(), 3);
        assert!(received.iter().all(|(request, _)| *request == id));
        assert!(received.windows(2).all(|pair| pair[0].1 > pair[1].1));
    }
    peer.received.lock().unwrap().clear();
    peer.replies.lock().unwrap().extend(std::iter::repeat_n(
        rejection(ErrorCode::QuotaExceeded, Outcome::Rejected, true),
        16,
    ));
    let started = Instant::now();
    let deadline = started + Duration::from_millis(100);
    assert!(
        peer.pool
            .call::<bool>(2, peer.frame(deadline), deadline, 1)
            .await
            .is_err()
    );
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(
        peer.received.lock().unwrap().len() < 8,
        "The original deadline must stop the bounded retry loop early"
    );
    peer.close().await;
}

#[tokio::test]
async fn unknown_outcomes_permanent_rejections_and_nonquota_errors_are_not_retried() {
    let peer = Peer::new().await;
    for (code, outcome, retryable) in [
        (ErrorCode::QuotaExceeded, Outcome::Unknown, true),
        (ErrorCode::QuotaExceeded, Outcome::Rejected, false),
        (ErrorCode::Forbidden, Outcome::Rejected, true),
        (ErrorCode::Unavailable, Outcome::Rejected, true),
    ] {
        peer.received.lock().unwrap().clear();
        peer.replies
            .lock()
            .unwrap()
            .push_back(rejection(code, outcome, retryable));
        let deadline = Instant::now() + Duration::from_secs(2);
        assert!(matches!(
            peer.pool
                .call::<bool>(2, peer.frame(deadline), deadline, 1)
                .await,
            Err(Error::PeerRejected { .. })
        ));
        assert_eq!(peer.received.lock().unwrap().len(), 1);
    }
    peer.close().await;
}

#[test]
fn raft_result_inner_identity_and_invalid_error_codes_remain_strict() {
    let limits = ProtocolLimits::default();
    let id = Id::new_v7().unwrap();
    let bytes = Bytes::copy_from_slice(id.as_bytes());
    let frame = new_frame(
        FrameKind::RaftResult,
        TrafficClass::Control,
        id,
        id,
        &RaftResult {
            rpc_id: Bytes::copy_from_slice(Id::new_v7().unwrap().as_bytes()),
            encoded_result: codec::encode(&true, 64).unwrap(),
        },
        1000,
        &limits,
    )
    .unwrap();
    assert!(matches!(
        decode_response::<bool>(&frame, TrafficClass::Control, &bytes, &limits),
        Err(Error::Invalid { .. })
    ));
    for (code, outcome) in [(0, 1), (999, 1), (5, 0), (5, 2)] {
        let frame = new_frame(
            FrameKind::Error,
            TrafficClass::Control,
            id,
            id,
            &ErrorReply {
                code,
                outcome,
                ..Default::default()
            },
            1000,
            &limits,
        )
        .unwrap();
        assert!(matches!(
            decode_response::<bool>(&frame, TrafficClass::Control, &bytes, &limits),
            Err(Error::Invalid { .. })
        ));
    }
}
