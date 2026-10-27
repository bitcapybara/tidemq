//! Real QUIC checks of SDK ownership; this peer models metadata and does not run Raft.
use super::*;
use std::{future::Future, pin::Pin};

use prost::Message;
use rcgen::{CertificateParams, KeyPair};
use tidemq_protocol::{
    ProtocolLimits,
    common::{ErrorCode, ErrorReply, Frame, Outcome},
    framing::{decode_payload, new_frame},
};
use tidemq_transport::{
    Authorization, CredentialGrant, FramedStream, Server, ServerAuthorization, ServerTls, Session,
    TlsIdentity, TokenAuthenticator, TransportConfig,
    budget::{MemoryBudget, MemoryLimits},
};
use tokio::{sync::watch, task::JoinSet};

const TOKEN: &str = "close-rejoin-peer-token-with-at-least-32-characters";
const TOPIC: &str = "close/app/events";
const REJOIN_EPOCH: u64 = 19;

fn id(byte: u8) -> Id {
    Id::from_slice(&[byte; 16]).unwrap()
}

struct Auth;

impl TokenAuthenticator for Auth {
    fn authenticate<'a>(
        &'a self,
        token: &'a str,
        _deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = tidemq_transport::Result<CredentialGrant>> + Send + 'a>> {
        Box::pin(async move {
            if token != TOKEN {
                return Err(tidemq_transport::Error::Authentication {
                    reason: "test credential was rejected",
                });
            }
            Ok(CredentialGrant {
                credential: id(6),
                tenant: Some(id(5)),
                allowed_kinds: vec![
                    FrameKind::GetTopic,
                    FrameKind::GetTopicLayout,
                    FrameKind::ConsumerControl,
                ],
                lease: None,
            })
        })
    }
}

#[derive(Clone, Debug)]
struct Call {
    operation: Id,
    request: public::ConsumerControl,
    client: Id,
    credential: Id,
    timeout_ms: u64,
}

#[derive(Default)]
struct Observed {
    expire: bool,
    original: Option<Call>,
    calls: Vec<Call>,
    rejoined: bool,
    retired: bool,
    delayed_commit: bool,
    foreign_epoch: bool,
}

struct Peer {
    client: Client,
    observed: Arc<Mutex<Observed>>,
    accepted: Arc<Notify>,
    retried: Arc<Notify>,
    reply: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Peer {
    async fn new() -> Self {
        Self::configured(false, Duration::from_secs(5)).await
    }

    async fn configured(delayed_commit: bool, close_timeout: Duration) -> Self {
        let key = KeyPair::generate().unwrap();
        let certificate = CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let ca = Bytes::from(certificate.pem());
        let server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            ServerTls {
                identity: TlsIdentity::new(ca.clone(), Bytes::from(key.serialize_pem())).unwrap(),
                client_trust_roots: None,
            },
            ServerAuthorization::Dynamic(Arc::new(Auth)),
            TransportConfig::default(),
            MemoryBudget::new(MemoryLimits {
                total_bytes: 64 * 1024 * 1024,
                control_reserved_bytes: 8 * 1024 * 1024,
            })
            .unwrap(),
        )
        .unwrap();
        let endpoint = format!("localhost:{}", server.local_addr().port());
        let observed = Arc::new(Mutex::new(Observed {
            delayed_commit,
            ..Default::default()
        }));
        let accepted = Arc::new(Notify::new());
        let retried = Arc::new(Notify::new());
        let (reply, release) = watch::channel(false);
        let task = tokio::spawn(accept(
            server,
            observed.clone(),
            accepted.clone(),
            retried.clone(),
            release,
        ));
        let client = Client::builder(
            endpoint,
            crate::TlsTrust::ca_pem(ca),
            crate::Credentials::token(TOKEN),
        )
        .close_timeout(close_timeout)
        .build()
        .await
        .unwrap();
        Self {
            client,
            observed,
            accepted,
            retried,
            reply,
            task,
        }
    }
}

async fn accept(
    mut server: Server,
    observed: Arc<Mutex<Observed>>,
    accepted: Arc<Notify>,
    retried: Arc<Notify>,
    release: watch::Receiver<bool>,
) {
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            session = server.accept() => {
                let Some(session) = session else { break };
                sessions.spawn(serve_session(session, observed.clone(), accepted.clone(), retried.clone(), release.clone()));
            }
            _ = sessions.join_next(), if !sessions.is_empty() => {},
        }
    }
}

async fn serve_session(
    session: Arc<Session>,
    observed: Arc<Mutex<Observed>>,
    accepted: Arc<Notify>,
    retried: Arc<Notify>,
    release: watch::Receiver<bool>,
) {
    let mut streams = JoinSet::new();
    for stream in session.take_initial_streams().await {
        streams.spawn(serve_stream(
            stream,
            session.clone(),
            observed.clone(),
            accepted.clone(),
            retried.clone(),
            release.clone(),
        ));
    }
    loop {
        tokio::select! {
            stream = session.accept() => {
                let Some(stream) = stream else { break };
                streams.spawn(serve_stream(stream, session.clone(), observed.clone(), accepted.clone(), retried.clone(), release.clone()));
            }
            _ = streams.join_next(), if !streams.is_empty() => {},
        }
    }
}

async fn serve_stream(
    stream: FramedStream,
    session: Arc<Session>,
    observed: Arc<Mutex<Observed>>,
    accepted: Arc<Notify>,
    retried: Arc<Notify>,
    release: watch::Receiver<bool>,
) {
    let (sender, mut receiver) = stream.split();
    let sender = Arc::new(tokio::sync::Mutex::new(sender));
    let mut responses = JoinSet::new();
    loop {
        tokio::select! {
            received = receiver.receive(Instant::now() + Duration::from_secs(15)) => {
                let Ok(Some(received)) = received else { return };
                let (response, hold, delayed) = {
                    let mut state = observed.lock().unwrap();
                    let (response, hold) = handle(received.frame(), &session, &mut state);
                    let request = decode_payload::<public::ConsumerControl>(received.frame().payload.clone(), &ProtocolLimits::default());
                    let join = received.frame().kind == i32::from(FrameKind::ConsumerControl)
                        && request.is_ok_and(|request| request.action == i32::from(public::ConsumerAction::JoinConsumer)
                            && request.member_epoch == REJOIN_EPOCH);
                    (response, hold, join && state.delayed_commit)
                };
                let sender = sender.clone();
                let mut release = release.clone();
                let observed = observed.clone();
                if hold { accepted.notify_one(); }
                if delayed && !hold { retried.notify_one(); }
                responses.spawn(async move {
                    if hold || delayed { let _ = release.wait_for(|released| *released).await; }
                    if delayed { observed.lock().unwrap().rejoined = true; }
                    let _ = sender.lock().await.send(&response, Instant::now() + Duration::from_secs(5)).await;
                    drop(received);
                });
            }
            _ = responses.join_next(), if !responses.is_empty() => {},
        }
    }
}

fn response<M: Message>(request: &Frame, kind: FrameKind, payload: &M) -> Frame {
    new_frame(
        kind,
        TrafficClass::Control,
        Id::from_slice(&request.request_id).unwrap(),
        Id::from_slice(&request.operation_id).unwrap(),
        payload,
        5000,
        &ProtocolLimits::default(),
    )
    .unwrap()
}

fn handle(frame: &Frame, session: &Session, state: &mut Observed) -> (Frame, bool) {
    match FrameKind::try_from(frame.kind).unwrap() {
        FrameKind::GetTopic => (
            response(
                frame,
                FrameKind::TopicResult,
                &public::TopicResult {
                    topic_id: bytes(id(1)),
                    name: TOPIC.into(),
                },
            ),
            false,
        ),
        FrameKind::GetTopicLayout => (
            response(
                frame,
                FrameKind::TopicLayoutResult,
                &public::TopicLayoutResult {
                    topic_id: bytes(id(1)),
                    revision: 1,
                    active: vec![public::ShardRoute {
                        shard_id: bytes(id(2)),
                        group_id: 2,
                        authority_epoch: 1,
                        ..Default::default()
                    }],
                    total_shards: 1,
                    ..Default::default()
                },
            ),
            false,
        ),
        FrameKind::ConsumerControl => control_reply(frame, session, state),
        other => panic!("Unexpected close peer frame: {other:?}"),
    }
}

fn control_reply(frame: &Frame, session: &Session, state: &mut Observed) -> (Frame, bool) {
    let request: public::ConsumerControl =
        decode_payload(frame.payload.clone(), &ProtocolLimits::default()).unwrap();
    let connection = session.connection();
    let Authorization::Credential {
        credential,
        session_id,
        ..
    } = connection.authorization()
    else {
        panic!("Expected authenticated credential binding");
    };
    let call = Call {
        operation: Id::from_slice(&frame.operation_id).unwrap(),
        request: request.clone(),
        client: *session_id,
        credential: *credential,
        timeout_ms: frame.remaining_timeout_ms,
    };
    state.calls.push(call.clone());
    let action = public::ConsumerAction::try_from(request.action).unwrap();
    if action == public::ConsumerAction::HeartbeatConsumer
        && state.expire
        && request.member_epoch == 1
    {
        return (
            response(
                frame,
                FrameKind::Error,
                &ErrorReply {
                    code: ErrorCode::SessionExpired.into(),
                    message: "The original member has expired".into(),
                    outcome: Outcome::Rejected.into(),
                    retryable: false,
                    ..Default::default()
                },
            ),
            false,
        );
    }
    let mut hold = false;
    let (epoch, status) = match action {
        public::ConsumerAction::EnsureConsumerSubscription => (0, 0),
        public::ConsumerAction::JoinConsumer if request.member_epoch == 1 => (1, 2),
        public::ConsumerAction::JoinConsumer => {
            assert_eq!(request.member_epoch, REJOIN_EPOCH);
            if let Some(original) = &state.original {
                assert_eq!(call.operation, original.operation);
                assert_eq!(call.request, original.request);
                assert_eq!(call.client, original.client);
                assert_eq!(call.credential, original.credential);
            } else {
                state.original = Some(call);
                state.rejoined = !state.delayed_commit;
                hold = true;
            }
            (REJOIN_EPOCH + u64::from(state.foreign_epoch), 1)
        }
        public::ConsumerAction::ConsumerStatus if state.expire && request.member_epoch == 1 => {
            (1, 5)
        }
        public::ConsumerAction::ConsumerStatus if request.member_epoch == REJOIN_EPOCH => {
            (REJOIN_EPOCH, 1)
        }
        public::ConsumerAction::LeaveConsumer if request.member_epoch == REJOIN_EPOCH => {
            (REJOIN_EPOCH, 4)
        }
        public::ConsumerAction::RetireConsumer if request.member_epoch == REJOIN_EPOCH => {
            state.retired = true;
            (REJOIN_EPOCH, 7)
        }
        public::ConsumerAction::HeartbeatConsumer => (1, 2),
        _ => {
            return (
                response(
                    frame,
                    FrameKind::Error,
                    &ErrorReply {
                        code: ErrorCode::InvalidRequest.into(),
                        message: "Cleanup targeted an unconfirmed member epoch".into(),
                        outcome: Outcome::Rejected.into(),
                        retryable: false,
                        ..Default::default()
                    },
                ),
                false,
            );
        }
    };
    (
        response(
            frame,
            FrameKind::ConsumerControlResult,
            &public::ConsumerControlResult {
                subscription_id: bytes(id(3)),
                member_epoch: epoch,
                member_status: status,
                next_member_epoch: if state.expire { REJOIN_EPOCH } else { 1 },
                assignment_version: 1,
                next_heartbeat_sequence: request.heartbeat_sequence + 1,
                remaining_lease_ms: if status == 2 { 15_000 } else { 0 },
                plan_active: status != 1,
                assignment_fenced: true,
                ..Default::default()
            },
        ),
        hold,
    )
}

async fn rejoining(peer: &Peer) -> QueueConsumer {
    let consumer = peer
        .client
        .new_queue_consumer(TOPIC, "jobs")
        .build()
        .await
        .unwrap();
    peer.observed.lock().unwrap().expire = true;
    tokio::time::timeout(Duration::from_secs(10), peer.accepted.notified())
        .await
        .unwrap();
    assert_eq!(consumer.owner.core.epoch.load(Ordering::Acquire), 0);
    consumer
}

fn assert_original_closed(peer: &Peer) {
    let state = peer.observed.lock().unwrap();
    let original = state.original.as_ref().unwrap();
    let joins = state
        .calls
        .iter()
        .filter(|call| {
            call.request.action == i32::from(public::ConsumerAction::JoinConsumer)
                && call.request.member_epoch == REJOIN_EPOCH
        })
        .collect::<Vec<_>>();
    assert_eq!(joins.len(), 2);
    assert!(state.retired);
    assert!(state.calls.iter().any(|call| {
        call.request.action == i32::from(public::ConsumerAction::LeaveConsumer)
            && call.request.member_epoch == REJOIN_EPOCH
            && call.client == original.client
            && call.credential == original.credential
    }));
    assert!(joins.last().unwrap().timeout_ms <= 5000);
    assert!(!state.calls.iter().any(|call| {
        call.request.action == i32::from(public::ConsumerAction::HeartbeatConsumer)
            && call.request.member_epoch == REJOIN_EPOCH
    }));
    eprintln!(
        "Confirmed original Join {} at epoch {}; requests={}, no renewal during close",
        original.operation,
        REJOIN_EPOCH,
        joins.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_resolves_the_original_committed_join_before_leaving_its_confirmed_epoch() {
    let peer = Peer::new().await;
    let consumer = rejoining(&peer).await;
    let report = consumer
        .close()
        .await
        .expect("The original committed Join must close within the unchanged budget");
    assert!(report.local_cleanup_complete());
    assert_eq!(report.members_left(), 1);
    assert_eq!(report.unresolved_joins(), 0);
    assert_original_closed(&peer);
    peer.reply.send(true).unwrap();
    peer.client.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_waits_for_the_same_inflight_join_to_commit_without_starting_a_new_join() {
    let peer = Peer::configured(true, Duration::from_secs(5)).await;
    let consumer = rejoining(&peer).await;
    assert!(!peer.observed.lock().unwrap().rejoined);
    let close = consumer.close();
    tokio::pin!(close);
    tokio::select! {
        _ = peer.retried.notified() => {},
        result = &mut close => panic!("Close finished before resolving its inflight Join: {result:?}"),
    }
    peer.reply.send(true).unwrap();
    let report = close.await.unwrap();
    assert!(report.local_cleanup_complete());
    assert_eq!(report.members_left(), 1);
    assert_eq!(report.unresolved_joins(), 0);
    assert_original_closed(&peer);
    peer.client.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_deadline_keeps_the_original_unknown_join_owned_through_quarantine() {
    let peer = Peer::configured(true, Duration::from_millis(150)).await;
    let consumer = rejoining(&peer).await;
    // Model a nearly elapsed old fence; recovery must cover its own possible late commit.
    *consumer.owner.core.lease_fence_after.lock().unwrap() = Instant::now();
    let close = consumer.close();
    tokio::pin!(close);
    tokio::select! {
        _ = peer.retried.notified() => {},
        result = &mut close => panic!("Close lost the pending Join before retrying its original identity: {result:?}"),
    }
    let error = close.await.unwrap_err();
    assert!(error.source.outcome_unknown());
    assert_eq!(error.report.unresolved_joins(), 1);
    assert_eq!(error.report.members_left(), 0);
    assert!(!error.report.local_cleanup_complete());
    let until = Instant::now() + Duration::from_secs(1);
    loop {
        let notified = consumer.owner.core.changed.notified();
        if consumer.owner.core.state().unwrap().closed.is_some() {
            break;
        }
        tokio::time::timeout_at(until, notified).await.unwrap();
    }
    let original_deadline = consumer.owner.core.close_deadline.lock().unwrap().unwrap();
    assert!(
        *consumer.owner.core.lease_fence_after.lock().unwrap()
            >= original_deadline + Duration::from_secs(15)
    );
    {
        let state = peer.observed.lock().unwrap();
        let original = state.original.as_ref().unwrap();
        let owned = consumer.owner.core.state().unwrap();
        let pending = owned
            .pending_join
            .as_ref()
            .expect("Quarantine must own the original Join");
        assert_eq!(pending.operation, original.operation);
        assert_eq!(pending.request, original.request);
        assert!(state.calls.iter().all(|call| call.request.action
            != i32::from(public::ConsumerAction::LeaveConsumer)));
        assert!(
            state
                .calls
                .iter()
                .filter(|call| call.request.member_epoch == REJOIN_EPOCH)
                .skip(1)
                .all(|call| call.timeout_ms <= 150)
        );
    }
    assert!(
        consumer
            .owner
            .core
            .resource_memory
            .lock()
            .unwrap()
            .is_some()
    );
    peer.reply.send(true).unwrap();
    let aggregate = peer.client.close().await.unwrap_err();
    assert_eq!(aggregate.report().unresolved_joins(), 1);
    assert_eq!(aggregate.report().members_left(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_rejects_a_foreign_join_epoch_without_guessing_or_leaving_it() {
    let peer = Peer::new().await;
    peer.observed.lock().unwrap().foreign_epoch = true;
    let consumer = rejoining(&peer).await;
    let error = consumer.close().await.unwrap_err();
    assert!(error.source.outcome_unknown());
    assert_eq!(error.report.unresolved_joins(), 1);
    assert_eq!(error.report.members_left(), 0);
    assert!(
        peer.observed
            .lock()
            .unwrap()
            .calls
            .iter()
            .all(|call| call.request.action != i32::from(public::ConsumerAction::LeaveConsumer))
    );
    peer.reply.send(true).unwrap();
    assert_eq!(
        peer.client
            .close()
            .await
            .unwrap_err()
            .report()
            .unresolved_joins(),
        1
    );
}
