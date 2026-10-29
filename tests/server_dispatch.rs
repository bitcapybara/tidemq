use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use rcgen::{CertificateParams, KeyPair};
use tidemq::budget::MemoryBudget;
use tidemq::config::MemoryConfig;
use tidemq::server::{DispatchConfig, SubmissionOrder, WireService};
use tidemq::transport::rpc::{RpcClient, RpcConfig};
use tidemq::transport::{
    Authorization, Client, ClientCredentials, ClientTls, PeerTarget, Server, ServerAuthorization,
    ServerTls, StreamPurpose, TlsIdentity, TokenGrant, TransportConfig,
};
use tidemq_protocol::{
    Id, ProtocolLimits,
    common::{Frame, FrameKind, TrafficClass},
    framing::{decode_payload, new_frame},
    public::{GroupAck, GroupAckResult, LogicalTopic, Publish, PublishResult},
};
use tokio::sync::{Semaphore, watch};
use tokio::time::Instant;

const TOKEN: &str = "dispatch-test-token-with-at-least-32-characters";

struct Service {
    submitted: Mutex<Vec<u64>>,
    replies: Semaphore,
    flushed: Semaphore,
    memory: MemoryBudget,
    client_memory: MemoryBudget,
    expired_ack: Mutex<Option<Id>>,
    expired_ack_prepared: watch::Sender<Option<Instant>>,
    sent_operations: Mutex<Vec<Id>>,
}

impl WireService for Service {
    type Error = tidemq_protocol::Error;

    fn response_requires_flush(&self, _peer: &Authorization, request: &Frame) -> bool {
        request.kind == i32::from(FrameKind::GroupAck)
    }

    fn response_sent(&self, _peer: &Authorization, request: &Frame, response: &Frame) {
        assert_eq!(request.kind, i32::from(FrameKind::GroupAck));
        assert_eq!(response.kind, i32::from(FrameKind::GroupAckResult));
        self.sent_operations
            .lock()
            .unwrap()
            .push(Id::from_slice(&request.operation_id).unwrap());
        self.flushed.add_permits(1);
    }

    async fn handle(
        &self,
        _peer: Authorization,
        request: &Frame,
        deadline: Instant,
        order: Option<SubmissionOrder>,
    ) -> Result<Option<Frame>, Self::Error> {
        let kind = FrameKind::try_from(request.kind).unwrap();
        let class = TrafficClass::try_from(request.traffic_class).unwrap();
        let mut response = match kind {
            FrameKind::Publish => {
                let batch: Publish =
                    decode_payload(request.payload.clone(), &ProtocolLimits::default())?;
                self.submitted.lock().unwrap().push(batch.sequence);
                drop(order);
                self.replies.acquire().await.unwrap().forget();
                new_frame(
                    FrameKind::PublishResult,
                    class,
                    Id::from_slice(&request.request_id)?,
                    Id::from_slice(&request.operation_id)?,
                    &PublishResult {
                        offset: Some(batch.sequence),
                        ..Default::default()
                    },
                    5000,
                    &ProtocolLimits::default(),
                )?
            }
            FrameKind::GroupAck => new_frame(
                FrameKind::GroupAckResult,
                class,
                Id::from_slice(&request.request_id)?,
                Id::from_slice(&request.operation_id)?,
                &GroupAckResult::default(),
                5000,
                &ProtocolLimits::default(),
            )?,
            _ => {
                let diagnostic: LogicalTopic =
                    decode_payload(request.payload.clone(), &ProtocolLimits::default())?;
                if diagnostic.name.starts_with("deferred") {
                    self.submitted.lock().unwrap().push(0);
                    self.replies.acquire().await.unwrap().forget();
                }
                let mut reply = request.clone();
                reply.kind = FrameKind::DiagnosticResult.into();
                reply
            }
        };
        if kind == FrameKind::GroupAck
            && *self.expired_ack.lock().unwrap() == Some(Id::from_slice(&request.operation_id)?)
        {
            response.remaining_timeout_ms = 0;
            self.expired_ack_prepared.send_replace(Some(deadline));
        }
        Ok(Some(response))
    }
}

fn request<M: prost::Message>(kind: FrameKind, class: TrafficClass, payload: &M) -> Frame {
    new_frame(
        kind,
        class,
        Id::new_v7().unwrap(),
        Id::new_v7().unwrap(),
        payload,
        5000,
        &ProtocolLimits::default(),
    )
    .unwrap()
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

#[path = "server_support/tenant_dispatch.rs"]
mod tenant_dispatch;

struct Fixture {
    client: Client,
    service: Arc<Service>,
    target: PeerTarget,
    credentials: ClientCredentials,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<tidemq::Result<()>>,
    transport: Arc<tidemq::transport::TransportMetrics>,
}

impl Fixture {
    async fn start(dispatch: DispatchConfig) -> Self {
        Self::start_with_transport(dispatch, TransportConfig::default()).await
    }

    async fn start_with_transport(dispatch: DispatchConfig, config: TransportConfig) -> Self {
        Self::start_with_authorization(
            dispatch,
            config,
            ServerAuthorization::External(vec![
                TokenGrant::new(
                    TOKEN,
                    "dispatch-test".into(),
                    "dispatch-test".into(),
                    vec![
                        FrameKind::Publish,
                        FrameKind::GroupAck,
                        FrameKind::CreateLogicalTopic,
                        FrameKind::Diagnose,
                    ],
                )
                .unwrap(),
            ]),
        )
        .await
    }

    async fn start_with_authorization(
        dispatch: DispatchConfig,
        config: TransportConfig,
        authorization: ServerAuthorization,
    ) -> Self {
        let key = KeyPair::generate().unwrap();
        let certificate = CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let budget = MemoryBudget::new(&MemoryConfig::default()).unwrap();
        let client_memory = MemoryBudget::new(&MemoryConfig::default()).unwrap();
        let server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            ServerTls {
                identity: TlsIdentity::new(
                    Bytes::from(certificate.pem()),
                    Bytes::from(key.serialize_pem()),
                )
                .unwrap(),
                client_trust_roots: None,
            },
            authorization,
            config.clone(),
            budget.clone(),
        )
        .unwrap();
        let address = server.local_addr();
        let transport = server.metrics.clone();
        let service = Arc::new(Service {
            submitted: Mutex::new(Vec::new()),
            replies: Semaphore::new(0),
            flushed: Semaphore::new(0),
            memory: budget.clone(),
            client_memory: client_memory.clone(),
            expired_ack: Mutex::new(None),
            expired_ack_prepared: watch::channel(None).0,
            sent_operations: Mutex::new(Vec::new()),
        });
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(tidemq::server::run(
            server,
            service.clone(),
            dispatch,
            budget,
            Arc::new(tidemq::server::DispatchMetrics::default()),
            receiver,
        ));
        let client = Client::bind(
            "127.0.0.1:0".parse().unwrap(),
            ClientTls {
                trust_roots: Bytes::from(certificate.pem()),
                identity: None,
                expected_nodes: vec![],
            },
            config,
            client_memory,
        )
        .unwrap();
        let credentials = ClientCredentials::External {
            session_id: Id::new_v7().unwrap(),
            access_token: TOKEN.into(),
        };
        let target = PeerTarget {
            address,
            server_name: "localhost".into(),
            local_role: "sdk".into(),
            remote_role: "gateway".into(),
            cluster_id: None,
            node_id: None,
        };
        Self {
            client,
            service,
            target,
            credentials,
            shutdown,
            task,
            transport,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn encode_expired_ack_reply_preserves_shared_requests_and_flush_notifications() {
    let Fixture {
        client,
        service,
        target,
        credentials,
        shutdown,
        mut task,
        transport,
    } = Fixture::start(DispatchConfig::default()).await;
    let session = client
        .connect(target, &credentials, TrafficClass::Control, deadline())
        .await
        .unwrap();
    let connection = session.connection();
    let rpc = RpcClient::from_session(session, RpcConfig::default())
        .await
        .unwrap();
    let mut expired = request(
        FrameKind::GroupAck,
        TrafficClass::Control,
        &GroupAck::default(),
    );
    let expired_operation = Id::new_v7().unwrap();
    expired.operation_id = Bytes::copy_from_slice(expired_operation.as_bytes());
    *service.expired_ack.lock().unwrap() = Some(expired_operation);
    let mut prepared = service.expired_ack_prepared.subscribe();
    let valid = request(
        FrameKind::GroupAck,
        TrafficClass::Control,
        &GroupAck::default(),
    );
    let valid_operation = Id::from_slice(&valid.operation_id).unwrap();
    let before_client = client.metrics.clone();

    // Every behavior failure is returned to the outer cleanup before it becomes an assertion.
    let outcome: anyhow::Result<()> = async {
        let deferred = |kind, name: &str| {
            request(kind, TrafficClass::Control, &LogicalTopic { name: name.into() })
        };
        let core = rpc
            .submit(deferred(FrameKind::Diagnose, "deferred-core"), deadline())
            .await?;
        let ordinary = rpc
            .submit(
                deferred(FrameKind::CreateLogicalTopic, "deferred-request"),
                deadline(),
            )
            .await?;
        tokio::time::timeout_at(deadline(), async {
            while service.submitted.lock().unwrap().len() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("Both sibling requests must be held before the expired ACK reply")?;
        let expired_waiter = rpc.submit(expired, deadline()).await?;
        let ack_streams = client.metrics.snapshot().streams_total;
        let queue_deadline = tokio::time::timeout_at(deadline(), async {
            loop {
                let observed = *prepared.borrow();
                if let Some(deadline) = observed {
                    break Ok::<_, anyhow::Error>(deadline);
                }
                prepared.changed().await?;
            }
        })
        .await
        .context("The selected ACK must reach the reply encoder")??;
        let queue_deadline_live = Instant::now() < queue_deadline;
        let premature_notifications = service.flushed.available_permits();
        let acknowledged = rpc.call(valid, deadline()).await;
        service.replies.add_permits(2);
        let (core, ordinary) = tokio::join!(core.receive(), ordinary.receive());
        eprintln!(
            "Encode-expired ACK reply: queue_deadline_live={queue_deadline_live}, ack={:?}, core={:?}, request={:?}, streams_before={ack_streams}, streams_after={}, rpc_closed={}, connection_closed={}",
            acknowledged.as_ref().map(|reply| reply.frame().kind),
            core.as_ref().map(|reply| reply.frame().kind),
            ordinary.as_ref().map(|reply| reply.frame().kind),
            client.metrics.snapshot().streams_total,
            rpc.is_closed(),
            connection.is_closed(),
        );
        anyhow::ensure!(queue_deadline_live, "The original reply queue deadline must still be live");
        anyhow::ensure!(premature_notifications == 0, "An unsent ACK must not notify response_sent");
        let acknowledged = acknowledged.context("The next valid ACK must succeed on the same ACK lane")?;
        anyhow::ensure!(
            acknowledged.frame().kind == i32::from(FrameKind::GroupAckResult)
                && acknowledged.frame().operation_id.as_ref() == valid_operation.as_bytes(),
            "Only the valid ACK operation may receive a reply"
        );
        for (label, response) in [("Core", core), ("Request", ordinary)] {
            let response = response.with_context(|| format!("The in-flight {label} request must survive"))?;
            anyhow::ensure!(
                response.frame().kind == i32::from(FrameKind::DiagnosticResult),
                "The sibling reply must preserve its response kind"
            );
        }
        let notified = tokio::time::timeout_at(deadline(), service.flushed.acquire())
            .await
            .context("The valid ACK must finish its flush notification")??;
        drop(notified);
        anyhow::ensure!(!rpc.is_closed() && !connection.is_closed(), "The shared connection must remain open");
        anyhow::ensure!(
            client.metrics.snapshot().streams_total == ack_streams
                && client.metrics.snapshot().connections_total == 1
                && rpc.lane_count() == 4,
            "The valid ACK must reuse the existing lane without reconnecting"
        );
        drop((acknowledged, expired_waiter));
        Ok(())
    }
    .await;

    service.replies.add_permits(2);
    let finish = deadline();
    rpc.shutdown(finish).await;
    let admission = rpc.snapshot();
    drop((rpc, connection));
    let client_closed = client.close(finish).await;
    drop(client);
    let _ = shutdown.send(true);
    let server_closed = tokio::time::timeout_at(finish, &mut task).await;
    if server_closed.is_err() {
        task.abort();
        let _ = task.await;
    }
    let available = |memory: &MemoryBudget| {
        [
            memory.available_bytes(),
            memory.available_data_bytes(),
            memory.available_normal_bytes(),
            memory.available_core_bytes(),
            memory.available_ack_bytes(),
            memory.available_handshake_bytes(),
        ]
    };
    let uncharged = MemoryBudget::new(&MemoryConfig::default()).unwrap();
    while Instant::now() < finish
        && (available(&service.memory) != available(&uncharged)
            || available(&service.client_memory) != available(&uncharged)
            || transport.snapshot().active_connections != 0
            || before_client.snapshot().active_connections != 0)
    {
        tokio::task::yield_now().await;
    }
    eprintln!(
        "Expired ACK cleanup: client_closed={client_closed:?}, server_closed={server_closed:?}, client_memory={:?}, server_memory={:?}, expected={:?}, admission={admission:?}, client_connections={}, server_connections={}, sent_operations={:?}, notifications={}",
        available(&service.client_memory),
        available(&service.memory),
        available(&uncharged),
        before_client.snapshot().active_connections,
        transport.snapshot().active_connections,
        service.sent_operations.lock().unwrap(),
        service.flushed.available_permits(),
    );
    assert!(client_closed.is_ok(), "The client endpoint must close");
    assert!(
        matches!(server_closed, Ok(Ok(Ok(())))),
        "The server must stop and join"
    );
    assert_eq!(available(&service.memory), available(&uncharged));
    assert_eq!(available(&service.client_memory), available(&uncharged));
    assert_eq!(before_client.snapshot().active_connections, 0);
    assert_eq!(transport.snapshot().active_connections, 0);
    assert_eq!(admission.admitted_requests, 0);
    assert_eq!(admission.admitted_bytes, 0);
    assert_eq!(admission.pending_responses, Some(0));
    assert!(outcome.is_ok(), "{outcome:#?}");
    assert_eq!(
        *service.sent_operations.lock().unwrap(),
        vec![valid_operation]
    );
    assert_eq!(service.flushed.available_permits(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_handler_reply_keeps_other_control_requests_live() {
    let Fixture {
        client,
        service,
        target,
        credentials,
        shutdown,
        task,
        transport: _,
    } = Fixture::start(DispatchConfig::default()).await;
    let session = client
        .connect(target, &credentials, TrafficClass::Control, deadline())
        .await
        .unwrap();
    let core = RpcClient::from_session(session, RpcConfig::default())
        .await
        .unwrap();
    let deferred = || {
        request(
            FrameKind::Diagnose,
            TrafficClass::Control,
            &LogicalTopic {
                name: "deferred".into(),
            },
        )
    };
    let mut expiring = deferred();
    expiring.remaining_timeout_ms = 100;
    // Expire the server budget before the caller deadline so release always enqueues a late reply.
    let first = core
        .submit(expiring, Instant::now() + Duration::from_millis(500))
        .await
        .unwrap();
    tokio::time::timeout_at(deadline(), async {
        while service.submitted.lock().unwrap().len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let second = core.submit(deferred(), deadline()).await.unwrap();
    tokio::time::timeout_at(deadline(), async {
        while service.submitted.lock().unwrap().len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(first.receive().await.is_err());
    service.replies.add_permits(1);
    core.call(
        request(
            FrameKind::Diagnose,
            TrafficClass::Control,
            &LogicalTopic {
                name: "live-control".into(),
            },
        ),
        deadline(),
    )
    .await
    .expect("An expired handler reply must not close the shared Control lane");
    service.replies.add_permits(1);
    assert_eq!(
        second.receive().await.unwrap().frame().kind,
        i32::from(FrameKind::DiagnosticResult)
    );
    core.shutdown(deadline()).await;
    client.close(deadline()).await.unwrap();
    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn simultaneous_control_replies_wait_for_the_bounded_writer_queue() {
    let Fixture {
        client,
        service,
        target,
        credentials,
        shutdown,
        task,
        transport: _,
    } = Fixture::start(DispatchConfig {
        core_handlers: 32,
        replies_per_stream: 1,
        ..Default::default()
    })
    .await;
    let session = client
        .connect(target, &credentials, TrafficClass::Control, deadline())
        .await
        .unwrap();
    let rpc = RpcClient::from_session(session, RpcConfig::default())
        .await
        .unwrap();
    let mut pending = Vec::new();
    for _ in 0..32 {
        pending.push(
            rpc.submit(
                request(
                    FrameKind::Diagnose,
                    TrafficClass::Control,
                    &LogicalTopic {
                        name: "deferred".into(),
                    },
                ),
                deadline(),
            )
            .await
            .unwrap(),
        );
    }
    tokio::time::timeout_at(deadline(), async {
        while service.submitted.lock().unwrap().len() != 32 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    service.replies.add_permits(32);
    for reply in pending {
        assert_eq!(
            reply.receive().await.unwrap().frame().kind,
            i32::from(FrameKind::DiagnosticResult)
        );
    }
    rpc.shutdown(deadline()).await;
    client.close(deadline()).await.unwrap();
    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_submission_order_and_core_progress_survive_blocked_data_handlers() {
    let Fixture {
        client,
        service,
        target,
        credentials,
        shutdown,
        task,
        transport: _,
    } = Fixture::start(DispatchConfig {
        data_handlers: 2,
        ..Default::default()
    })
    .await;
    let produce = client
        .connect(
            target.clone(),
            &credentials,
            TrafficClass::Produce,
            deadline(),
        )
        .await
        .unwrap();
    let produce = RpcClient::from_session(produce, RpcConfig::default())
        .await
        .unwrap();
    let first = produce
        .submit(
            request(
                FrameKind::Publish,
                TrafficClass::Produce,
                &Publish {
                    sequence: 0,
                    ..Default::default()
                },
            ),
            deadline(),
        )
        .await
        .unwrap();
    let second = produce
        .submit(
            request(
                FrameKind::Publish,
                TrafficClass::Produce,
                &Publish {
                    sequence: 1,
                    ..Default::default()
                },
            ),
            deadline(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.submitted.lock().unwrap().len() != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(*service.submitted.lock().unwrap(), vec![0, 1]);
    let control = client
        .connect(target, &credentials, TrafficClass::Control, deadline())
        .await
        .unwrap();
    let core = RpcClient::from_session(control.clone(), RpcConfig::default())
        .await
        .unwrap();
    let diagnostic = core
        .call(
            request(
                FrameKind::Diagnose,
                TrafficClass::Control,
                &LogicalTopic {
                    name: "diagnostic".into(),
                },
            ),
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(
        diagnostic.frame().kind,
        i32::from(FrameKind::DiagnosticResult)
    );
    let mut ack = control
        .connection()
        .open(StreamPurpose::Ack, deadline())
        .await
        .unwrap();
    ack.sender
        .send(
            &request(
                FrameKind::GroupAck,
                TrafficClass::Control,
                &GroupAck::default(),
            ),
            deadline(),
        )
        .await
        .unwrap();
    ack.sender.finish().unwrap();
    let acknowledged = ack.receiver.receive(deadline()).await.unwrap().unwrap();
    assert_eq!(
        acknowledged.frame().kind,
        i32::from(FrameKind::GroupAckResult)
    );
    tokio::time::timeout_at(deadline(), service.flushed.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    service.replies.add_permits(2);
    let (first, second) = tokio::join!(first.receive(), second.receive());
    for (response, expected) in [(first.unwrap(), 0), (second.unwrap(), 1)] {
        let result: PublishResult =
            decode_payload(response.frame().payload.clone(), &ProtocolLimits::default()).unwrap();
        assert_eq!(result.offset, Some(expected));
    }
    let unanswered = produce
        .submit(
            request(
                FrameKind::Publish,
                TrafficClass::Produce,
                &Publish {
                    sequence: 2,
                    ..Default::default()
                },
            ),
            deadline(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while service.submitted.lock().unwrap().len() != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), produce.shutdown(deadline()))
        .await
        .unwrap();
    assert!(matches!(
        unanswered.receive().await,
        Err(tidemq::transport::rpc::Error::Closed)
    ));
    service.replies.add_permits(1);
    client.close(deadline()).await.unwrap();
    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
}

async fn idle_request_lanes(client_timeout: Duration, server_timeout: Duration) {
    let Fixture {
        client,
        service,
        target,
        credentials,
        shutdown,
        task,
        transport,
    } = Fixture::start(DispatchConfig {
        idle_timeout: server_timeout,
        drain_timeout: Duration::from_millis(100),
        ..Default::default()
    })
    .await;
    service.replies.add_permits(64);
    let session = client
        .connect(target, &credentials, TrafficClass::Produce, deadline())
        .await
        .unwrap();
    let connection = session.connection();
    let rpc = RpcClient::from_session_with_lanes(
        session,
        RpcConfig {
            read_idle_timeout: client_timeout,
            ..Default::default()
        },
        4,
        deadline(),
    )
    .await
    .unwrap();
    assert_eq!(rpc.lane_count(), 4);
    let started = Instant::now();
    let mut sequence = 0;
    while started.elapsed() < Duration::from_millis(360) {
        let response = rpc
            .call_affinity(
                request(
                    FrameKind::Publish,
                    TrafficClass::Produce,
                    &Publish {
                        sequence,
                        ..Default::default()
                    },
                ),
                Instant::now() + Duration::from_millis(100),
                0,
            )
            .await
            .expect("An idle sibling must not close the active affinity lane");
        let result: PublishResult =
            decode_payload(response.frame().payload.clone(), &ProtocolLimits::default()).unwrap();
        assert_eq!(result.offset, Some(sequence));
        sequence += 1;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(sequence >= 10);
    assert!(!rpc.is_closed());
    assert!(!connection.is_closed());
    for metrics in [client.metrics.snapshot(), transport.snapshot()] {
        assert_eq!(metrics.connections_total, 1);
        assert_eq!(metrics.deadlines_total, 0);
        assert_eq!(metrics.malformed_total, 0);
    }
    let finish = Instant::now() + Duration::from_millis(500);
    rpc.shutdown(finish).await;
    client.close(finish).await.unwrap();
    shutdown.send(true).unwrap();
    tokio::time::timeout_at(finish, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_request_lanes_preserve_active_client_responses() {
    idle_request_lanes(Duration::from_millis(120), Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_request_lanes_preserve_active_server_requests() {
    idle_request_lanes(Duration::from_secs(5), Duration::from_millis(120)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_idle_produce_connection_records_lane_read_errors_while_control_progresses() {
    let native_idle = Duration::from_secs(1);
    let Fixture {
        client,
        service,
        target,
        credentials,
        shutdown,
        task,
        transport,
    } = Fixture::start_with_transport(
        DispatchConfig {
            idle_timeout: Duration::from_millis(120),
            drain_timeout: Duration::from_millis(100),
            ..Default::default()
        },
        TransportConfig {
            idle_timeout: native_idle,
            handshake_timeout: Duration::from_millis(500),
            stream_prelude_timeout: Duration::from_millis(200),
            ..Default::default()
        },
    )
    .await;
    let config = RpcConfig {
        read_idle_timeout: Duration::from_millis(120),
        ..Default::default()
    };
    let producer = client
        .connect(
            target.clone(),
            &credentials,
            TrafficClass::Produce,
            deadline(),
        )
        .await
        .unwrap();
    let producer_connection = producer.connection();
    let producer = RpcClient::from_session_with_lanes(producer, config.clone(), 4, deadline())
        .await
        .unwrap();
    assert_eq!(producer.lane_count(), 4);
    service.replies.add_permits(1);
    let response = producer
        .call_affinity(
            request(
                FrameKind::Publish,
                TrafficClass::Produce,
                &Publish::default(),
            ),
            deadline(),
            0,
        )
        .await
        .unwrap();
    let result: PublishResult =
        decode_payload(response.frame().payload.clone(), &ProtocolLimits::default()).unwrap();
    assert_eq!(result.offset, Some(0));
    drop(response);
    let control = client
        .connect(target, &credentials, TrafficClass::Control, deadline())
        .await
        .unwrap();
    let control_connection = control.connection();
    let control = RpcClient::from_session(control, config).await.unwrap();
    let initial_client = client.metrics.snapshot();
    let initial_server = transport.snapshot();
    let started = Instant::now();
    let mut checked_application_idle = false;
    let mut control_responses = 0;
    loop {
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "The native-idle producer connection did not close"
        );
        let response = control
            .call_affinity(
                request(
                    FrameKind::Diagnose,
                    TrafficClass::Control,
                    &LogicalTopic {
                        name: "idle-connection-diagnostic".into(),
                    },
                ),
                Instant::now() + Duration::from_millis(200),
                0,
            )
            .await
            .unwrap();
        assert_eq!(
            response.frame().kind,
            i32::from(FrameKind::DiagnosticResult)
        );
        control_responses += 1;
        if !checked_application_idle && started.elapsed() >= Duration::from_millis(250) {
            assert!(started.elapsed() < native_idle);
            assert!(!producer.is_closed());
            assert!(!producer_connection.is_closed());
            assert_eq!(
                client.metrics.snapshot().malformed_total,
                initial_client.malformed_total
            );
            checked_application_idle = true;
        }
        let observed = client.metrics.snapshot();
        if started.elapsed() >= native_idle + Duration::from_millis(200)
            && producer.is_closed()
            && producer_connection.is_closed()
            && observed
                .malformed_total
                .saturating_sub(initial_client.malformed_total)
                == 4
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(checked_application_idle);
    assert!(control_responses >= 10);
    assert!(!control.is_closed());
    assert!(!control_connection.is_closed());
    let response = control
        .call_affinity(
            request(
                FrameKind::Diagnose,
                TrafficClass::Control,
                &LogicalTopic {
                    name: "after-native-idle-close".into(),
                },
            ),
            Instant::now() + Duration::from_millis(200),
            0,
        )
        .await
        .unwrap();
    assert_eq!(
        response.frame().kind,
        i32::from(FrameKind::DiagnosticResult)
    );
    control_responses += 1;
    let observed_client = client.metrics.snapshot();
    let observed_server = transport.snapshot();
    assert_eq!(observed_client.connections_total, 2);
    assert_eq!(
        observed_client.malformed_total - initial_client.malformed_total,
        4
    );
    for metrics in [&observed_client, &observed_server] {
        assert_eq!(metrics.deadlines_total, 0);
        assert_eq!(metrics.liveness_failures_total, 0);
        assert_eq!(metrics.rejected_total, 0);
    }
    eprintln!(
        "Native-idle P/4-lane closure with active C: elapsed={:?}, control_responses={control_responses}, client_malformed_delta={}, server_malformed_delta={}, client_deadlines={}, server_deadlines={}, client_liveness_failures={}, server_liveness_failures={}, client_connections={}, server_connections={}, client_rejected={}, server_rejected={}",
        started.elapsed(),
        observed_client.malformed_total - initial_client.malformed_total,
        observed_server.malformed_total - initial_server.malformed_total,
        observed_client.deadlines_total,
        observed_server.deadlines_total,
        observed_client.liveness_failures_total,
        observed_server.liveness_failures_total,
        observed_client.connections_total,
        observed_server.connections_total,
        observed_client.rejected_total,
        observed_server.rejected_total
    );
    let finish = Instant::now() + Duration::from_millis(500);
    producer.shutdown(finish).await;
    control.shutdown(finish).await;
    client.close(finish).await.unwrap();
    shutdown.send(true).unwrap();
    tokio::time::timeout_at(finish, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn modern_ack_rpc_keeps_its_lane_and_budget_when_normal_control_is_saturated() {
    for (capacity, padding, normal_requests) in [
        (
            RpcConfig {
                max_pending_requests: 5,
                ..Default::default()
            },
            0,
            3,
        ),
        (
            RpcConfig {
                max_pending_requests: 32,
                max_pending_bytes: 24 * 1024,
                ..Default::default()
            },
            8192,
            1,
        ),
    ] {
        let Fixture {
            client,
            service,
            target,
            credentials,
            shutdown,
            task,
            ..
        } = Fixture::start(DispatchConfig::default()).await;
        let session = client
            .connect(target, &credentials, TrafficClass::Control, deadline())
            .await
            .unwrap();
        let rpc = RpcClient::from_session(session, capacity).await.unwrap();
        let mut pending = Vec::new();
        for _ in 0..normal_requests {
            pending.push(
                rpc.submit(
                    request(
                        FrameKind::CreateLogicalTopic,
                        TrafficClass::Control,
                        &LogicalTopic {
                            name: format!("deferred{}", "x".repeat(padding)),
                        },
                    ),
                    deadline(),
                )
                .await
                .unwrap(),
            );
        }
        assert!(matches!(
            rpc.submit(
                request(
                    FrameKind::CreateLogicalTopic,
                    TrafficClass::Control,
                    &LogicalTopic {
                        name: format!("deferred{}", "x".repeat(padding)),
                    }
                ),
                deadline()
            )
            .await,
            Err(tidemq::transport::rpc::Error::Admission { .. })
        ));
        let until = deadline();
        while service.submitted.lock().unwrap().len() < normal_requests {
            assert!(
                Instant::now() < until,
                "Normal control requests did not reach the handler"
            );
            tokio::task::yield_now().await;
        }
        let held_normal = service
            .memory
            .try_reserve(
                TrafficClass::Control,
                u32::try_from(service.memory.available_normal_bytes()).unwrap(),
            )
            .unwrap();
        let inspected = rpc
            .call(
                request(
                    FrameKind::Diagnose,
                    TrafficClass::Control,
                    &LogicalTopic {
                        name: "core-inspection".into(),
                    },
                ),
                deadline(),
            )
            .await
            .unwrap();
        assert_eq!(
            inspected.frame().kind,
            i32::from(FrameKind::DiagnosticResult)
        );
        let acknowledged = rpc
            .call(
                request(
                    FrameKind::GroupAck,
                    TrafficClass::Control,
                    &tidemq_protocol::public::GroupAck::default(),
                ),
                deadline(),
            )
            .await
            .unwrap();
        assert_eq!(
            acknowledged.frame().kind,
            i32::from(FrameKind::GroupAckResult)
        );
        assert_eq!(rpc.lane_count(), 4);
        // A reply can wake its caller before the transport tasks drop their permits.
        let until = deadline();
        while rpc.snapshot().admitted_requests > normal_requests {
            assert!(
                Instant::now() < until,
                "Completed priority RPCs did not release their admission budget"
            );
            tokio::task::yield_now().await;
        }
        assert_eq!(rpc.snapshot().admitted_requests, normal_requests);
        assert_eq!(rpc.snapshot().pending_responses, Some(normal_requests));
        drop(held_normal);
        service.replies.add_permits(normal_requests);
        for request in pending {
            request.receive().await.unwrap();
        }
        rpc.shutdown(deadline()).await;
        client.close(deadline()).await.unwrap();
        shutdown.send(true).unwrap();
        task.await.unwrap().unwrap();
    }
}
