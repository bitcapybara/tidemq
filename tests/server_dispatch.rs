use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    public::{AckBatch, AckResult, CreateTopic, ProduceBatch, ProduceResult},
};
use tokio::sync::{Semaphore, watch};
use tokio::time::Instant;

const TOKEN: &str = "dispatch-test-token-with-at-least-32-characters";

struct Service {
    submitted: Mutex<Vec<u64>>,
    replies: Semaphore,
    flushed: Semaphore,
}

impl WireService for Service {
    type Error = tidemq_protocol::Error;

    fn response_requires_flush(&self, _peer: &Authorization, request: &Frame) -> bool {
        request.kind == i32::from(FrameKind::AckBatch)
    }

    fn response_sent(&self, _peer: &Authorization, request: &Frame, response: &Frame) {
        assert_eq!(request.kind, i32::from(FrameKind::AckBatch));
        assert_eq!(response.kind, i32::from(FrameKind::AckResult));
        self.flushed.add_permits(1);
    }

    async fn handle(
        &self,
        _peer: Authorization,
        request: &Frame,
        _deadline: Instant,
        order: Option<SubmissionOrder>,
    ) -> Result<Option<Frame>, Self::Error> {
        let kind = FrameKind::try_from(request.kind).unwrap();
        let class = TrafficClass::try_from(request.traffic_class).unwrap();
        let response = match kind {
            FrameKind::ProduceBatch => {
                let batch: ProduceBatch =
                    decode_payload(request.payload.clone(), &ProtocolLimits::default())?;
                self.submitted.lock().unwrap().push(batch.first_sequence);
                drop(order);
                self.replies.acquire().await.unwrap().forget();
                new_frame(
                    FrameKind::ProduceResult,
                    class,
                    Id::from_slice(&request.request_id)?,
                    Id::from_slice(&request.operation_id)?,
                    &ProduceResult {
                        first_offset: batch.first_sequence,
                        message_count: 1,
                        duplicate: false,
                    },
                    5000,
                    &ProtocolLimits::default(),
                )?
            }
            FrameKind::AckBatch => new_frame(
                FrameKind::AckResult,
                class,
                Id::from_slice(&request.request_id)?,
                Id::from_slice(&request.operation_id)?,
                &AckResult {
                    acknowledged_before: 1,
                    accepted: 1,
                    already_acknowledged: 0,
                },
                5000,
                &ProtocolLimits::default(),
            )?,
            _ => {
                let diagnostic: CreateTopic =
                    decode_payload(request.payload.clone(), &ProtocolLimits::default())?;
                if diagnostic.name == "deferred" {
                    self.submitted.lock().unwrap().push(0);
                    self.replies.acquire().await.unwrap().forget();
                }
                let mut reply = request.clone();
                reply.kind = FrameKind::DiagnosticResult.into();
                reply
            }
        };
        Ok(Some(response))
    }
}

fn request<M: prost::Message>(kind: FrameKind, class: TrafficClass, payload: &M) -> Frame {
    new_frame(
        kind,
        class,
        Id::random().unwrap(),
        Id::random().unwrap(),
        payload,
        5000,
        &ProtocolLimits::default(),
    )
    .unwrap()
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

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
        let key = KeyPair::generate().unwrap();
        let certificate = CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let budget = MemoryBudget::new(&MemoryConfig::default()).unwrap();
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
            ServerAuthorization::External(vec![
                TokenGrant::new(
                    TOKEN,
                    "dispatch-test".into(),
                    "dispatch-test".into(),
                    vec![
                        FrameKind::ProduceBatch,
                        FrameKind::AckBatch,
                        FrameKind::Diagnose,
                    ],
                )
                .unwrap(),
            ]),
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
            MemoryBudget::new(&MemoryConfig::default()).unwrap(),
        )
        .unwrap();
        let credentials = ClientCredentials::External {
            session_id: Id::random().unwrap(),
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
            &CreateTopic {
                name: "deferred".into(),
                partitions: 1,
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
            &CreateTopic {
                name: "live-control".into(),
                partitions: 1,
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
                FrameKind::ProduceBatch,
                TrafficClass::Produce,
                &ProduceBatch {
                    first_sequence: 0,
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
                FrameKind::ProduceBatch,
                TrafficClass::Produce,
                &ProduceBatch {
                    first_sequence: 1,
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
                &CreateTopic {
                    name: "diagnostic".into(),
                    partitions: 1,
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
                FrameKind::AckBatch,
                TrafficClass::Control,
                &AckBatch::default(),
            ),
            deadline(),
        )
        .await
        .unwrap();
    ack.sender.finish().unwrap();
    let acknowledged = ack.receiver.receive(deadline()).await.unwrap().unwrap();
    assert_eq!(acknowledged.frame().kind, i32::from(FrameKind::AckResult));
    tokio::time::timeout_at(deadline(), service.flushed.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    service.replies.add_permits(2);
    let (first, second) = tokio::join!(first.receive(), second.receive());
    for (response, expected) in [(first.unwrap(), 0), (second.unwrap(), 1)] {
        let result: ProduceResult =
            decode_payload(response.frame().payload.clone(), &ProtocolLimits::default()).unwrap();
        assert_eq!(result.first_offset, expected);
    }
    let unanswered = produce
        .submit(
            request(
                FrameKind::ProduceBatch,
                TrafficClass::Produce,
                &ProduceBatch {
                    first_sequence: 2,
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
                    FrameKind::ProduceBatch,
                    TrafficClass::Produce,
                    &ProduceBatch {
                        first_sequence: sequence,
                        ..Default::default()
                    },
                ),
                Instant::now() + Duration::from_millis(100),
                0,
            )
            .await
            .expect("An idle sibling must not close the active affinity lane");
        let result: ProduceResult =
            decode_payload(response.frame().payload.clone(), &ProtocolLimits::default()).unwrap();
        assert_eq!(result.first_offset, sequence);
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
                FrameKind::ProduceBatch,
                TrafficClass::Produce,
                &ProduceBatch::default(),
            ),
            deadline(),
            0,
        )
        .await
        .unwrap();
    let result: ProduceResult =
        decode_payload(response.frame().payload.clone(), &ProtocolLimits::default()).unwrap();
    assert_eq!(result.first_offset, 0);
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
                    &CreateTopic {
                        name: "idle-connection-diagnostic".into(),
                        partitions: 1,
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
                &CreateTopic {
                    name: "after-native-idle-close".into(),
                    partitions: 1,
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
