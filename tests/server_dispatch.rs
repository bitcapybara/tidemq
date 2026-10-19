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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_submission_order_and_core_progress_survive_blocked_data_handlers() {
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
        TransportConfig::default(),
        budget.clone(),
    )
    .unwrap();
    let address = server.local_addr();
    let service = Arc::new(Service {
        submitted: Mutex::new(Vec::new()),
        replies: Semaphore::new(0),
        flushed: Semaphore::new(0),
    });
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(tidemq::server::run(
        server,
        service.clone(),
        DispatchConfig {
            data_handlers: 2,
            ..Default::default()
        },
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
        TransportConfig::default(),
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
