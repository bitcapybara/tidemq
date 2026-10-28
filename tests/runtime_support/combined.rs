use super::read_batches::{finish_original_ack, finish_original_send};
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use tidemq_protocol::common::{ErrorCode, Outcome};

#[derive(Default, Debug)]
struct MetadataReadRetries {
    admission_rejected: u64,
    unavailable_rejected: u64,
    unavailable_unknown: u64,
}

fn metadata_read_retry(error: &tidemq_sdk::Error) -> Option<(ErrorCode, Outcome)> {
    match error {
        tidemq_sdk::Error::Remote {
            code: code @ (ErrorCode::QuotaExceeded | ErrorCode::Unavailable),
            outcome: Outcome::Rejected,
            retryable: true,
            original_outcome_known: false,
            ..
        } => Some((*code, Outcome::Rejected)),
        // Query marks a read-only unknown; no publish or ACK identity is recreated.
        tidemq_sdk::Error::Query { source }
            if matches!(
                source.as_ref(),
                tidemq_sdk::Error::Remote {
                    code: ErrorCode::Unavailable,
                    outcome: Outcome::Unknown,
                    retryable: true,
                    original_outcome_known: false,
                    ..
                }
            ) =>
        {
            Some((ErrorCode::Unavailable, Outcome::Unknown))
        }
        _ => None,
    }
}

async fn metadata_read_until(
    client: &Client,
    name: &str,
    until: Instant,
    retries: &mut MetadataReadRetries,
) -> tidemq_sdk::Result<tidemq_sdk::TopicInfo> {
    let context = tidemq_sdk::RequestContext::with_deadline(until);
    loop {
        if Instant::now() >= until {
            return Err(tidemq_sdk::Error::Deadline);
        }
        let error = match context.scope(client.admin().topic_info(name)).await {
            Ok(info) => return Ok(info),
            Err(error) => error,
        };
        if Instant::now() >= until {
            return Err(tidemq_sdk::Error::Deadline);
        }
        let Some((code, outcome)) = metadata_read_retry(&error) else {
            return Err(error);
        };
        match (code, outcome) {
            (ErrorCode::QuotaExceeded, _) => retries.admission_rejected += 1,
            (_, Outcome::Rejected) => retries.unavailable_rejected += 1,
            _ => retries.unavailable_unknown += 1,
        }
        eprintln!(
            "RF3 metadata read retry ledger: code={code:?} outcome={outcome:?} remaining_ms={}",
            until.saturating_duration_since(Instant::now()).as_millis()
        );
        tokio::time::sleep_until(until.min(Instant::now() + Duration::from_millis(10))).await;
    }
}

#[test]
fn metadata_read_retry_accepts_only_explicit_temporary_read_failures() {
    let error = tidemq_sdk::Error::Query {
        source: Box::new(tidemq_sdk::Error::Remote {
            code: ErrorCode::Unavailable,
            outcome: Outcome::Unknown,
            retryable: true,
            original_outcome_known: false,
            message: "The internal read result was lost".into(),
            expected_sequence: None,
            expected_registration_epoch: None,
            retry_after_ms: None,
        }),
    };
    assert_eq!(
        metadata_read_retry(&error),
        Some((ErrorCode::Unavailable, Outcome::Unknown))
    );
}

// This peer exercises the real SDK/QUIC error boundary; the RF3 phase proves Raft progress.
mod metadata_quic {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tidemq::server::{DispatchConfig, DispatchMetrics, SubmissionOrder, WireService};
    use tidemq_protocol::{
        common::{ErrorReply, Frame, FrameKind, TrafficClass},
        framing::{decode_payload, new_frame},
    };
    use tidemq_transport::{
        Authorization, Server, ServerAuthorization, ServerTls, TlsIdentity, TokenGrant,
        TransportConfig,
        budget::{MemoryBudget, MemoryLimits},
    };
    use tokio::sync::watch;

    const NAME: &str = "test/app/metadata";

    struct Oracle {
        topic: Id,
        failure: Mutex<Option<(ErrorReply, bool)>>,
        frames: Mutex<Vec<(Instant, Frame)>>,
    }

    fn reply(frame: &Frame, kind: FrameKind, body: &impl prost::Message) -> Frame {
        new_frame(
            kind,
            TrafficClass::Control,
            Id::from_slice(&frame.request_id).unwrap(),
            Id::from_slice(&frame.operation_id).unwrap(),
            body,
            5_000,
            &tidemq_protocol::ProtocolLimits::default(),
        )
        .unwrap()
    }

    impl Oracle {
        fn respond(&self, frame: &Frame) -> Frame {
            let mut frames = self.frames.lock().unwrap();
            assert!(
                frames.len() < 256,
                "Metadata retry exceeded its finite frame ledger"
            );
            frames.push((Instant::now(), frame.clone()));
            drop(frames);
            match FrameKind::try_from(frame.kind).unwrap() {
                FrameKind::GetTopic => {
                    let request: public::GetTopic = decode_payload(
                        frame.payload.clone(),
                        &tidemq_protocol::ProtocolLimits::default(),
                    )
                    .unwrap();
                    assert_eq!(request.name, NAME);
                    let mut failure = self.failure.lock().unwrap();
                    let error = failure.as_ref().map(|(reply, _)| reply.clone());
                    if failure.as_ref().is_some_and(|(_, once)| *once) {
                        *failure = None;
                    }
                    match error {
                        Some(error) => reply(frame, FrameKind::Error, &error),
                        None => reply(
                            frame,
                            FrameKind::TopicResult,
                            &public::TopicResult {
                                topic_id: Bytes::copy_from_slice(self.topic.as_bytes()),
                                name: NAME.into(),
                            },
                        ),
                    }
                }
                FrameKind::GetTopicLayout => reply(
                    frame,
                    FrameKind::TopicLayoutResult,
                    &public::TopicLayoutResult {
                        topic_id: Bytes::copy_from_slice(self.topic.as_bytes()),
                        revision: 1,
                        total_shards: 1,
                        active: vec![public::ShardRoute {
                            shard_id: Bytes::from_static(&[1; 16]),
                            group_id: 1,
                            authority_epoch: 1,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ),
                other => panic!("A read-only metadata retry submitted {other:?}"),
            }
        }
    }

    impl WireService for Oracle {
        type Error = std::io::Error;

        async fn handle(
            &self,
            _peer: Authorization,
            frame: &Frame,
            _deadline: Instant,
            _order: Option<SubmissionOrder>,
        ) -> Result<Option<Frame>, Self::Error> {
            Ok(Some(self.respond(frame)))
        }
    }

    fn server() -> (Server, Bytes, MemoryBudget) {
        let key = KeyPair::generate().unwrap();
        let certificate = CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let ca = Bytes::from(certificate.pem());
        let budget = MemoryBudget::new(MemoryLimits {
            total_bytes: 64 * 1024 * 1024,
            control_reserved_bytes: 8 * 1024 * 1024,
        })
        .unwrap();
        let server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            ServerTls {
                identity: TlsIdentity::new(ca.clone(), Bytes::from(key.serialize_pem())).unwrap(),
                client_trust_roots: None,
            },
            ServerAuthorization::External(vec![
                TokenGrant::new(
                    TOKEN,
                    "metadata-test".into(),
                    "metadata-test".into(),
                    vec![FrameKind::GetTopic, FrameKind::GetTopicLayout],
                )
                .unwrap(),
            ]),
            TransportConfig::default(),
            budget.clone(),
        )
        .unwrap();
        (server, ca, budget)
    }

    struct Peer {
        client: Client,
        oracle: Arc<Oracle>,
        stop: watch::Sender<bool>,
        task: tokio::task::JoinHandle<tidemq::Result<()>>,
    }

    impl Peer {
        async fn new() -> Self {
            let (server, ca, budget) = server();
            let endpoint = format!("localhost:{}", server.local_addr().port());
            let oracle = Arc::new(Oracle {
                topic: Id::new_v7().unwrap(),
                failure: Mutex::new(None),
                frames: Mutex::new(Vec::new()),
            });
            let (stop, shutdown) = watch::channel(false);
            let task = tokio::spawn(tidemq::server::run(
                server,
                oracle.clone(),
                DispatchConfig::default(),
                budget,
                Arc::new(DispatchMetrics::default()),
                shutdown,
            ));
            let client = Client::builder(
                endpoint,
                tidemq_sdk::TlsTrust::ca_pem(ca),
                tidemq_sdk::Credentials::token(TOKEN),
            )
            .build()
            .await
            .unwrap();
            Self {
                client,
                oracle,
                stop,
                task,
            }
        }

        fn fail(
            &self,
            code: ErrorCode,
            outcome: Outcome,
            retryable: bool,
            known: bool,
            once: bool,
        ) {
            *self.oracle.failure.lock().unwrap() = Some((
                ErrorReply {
                    code: code.into(),
                    outcome: outcome.into(),
                    retryable,
                    original_outcome_known: known,
                    message: "Injected metadata semantic-result failure".into(),
                    ..Default::default()
                },
                once,
            ));
        }

        async fn close(&mut self) {
            assert!(self.client.close().await.unwrap().local_cleanup_complete());
            self.stop.send_replace(true);
            (&mut self.task).await.unwrap().unwrap();
            assert_eq!(
                self.client
                    .transport_metrics()
                    .snapshot()
                    .active_connections,
                0
            );
        }
    }

    impl Drop for Peer {
        fn drop(&mut self) {
            self.stop.send_replace(true);
            self.task.abort();
        }
    }

    #[tokio::test]
    async fn unknown_metadata_result_recovers_over_real_quic() {
        let mut peer = Peer::new().await;
        peer.fail(ErrorCode::Unavailable, Outcome::Unknown, true, false, true);
        let error = peer.client.admin().topic_info(NAME).await.unwrap_err();
        assert!(matches!(error, tidemq_sdk::Error::Query { .. }));
        peer.fail(ErrorCode::Unavailable, Outcome::Unknown, true, false, true);
        let until = Instant::now() + Duration::from_secs(40);
        let mut retries = MetadataReadRetries::default();
        let result = metadata_read_until(&peer.client, NAME, until, &mut retries).await;
        peer.close().await;
        assert_eq!(Id::from(result.unwrap().id()), peer.oracle.topic);
        assert_eq!(retries.unavailable_unknown, 1);
        assert!(Instant::now() < until);
        let frames = peer.oracle.frames.lock().unwrap();
        assert_eq!(frames.len(), 4);
        assert!(
            frames
                .iter()
                .all(|(_, frame)| frame.traffic_class == i32::from(TrafficClass::Control))
        );
        eprintln!(
            "Metadata QUIC recovery ledger: topic={} frames={} unknown_retries={} original_deadline_preserved=true",
            peer.oracle.topic,
            frames.len(),
            retries.unavailable_unknown
        );
    }

    #[tokio::test]
    async fn permanent_metadata_failures_do_not_start_another_query() {
        let mut peer = Peer::new().await;
        for (code, outcome, retryable, known) in [
            (ErrorCode::Forbidden, Outcome::Rejected, false, false),
            (ErrorCode::Forbidden, Outcome::Unknown, true, false),
            (ErrorCode::InvalidRequest, Outcome::Rejected, false, false),
            (ErrorCode::Unavailable, Outcome::Rejected, false, false),
            (ErrorCode::Unavailable, Outcome::Unknown, false, false),
            (ErrorCode::Unavailable, Outcome::Unknown, true, true),
            (ErrorCode::QuotaExceeded, Outcome::Unknown, true, false),
        ] {
            peer.oracle.frames.lock().unwrap().clear();
            peer.fail(code, outcome, retryable, known, false);
            let mut retries = MetadataReadRetries::default();
            let error = metadata_read_until(
                &peer.client,
                NAME,
                Instant::now() + Duration::from_secs(2),
                &mut retries,
            )
            .await
            .unwrap_err();
            assert_eq!(metadata_read_retry(&error), None);
            assert_eq!(peer.oracle.frames.lock().unwrap().len(), 1);
            assert_eq!(
                (
                    retries.admission_rejected,
                    retries.unavailable_rejected,
                    retries.unavailable_unknown
                ),
                (0, 0, 0)
            );
            eprintln!(
                "Metadata QUIC terminal ledger: code={code:?} outcome={outcome:?} retryable={retryable} known={known} requests=1"
            );
        }
        peer.close().await;
    }

    #[tokio::test]
    async fn repeated_unknown_metadata_results_cannot_extend_the_original_deadline() {
        let mut peer = Peer::new().await;
        peer.client.admin().topic_info(NAME).await.unwrap();
        peer.oracle.frames.lock().unwrap().clear();
        peer.fail(ErrorCode::Unavailable, Outcome::Unknown, true, false, false);
        let until = Instant::now() + Duration::from_millis(200);
        let mut retries = MetadataReadRetries::default();
        let error = tokio::time::timeout_at(
            until + Duration::from_secs(1),
            metadata_read_until(&peer.client, NAME, until, &mut retries),
        )
        .await
        .expect("Metadata retry outlived its original deadline")
        .unwrap_err();
        // A native lane can close at its wire cutoff before the enclosing context reports Deadline.
        assert!(
            matches!(
                error.kind(),
                tidemq_sdk::ErrorKind::DeadlineExceeded | tidemq_sdk::ErrorKind::Unavailable
            ),
            "Unexpected fixed-deadline result: {error:?}"
        );
        assert!(retries.unavailable_unknown > 0);
        tokio::time::sleep_until(until).await;
        let before = peer.oracle.frames.lock().unwrap().len();
        let expired = metadata_read_until(&peer.client, NAME, until, &mut retries)
            .await
            .unwrap_err();
        assert!(matches!(expired, tidemq_sdk::Error::Deadline));
        assert_eq!(peer.oracle.frames.lock().unwrap().len(), before);
        peer.close().await;
        let frames = peer.oracle.frames.lock().unwrap();
        assert!(
            frames
                .iter()
                .all(|(sent, frame)| *sent < until && frame.remaining_timeout_ms <= 200)
        );
        assert!(
            frames
                .windows(2)
                .all(|pair| pair.first().unwrap().1.remaining_timeout_ms
                    >= pair.last().unwrap().1.remaining_timeout_ms)
        );
        assert!(
            frames.last().unwrap().1.remaining_timeout_ms
                < frames.first().unwrap().1.remaining_timeout_ms
        );
        eprintln!(
            "Metadata QUIC deadline ledger: original_budget_ms=200 attempts={before} expired_attempts=0 cutoff_error_kind={:?}",
            error.kind()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn separated_rf3_keeps_metadata_and_data_read_barriers_live_during_durable_acks() {
    let fixture = Fixture::new(
        &[
            (11, Role::Metadata),
            (12, Role::Metadata),
            (13, Role::Metadata),
            (21, Role::Storage),
            (22, Role::Storage),
            (23, Role::Storage),
            (31, Role::Broker),
            (32, Role::Broker),
            (41, Role::Gateway),
            (42, Role::Gateway),
        ],
        &[11, 12, 13],
        &[21, 22, 23],
    )
    .await;
    let mut processes = Vec::new();
    for node in [11, 12, 13, 21, 22, 23, 31, 32, 41, 42] {
        processes.push((node, spawn_process(&fixture, node).await));
    }
    for (node, child) in &mut processes {
        wait_process(&fixture, *node, child).await;
    }
    let client = fixture.client(41).await;
    let tenant = client.admin().create_tenant("separated").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let mut work = Vec::new();
    for index in 0..4 {
        let name = format!("separated/app/read-barrier-{index}");
        let topic = client.admin().create_topic(&name).await.unwrap();
        let consumer = client
            .new_stream_consumer(&name, "durable")
            .build()
            .await
            .unwrap();
        let producer = client.new_producer(&name).build().await.unwrap();
        work.push((name, topic.id(), producer, consumer));
    }
    let phase_budget = Duration::from_secs(40);
    let mut running = tokio::task::JoinSet::new();
    for (name, topic, producer, consumer) in work {
        let client = client.clone();
        running.spawn(async move {
            let started = Instant::now();
            let until = started + phase_budget;
            let context = tidemq_sdk::RequestContext::with_timeout(phase_budget).unwrap();
            let produced = AtomicU64::new(0);
            let acknowledged = AtomicU64::new(0);
            let metadata_reads = AtomicU64::new(0);
            let mut metadata_retries = MetadataReadRetries::default();
            let produce = async {
                for sequence in 0..64_u64 {
                    let result = context.scope(producer.send(tidemq_sdk::Message::new(sequence.to_le_bytes().to_vec()).key("combined"))).await;
                    finish_original_send(result, until).await.unwrap();
                    produced.store(sequence + 1, Ordering::Relaxed);
                }
            };
            let consume = async {
                let mut seen = std::collections::HashSet::new();
                for sequence in 0..64_u64 {
                    let delivery = context.scope(consumer.receive()).await.unwrap();
                    assert_eq!(delivery.key(), Some(b"combined".as_slice()));
                    assert_eq!(delivery.payload(), &sequence.to_le_bytes());
                    assert!(seen.insert(delivery.message_id()), "A healthy Stream repeated an identity");
                    let result = context.scope(consumer.ack_cumulative(&delivery)).await;
                    finish_original_ack(result, until).await.unwrap();
                    acknowledged.store(sequence + 1, Ordering::Relaxed);
                }
            };
            let read_metadata = async {
                for _ in 0..64 {
                    let info = metadata_read_until(&client, &name, until, &mut metadata_retries).await.unwrap();
                    assert_eq!(info.id(), topic);
                    metadata_reads.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
            };
            tokio::join!(produce, consume, read_metadata);
            assert!(Instant::now() < until, "The RF3 phase extended its original forty-second deadline");
            assert_eq!(produced.load(Ordering::Relaxed), 64);
            assert_eq!(acknowledged.load(Ordering::Relaxed), 64);
            assert_eq!(metadata_reads.load(Ordering::Relaxed), 64);
            eprintln!("Separated RF3 phase completed: topic={name}, elapsed_ms={}, produced=64, acknowledged=64, metadata_reads=64, metadata_admission_rejections={}, metadata_unavailable_rejections={}, metadata_unavailable_unknown={}", started.elapsed().as_millis(), metadata_retries.admission_rejected, metadata_retries.unavailable_rejected, metadata_retries.unavailable_unknown);
            consumer.close().await.unwrap();
            producer.close().await.unwrap();
        });
    }
    tokio::time::timeout(phase_budget + Duration::from_secs(5), async {
        while let Some(result) = running.join_next().await {
            result.unwrap();
        }
    })
    .await
    .unwrap();
    client.close().await.unwrap();
    for (_, mut process) in processes {
        process.kill().await.unwrap();
        let _ = process.wait().await.unwrap();
    }
    fixture.close().await;
}

#[tokio::test]
async fn combined_fixture_provisions_real_assets_and_identity_without_raft_material() {
    let fixture = Fixture::new(&[(7, Role::Combined)], &[7], &[7]).await;
    let config = fixture.configs.get(&7).unwrap();
    config.runtime.validate(config).unwrap();
    assert!(config.tls.trust_roots.as_os_str().is_empty());
    assert_eq!(config.runtime.internal_listen.port(), 0);
    assert!(config.runtime.metadata_voters.is_empty());
    assert!(config.runtime.peers.is_empty());
    assert!(!config.runtime.bootstrap);
    for group in &config.runtime.groups {
        assert!(group.voters.is_empty());
        assert!(group.standby.is_empty());
    }
    assert!(!fixture.directory.join("node-7.der").exists());
    let console = config.console.as_ref().unwrap();
    assert_eq!(
        tokio::fs::read(&console.session.key_file)
            .await
            .unwrap()
            .len(),
        32
    );
    tidemq::console::validate_embedded(console, &config.tls)
        .await
        .unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn separated_fixture_has_real_role_topology_and_no_embedded_console() {
    let fixture = Fixture::new(
        &[
            (11, Role::Metadata),
            (21, Role::Storage),
            (31, Role::Broker),
            (41, Role::Gateway),
        ],
        &[11],
        &[21],
    )
    .await;
    for config in fixture.configs.values() {
        config.runtime.validate(config).unwrap();
        assert!(config.console.is_none());
        assert_eq!(config.replication_factor, 1);
        assert!(!config.tls.trust_roots.as_os_str().is_empty());
        assert_ne!(config.runtime.internal_listen.port(), 0);
        assert_eq!(config.runtime.metadata_voters, [11]);
        assert_eq!(config.runtime.groups.first().unwrap().voters, [21]);
        assert_eq!(config.runtime.peers.len(), 4);
    }
    fixture.close().await;
}

#[tokio::test]
#[should_panic(expected = "Combined fixtures require one local process and cannot mix roles")]
async fn fixture_rejects_multiple_combined_nodes() {
    Fixture::new(
        &[
            (1, Role::Combined),
            (2, Role::Combined),
            (3, Role::Combined),
        ],
        &[1, 2, 3],
        &[1, 2, 3],
    )
    .await;
}

#[tokio::test]
#[should_panic(expected = "Combined fixtures require one local process and cannot mix roles")]
async fn fixture_rejects_combined_mixed_with_separated_roles() {
    Fixture::new(&[(1, Role::Combined), (2, Role::Gateway)], &[1], &[1]).await;
}

fn https_client(fixture: &Fixture) -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .tls_certs_only(reqwest::Certificate::from_pem_bundle(&fixture.roots).unwrap())
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
}

fn console_url(config: &Config, path: &str) -> String {
    format!(
        "{}{path}",
        config.console.as_ref().unwrap().session.public_origin
    )
}

async fn assert_ports_released(config: &Config) {
    let until = Instant::now() + Duration::from_secs(3);
    loop {
        let public = UdpSocket::bind(config.listen);
        let management = std::net::TcpListener::bind(config.management_listen);
        let console = std::net::TcpListener::bind(config.console.as_ref().unwrap().listen);
        if public.is_ok() && management.is_ok() && console.is_ok() {
            return;
        }
        assert!(
            Instant::now() < until,
            "Combined listeners were not released: QUIC={public:?}, management={management:?}, HTTPS={console:?}"
        );
        drop((public, management, console));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn management_response(address: SocketAddr, path: &str) -> Option<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tokio::time::timeout(Duration::from_millis(500), async {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await?;
        let mut response = String::new();
        stream.take(65_536).read_to_string(&mut response).await?;
        Ok::<_, std::io::Error>(response)
    })
    .await
    .ok()
    .and_then(Result::ok)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_successful_ack_survives_kill_and_unknown_send_keeps_original_identity() {
    let fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    let config = fixture.configs.get(&1).unwrap();
    let mut process = start_process(&fixture, 1).await;
    let client = fixture.client(1).await;
    create_logical_resources(&client).await;
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "combined-recovery")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let first = producer
        .send("committed-before-kill")
        .await
        .unwrap()
        .message_id();
    let delivery = consumer.receive().await.unwrap();
    assert_eq!(delivery.message_id(), first);
    assert_eq!(
        consumer.ack(&delivery).await.unwrap(),
        tidemq_sdk::AckReceipt::Acknowledged
    );
    process.kill().await.unwrap();
    assert!(!process.wait().await.unwrap().success());
    assert_ports_released(config).await;
    let error = tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(200))
        .unwrap()
        .scope(producer.send("original-unknown-send"))
        .await
        .unwrap_err();
    assert!(error.outcome_unknown());
    let pending = producer.pending_sends().await.unwrap();
    assert_eq!(pending.len(), 1);
    let original = pending.first().unwrap();
    let mut process = start_process(&fixture, 1).await;
    let second = original.retry().await.unwrap().message_id();
    assert_ne!(first, second);
    assert_eq!(original.retry().await.unwrap().message_id(), second);
    assert!(producer.pending_sends().await.unwrap().is_empty());
    let delivery = consumer.receive().await.unwrap();
    assert_eq!(
        delivery.message_id(),
        second,
        "A successful durable ACK was lost after restart"
    );
    assert_eq!(delivery.payload(), b"original-unknown-send");
    consumer.ack(&delivery).await.unwrap();
    tokio::fs::write(fixture.directory.join("combined-recovery.json"),
        serde_json::to_vec_pretty(&serde_json::json!({"first":first.to_string(),"recovered":second.to_string(),"original_retry_stable":true,"durable_ack_preserved":true})).unwrap()).await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    process.kill().await.unwrap();
    let _ = process.wait().await.unwrap();
    assert_ports_released(config).await;
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_startup_material_failures_release_ports_and_allow_same_directory_retry() {
    for material in [
        "index.html",
        "assets/fixture.js",
        "assets/fixture.css",
        "session",
        "certificate",
        "private-key",
    ] {
        let fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        let config = fixture.configs.get(&1).unwrap();
        let console = config.console.as_ref().unwrap();
        let path = match material {
            "session" => console.session.key_file.clone(),
            "certificate" => config.tls.certificate.clone(),
            "private-key" => config.tls.private_key.clone(),
            asset => console.static_dir.join(asset),
        };
        let original = tokio::fs::read(&path).await.unwrap();
        tokio::fs::remove_file(&path).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(10), RunningNode::start(config.clone()))
                .await
                .unwrap()
                .is_err(),
            "Missing {material} was accepted"
        );
        assert_ports_released(config).await;
        tokio::fs::write(&path, original).await.unwrap();
        if matches!(material, "session" | "private-key") {
            private_file(path).await;
        }
        let node = RunningNode::start(config.clone()).await.unwrap();
        assert!(node.internal_address.is_none());
        node.shutdown().await.unwrap();
        assert_ports_released(config).await;
        fixture.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_partial_bind_failures_release_groups_and_all_listeners() {
    for occupied in ["quic", "https", "management"] {
        let fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        let config = fixture.configs.get(&1).unwrap();
        let public = (occupied == "quic").then(|| UdpSocket::bind(config.listen).unwrap());
        let tcp = match occupied {
            "https" => {
                Some(std::net::TcpListener::bind(config.console.as_ref().unwrap().listen).unwrap())
            }
            "management" => Some(std::net::TcpListener::bind(config.management_listen).unwrap()),
            _ => None,
        };
        assert!(
            tokio::time::timeout(Duration::from_secs(10), RunningNode::start(config.clone()))
                .await
                .unwrap()
                .is_err(),
            "Occupied {occupied} port was accepted"
        );
        drop((public, tcp));
        assert_ports_released(config).await;
        let node = RunningNode::start(config.clone()).await.unwrap();
        assert!(node.internal_address.is_none());
        node.shutdown().await.unwrap();
        assert_ports_released(config).await;
        fixture.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_draining_completes_ack_and_releases_the_original_directory_before_deadline() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let config = fixture.configs.get(&1).unwrap().clone();
    let client = fixture.client(1).await;
    create_logical_resources(&client).await;
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "combined-drain")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let first = producer
        .send("ack-during-drain")
        .await
        .unwrap()
        .message_id();
    let delivery = consumer.receive().await.unwrap();
    let node = fixture.running.remove(&1).unwrap();
    assert!(node.internal_address.is_none());
    assert_eq!(
        node.console_address,
        Some(config.console.as_ref().unwrap().listen)
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let shutdown = tokio::spawn(async move {
        tokio::time::timeout_at(deadline, node.shutdown_until(deadline))
            .await
            .expect("Combined extended its absolute shutdown deadline")
    });
    loop {
        if management_response(config.management_listen, "/readyz")
            .await
            .is_some_and(|response| response.starts_with("HTTP/1.1 503"))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Shutdown did not close readiness before completion admission"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let receipt = tidemq_sdk::RequestContext::with_deadline(deadline)
        .scope(consumer.ack(&delivery))
        .await
        .unwrap();
    assert_eq!(receipt, tidemq_sdk::AckReceipt::Acknowledged);
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    shutdown.await.unwrap().unwrap();
    tokio::time::timeout_at(deadline, assert_ports_released(&config))
        .await
        .expect("Combined retained listeners past its shutdown deadline");
    tokio::time::timeout_at(deadline, fixture.restart(1))
        .await
        .expect("Combined retained its original directory past the shutdown deadline");
    let client = fixture.client(1).await;
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "combined-drain")
        .build()
        .await
        .unwrap();
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let second = producer
        .send("after-drain-restart")
        .await
        .unwrap()
        .message_id();
    assert_ne!(second, first);
    let delivery = consumer.receive().await.unwrap();
    assert_eq!(delivery.message_id(), second);
    consumer.ack(&delivery).await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
    assert_ports_released(&config).await;
}

#[cfg(target_os = "linux")]
async fn process_ports(pid: u32, protocol: &str) -> BTreeSet<u16> {
    let mut inodes = BTreeSet::new();
    let mut descriptors = tokio::fs::read_dir(format!("/proc/{pid}/fd"))
        .await
        .unwrap();
    while let Some(descriptor) = descriptors.next_entry().await.unwrap() {
        let target = match tokio::fs::read_link(descriptor.path()).await {
            Ok(target) => target,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => panic!("Failed to observe a Combined descriptor: {error}"),
        };
        let target = target.to_string_lossy();
        if let Some(inode) = target
            .strip_prefix("socket:[")
            .and_then(|value| value.strip_suffix(']'))
        {
            inodes.insert(inode.to_owned());
        }
    }
    let mut ports = BTreeSet::new();
    for family in [protocol.to_owned(), format!("{protocol}6")] {
        let entries = match tokio::fs::read_to_string(format!("/proc/{pid}/net/{family}")).await {
            Ok(entries) => entries,
            Err(error) if family.ends_with('6') && error.kind() == std::io::ErrorKind::NotFound => {
                continue;
            }
            Err(error) => panic!("Process socket observation failed: {error}"),
        };
        for line in entries.lines().skip(1) {
            let fields: Vec<_> = line.split_ascii_whitespace().collect();
            if !fields.get(9).is_some_and(|inode| inodes.contains(*inode)) {
                continue;
            }
            if protocol == "tcp" && fields.get(3) != Some(&"0A") {
                continue;
            }
            let (_, port) = fields.get(1).unwrap().rsplit_once(':').unwrap();
            ports.insert(u16::from_str_radix(port, 16).unwrap());
        }
    }
    ports
}

#[cfg(target_os = "macos")]
async fn process_ports(pid: u32, protocol: &str) -> BTreeSet<u16> {
    let mut command = tokio::process::Command::new("lsof");
    command.args([
        "-nP",
        "-a",
        "-p",
        &pid.to_string(),
        &format!("-i{}", protocol.to_ascii_uppercase()),
        "-Fpn",
    ]);
    if protocol == "tcp" {
        command.arg("-sTCP:LISTEN");
    }
    let output = command.output().await.unwrap();
    assert!(
        output.status.success(),
        "Process socket observation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = String::from_utf8(output.stdout).unwrap();
    assert!(
        output.lines().any(|line| line == format!("p{pid}")),
        "Socket observations belong to another process"
    );
    output
        .lines()
        .filter_map(|line| line.strip_prefix('n'))
        .map(|address| {
            assert!(
                !address.contains("->"),
                "Combined opened an internal client socket: {address}"
            );
            address.rsplit_once(':').unwrap().1.parse().unwrap()
        })
        .collect()
}

fn assert_local_diagnostics(document: &serde_json::Value) {
    let metadata = document
        .get("metadata")
        .expect("Local metadata diagnostics are missing");
    let groups = document
        .get("data_groups")
        .and_then(serde_json::Value::as_array)
        .expect("Local data diagnostics are missing");
    assert!(!groups.is_empty(), "Local data group diagnostics are empty");
    let metadata_storage = document
        .get("metadata_storage")
        .expect("Local metadata storage diagnostics are missing");
    for group in [metadata, metadata_storage].into_iter().chain(groups) {
        assert!(group.is_object(), "Local group diagnostics are missing");
        for field in [
            "leader",
            "term",
            "voters",
            "joint_configs",
            "bootstrap_voters",
            "membership",
        ] {
            assert!(
                group.get(field).is_none(),
                "Local diagnostics fabricated Raft field {field}"
            );
        }
    }
}

async fn assert_no_child_processes(pid: u32) {
    #[cfg(target_os = "linux")]
    {
        let mut threads = tokio::fs::read_dir(format!("/proc/{pid}/task"))
            .await
            .unwrap();
        while let Some(thread) = threads.next_entry().await.unwrap() {
            match tokio::fs::read_to_string(thread.path().join("children")).await {
                Ok(children) => assert!(
                    children.trim().is_empty(),
                    "Combined spawned child processes: {children}"
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    assert!(
                        !tokio::fs::try_exists(thread.path()).await.unwrap(),
                        "A live Combined thread has no child process observation"
                    );
                }
                Err(error) => panic!("Failed to observe Combined child processes: {error}"),
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        let output = tokio::process::Command::new("pgrep")
            .args(["-P", &pid.to_string()])
            .output()
            .await
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(1),
            "Combined spawned child processes: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_quic_https_and_management_belong_to_one_pid_without_internal_sockets() {
    let fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    let config = fixture.configs.get(&1).unwrap();
    let mut process = start_process(&fixture, 1).await;
    let pid = process.id().unwrap();
    let browser = https_client(&fixture);
    let response = browser.get(console_url(config, "/")).send().await.unwrap();
    assert!(response.status().is_success());
    assert!(response.text().await.unwrap().contains("fixture.js"));
    let response = browser
        .post(console_url(config, "/api/v1/session"))
        .header(
            "origin",
            config
                .console
                .as_ref()
                .unwrap()
                .session
                .public_origin
                .as_str(),
        )
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&serde_json::json!({"token":TOKEN})).unwrap())
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Native Console login failed"
    );
    let identity: serde_json::Value =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(
        identity.pointer("/capabilities/execution"),
        Some(&serde_json::json!("local"))
    );
    let client = fixture.client(1).await;
    create_logical_resources(&client).await;
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    producer.send("same-process").await.unwrap();
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "same-process")
        .build()
        .await
        .unwrap();
    let delivery = consumer.receive().await.unwrap();
    consumer.ack(&delivery).await.unwrap();
    let udp = process_ports(pid, "udp").await;
    let tcp = process_ports(pid, "tcp").await;
    assert_no_child_processes(pid).await;
    assert!(process.try_wait().unwrap().is_none());
    assert_eq!(
        udp,
        BTreeSet::from([config.listen.port()]),
        "Combined opened an internal QUIC endpoint"
    );
    assert_eq!(
        tcp,
        BTreeSet::from([
            config.management_listen.port(),
            config.console.as_ref().unwrap().listen.port()
        ])
    );
    let response = management_response(config.management_listen, "/diagnostics")
        .await
        .unwrap();
    let (_, body) = response.split_once("\r\n\r\n").unwrap();
    let diagnostics: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_local_diagnostics(&diagnostics);
    tokio::fs::write(fixture.directory.join("combined-process.json"), serde_json::to_vec_pretty(
        &serde_json::json!({"pid":pid,"udp_ports":udp,"tcp_listeners":tcp,"diagnostics":diagnostics})
    ).unwrap()).await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    process.kill().await.unwrap();
    let _ = process.wait().await.unwrap();
    assert_ports_released(config).await;
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_required_asset_loss_fails_readiness_and_cannot_reopen_after_restore() {
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let config = fixture.configs.get(&1).unwrap().clone();
    let browser = https_client(&fixture);
    let client = fixture.client(1).await;
    create_logical_resources(&client).await;
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    let response = browser
        .get(console_url(&config, "/missing.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    response.bytes().await.unwrap();
    let response = browser
        .get(console_url(&config, "/assets/fixture.js"))
        .header("range", "bytes=invalid")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE
    );
    response.bytes().await.unwrap();
    assert!(
        management_response(config.management_listen, "/readyz")
            .await
            .unwrap()
            .starts_with("HTTP/1.1 200")
    );
    let asset = config
        .console
        .as_ref()
        .unwrap()
        .static_dir
        .join("assets/fixture.js");
    let original = tokio::fs::read(&asset).await.unwrap();
    tokio::fs::remove_file(&asset).await.unwrap();
    let _ = browser
        .get(console_url(&config, "/assets/fixture.js"))
        .send()
        .await;
    let node = fixture.running.remove(&1).unwrap();
    tokio::time::timeout(Duration::from_secs(5), node.wait_for_shutdown_request())
        .await
        .unwrap();
    assert!(
        management_response(config.management_listen, "/readyz")
            .await
            .unwrap()
            .starts_with("HTTP/1.1 503"),
        "Required asset failure did not close process readiness"
    );
    let refused =
        tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(300))
            .unwrap()
            .scope(producer.send(
                tidemq_sdk::Message::new("must-not-commit-after-failure").key("failure-probe"),
            ))
            .await
            .expect_err("A failed Combined process accepted a new publish");
    tokio::fs::write(&asset, original).await.unwrap();
    let _ = browser
        .get(console_url(&config, "/assets/fixture.js"))
        .send()
        .await;
    assert!(
        management_response(config.management_listen, "/readyz")
            .await
            .unwrap()
            .starts_with("HTTP/1.1 503"),
        "A late asset restore reopened failed admission"
    );
    let _ = producer.close().await;
    // A deliberately unknown send remains reported even after all local SDK resources close.
    let close = client.close().await.unwrap_or_else(|error| {
        assert!(refused.outcome_unknown());
        assert_eq!(error.report().unresolved_sends(), 1);
        error.report().clone()
    });
    assert!(close.local_cleanup_complete());
    assert_eq!(close.members_left(), 0);
    assert_eq!(close.unresolved_joins(), 0);
    assert_eq!(close.unresolved_acks(), 0);
    assert_eq!(close.unresolved_reads(), 0);
    assert_eq!(close.unresolved_watches(), 0);
    assert_eq!(close.unresolved_deletions(), 0);
    assert_eq!(close.unresolved_administrations(), 0);
    assert_eq!(close.unclaimed_results(), 0);
    assert!(
        tokio::time::timeout(Duration::from_secs(6), node.shutdown())
            .await
            .unwrap()
            .is_err()
    );
    assert_ports_released(&config).await;
    drop(browser);
    fixture.restart(1).await;
    let client = fixture.client(1).await;
    let producer = client.new_producer(RUNTIME_TOPIC).build().await.unwrap();
    // A committed marker on the same shard proves recovery is live before checking absence.
    let marker = producer
        .send(tidemq_sdk::Message::new("committed-after-recovery").key("failure-probe"))
        .await
        .unwrap()
        .message_id();
    let consumer = client
        .new_queue_consumer(RUNTIME_TOPIC, "after-asset-failure")
        .build()
        .await
        .unwrap();
    let delivery = consumer.receive().await.unwrap();
    assert_eq!(
        delivery.message_id(),
        marker,
        "A post-failure publish survived restart"
    );
    assert_eq!(delivery.payload(), b"committed-after-recovery");
    consumer.ack(&delivery).await.unwrap();
    let delivery = tidemq_sdk::RequestContext::with_timeout(Duration::from_millis(300))
        .unwrap()
        .scope(consumer.receive())
        .await;
    assert!(delivery.is_err(), "A post-failure publish survived restart");
    tokio::fs::write(
        fixture.directory.join("combined-asset-failure.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "failed_send_outcome_unknown": refused.outcome_unknown(),
            "sdk_close_unresolved_sends": close.unresolved_sends(),
            "sdk_local_cleanup_complete": close.local_cleanup_complete(),
            "recovery_marker": marker.to_string(),
            "failed_publish_absent_after_recovery": true,
            "readiness_failed_after_restore": true
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    fixture.close().await;
    assert_ports_released(&config).await;
}
