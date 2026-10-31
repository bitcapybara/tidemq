use super::*;

use tokio::io::AsyncWriteExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_http_rejects_business_csrf_before_admitting_any_core_operation() {
    let fixture = dynamic_fixture().await;
    let root = fixture.client(1).await;
    let tenant = root.admin().create_tenant("acme").await.unwrap();
    let namespace = root.admin().create_namespace(tenant, "app").await.unwrap();
    let topic = root
        .admin()
        .create_topic("acme/app/protected")
        .await
        .unwrap();
    let token = manager_token(&root, tenant, namespace).await;
    let direct = token_client(&fixture, token.expose_secret()).await;
    let consoles = Consoles::start(&fixture).await;
    let session = consoles.login(0, token.expose_secret()).await;
    let other = consoles.login(1, token.expose_secret()).await;
    assert_ne!(session.csrf, other.csrf);
    let create_id = Id::new_v7().unwrap();
    let create = json!({
        "operationId": create_id,
        "tenantId": tenant.to_string(),
        "namespaceId": namespace.to_string(),
        "name": "csrf-checked"
    });
    let requests = [
        (Method::POST, "topics".to_owned(), create.clone()),
        (
            Method::POST,
            format!("topics/{}/send", topic.id()),
            json!({
                "operationId": Id::new_v7().unwrap(),
                "payloadBase64": STANDARD.encode("must-not-be-published"),
                "keyBase64": null,
                "propertiesBase64": {}
            }),
        ),
        (
            Method::DELETE,
            format!("topics/{}", topic.id()),
            json!({"operationId": Id::new_v7().unwrap()}),
        ),
    ];
    for (method, path, body) in requests {
        let operation = serde_json::from_value(body.get("operationId").unwrap().clone()).unwrap();
        for (origin, csrf, code) in [
            (Some(ORIGIN), None, "csrf_rejected"),
            (Some(ORIGIN), Some(other.csrf.as_str()), "csrf_rejected"),
            (None, Some(session.csrf.as_str()), "origin_rejected"),
            (
                Some("https://evil.example.test"),
                Some(session.csrf.as_str()),
                "origin_rejected",
            ),
        ] {
            let mut request = consoles
                .client
                .request(method.clone(), consoles.url(1, &path))
                .header(header::COOKIE, &session.cookie);
            if let Some(origin) = origin {
                request = request.header(header::ORIGIN, origin);
            }
            if let Some(csrf) = csrf {
                request = request.header("x-csrf-token", csrf);
            }
            let rejected = response_json(
                json_body(request, &body).send().await.unwrap(),
                StatusCode::FORBIDDEN,
            )
            .await;
            assert_eq!(rejected.get("code").unwrap(), code);
            assert_eq!(
                direct.console_operation(operation).await.unwrap().status,
                ConsoleStatus::Unknown,
                "A rejected browser request must not reserve an operation identity"
            );
        }
    }
    let Reply::Topic(untouched) = root
        .console_read(Query::Topic {
            topic: topic.id().into(),
        })
        .await
        .unwrap()
    else {
        panic!("Expected the protected Topic to remain available");
    };
    assert_eq!(untouched.shard_details.len(), 1);
    assert_eq!(
        untouched.shard_details.first().unwrap().committed_end,
        Some(tidemq_protocol::console::Decimal(0))
    );
    let accepted = write(&consoles, 0, &session, Method::POST, "topics", &create).await;
    succeeded(&accepted);
    assert_eq!(accepted.get("id").unwrap(), &json!(create_id));
    direct.close().await.unwrap();
    consoles.close().await;
    root.close().await.unwrap();
    fixture.close().await;
}

async fn assert_revoked_browser_write(
    consoles: &Consoles,
    instance: usize,
    session: &BrowserSession,
    operation: Id,
    create: &Value,
) {
    for request in [
        consoles.request(
            instance,
            session,
            Method::GET,
            &format!("operations/{operation}"),
        ),
        json_body(
            consoles.request(instance, session, Method::POST, "topics"),
            create,
        ),
    ] {
        let response = request.send().await.unwrap();
        assert!(
            response
                .headers()
                .get(header::SET_COOKIE)
                .unwrap()
                .to_str()
                .unwrap()
                .contains("Max-Age=0")
        );
        let refused = response_json(response, StatusCode::UNAUTHORIZED).await;
        assert_eq!(refused.get("code").unwrap(), "authentication_required");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_http_revoked_session_cannot_recover_or_submit_writes_after_restart() {
    let mut fixture = dynamic_fixture().await;
    let root = fixture.client(1).await;
    let tenant = root.admin().create_tenant("acme").await.unwrap();
    let namespace = root.admin().create_namespace(tenant, "app").await.unwrap();
    let token = manager_token(&root, tenant, namespace).await;
    let consoles = Consoles::start(&fixture).await;
    let session = consoles.login(0, token.expose_secret()).await;
    let original_id = Id::new_v7().unwrap();
    let create = |operation: Id, name| {
        json!({
            "operationId": operation,
            "tenantId": tenant.to_string(),
            "namespaceId": namespace.to_string(),
            "name": name
        })
    };
    let original = write(
        &consoles,
        0,
        &session,
        Method::POST,
        "topics",
        &create(original_id, "before-revocation"),
    )
    .await;
    succeeded(&original);
    root.admin().revoke_token(token.id()).await.unwrap();
    let forbidden = create(Id::new_v7().unwrap(), "must-not-be-created");
    for instance in [0, 1] {
        assert_revoked_browser_write(&consoles, instance, &session, original_id, &forbidden).await;
    }
    root.close().await.unwrap();
    fixture.stop(1).await;
    fixture.restart(1).await;
    assert_revoked_browser_write(&consoles, 1, &session, original_id, &forbidden).await;
    let root = fixture.client(1).await;
    assert_eq!(
        root.admin()
            .topic_info("acme/app/before-revocation")
            .await
            .unwrap()
            .id()
            .to_string(),
        original.get("resourceId").unwrap().as_str().unwrap()
    );
    let Reply::Topics(topics) = root
        .console_read(Query::Topics {
            tenant: Some(tenant.into()),
            namespace: Some(namespace.into()),
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected the authoritative Topic directory after revocation");
    };
    assert_eq!(topics.items.len(), 1);
    assert_eq!(
        topics.items.first().unwrap().name,
        "acme/app/before-revocation"
    );
    assert!(topics.next_cursor.is_none());
    root.close().await.unwrap();
    consoles.close().await;
    fixture.close().await;
}

async fn submit_without_reading_response(
    consoles: &Consoles,
    trust_roots: &[u8],
    session: &BrowserSession,
    path: &str,
    body: &Value,
) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
    use std::sync::Arc;

    use tokio_rustls::rustls::{
        self, RootCertStore,
        pki_types::{CertificateDer, ServerName, pem::PemObject},
    };

    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(trust_roots) {
        roots.add(certificate.unwrap()).unwrap();
    }
    assert!(!roots.is_empty(), "The fixture CA must be configured");
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let socket = tokio::net::TcpStream::connect(("127.0.0.1", consoles.first.address.port()))
        .await
        .unwrap();
    let mut stream = connector
        .connect(ServerName::try_from("127.0.0.1").unwrap(), socket)
        .await
        .unwrap();
    assert!(!stream.get_ref().1.is_handshaking());
    eprintln!(
        "HTTPS fixture CA and server identity verified before submitting the unread-response request"
    );
    let body = serde_json::to_vec(body).unwrap();
    let headers = format!(
        "POST /api/v1/{path} HTTP/1.1\r\nHost: localhost\r\nOrigin: {ORIGIN}\r\nCookie: {}\r\nx-csrf-token: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        session.cookie,
        session.csrf,
        body.len()
    );
    stream.write_all(headers.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    stream.flush().await.unwrap();
    stream
}

async fn committed_console_operation(client: &Client, operation: Id) -> Value {
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        let result = client.console_operation(operation).await.unwrap();
        if result.status == ConsoleStatus::Succeeded {
            assert!(result.receipt.is_some());
            return serde_json::to_value(result).unwrap();
        }
        assert_eq!(
            result.status,
            ConsoleStatus::Unknown,
            "The original HTTP operation was refused: reason={:?}",
            result.reason
        );
        assert!(
            Instant::now() < until,
            "The HTTP write did not commit in time"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_http_lost_send_response_recovers_after_restart_without_republishing() {
    let mut fixture = Fixture::new(
        &[
            (1, Role::Gateway),
            (11, Role::Metadata),
            (21, Role::Storage),
            (22, Role::Storage),
            (31, Role::Broker),
        ],
        &[11],
        &[21],
    )
    .await;
    for config in fixture.configs.values_mut() {
        config.runtime.groups.push(GroupConfig {
            group_id: 3,
            voters: vec![22],
            standby: Vec::new(),
        });
    }
    fixture.start_all().await;
    let root = fixture.client(1).await;
    let tenant = root.admin().create_tenant("acme").await.unwrap();
    let namespace = root.admin().create_namespace(tenant, "app").await.unwrap();
    let topic = root
        .admin()
        .create_topic("acme/app/recovery")
        .await
        .unwrap();
    let stream = root
        .new_stream_consumer("acme/app/recovery", "durable")
        .build()
        .await
        .unwrap();
    let token = manager_token(&root, tenant, namespace).await;
    let direct = token_client(&fixture, token.expose_secret()).await;
    let consoles = Consoles::start(&fixture).await;
    let session = consoles.login(0, token.expose_secret()).await;
    let operation = Id::new_v7().unwrap();
    let send = |operation, body| {
        json!({
            "operationId": operation,
            "payloadBase64": STANDARD.encode(body),
            "keyBase64": STANDARD.encode("recovery-key"),
            "propertiesBase64": {}
        })
    };
    let path = format!("topics/{}/send", topic.id());
    let original = send(operation, "unobserved-response");
    let abandoned =
        submit_without_reading_response(&consoles, &fixture.roots, &session, &path, &original)
            .await;
    let expected = committed_console_operation(&direct, operation).await;
    eprintln!("Original Console receipt observed before discarding the unread HTTPS response");
    drop(abandoned);
    let first = tokio::time::timeout(Duration::from_secs(10), stream.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.payload(), b"unobserved-response");
    let original_message = first.message_id();
    stream.ack_cumulative(&first).await.unwrap();
    direct.close().await.unwrap();
    for node in [11, 21, 22] {
        fixture.stop(node).await;
    }
    // Recover data before restarting the controller, as the single-process fixture did.
    for node in [21, 22, 11] {
        fixture.running.insert(
            node,
            RunningNode::start(fixture.configs.get(&node).unwrap().clone())
                .await
                .unwrap(),
        );
    }
    fixture.wait_ready().await;
    let probe = fixture.client(1).await;
    fixture
        .wait_for_group_leader(&probe, 1, "metadata", 1)
        .await;
    fixture.wait_for_group_leader(&probe, 2, "storage", 1).await;
    fixture.wait_for_group_leader(&probe, 3, "storage", 1).await;
    probe.close().await.unwrap();
    assert_original_console_receipt(&consoles, &session, &expected).await;
    let replay = write(&consoles, 1, &session, Method::POST, &path, &original).await;
    succeeded(&replay);
    assert_eq!(replay.get("receipt"), expected.get("receipt"));
    let marker = write(
        &consoles,
        1,
        &session,
        Method::POST,
        &path,
        &send(Id::new_v7().unwrap(), "after-restart"),
    )
    .await;
    succeeded(&marker);
    let next = tokio::time::timeout(Duration::from_secs(10), stream.receive())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(next.message_id(), original_message);
    assert_eq!(next.payload(), b"after-restart");
    stream.ack_cumulative(&next).await.unwrap();
    let Reply::Topic(observed) = root
        .console_read(Query::Topic {
            topic: topic.id().into(),
        })
        .await
        .unwrap()
    else {
        panic!("Expected the persisted message boundaries after recovery");
    };
    assert_eq!(observed.shard_details.len(), 1);
    assert_eq!(
        observed.shard_details.first().unwrap().committed_end,
        Some(tidemq_protocol::console::Decimal(2))
    );
    stream.close().await.unwrap();
    root.close().await.unwrap();
    consoles.close().await;
    fixture.close().await;
}

async fn recover_original_console_send(
    consoles: &Consoles,
    session: &BrowserSession,
    path: &str,
    body: &Value,
) -> Value {
    let operation = body.get("operationId").unwrap().as_str().unwrap();
    let lookup = format!("operations/{operation}");
    let until = Instant::now() + Duration::from_secs(30);
    let mut submit = true;
    let mut unknown = 0;
    loop {
        let request = if submit {
            json_body(consoles.request(1, session, Method::POST, path), body)
        } else {
            consoles.request(1, session, Method::GET, &lookup)
        };
        let response = tokio::time::timeout_at(until, request.send())
            .await
            .expect("The original Console operation did not recover within its bound")
            .unwrap();
        let status = response.status();
        let result = response_json(response, status).await;
        if status == StatusCode::OK {
            if result.get("status").unwrap() != "unknown" {
                succeeded(&result);
                assert_eq!(result.get("id").unwrap(), operation);
                eprintln!(
                    "Original Console send recovered: operation={operation}, unknown_results={unknown}"
                );
                return result;
            }
            submit = !submit;
        } else {
            assert!(matches!(
                status,
                StatusCode::GATEWAY_TIMEOUT | StatusCode::SERVICE_UNAVAILABLE
            ));
            assert!(matches!(
                result.get("code").and_then(Value::as_str),
                Some("outcome_unknown" | "upstream_unavailable")
            ));
            submit = false;
        }
        unknown += 1;
        assert!(
            Instant::now() < until,
            "The original Console send remained unresolved"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn receive_original_stream(
    stream: &tidemq_sdk::StreamConsumer,
) -> tidemq_sdk::ReceivedMessage {
    let until = Instant::now() + Duration::from_secs(45);
    let mut first_error = true;
    loop {
        match tidemq_sdk::RequestContext::with_deadline(until)
            .scope(stream.receive())
            .await
        {
            Ok(message) => return message,
            Err(error) => {
                assert!(
                    matches!(
                        error.kind(),
                        tidemq_sdk::ErrorKind::Unavailable
                            | tidemq_sdk::ErrorKind::DeadlineExceeded
                            | tidemq_sdk::ErrorKind::OutcomeUnknown
                    ),
                    "The original stream failed permanently: {error:?}"
                );
                assert!(
                    Instant::now() < until,
                    "The original stream did not recover: {error:?}"
                );
                if first_error {
                    eprintln!("The original stream is recovering: {error:?}");
                    first_error = false;
                }
            }
        }
        assert!(
            Instant::now() < until,
            "The original stream did not recover"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn console_http_lost_send_receipt_recovers_across_rf3_leader_failover_without_duplicates() {
    let nodes = [
        (1, Role::Gateway),
        (11, Role::Metadata),
        (12, Role::Metadata),
        (13, Role::Metadata),
        (21, Role::Storage),
        (22, Role::Storage),
        (23, Role::Storage),
        (31, Role::Broker),
    ];
    let mut fixture = Fixture::new(&nodes, &[11, 12, 13], &[21, 22, 23]).await;
    fixture.start_all().await;
    let root = fixture.client(1).await;
    fixture.wait_for_quorum_leaders(&root, 2).await;
    let tenant = root.admin().create_tenant("acme").await.unwrap();
    let namespace = root.admin().create_namespace(tenant, "app").await.unwrap();
    let topic = root
        .admin()
        .create_topic("acme/app/recovery")
        .await
        .unwrap();
    let stream = root
        .new_stream_consumer("acme/app/recovery", "durable")
        .build()
        .await
        .unwrap();
    let (principal, token) = personal_token(
        &root,
        tenant,
        namespace,
        "console-manager",
        [
            Permission::ManageCredentials,
            Permission::ManageTopics,
            Permission::Inspect,
            Permission::InspectMessageContent,
            Permission::Produce,
        ],
    )
    .await;
    let direct = token_client(&fixture, token.expose_secret()).await;
    let control = direct
        .new_producer("acme/app/recovery")
        .build()
        .await
        .unwrap();
    let control_id = control.send("sdk-control").await.unwrap().message_id();
    let control_message = tokio::time::timeout(Duration::from_secs(10), stream.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(control_message.message_id(), control_id);
    stream.ack_cumulative(&control_message).await.unwrap();
    let consoles = Consoles::start(&fixture).await;
    let session = consoles.login(0, token.expose_secret()).await;
    let operation = Id::new_v7().unwrap();
    let send = |operation, body| {
        json!({
            "operationId": operation,
            "payloadBase64": STANDARD.encode(body),
            "keyBase64": STANDARD.encode("recovery-key"),
            "propertiesBase64": {"source": STANDARD.encode("lost-http-receipt")}
        })
    };
    let path = format!("topics/{}/send", topic.id());
    let original = send(operation, "original-body");
    let abandoned =
        submit_without_reading_response(&consoles, &fixture.roots, &session, &path, &original)
            .await;
    // Observe core admission before discarding the response, avoiding a cancellation-before-submit race.
    let expected = committed_console_operation(&direct, operation).await;
    eprintln!("RF3 Console original send committed: operation={operation}");
    eprintln!("Original Console receipt observed before discarding the unread HTTPS response");
    drop(abandoned);
    let first = tokio::time::timeout(Duration::from_secs(10), stream.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.payload(), b"original-body");
    let original_message = first.message_id();
    stream.ack_cumulative(&first).await.unwrap();
    let operator = consoles.login(1, TOKEN).await;
    let operator_result = write(
        &consoles,
        1,
        &operator,
        Method::POST,
        &path,
        &send(Id::new_v7().unwrap(), "operator-body"),
    )
    .await;
    succeeded(&operator_result);
    let operator_message = receive_original_stream(&stream).await;
    assert_eq!(operator_message.payload(), b"operator-body");
    stream.ack_cumulative(&operator_message).await.unwrap();
    eprintln!("RF3 Console operator send preserved the credential tenant scope");
    let delegated =
        super::delegation::assert_bindings(&fixture, &root, operation, &token, tenant, principal)
            .await;
    let (metadata, storage) = fixture.wait_for_quorum_leaders(&root, 2).await;
    fixture.stop(metadata).await;
    fixture.stop(storage).await;
    let (next_metadata, next_storage) = fixture.wait_for_quorum_leaders(&root, 2).await;
    eprintln!("RF3 Console leaders changed: metadata={next_metadata}, storage={next_storage}");
    assert_ne!(metadata, next_metadata);
    assert_ne!(storage, next_storage);
    let sdk_until = Instant::now() + Duration::from_secs(15);
    let sdk_after = match tidemq_sdk::RequestContext::with_deadline(sdk_until)
        .scope(control.send("post-failover-sdk-control"))
        .await
    {
        Ok(receipt) => receipt.message_id(),
        Err(error) => {
            assert!(
                error.outcome_unknown(),
                "The original SDK control send was rejected: {error:?}"
            );
            let pending = control.pending_sends().await.unwrap();
            assert_eq!(pending.len(), 1);
            let pending = pending.first().unwrap();
            loop {
                let retry = tidemq_sdk::RequestContext::with_deadline(sdk_until)
                    .scope(pending.retry())
                    .await;
                match retry {
                    Ok(receipt) => break receipt.message_id(),
                    Err(error) => {
                        assert!(
                            error.outcome_unknown(),
                            "The original send was rejected: {error:?}"
                        );
                        assert!(
                            Instant::now() < sdk_until,
                            "The original send could not recover: {error:?}"
                        );
                    }
                }
            }
        }
    };
    let sdk_message = receive_original_stream(&stream).await;
    assert_eq!(sdk_message.message_id(), sdk_after);
    stream.ack_cumulative(&sdk_message).await.unwrap();
    control.close().await.unwrap();
    assert_original_console_receipt(&consoles, &session, &expected).await;
    eprintln!("RF3 Console replaying the original committed send");
    let replay = write(&consoles, 1, &session, Method::POST, &path, &original).await;
    assert_eq!(replay.get("receipt"), expected.get("receipt"));
    assert_eq!(replay.get("resourceId"), expected.get("resourceId"));
    succeeded(&replay);
    let changed = send(operation, "changed-body");
    response_json(
        json_body(consoles.request(1, &session, Method::POST, &path), &changed)
            .send()
            .await
            .unwrap(),
        StatusCode::CONFLICT,
    )
    .await;
    let marker = recover_original_console_send(
        &consoles,
        &session,
        &path,
        &send(Id::new_v7().unwrap(), "after-failover"),
    )
    .await;
    eprintln!("RF3 Console marker send completed");
    succeeded(&marker);
    let next = receive_original_stream(&stream).await;
    assert_ne!(next.message_id(), original_message);
    assert_eq!(next.payload(), b"after-failover");
    stream.ack_cumulative(&next).await.unwrap();
    fixture.restart(metadata).await;
    fixture.restart(storage).await;
    eprintln!("RF3 Console stopped replicas restarted");
    assert_original_console_receipt(&consoles, &session, &expected).await;
    let replay = write(&consoles, 0, &session, Method::POST, &path, &original).await;
    assert_eq!(replay.get("receipt"), expected.get("receipt"));
    assert_eq!(replay.get("resourceId"), expected.get("resourceId"));
    succeeded(&replay);
    let Reply::Topic(observed) = root
        .console_read(Query::Topic {
            topic: topic.id().into(),
        })
        .await
        .unwrap()
    else {
        panic!("Expected the restored Topic after receipt recovery");
    };
    assert_eq!(observed.shard_details.len(), 1);
    assert_eq!(
        observed.shard_details.first().unwrap().committed_end,
        Some(tidemq_protocol::console::Decimal(5))
    );
    root.admin().revoke_token(token.id()).await.unwrap();
    super::delegation::assert_revoked(&fixture, operation, &delegated).await;
    stream.close().await.unwrap();
    direct.close().await.unwrap();
    root.close().await.unwrap();
    consoles.close().await;
    fixture.close().await;
}
