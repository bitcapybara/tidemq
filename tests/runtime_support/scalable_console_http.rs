use super::*;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::{Method, StatusCode, header};
use serde_json::{Value, json};
use tidemq::console::{ConsoleConfig, RunningConsole};
use tidemq_protocol::console::{Query, Reply};
use tidemq_protocol::console_operation::Status as ConsoleStatus;
use tidemq_sdk::{AccessToken, Permission, PrincipalId};

#[path = "scalable_console_delegation.rs"]
mod delegation;
#[path = "scalable_console_regressions.rs"]
mod regressions;
#[path = "console_udp_relay.rs"]
mod udp_relay;

const ORIGIN: &str = "https://localhost:8443";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_http_cannot_claim_an_old_native_decision_with_a_different_command() {
    let fixture = dynamic_fixture().await;
    let root = fixture.client(1).await;
    let tenant = root.admin().create_tenant("acme").await.unwrap();
    let namespace = root.admin().create_namespace(tenant, "app").await.unwrap();
    let original_topic = root
        .admin()
        .create_topic("acme/app/original")
        .await
        .unwrap()
        .id();
    let original_principal = root
        .admin()
        .new_principal(tenant, "original-user", [Permission::Produce])
        .namespace(namespace)
        .build()
        .await
        .unwrap();
    let token = manager_token(&root, tenant, namespace).await;
    let consoles = Consoles::start(&fixture).await;
    let manager = consoles.login(0, token.expose_secret()).await;
    let commands = [
        (
            "topics",
            original_topic.to_string(),
            json!({"operationId": original_topic.to_string(), "tenantId": tenant.to_string(), "namespaceId": namespace.to_string(), "name": "must-not-exist"}),
        ),
        (
            "principals",
            original_principal.to_string(),
            json!({"operationId": original_principal.to_string(), "tenantId": tenant.to_string(), "namespaceId": namespace.to_string(), "name": "must-not-exist", "permissions": ["produce"]}),
        ),
    ];
    for (path, operation, input) in commands {
        let refused = write(&consoles, 0, &manager, Method::POST, path, &input).await;
        assert_eq!(refused.get("status").unwrap(), "rejected");
        assert_eq!(refused.get("reason").unwrap(), "content_conflict");
        assert!(refused.get("resourceId").unwrap().is_null());
        assert!(refused.get("receipt").unwrap().is_null());
        let recovered = response_json(
            consoles
                .request(1, &manager, Method::GET, &format!("operations/{operation}"))
                .send()
                .await
                .unwrap(),
            StatusCode::OK,
        )
        .await;
        assert_eq!(recovered.get("status").unwrap(), "rejected");
        assert_eq!(recovered.get("reason").unwrap(), "content_conflict");
        assert!(recovered.get("resourceId").unwrap().is_null());
    }
    assert!(
        root.admin()
            .topic_info("acme/app/must-not-exist")
            .await
            .is_err()
    );
    assert_eq!(
        root.admin()
            .topic_info("acme/app/original")
            .await
            .unwrap()
            .id(),
        original_topic
    );
    let principals = root
        .admin()
        .principals(tenant)
        .next_page()
        .await
        .unwrap()
        .unwrap();
    assert!(
        principals.iter().any(
            |principal| principal.id == original_principal && principal.name == "original-user"
        )
    );
    assert!(
        principals
            .iter()
            .all(|principal| principal.name != "must-not-exist")
    );
    consoles.close().await;
    root.close().await.unwrap();
    fixture.close().await;
}

struct Consoles {
    first: RunningConsole,
    second: RunningConsole,
    client: reqwest::Client,
}

struct BrowserSession {
    cookie: String,
    csrf: String,
    identity: Value,
}

impl Consoles {
    async fn start(fixture: &Fixture) -> Self {
        Self::start_with_upstream(fixture, fixture.configs.get(&1).unwrap().listen).await
    }

    async fn start_with_upstream(fixture: &Fixture, upstream: SocketAddr) -> Self {
        let directory = fixture.directory.join("console-http");
        let assets = directory.join("dist/assets");
        tokio::fs::create_dir_all(&assets).await.unwrap();
        tokio::fs::create_dir(directory.join("dist/.vite"))
            .await
            .unwrap();
        tokio::fs::write(
            directory.join("dist/index.html"),
            "<html>Console integration shell</html>",
        )
        .await
        .unwrap();
        tokio::fs::write(assets.join("fixture.js"), "export const ready = true;")
            .await
            .unwrap();
        tokio::fs::write(
            directory.join("dist/.vite/manifest.json"),
            r#"{"index.html":{"file":"assets/fixture.js","isEntry":true}}"#,
        )
        .await
        .unwrap();
        let key = directory.join("session.key");
        tokio::fs::write(&key, [37_u8; 32]).await.unwrap();
        private_file(key.clone()).await;
        let node = fixture.configs.get(&1).unwrap();
        let text = format!(
            r#"
role = "console"
listen = "127.0.0.1:8443"
management_listen = "127.0.0.1:0"
static_dir = {}
[cluster]
address = "{}"
server_name = "node-1.local"
trust_roots = {}
[session]
key_file = {}
public_origin = "{ORIGIN}"
max_age_seconds = 1800
"#,
            serde_json::to_string(&directory.join("dist")).unwrap(),
            upstream,
            serde_json::to_string(&node.tls.trust_roots).unwrap(),
            serde_json::to_string(&key).unwrap(),
        );
        let text = format!(
            "{text}\n[tls]\ncertificate = {}\nprivate_key = {}\n",
            serde_json::to_string(&node.tls.certificate).unwrap(),
            serde_json::to_string(&node.tls.private_key).unwrap()
        );
        let mut config: ConsoleConfig = toml::from_str(&text).unwrap();
        if let Some(path) = std::env::var_os("TIDEMQ_CONSOLE_BROWSER_STATIC_DIR") {
            config.static_dir = PathBuf::from(path);
        }
        let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        config.listen = reservation.local_addr().unwrap();
        drop(reservation);
        let first = RunningConsole::start(config.clone()).await.unwrap();
        let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        config.listen = reservation.local_addr().unwrap();
        drop(reservation);
        let second = RunningConsole::start(config).await.unwrap();
        let client = reqwest::Client::builder()
            .no_proxy()
            .tls_certs_only(
                reqwest::Certificate::from_pem_bundle(
                    &tokio::fs::read(&node.tls.trust_roots).await.unwrap(),
                )
                .unwrap(),
            )
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();
        Self {
            first,
            second,
            client,
        }
    }

    fn url(&self, instance: usize, path: &str) -> String {
        let port = if instance == 0 {
            self.first.address.port()
        } else {
            self.second.address.port()
        };
        format!("https://127.0.0.1:{port}/api/v1/{path}")
    }

    fn request(
        &self,
        instance: usize,
        session: &BrowserSession,
        method: Method,
        path: &str,
    ) -> reqwest::RequestBuilder {
        self.client
            .request(method, self.url(instance, path))
            .header(header::COOKIE, &session.cookie)
            .header(header::ORIGIN, ORIGIN)
            .header("x-csrf-token", &session.csrf)
    }

    async fn login(&self, instance: usize, token: &str) -> BrowserSession {
        let response = json_body(
            self.client
                .post(self.url(instance, "session"))
                .header(header::ORIGIN, ORIGIN),
            &json!({"token": token}),
        )
        .send()
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        let cookie = response
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap();
        for protection in [
            "__Host-tidemq-session=s1.",
            "; Secure",
            "; HttpOnly",
            "; Path=/",
            "; SameSite=Lax",
        ] {
            assert!(cookie.contains(protection));
        }
        assert!(!cookie.contains("Domain="));
        assert!(!cookie.contains(token));
        let cookie = cookie.split(';').next().unwrap().to_owned();
        let identity = response_json(response, StatusCode::OK).await;
        assert!(!identity.to_string().contains(token));
        let csrf = identity
            .get("csrfToken")
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        BrowserSession {
            cookie,
            csrf,
            identity,
        }
    }

    async fn close(self) {
        self.first
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        self.second
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "keeps an isolated MQ and two Console instances running for interactive browser acceptance"]
async fn console_browser_acceptance_fixture() {
    if std::env::var_os("TIDEMQ_CONSOLE_BROWSER_STATIC_DIR").is_none() {
        return;
    }
    let fixture = dynamic_fixture().await;
    let client = fixture.client(1).await;
    let tenant = client.admin().create_tenant("acme").await.unwrap();
    let namespace = client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    client
        .admin()
        .create_topic("acme/app/orders")
        .await
        .unwrap();
    let producer = client
        .new_producer("acme/app/orders")
        .build()
        .await
        .unwrap();
    for index in 0..5 {
        producer
            .send(
                tidemq_sdk::Message::new(format!("{{\"order\":{index}}}"))
                    .key("order")
                    .property("source", "console-browser-fixture"),
            )
            .await
            .unwrap();
    }
    let consumer = client
        .new_queue_consumer("acme/app/orders", "fulfillment")
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();
    let principal = client
        .admin()
        .new_principal(
            tenant,
            "browser-tenant",
            [
                Permission::Inspect,
                Permission::Produce,
                Permission::Consume,
                Permission::InspectMessageContent,
            ],
        )
        .namespace(namespace)
        .build()
        .await
        .unwrap();
    // Interactive acceptance needs a longer credential than the five-minute regression fixtures.
    let token = client
        .admin()
        .issue_token(principal, Duration::from_secs(1800))
        .await
        .unwrap();
    let consoles = Consoles::start(&fixture).await;
    let info = fixture.directory.join("console-browser.json");
    tokio::fs::write(&info,serde_json::to_vec(&json!({"firstPort":consoles.first.address.port(),"secondPort":consoles.second.address.port(),"certificate":fixture.directory.join("node-1.pem"),"key":fixture.directory.join("node-1.key"),"tenant":tenant.to_string(),"namespace":namespace.to_string(),"principal":principal.to_string(),"tenantToken":token.expose_secret(),"rootToken":TOKEN})).unwrap()).await.unwrap();
    private_file(info.clone()).await;
    println!("Browser fixture descriptor: {}", info.display());
    let stop = fixture.directory.join("console-browser.stop");
    for _ in 0..1800 {
        if tokio::fs::try_exists(&stop).await.unwrap() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    tokio::fs::remove_file(info).await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    client.close().await.unwrap();
    consoles.close().await;
    fixture.close().await;
}

fn json_body(request: reqwest::RequestBuilder, value: &Value) -> reqwest::RequestBuilder {
    request
        .header(header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(value).unwrap())
}

async fn response_json(response: reqwest::Response, status: StatusCode) -> Value {
    let actual = response.status();
    let path = response.url().path().to_owned();
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.parse::<Id>().is_ok())
        .unwrap_or("missing")
        .to_owned();
    let cache_control = response.headers().get(header::CACHE_CONTROL).cloned();
    let value = serde_json::from_slice::<Value>(&response.bytes().await.unwrap());
    let code = value
        .as_ref()
        .ok()
        .and_then(|value| value.get("code"))
        .and_then(Value::as_str)
        .filter(|code| {
            code.len() <= 128
                && code
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        })
        .unwrap_or("not_provided");
    assert_eq!(
        actual, status,
        "Unexpected HTTP response status for {path}; code={code}; request_id={request_id}"
    );
    assert_eq!(cache_control.unwrap(), "no-store");
    value.expect("The Console response must contain valid JSON")
}

async fn personal_token(
    client: &Client,
    tenant: tidemq_sdk::TenantId,
    namespace: tidemq_sdk::NamespaceId,
    name: &str,
    permissions: impl IntoIterator<Item = Permission>,
) -> (PrincipalId, AccessToken) {
    let principal = client
        .admin()
        .new_principal(tenant, name, permissions)
        .namespace(namespace)
        .build()
        .await
        .unwrap();
    let token = client
        .admin()
        .issue_token(principal, Duration::from_secs(300))
        .await
        .unwrap();
    (principal, token)
}

async fn manager_token(
    client: &Client,
    tenant: tidemq_sdk::TenantId,
    namespace: tidemq_sdk::NamespaceId,
) -> AccessToken {
    personal_token(
        client,
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
    .await
    .1
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_http_stateless_sessions_recheck_credentials_and_distinguish_upstream_failure() {
    let mut fixture = dynamic_fixture().await;
    let root = fixture.client(1).await;
    let tenant = root.admin().create_tenant("acme").await.unwrap();
    let namespace = root.admin().create_namespace(tenant, "app").await.unwrap();
    let (_, token) =
        personal_token(&root, tenant, namespace, "reader", [Permission::Inspect]).await;
    let consoles = Consoles::start(&fixture).await;
    let session = consoles.login(0, token.expose_secret()).await;
    assert_eq!(
        session.identity.get("credentialId").unwrap(),
        &json!(token.id().to_string())
    );
    assert!(
        session
            .identity
            .get("expiresAtMs")
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            <= token.expires_at_unix_ms()
    );
    let me = response_json(
        consoles
            .client
            .get(consoles.url(1, "me"))
            .header(header::COOKIE, &session.cookie)
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(me.get("principal"), session.identity.get("principal"));
    assert_eq!(me.get("csrfToken"), session.identity.get("csrfToken"));
    assert_eq!(me.get("operator").unwrap(), false);
    response_json(
        consoles
            .request(1, &session, Method::GET, "components")
            .send()
            .await
            .unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;
    let rejected = response_json(
        json_body(
            consoles
                .client
                .post(consoles.url(0, "session"))
                .header(header::ORIGIN, "https://evil.example.test"),
            &json!({"token": token.expose_secret()}),
        )
        .send()
        .await
        .unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;
    assert_eq!(rejected.get("code").unwrap(), "origin_rejected");
    let no_csrf = consoles
        .client
        .delete(consoles.url(1, "session"))
        .header(header::COOKIE, &session.cookie)
        .header(header::ORIGIN, ORIGIN)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response_json(no_csrf, StatusCode::FORBIDDEN)
            .await
            .get("code")
            .unwrap(),
        "csrf_rejected"
    );
    let wrong_origin = consoles
        .client
        .delete(consoles.url(1, "session"))
        .header(header::COOKIE, &session.cookie)
        .header(header::ORIGIN, "https://evil.example.test")
        .header("x-csrf-token", &session.csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response_json(wrong_origin, StatusCode::FORBIDDEN)
            .await
            .get("code")
            .unwrap(),
        "origin_rejected"
    );
    let logout = consoles
        .request(1, &session, Method::DELETE, "session")
        .send()
        .await
        .unwrap();
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    assert!(
        logout
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );
    assert!(logout.bytes().await.unwrap().is_empty());
    root.admin().revoke_token(token.id()).await.unwrap();
    let revoked = consoles
        .request(1, &session, Method::GET, "me")
        .send()
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);
    assert!(
        revoked
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );
    let (_, active_token) = personal_token(
        &root,
        tenant,
        namespace,
        "active-reader",
        [Permission::Inspect],
    )
    .await;
    let active = consoles.login(0, active_token.expose_secret()).await;
    root.close().await.unwrap();
    fixture.stop(1).await;
    let failed = response_json(
        consoles
            .request(1, &active, Method::GET, "me")
            .send()
            .await
            .unwrap(),
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;
    assert_eq!(failed.get("code").unwrap(), "upstream_unavailable");
    let static_url = format!(
        "https://127.0.0.1:{}/topics/example",
        consoles.first.address.port()
    );
    assert_eq!(
        consoles
            .client
            .get(static_url)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        consoles
            .client
            .get(format!(
                "http://{}/health/ready",
                consoles.second.management_address.unwrap()
            ))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    consoles.close().await;
    fixture.close().await;
}

async fn write(
    consoles: &Consoles,
    instance: usize,
    session: &BrowserSession,
    method: Method,
    path: &str,
    value: &Value,
) -> Value {
    response_json(
        json_body(consoles.request(instance, session, method, path), value)
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await
}

fn succeeded(value: &Value) {
    assert_eq!(
        value.get("status").unwrap(),
        "succeeded",
        "The core did not accept this isolated test operation: reason={:?}",
        value.get("reason")
    );
}

async fn consumer_observation(client: &Client, topic: Id, subscription: Id) -> (Value, Value) {
    let Reply::Members(members) = client
        .console_read(Query::Members {
            topic,
            subscription,
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected a member page")
    };
    let Reply::Progress(progress) = client
        .console_read(Query::Progress {
            topic,
            subscription,
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected a progress page")
    };
    let mut progress = serde_json::to_value(progress.items).unwrap();
    for shard in progress.as_array_mut().unwrap() {
        shard.as_object_mut().unwrap().remove("observedAtMs");
    }
    (serde_json::to_value(members.items).unwrap(), progress)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_http_topic_send_receipts_and_opaque_message_pages_work_across_replicas_without_consuming()
 {
    let fixture = dynamic_fixture().await;
    let root = fixture.client(1).await;
    let tenant = root.admin().create_tenant("acme").await.unwrap();
    let namespace = root.admin().create_namespace(tenant, "app").await.unwrap();
    let token = manager_token(&root, tenant, namespace).await;
    let consoles = Consoles::start(&fixture).await;
    let session = consoles.login(0, token.expose_secret()).await;
    let topic_operation = Id::new_v7().unwrap();
    let create = json!({"operationId": topic_operation, "tenantId": tenant.to_string(), "namespaceId": namespace.to_string(), "name": "orders"});
    let created = write(&consoles, 0, &session, Method::POST, "topics", &create).await;
    succeeded(&created);
    let topic: Id = created
        .get("resourceId")
        .unwrap()
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        root.admin()
            .topic_info("acme/app/orders")
            .await
            .unwrap()
            .id()
            .to_string(),
        topic.to_string()
    );
    let topics = response_json(
        consoles
            .request(1, &session, Method::GET, "topics")
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert!(
        topics
            .get("items")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row.get("id").and_then(Value::as_str) == Some(topic.to_string().as_str()))
    );
    let replay = write(&consoles, 1, &session, Method::POST, "topics", &create).await;
    assert_eq!(replay.get("resourceId"), created.get("resourceId"));
    let changed = json!({"operationId": topic_operation, "tenantId": tenant.to_string(), "namespaceId": namespace.to_string(), "name": "different"});
    response_json(
        json_body(
            consoles.request(1, &session, Method::POST, "topics"),
            &changed,
        )
        .send()
        .await
        .unwrap(),
        StatusCode::CONFLICT,
    )
    .await;
    let consumer = root
        .new_stream_consumer("acme/app/orders", "events")
        .build()
        .await
        .unwrap();
    let first_send = Id::new_v7().unwrap();
    let send = json!({"operationId": first_send, "payloadBase64": STANDARD.encode("private-http-message"), "keyBase64": STANDARD.encode("order-key"), "propertiesBase64": {"source": STANDARD.encode("console")}});
    let sent = write(
        &consoles,
        0,
        &session,
        Method::POST,
        &format!("topics/{topic}/send"),
        &send,
    )
    .await;
    succeeded(&sent);
    assert!(!sent.to_string().contains("private-http-message"));
    assert!(
        !sent
            .to_string()
            .contains(&STANDARD.encode("private-http-message"))
    );
    let delivery = tokio::time::timeout(Duration::from_secs(10), consumer.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.payload(), b"private-http-message");
    consumer.ack_cumulative(&delivery).await.unwrap();
    consumer.close().await.unwrap();
    let second_send = json!({"operationId": Id::new_v7().unwrap(), "payloadBase64": STANDARD.encode("unconfirmed-http-message"), "keyBase64": null, "propertiesBase64": {}});
    succeeded(
        &write(
            &consoles,
            1,
            &session,
            Method::POST,
            &format!("topics/{topic}/send"),
            &second_send,
        )
        .await,
    );
    let receipt = response_json(
        consoles
            .request(
                1,
                &session,
                Method::GET,
                &format!("operations/{first_send}"),
            )
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    succeeded(&receipt);
    assert_eq!(receipt.get("receipt"), sent.get("receipt"));
    assert!(receipt.get("secret").is_none());
    let Reply::Topic(detail) = root.console_read(Query::Topic { topic }).await.unwrap() else {
        panic!("Expected a topic detail")
    };
    let shard = detail.shard_details.first().unwrap().id;
    let Reply::Subscriptions(subscriptions) = root
        .console_read(Query::Subscriptions {
            topic: Some(topic),
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected subscriptions")
    };
    let subscription = subscriptions
        .items
        .iter()
        .find(|row| row.name == "events")
        .unwrap()
        .id;
    let visible = response_json(
        consoles
            .request(
                1,
                &session,
                Method::GET,
                &format!("subscriptions?topicId={topic}"),
            )
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert!(
        visible
            .get("items")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row.get("id").and_then(Value::as_str)
                == Some(subscription.to_string().as_str()))
    );
    let progress = response_json(
        consoles
            .request(
                1,
                &session,
                Method::GET,
                &format!("subscriptions/{subscription}/progress?topicId={topic}"),
            )
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert!(progress.get("items").unwrap().as_array().unwrap().iter().any(|row| row.get("shard").and_then(Value::as_str) == Some(shard.to_string().as_str())));
    let before = consumer_observation(&root, topic, subscription).await;
    let input = json!({"shard": shard, "startOffset": "0", "limit": 1, "propertiesBase64": {}});
    let page = write(
        &consoles,
        0,
        &session,
        Method::POST,
        &format!("topics/{topic}/messages"),
        &input,
    )
    .await;
    assert_eq!(page.get("items").unwrap().as_array().unwrap().len(), 1);
    let cursor = page.get("nextCursor").unwrap().as_str().unwrap();
    assert!(cursor.starts_with("c1."));
    let continued = json!({"shard": shard, "startOffset": "0", "limit": 1, "propertiesBase64": {}, "cursor": cursor});
    let next = write(
        &consoles,
        1,
        &session,
        Method::POST,
        &format!("topics/{topic}/messages"),
        &continued,
    )
    .await;
    assert_eq!(
        next.get("items")
            .unwrap()
            .get(0)
            .unwrap()
            .get("offset")
            .unwrap(),
        "1"
    );
    assert_eq!(next.get("observedEnd"), page.get("observedEnd"));
    let after = consumer_observation(&root, topic, subscription).await;
    assert_eq!(
        before, after,
        "Message inspection changed membership, leases, or ACK progress"
    );
    let tampered_query = json!({"shard": shard, "startOffset": "0", "limit": 1, "propertiesBase64": {}, "cursor": cursor, "keyBase64": STANDARD.encode("different")});
    response_json(
        json_body(
            consoles.request(
                1,
                &session,
                Method::POST,
                &format!("topics/{topic}/messages"),
            ),
            &tampered_query,
        )
        .send()
        .await
        .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    let (_, reader_token) = personal_token(
        &root,
        tenant,
        namespace,
        "consumer-without-content",
        [Permission::Consume],
    )
    .await;
    let reader = consoles.login(1, reader_token.expose_secret()).await;
    response_json(
        consoles
            .request(1, &reader, Method::GET, &format!("topics/{topic}"))
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    response_json(
        json_body(
            consoles.request(
                1,
                &reader,
                Method::POST,
                &format!("topics/{topic}/messages"),
            ),
            &input,
        )
        .send()
        .await
        .unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;
    response_json(
        consoles
            .request(1, &reader, Method::GET, &format!("operations/{first_send}"))
            .send()
            .await
            .unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;
    consoles.close().await;
    root.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_http_identity_credentials_and_topic_grants_preserve_scope_and_one_time_secrets() {
    let fixture = dynamic_fixture().await;
    let root = fixture.client(1).await;
    let tenant = root.admin().create_tenant("acme").await.unwrap();
    let namespace = root.admin().create_namespace(tenant, "app").await.unwrap();
    let topic = root
        .admin()
        .create_topic("acme/app/identity")
        .await
        .unwrap()
        .id();
    let other = root.admin().create_tenant("other").await.unwrap();
    let foreign = root
        .admin()
        .new_principal(other, "foreign", [])
        .build()
        .await
        .unwrap();
    root.admin()
        .issue_token(foreign, Duration::from_secs(300))
        .await
        .unwrap();
    let token = manager_token(&root, tenant, namespace).await;
    let consoles = Consoles::start(&fixture).await;
    let manager = consoles.login(0, token.expose_secret()).await;
    let principal = write(&consoles, 0, &manager, Method::POST, "principals", &json!({"operationId": Id::new_v7().unwrap(), "name": "http-producer", "tenantId": tenant.to_string(), "namespaceId": namespace.to_string(), "permissions": ["produce"]})).await;
    succeeded(&principal);
    let identity_scope = format!("namespace:{tenant}/{namespace}");
    assert_eq!(
        principal.get("scope").and_then(Value::as_str),
        Some(identity_scope.as_str())
    );
    let principal = principal
        .get("resourceId")
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    let credential_operation = Id::new_v7().unwrap();
    let credential_request = json!({"operationId": credential_operation, "principalId": principal, "lifetimeMs": "300000"});
    let issued = write(
        &consoles,
        0,
        &manager,
        Method::POST,
        "credentials",
        &credential_request,
    )
    .await;
    succeeded(&issued);
    assert_eq!(
        issued.get("target").and_then(Value::as_str),
        Some(principal.as_str())
    );
    assert_eq!(
        issued.get("scope").and_then(Value::as_str),
        Some(identity_scope.as_str())
    );
    let secret = issued.get("secret").unwrap().as_str().unwrap().to_owned();
    let credential = issued
        .get("resourceId")
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    let replay = write(
        &consoles,
        1,
        &manager,
        Method::POST,
        "credentials",
        &credential_request,
    )
    .await;
    succeeded(&replay);
    assert_eq!(replay.get("secretUnavailable").unwrap(), true);
    assert!(replay.get("secret").is_none());
    let receipt = response_json(
        consoles
            .request(
                1,
                &manager,
                Method::GET,
                &format!("operations/{credential_operation}"),
            )
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert!(!receipt.to_string().contains(&secret));
    assert!(receipt.get("secret").is_none());
    assert_eq!(receipt.get("target"), issued.get("target"));
    assert_eq!(receipt.get("scope"), issued.get("scope"));
    let listing = response_json(
        consoles
            .request(
                1,
                &manager,
                Method::GET,
                &format!("credentials?principalId={principal}"),
            )
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert!(!listing.to_string().contains(&secret));
    assert!(
        listing
            .get("items")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row.get("id").and_then(Value::as_str) == Some(credential.as_str()))
    );
    response_json(
        consoles
            .request(
                1,
                &manager,
                Method::GET,
                &format!("credentials?principalId={foreign}"),
            )
            .send()
            .await
            .unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;
    let producer = consoles.login(1, &secret).await;
    let Reply::Topic(detail) = root
        .console_read(Query::Topic {
            topic: topic.to_string().parse().unwrap(),
        })
        .await
        .unwrap()
    else {
        panic!("Expected a topic detail")
    };
    let inspect_request =
        json!({"shard": detail.shard_details.first().unwrap().id, "startOffset": "0", "limit": 1});
    response_json(
        json_body(
            consoles.request(
                1,
                &producer,
                Method::POST,
                &format!("topics/{topic}/messages"),
            ),
            &inspect_request,
        )
        .send()
        .await
        .unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;
    let grant_path = format!(
        "grants?principalId={principal}&topicId={topic}&tenantId={tenant}&namespaceId={namespace}"
    );
    let grants = response_json(
        consoles
            .request(0, &manager, Method::GET, &grant_path)
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    let version = grants
        .get("items")
        .unwrap()
        .get(0)
        .unwrap()
        .get("version")
        .unwrap()
        .as_str()
        .unwrap();
    let grant = json!({"operationId": Id::new_v7().unwrap(), "principalId": principal, "tenantId": tenant.to_string(), "namespaceId": namespace.to_string(), "topicId": topic.to_string(), "permissions": ["inspect_message_content"], "expectedVersion": version});
    response_json(
        consoles
            .request(
                0,
                &manager,
                Method::GET,
                &format!("{grant_path}&resourceKind=topic&resourceId={topic}"),
            )
            .send()
            .await
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    let mut alternate_scope = grant.clone();
    alternate_scope["resourceKind"] = json!("topic");
    alternate_scope["resourceId"] = json!(topic.to_string());
    response_json(
        json_body(
            consoles.request(0, &manager, Method::PUT, "grants"),
            &alternate_scope,
        )
        .send()
        .await
        .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    let granted = write(&consoles, 0, &manager, Method::PUT, "grants", &grant).await;
    succeeded(&granted);
    assert_eq!(
        granted.get("scope").and_then(Value::as_str),
        Some(format!("topic:{tenant}/{namespace}/{topic}").as_str())
    );
    response_json(
        json_body(
            consoles.request(
                1,
                &producer,
                Method::POST,
                &format!("topics/{topic}/messages"),
            ),
            &inspect_request,
        )
        .send()
        .await
        .unwrap(),
        StatusCode::OK,
    )
    .await;
    let revoked = write(
        &consoles,
        0,
        &manager,
        Method::DELETE,
        &format!("credentials/{credential}"),
        &json!({"operationId": Id::new_v7().unwrap()}),
    )
    .await;
    succeeded(&revoked);
    assert_eq!(
        revoked.get("target").and_then(Value::as_str),
        Some(credential.as_str())
    );
    assert_eq!(
        revoked.get("scope").and_then(Value::as_str),
        Some(identity_scope.as_str())
    );
    response_json(
        consoles
            .request(1, &producer, Method::GET, "me")
            .send()
            .await
            .unwrap(),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    succeeded(
        &write(
            &consoles,
            0,
            &manager,
            Method::PATCH,
            &format!("principals/{principal}"),
            &json!({"operationId": Id::new_v7().unwrap(), "enabled": false}),
        )
        .await,
    );
    let sdk_principal: PrincipalId = principal.parse().unwrap();
    assert!(
        !root
            .principal_identity(sdk_principal, None)
            .await
            .unwrap()
            .principal
            .enabled
    );
    consoles.close().await;
    root.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_http_can_list_and_revoke_a_principals_initial_produce_permission() {
    let mut fixture = dynamic_fixture().await;
    let root = fixture.client(1).await;
    let tenant = root.admin().create_tenant("acme").await.unwrap();
    let namespace = root.admin().create_namespace(tenant, "app").await.unwrap();
    let topic = root
        .admin()
        .create_topic("acme/app/initial-produce")
        .await
        .unwrap()
        .id();
    let token = manager_token(&root, tenant, namespace).await;
    let consoles = Consoles::start(&fixture).await;
    let manager = consoles.login(0, token.expose_secret()).await;
    let created = write(
        &consoles,
        0,
        &manager,
        Method::POST,
        "principals",
        &json!({
            "operationId": Id::new_v7().unwrap(),
            "name": "initial-producer",
            "tenantId": tenant.to_string(),
            "namespaceId": namespace.to_string(),
            "permissions": ["produce"]
        }),
    )
    .await;
    succeeded(&created);
    let principal = created.get("resourceId").unwrap().as_str().unwrap();
    let issued = write(
        &consoles,
        0,
        &manager,
        Method::POST,
        "credentials",
        &json!({
            "operationId": Id::new_v7().unwrap(),
            "principalId": principal,
            "lifetimeMs": "300000"
        }),
    )
    .await;
    succeeded(&issued);
    let producer = consoles
        .login(1, issued.get("secret").unwrap().as_str().unwrap())
        .await;
    let send_path = format!("topics/{topic}/send");
    let send = |operation| {
        json!({
            "operationId": operation,
            "payloadBase64": STANDARD.encode("initial permission"),
            "keyBase64": null,
            "propertiesBase64": {}
        })
    };
    succeeded(
        &write(
            &consoles,
            1,
            &producer,
            Method::POST,
            &send_path,
            &send(Id::new_v7().unwrap()),
        )
        .await,
    );
    let grants_path = format!("grants?principalId={principal}");
    let listed = response_json(
        consoles
            .request(1, &manager, Method::GET, &grants_path)
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    let rows = listed.get("items").unwrap().as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let initial = rows.first().unwrap();
    assert_eq!(initial.get("principalId").unwrap(), principal);
    assert_eq!(initial.get("resourceKind").unwrap(), "namespace");
    assert_eq!(
        initial.get("resourceId").unwrap(),
        &json!(namespace.to_string())
    );
    assert_eq!(initial.get("direct").unwrap(), &json!(["produce"]));
    let exact_path = format!("{grants_path}&tenantId={tenant}&namespaceId={namespace}");
    let preview = response_json(
        consoles
            .request(0, &manager, Method::GET, &exact_path)
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    let preview = preview.get("items").unwrap().as_array().unwrap();
    assert_eq!(preview.len(), 1);
    let preview = preview.first().unwrap();
    assert_eq!(preview.get("direct").unwrap(), &json!(["produce"]));
    assert_eq!(preview.get("inherited").unwrap(), &json!([]));
    let replace_id = Id::new_v7().unwrap();
    let replace = json!({
        "operationId": replace_id,
        "principalId": principal,
        "tenantId": tenant.to_string(),
        "namespaceId": namespace.to_string(),
        "permissions": [],
        "expectedVersion": preview.get("version").unwrap()
    });
    let cleared = write(&consoles, 0, &manager, Method::PUT, "grants", &replace).await;
    succeeded(&cleared);
    let scope = format!("namespace:{tenant}/{namespace}");
    assert_eq!(
        cleared.get("scope").and_then(Value::as_str),
        Some(scope.as_str())
    );
    assert_eq!(
        cleared.get("target").and_then(Value::as_str),
        Some(principal)
    );
    let native = root
        .principal_identity(principal.parse().unwrap(), None)
        .await
        .unwrap();
    assert!(native.principal.permissions.is_empty());
    assert!(native.principal.resource_grants.is_empty());
    let listed = response_json(
        consoles
            .request(1, &manager, Method::GET, &grants_path)
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(listed.get("items").unwrap(), &json!([]));
    let preview = response_json(
        consoles
            .request(1, &manager, Method::GET, &exact_path)
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    let rows = preview.get("items").unwrap().as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let preview = rows.first().unwrap();
    assert_eq!(preview.get("direct").unwrap(), &json!([]));
    assert_eq!(preview.get("inherited").unwrap(), &json!([]));
    assert_eq!(preview.get("effective").unwrap(), &json!([]));
    response_json(
        json_body(
            consoles.request(1, &producer, Method::POST, &send_path),
            &send(Id::new_v7().unwrap()),
        )
        .send()
        .await
        .unwrap(),
        StatusCode::FORBIDDEN,
    )
    .await;
    root.close().await.unwrap();
    fixture.stop(1).await;
    fixture.restart(1).await;
    let recovered = response_json(
        consoles
            .request(
                1,
                &manager,
                Method::GET,
                &format!("operations/{replace_id}"),
            )
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    succeeded(&recovered);
    assert_eq!(recovered.get("scope"), cleared.get("scope"));
    assert_eq!(recovered.get("target"), cleared.get("target"));
    assert_eq!(recovered.get("actor"), cleared.get("actor"));
    assert_eq!(recovered.get("receipt"), cleared.get("receipt"));
    consoles.close().await;
    fixture.close().await;
}

async fn assert_original_console_receipt(
    consoles: &Consoles,
    session: &BrowserSession,
    expected: &Value,
) {
    let operation = expected.get("id").unwrap().as_str().unwrap();
    let recovered = response_json(
        consoles
            .request(1, session, Method::GET, &format!("operations/{operation}"))
            .send()
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    succeeded(&recovered);
    for field in [
        "id",
        "status",
        "receipt",
        "actor",
        "target",
        "scope",
        "resourceId",
        "secretUnavailable",
        "reason",
    ] {
        assert_eq!(
            recovered.get(field),
            expected.get(field),
            "The original Console operation changed its {field} during result recovery"
        );
    }
    assert!(recovered.get("secret").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_http_retired_topic_receipts_survive_same_name_recreation_across_replicas() {
    let fixture = dynamic_fixture().await;
    let root = fixture.client(1).await;
    let tenant = root.admin().create_tenant("acme").await.unwrap();
    let namespace = root.admin().create_namespace(tenant, "app").await.unwrap();
    let token = manager_token(&root, tenant, namespace).await;
    let consoles = Consoles::start(&fixture).await;
    let manager = consoles.login(0, token.expose_secret()).await;
    let create = |operation| {
        json!({
            "operationId": operation,
            "tenantId": tenant.to_string(),
            "namespaceId": namespace.to_string(),
            "name": "retired-history"
        })
    };
    let created = write(
        &consoles,
        0,
        &manager,
        Method::POST,
        "topics",
        &create(Id::new_v7().unwrap()),
    )
    .await;
    succeeded(&created);
    let original_topic = created.get("resourceId").unwrap().as_str().unwrap();
    let send_id = Id::new_v7().unwrap();
    let sent = write(
        &consoles,
        0,
        &manager,
        Method::POST,
        &format!("topics/{original_topic}/send"),
        &json!({
            "operationId": send_id,
            "payloadBase64": STANDARD.encode("retired-topic-message"),
            "keyBase64": null,
            "propertiesBase64": {}
        }),
    )
    .await;
    succeeded(&sent);
    let delete_id = Id::new_v7().unwrap();
    let deleted = write(
        &consoles,
        0,
        &manager,
        Method::DELETE,
        &format!("topics/{original_topic}"),
        &json!({"operationId": delete_id}),
    )
    .await;
    succeeded(&deleted);
    for original in [&sent, &deleted] {
        assert_eq!(
            original.get("resourceId").and_then(Value::as_str),
            Some(original_topic)
        );
        assert_eq!(
            original.get("target").and_then(Value::as_str),
            Some(original_topic)
        );
        assert_eq!(
            original.get("scope").and_then(Value::as_str),
            Some(original_topic)
        );
        assert!(
            original
                .get("receipt")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
        );
        assert_original_console_receipt(&consoles, &manager, original).await;
    }
    let recreated = write(
        &consoles,
        0,
        &manager,
        Method::POST,
        "topics",
        &create(Id::new_v7().unwrap()),
    )
    .await;
    succeeded(&recreated);
    let replacement_topic = recreated.get("resourceId").unwrap().as_str().unwrap();
    assert_ne!(replacement_topic, original_topic);
    for original in [&sent, &deleted] {
        assert_original_console_receipt(&consoles, &manager, original).await;
    }
    consoles.close().await;
    root.close().await.unwrap();
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_http_recovers_only_connection_failures_before_submitting_a_write() {
    let fixture = dynamic_fixture().await;
    let root = fixture.client(1).await;
    let tenant = root.admin().create_tenant("acme").await.unwrap();
    let namespace = root.admin().create_namespace(tenant, "app").await.unwrap();
    let token = manager_token(&root, tenant, namespace).await;
    let direct = token_client(&fixture, token.expose_secret()).await;
    let relay = udp_relay::Relay::start(fixture.configs.get(&1).unwrap().listen).await;
    let consoles = Consoles::start_with_upstream(&fixture, relay.address).await;
    let manager = consoles.login(0, token.expose_secret()).await;
    let create = |operation, name| {
        json!({
            "operationId": operation,
            "tenantId": tenant.to_string(),
            "namespaceId": namespace.to_string(),
            "name": name
        })
    };

    let operation = Id::new_v7().unwrap();
    assert_eq!(
        direct.console_operation(operation).await.unwrap().status,
        ConsoleStatus::Unknown
    );
    relay.arm(udp_relay::Fault::FirstNewPeer);
    let started = Instant::now();
    let created = write(
        &consoles,
        0,
        &manager,
        Method::POST,
        "topics",
        &create(operation, "connection-recovered"),
    )
    .await;
    succeeded(&created);
    assert_eq!(created.get("id").unwrap(), &operation.to_string());
    assert!(started.elapsed() >= Duration::from_secs(3));
    let peers = relay.peers();
    assert_eq!(
        peers.len(),
        2,
        "Connection recovery must use exactly two SDK endpoints"
    );
    let first = peers.first().unwrap();
    let second = peers.get(1).unwrap();
    assert_ne!(first.address.port(), second.address.port());
    assert_eq!(first.forwarded, 0);
    assert!(first.dropped > 0);
    assert!(second.forwarded > 0);
    assert_eq!(second.dropped, 0);
    let recovered = direct.console_operation(operation).await.unwrap();
    assert_eq!(recovered.id, operation);
    assert_eq!(recovered.status, ConsoleStatus::Succeeded);
    assert!(recovered.receipt.is_some());
    assert_eq!(
        recovered.receipt.as_deref(),
        created.get("receipt").and_then(Value::as_str)
    );
    let topic = root
        .admin()
        .topic_info("acme/app/connection-recovered")
        .await
        .unwrap();
    assert_eq!(recovered.resource_id, Some(topic.id().into()));

    relay.arm(udp_relay::Fault::Pass);
    let refused = response_json(
        json_body(
            consoles
                .client
                .post(consoles.url(1, "session"))
                .header(header::ORIGIN, ORIGIN),
            &json!({"token": "invalid-console-credential-with-at-least-thirty-two-characters"}),
        )
        .send()
        .await
        .unwrap(),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    assert_eq!(refused.get("code").unwrap(), "authentication_required");
    let peers = relay.peers();
    assert_eq!(
        peers.len(),
        1,
        "Credential refusal must not retry the SDK connection"
    );
    assert!(peers.first().unwrap().forwarded > 0);
    assert_eq!(peers.first().unwrap().dropped, 0);

    let blocked_operation = Id::new_v7().unwrap();
    relay.arm(udp_relay::Fault::AllNewPeers);
    let started = Instant::now();
    // Two unchanged three-second connect budgets leave one second for HTTP scheduling and cleanup.
    let response = tokio::time::timeout(
        Duration::from_secs(7),
        json_body(
            consoles.request(0, &manager, Method::POST, "topics"),
            &create(blocked_operation, "must-not-be-submitted"),
        )
        .send(),
    )
    .await
    .expect("Two connection attempts must finish within their bounded HTTP budget")
    .unwrap();
    let unavailable = response_json(response, StatusCode::SERVICE_UNAVAILABLE).await;
    assert_eq!(unavailable.get("code").unwrap(), "upstream_unavailable");
    assert!(started.elapsed() >= Duration::from_secs(6));
    let peers = relay.peers();
    assert_eq!(
        peers.len(),
        2,
        "A persistent outage must stop after two SDK endpoints"
    );
    assert!(
        peers
            .iter()
            .all(|peer| peer.forwarded == 0 && peer.dropped > 0)
    );
    assert_eq!(
        direct
            .console_operation(blocked_operation)
            .await
            .unwrap()
            .status,
        ConsoleStatus::Unknown
    );
    let Reply::Topics(topics) = direct
        .console_read(Query::Topics {
            tenant: Some(tenant.into()),
            namespace: Some(namespace.into()),
            after: None,
            limit: 32,
        })
        .await
        .unwrap()
    else {
        panic!("Expected the authoritative topic directory after connection recovery");
    };
    assert_eq!(topics.items.len(), 1);
    assert_eq!(topics.items.first().unwrap().id, topic.id().into());
    assert!(topics.next_cursor.is_none());
    consoles.close().await;
    relay.close().await;
    direct.close().await.unwrap();
    root.close().await.unwrap();
    fixture.close().await;
}
