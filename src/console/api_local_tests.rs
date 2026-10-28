use super::*;
use tidemq_protocol::{Id, public};

async fn request(
    api: &ApiState,
    method: Method,
    path: &str,
    headers: &HeaderMap,
    body: Value,
) -> (StatusCode, HeaderMap, Value) {
    let response = route(
        api,
        method,
        path.parse().unwrap(),
        headers.clone(),
        Bytes::from(serde_json::to_vec(&body).unwrap()),
    )
    .await
    .unwrap_or_else(IntoResponse::into_response);
    let (parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, 1_500_000).await.unwrap();
    (
        parts.status,
        parts.headers,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn login_headers(api: &ApiState, token: &str, origin: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(header::ORIGIN, origin.parse().unwrap());
    let (status, response, me) = request(
        api,
        Method::POST,
        "/api/v1/session",
        &headers,
        json!({"token":token}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Local login failed: {me}");
    let cookie = response.get(header::SET_COOKIE).unwrap().to_str().unwrap();
    assert!(cookie.contains("HttpOnly; Secure; SameSite=Lax"));
    headers.insert(
        header::COOKIE,
        cookie.split(';').next().unwrap().parse().unwrap(),
    );
    headers.insert(
        "x-csrf-token",
        me["csrfToken"].as_str().unwrap().parse().unwrap(),
    );
    assert_eq!(me["capabilities"]["execution"], "local");
    headers
}

#[tokio::test]
async fn local_http_login_identity_pagination_grants_and_operations_preserve_security_contracts() {
    let fixture = crate::runtime::LocalConsole::fixture().await;
    let (tenant, namespace) = fixture.tenant("tenant-a").await;
    let (foreign, _) = fixture.tenant("tenant-b").await;
    let (principal, credential, token) = fixture
        .principal(tenant, namespace, &["inspect", "manage_credentials"])
        .await;
    let config = fixture.console_config();
    let api = ApiState::load_local(&config, fixture.backend.clone())
        .await
        .unwrap();
    let before = fixture.connections();
    let root = login_headers(&api, fixture.token(), &config.session.public_origin).await;
    let scoped = login_headers(&api, &token, &config.session.public_origin).await;
    for path in [
        "me",
        "capabilities",
        "tenants",
        "namespaces",
        "principals",
        "credentials",
        "grants",
        "cluster",
        "components",
        "components/storage/instances",
        "topics",
        "subscriptions",
        "overview",
    ] {
        let suffix = match path {
            "namespaces" | "principals" => format!("?tenantId={tenant}"),
            "credentials" => format!("?principalId={principal}"),
            "grants" => format!("?tenantId={tenant}&namespaceId={namespace}"),
            _ => String::new(),
        };
        let (status, _, result) = request(
            &api,
            Method::GET,
            &format!("/api/v1/{path}{suffix}"),
            &root,
            Value::Null,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "Local endpoint {path} failed: {result}"
        );
        if matches!(
            path,
            "cluster" | "components" | "components/storage/instances"
        ) {
            let encoded = result.to_string();
            assert!(
                !encoded.contains("leader")
                    && !encoded.contains("quorum")
                    && !encoded.contains("term")
            );
            assert!(encoded.contains("local_process"));
        }
    }
    let (status, _, first) = request(
        &api,
        Method::GET,
        "/api/v1/tenants?limit=1",
        &root,
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let cursor = first["nextCursor"].as_str().unwrap();
    let (status, _, second) = request(
        &api,
        Method::GET,
        &format!("/api/v1/tenants?limit=1&cursor={cursor}"),
        &root,
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(first["items"][0]["id"], second["items"][0]["id"]);
    for (headers, path) in [
        (&scoped, format!("/api/v1/tenants?limit=1&cursor={cursor}")),
        (
            &root,
            format!("/api/v1/namespaces?tenantId={tenant}&cursor={cursor}"),
        ),
        (
            &root,
            format!("/api/v1/tenants?tenantId={foreign}&cursor={cursor}"),
        ),
    ] {
        assert_eq!(
            request(&api, Method::GET, &path, headers, Value::Null)
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    for path in [
        format!("/api/v1/namespaces?tenantId={foreign}"),
        format!("/api/v1/principals?tenantId={foreign}"),
        "/api/v1/cluster".into(),
    ] {
        assert_eq!(
            request(&api, Method::GET, &path, &scoped, Value::Null)
                .await
                .0,
            StatusCode::FORBIDDEN
        );
    }
    assert_eq!(
        request(
            &api,
            Method::GET,
            "/api/v1/tenants?limit=33",
            &root,
            Value::Null
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let operation = Id::new_v7().unwrap();
    let body = json!({"operationId":operation,"name":"local-write","tenantId":tenant,"namespaceId":namespace,"permissions":["inspect"]});
    let mut no_csrf = root.clone();
    no_csrf.remove("x-csrf-token");
    assert_eq!(
        request(
            &api,
            Method::POST,
            "/api/v1/principals",
            &no_csrf,
            body.clone()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let mut wrong_origin = root.clone();
    wrong_origin.insert(header::ORIGIN, "https://foreign.local".parse().unwrap());
    assert_eq!(
        request(
            &api,
            Method::POST,
            "/api/v1/principals",
            &wrong_origin,
            body.clone()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, _, submitted) = request(
        &api,
        Method::POST,
        "/api/v1/principals",
        &root,
        body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(submitted["status"], "succeeded");
    let recovered = request(
        &api,
        Method::GET,
        &format!("/api/v1/operations/{operation}"),
        &root,
        Value::Null,
    )
    .await;
    assert_eq!(recovered.0, StatusCode::OK);
    assert_eq!(recovered.2["resourceId"], submitted["resourceId"]);
    assert_eq!(
        request(
            &api,
            Method::GET,
            &format!("/api/v1/operations/{operation}"),
            &scoped,
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let mut changed = body;
    changed["name"] = json!("changed");
    assert_eq!(
        request(&api, Method::POST, "/api/v1/principals", &root, changed)
            .await
            .0,
        StatusCode::CONFLICT
    );
    let unsupported = request(
        &api,
        Method::POST,
        "/api/v1/cluster/members",
        &root,
        json!({"operationId":Id::new_v7().unwrap()}),
    )
    .await;
    assert_eq!(unsupported.0, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(unsupported.2["code"], "unsupported_operation");
    assert_eq!(
        request(
            &api,
            Method::GET,
            &format!("/api/v1/tenants?limit=1&cursor={cursor}"),
            &root,
            Value::Null
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    fixture
        .manage(public::ManageIdentity {
            action: public::IdentityAction::RevokeCredential.into(),
            target_id: Bytes::copy_from_slice(credential.as_bytes()),
            ..Default::default()
        })
        .await;
    let rejected = request(&api, Method::GET, "/api/v1/me", &scoped, Value::Null).await;
    assert_eq!(rejected.0, StatusCode::UNAUTHORIZED);
    assert!(
        rejected
            .1
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );
    assert_eq!(fixture.connections(), before);
    fixture.close().await;
}
