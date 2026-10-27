use std::path::{Path, PathBuf};

use axum::http::{HeaderMap, HeaderValue, header};

use super::*;

const NOW: u64 = 1_000_000;
const TOKEN: &str = "personal-token-secret-that-never-enters-page-output-or-logs";

struct Fixture {
    directory: PathBuf,
    config: SessionConfig,
}

impl Fixture {
    async fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "tidemq-console-session-{}",
            tidemq_protocol::Id::new_v7().unwrap()
        ));
        tokio::fs::create_dir(&directory).await.unwrap();
        let config = SessionConfig {
            key_file: directory.join("current.key"),
            previous_key_files: Vec::new(),
            public_origin: "https://console.example.test".into(),
            max_age_seconds: 1_800,
        };
        write_key(&config.key_file, [1; 32]).await;
        Self { directory, config }
    }

    async fn manager(&self) -> SessionManager {
        SessionManager::load(&self.config).await.unwrap()
    }

    async fn clean(self) {
        tokio::fs::remove_dir_all(self.directory).await.unwrap();
    }
}

async fn write_key(file: &Path, bytes: [u8; 32]) {
    tokio::fs::write(file, bytes).await.unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600))
            .await
            .unwrap();
    }
}

fn headers(issued: &IssuedSession) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let cookie = issued
        .set_cookie
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    headers.insert(header::COOKIE, HeaderValue::from_str(cookie).unwrap());
    headers.insert(
        header::ORIGIN,
        HeaderValue::from_static("https://console.example.test"),
    );
    headers.insert(
        "x-csrf-token",
        HeaderValue::from_str(&issued.csrf_token).unwrap(),
    );
    headers
}

#[tokio::test]
async fn encrypted_host_cookie_works_across_instances_and_debug_hides_secrets() {
    let fixture = Fixture::new().await;
    let first = fixture.manager().await;
    let second = fixture.manager().await;
    let issued = first.establish(TOKEN, u64::MAX, NOW).unwrap();
    let cookie = issued.set_cookie.to_str().unwrap();
    for attribute in [
        "__Host-tidemq-session=s1.",
        "; Path=/",
        "; HttpOnly",
        "; Secure",
        "; SameSite=Lax",
        "; Max-Age=1800",
    ] {
        assert!(
            cookie.contains(attribute),
            "Missing cookie protection: {attribute}"
        );
    }
    assert!(!cookie.contains("Domain="));
    assert!(!cookie.contains(TOKEN));
    assert!(issued.set_cookie.is_sensitive());
    let request = headers(&issued);
    let session = second.authenticate(&request, NOW + 500).unwrap();
    assert_eq!(session.token(), TOKEN);
    assert_eq!(session.csrf_token(), issued.csrf_token);
    second.check_csrf(&request, &session).unwrap();
    assert!(!format!("{issued:?} {session:?}").contains(TOKEN));
    assert!(!format!("{issued:?} {session:?}").contains(&issued.csrf_token));
    let another = first.establish(TOKEN, u64::MAX, NOW).unwrap();
    assert_ne!(another.set_cookie, issued.set_cookie);
    assert_ne!(another.csrf_token, issued.csrf_token);
    let cleared = SessionManager::clear_cookie();
    assert!(cleared.to_str().unwrap().contains("Max-Age=0"));
    assert!(cleared.is_sensitive());
    fixture.clean().await;
}

#[tokio::test]
async fn session_is_bounded_by_real_credential_expiry_and_never_slides_on_read() {
    let fixture = Fixture::new().await;
    let manager = fixture.manager().await;
    let issued = manager.establish(TOKEN, NOW + 3_000, NOW).unwrap();
    assert_eq!(issued.expires_at_unix_ms, NOW + 3_000);
    assert!(issued.set_cookie.to_str().unwrap().contains("Max-Age=3"));
    let request = headers(&issued);
    assert_eq!(
        manager
            .authenticate(&request, NOW + 2_999)
            .unwrap()
            .expires_at_unix_ms(),
        NOW + 3_000
    );
    assert!(matches!(
        manager.authenticate(&request, NOW + 3_000),
        Err(SessionError::Expired)
    ));
    assert!(matches!(
        manager.establish(TOKEN, NOW, NOW),
        Err(SessionError::Expired)
    ));
    assert!(matches!(
        manager.establish(TOKEN, NOW + 999, NOW),
        Err(SessionError::Expired)
    ));
    assert!(matches!(
        manager.establish("short", u64::MAX, NOW),
        Err(SessionError::Invalid)
    ));
    fixture.clean().await;
}

#[tokio::test]
async fn origin_and_csrf_checks_reject_missing_forged_and_duplicate_headers() {
    let fixture = Fixture::new().await;
    let manager = fixture.manager().await;
    let issued = manager.establish(TOKEN, u64::MAX, NOW).unwrap();
    let valid = headers(&issued);
    let session = manager.authenticate(&valid, NOW).unwrap();
    for origin in [
        None,
        Some("null"),
        Some("https://evil.example.test"),
        Some("https://console.example.test.evil.test"),
        Some("https://console.example.test/"),
    ] {
        let mut request = valid.clone();
        request.remove(header::ORIGIN);
        if let Some(origin) = origin {
            request.insert(header::ORIGIN, HeaderValue::from_str(origin).unwrap());
        }
        assert!(matches!(
            manager.check_csrf(&request, &session),
            Err(SessionError::Origin)
        ));
    }
    let mut duplicate = valid.clone();
    duplicate.append(
        header::ORIGIN,
        HeaderValue::from_static("https://console.example.test"),
    );
    assert!(matches!(
        manager.check_origin(&duplicate),
        Err(SessionError::Origin)
    ));
    let mut forged = valid.clone();
    forged.insert("x-csrf-token", HeaderValue::from_static("forged"));
    assert!(matches!(
        manager.check_csrf(&forged, &session),
        Err(SessionError::Csrf)
    ));
    let mut missing = valid.clone();
    missing.remove("x-csrf-token");
    assert!(matches!(
        manager.check_csrf(&missing, &session),
        Err(SessionError::Csrf)
    ));
    let mut duplicate = valid.clone();
    duplicate.append(
        "x-csrf-token",
        HeaderValue::from_str(&issued.csrf_token).unwrap(),
    );
    assert!(matches!(
        manager.check_csrf(&duplicate, &session),
        Err(SessionError::Csrf)
    ));
    let mut cross_site = valid.clone();
    cross_site.insert("sec-fetch-site", HeaderValue::from_static("cross-site"));
    assert!(matches!(
        manager.check_origin(&cross_site),
        Err(SessionError::Origin)
    ));
    let mut no_origin = valid;
    no_origin.remove(header::ORIGIN);
    manager.authenticate(&no_origin, NOW).unwrap();
    fixture.clean().await;
}

#[tokio::test]
async fn malformed_tampered_duplicate_and_cross_origin_cookies_fail_closed() {
    let fixture = Fixture::new().await;
    let manager = fixture.manager().await;
    let issued = manager.establish(TOKEN, u64::MAX, NOW).unwrap();
    let valid = headers(&issued);
    let cookie = valid.get(header::COOKIE).unwrap().to_str().unwrap();
    let value = cookie.split_once('=').unwrap().1;
    let mut tampered = URL_SAFE_NO_PAD
        .decode(value.strip_prefix(COOKIE_PREFIX).unwrap())
        .unwrap();
    *tampered.last_mut().unwrap() ^= 1;
    for value in [
        "".to_owned(),
        "s0.invalid".into(),
        "s1.bad".into(),
        format!("s1.{}", URL_SAFE_NO_PAD.encode(&tampered)),
        "x".repeat(MAX_COOKIE_BYTES + 1),
    ] {
        let mut request = valid.clone();
        request.insert(
            header::COOKIE,
            HeaderValue::from_str(&format!("{COOKIE_NAME}={value}")).unwrap(),
        );
        assert!(matches!(
            manager.authenticate(&request, NOW),
            Err(SessionError::Invalid)
        ));
    }
    let mut duplicate = valid.clone();
    duplicate.append(header::COOKIE, valid.get(header::COOKIE).unwrap().clone());
    assert!(matches!(
        manager.authenticate(&duplicate, NOW),
        Err(SessionError::Invalid)
    ));
    let mut enormous = valid.clone();
    enormous.append(
        header::COOKIE,
        HeaderValue::from_str(&format!("other={}", "x".repeat(MAX_HEADER_BYTES))).unwrap(),
    );
    assert!(matches!(
        manager.authenticate(&enormous, NOW),
        Err(SessionError::Invalid)
    ));
    assert!(matches!(
        manager.authenticate(&HeaderMap::new(), NOW),
        Err(SessionError::Missing)
    ));
    let mut foreign = fixture.config.clone();
    foreign.public_origin = "https://other.example.test".into();
    let foreign = SessionManager::load(&foreign).await.unwrap();
    assert!(matches!(
        foreign.authenticate(&valid, NOW),
        Err(SessionError::Invalid)
    ));
    fixture.clean().await;
}

#[tokio::test]
async fn shared_key_rotation_accepts_old_sessions_and_issues_only_with_the_current_key() {
    let fixture = Fixture::new().await;
    let old = fixture.manager().await;
    let issued_old = old.establish(TOKEN, u64::MAX, NOW).unwrap();
    let new_key = fixture.directory.join("new.key");
    write_key(&new_key, [2; 32]).await;
    let mut config = fixture.config.clone();
    config.key_file = new_key.clone();
    config.previous_key_files = vec![fixture.config.key_file.clone()];
    let rotated = SessionManager::load(&config).await.unwrap();
    rotated.authenticate(&headers(&issued_old), NOW).unwrap();
    let issued_new = rotated.establish(TOKEN, u64::MAX, NOW).unwrap();
    assert!(matches!(
        old.authenticate(&headers(&issued_new), NOW),
        Err(SessionError::Invalid)
    ));
    config.previous_key_files.clear();
    let current_only = SessionManager::load(&config).await.unwrap();
    current_only
        .authenticate(&headers(&issued_new), NOW)
        .unwrap();
    assert!(matches!(
        current_only.authenticate(&headers(&issued_old), NOW),
        Err(SessionError::Invalid)
    ));
    let mut staged = fixture.config.clone();
    staged.previous_key_files = vec![new_key];
    let staged = SessionManager::load(&staged).await.unwrap();
    staged.authenticate(&headers(&issued_new), NOW).unwrap();
    fixture.clean().await;
}

#[tokio::test]
async fn keys_reject_invalid_bounds_duplicates_and_insecure_permissions() {
    let fixture = Fixture::new().await;
    let duplicate = fixture.directory.join("same-material.key");
    write_key(&duplicate, [1; 32]).await;
    let mut config = fixture.config.clone();
    config.previous_key_files = vec![duplicate.clone()];
    assert!(SessionManager::load(&config).await.is_err());
    config.previous_key_files = vec![fixture.config.key_file.clone()];
    assert!(config.validate().is_err());
    config.previous_key_files = vec![duplicate.clone(), duplicate.clone(), duplicate];
    assert!(config.validate().is_err());
    tokio::fs::write(&fixture.config.key_file, [1; 31])
        .await
        .unwrap();
    assert!(SessionManager::load(&fixture.config).await.is_err());
    write_key(&fixture.config.key_file, [1; 32]).await;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(
            &fixture.config.key_file,
            std::fs::Permissions::from_mode(0o644),
        )
        .await
        .unwrap();
        assert!(SessionManager::load(&fixture.config).await.is_err());
        tokio::fs::set_permissions(
            &fixture.config.key_file,
            std::fs::Permissions::from_mode(0o440),
        )
        .await
        .unwrap();
        fixture.manager().await;
    }
    fixture.clean().await;
}

#[tokio::test]
async fn authenticated_claims_cannot_extend_lifetime_or_arrive_from_the_future() {
    let fixture = Fixture::new().await;
    let manager = fixture.manager().await;
    for (issued_ms, expires_ms) in [
        (NOW + CLOCK_SKEW_MS + 1, NOW + CLOCK_SKEW_MS + 1_000),
        (NOW, NOW + manager.lifetime_ms + 1),
        (NOW, NOW),
    ] {
        let claims = Claims {
            token: Zeroizing::new(TOKEN.to_owned()),
            csrf: Zeroizing::new("c".repeat(43)),
            issued_ms,
            expires_ms,
        };
        let sealed = manager.seal(&claims).unwrap();
        let mut request = HeaderMap::new();
        request.insert(
            header::COOKIE,
            HeaderValue::from_str(&format!("{COOKIE_NAME}={COOKIE_PREFIX}{sealed}")).unwrap(),
        );
        assert!(matches!(
            manager.authenticate(&request, NOW),
            Err(SessionError::Invalid)
        ));
    }
    fixture.clean().await;
}

#[tokio::test]
async fn opaque_cursors_are_bounded_tamper_resistant_and_isolated_from_sessions() {
    let fixture = Fixture::new().await;
    let old = fixture.manager().await;
    let payload = br#"{"actor":"17","topic":"31","expiresAtMs":"1030000"}"#;
    let cursor = old.seal_cursor(payload).unwrap();
    assert_eq!(old.open_cursor(&cursor).unwrap(), payload);
    assert!(!cursor.contains("actor"));
    assert!(!cursor.contains("topic"));
    assert_ne!(cursor, old.seal_cursor(payload).unwrap());
    let issued = old.establish(TOKEN, u64::MAX, NOW).unwrap();
    let cookie = headers(&issued)
        .get(header::COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let cookie_value = cookie.split_once('=').unwrap().1;
    let forged_cursor = cookie_value.replacen(COOKIE_PREFIX, CURSOR_PREFIX, 1);
    assert!(old.open_cursor(&forged_cursor).is_err());
    let mut forged_headers = HeaderMap::new();
    forged_headers.insert(
        header::COOKIE,
        HeaderValue::from_str(&format!(
            "{COOKIE_NAME}={}",
            cursor.replacen(CURSOR_PREFIX, COOKIE_PREFIX, 1)
        ))
        .unwrap(),
    );
    assert!(old.authenticate(&forged_headers, NOW).is_err());
    let mut tampered = URL_SAFE_NO_PAD
        .decode(cursor.strip_prefix(CURSOR_PREFIX).unwrap())
        .unwrap();
    *tampered.last_mut().unwrap() ^= 1;
    assert!(
        old.open_cursor(&format!(
            "{CURSOR_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(tampered)
        ))
        .is_err()
    );
    assert!(old.open_cursor(&"x".repeat(MAX_CURSOR_BYTES + 1)).is_err());
    assert!(old.seal_cursor(&[]).is_err());
    assert!(
        old.seal_cursor(&vec![0; MAX_CURSOR_PAYLOAD_BYTES + 1])
            .is_err()
    );
    let new_key = fixture.directory.join("new.key");
    write_key(&new_key, [2; 32]).await;
    let mut config = fixture.config.clone();
    config.key_file = new_key;
    config.previous_key_files = vec![fixture.config.key_file.clone()];
    let rotated = SessionManager::load(&config).await.unwrap();
    assert_eq!(rotated.open_cursor(&cursor).unwrap(), payload);
    assert!(
        old.open_cursor(&rotated.seal_cursor(payload).unwrap())
            .is_err()
    );
    fixture.clean().await;
}
