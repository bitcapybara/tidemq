//! Encrypted, host-bound cookies carry credentials without caching MQ authority.

mod keys;

use std::fmt;

use aws_lc_rs::aead::{Aad, Nonce, RandomizedNonceKey};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use super::SessionConfig;

pub const COOKIE_NAME: &str = "__Host-tidemq-session";
const COOKIE_PREFIX: &str = "s1.";
const CURSOR_PREFIX: &str = "c1.";
const MAX_COOKIE_BYTES: usize = 3_072;
const MAX_CURSOR_BYTES: usize = 8_192;
const MAX_CURSOR_PAYLOAD_BYTES: usize = 4_096;
const MAX_HEADER_BYTES: usize = 16_384;
const NONCE_BYTES: usize = 12;
const CLOCK_SKEW_MS: u64 = 30_000;

pub struct SessionManager {
    current: RandomizedNonceKey,
    previous: Vec<RandomizedNonceKey>,
    origin: String,
    aad: String,
    lifetime_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    token: Zeroizing<String>,
    csrf: Zeroizing<String>,
    issued_ms: u64,
    expires_ms: u64,
}

pub struct Session {
    claims: Claims,
}

impl Session {
    pub fn token(&self) -> &str {
        self.claims.token.as_str()
    }

    pub fn csrf_token(&self) -> &str {
        self.claims.csrf.as_str()
    }

    pub fn expires_at_unix_ms(&self) -> u64 {
        self.claims.expires_ms
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Session")
            .field("expires_ms", &self.claims.expires_ms)
            .finish_non_exhaustive()
    }
}

pub struct IssuedSession {
    pub set_cookie: HeaderValue,
    pub csrf_token: String,
    pub expires_at_unix_ms: u64,
}

impl fmt::Debug for IssuedSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IssuedSession")
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Snafu)]
pub enum SessionError {
    #[snafu(display("Authentication is required"))]
    Missing,
    #[snafu(display("The session is invalid; sign in again"))]
    Invalid,
    #[snafu(display("The session has expired; sign in again"))]
    Expired,
    #[snafu(display("The request origin is not allowed"))]
    Origin,
    #[snafu(display("The request CSRF token is missing or invalid"))]
    Csrf,
    #[snafu(display("The secure session could not be created"))]
    Unavailable,
}

impl SessionError {
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Missing | Self::Invalid | Self::Expired => StatusCode::UNAUTHORIZED,
            Self::Origin | Self::Csrf => StatusCode::FORBIDDEN,
            Self::Unavailable => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Missing => "authentication_required",
            Self::Invalid => "invalid_session",
            Self::Expired => "session_expired",
            Self::Origin => "origin_rejected",
            Self::Csrf => "csrf_rejected",
            Self::Unavailable => "session_unavailable",
        }
    }

    pub fn message(&self) -> String {
        self.to_string()
    }
}

impl SessionManager {
    pub async fn load(config: &SessionConfig) -> crate::Result<Self> {
        config.validate()?;
        let origin = reqwest::Url::parse(&config.public_origin)
            .map_err(|_| crate::Error::Configuration {
                reason: "console session origin is invalid",
            })?
            .origin()
            .ascii_serialization();
        let (current, previous) = keys::load(config).await?;
        Ok(Self {
            current,
            previous,
            aad: format!("tidemq-console:{COOKIE_NAME}:s1:{origin}"),
            origin,
            lifetime_ms: u64::from(config.max_age_seconds) * 1_000,
        })
    }

    /// Call only after the core has authenticated this credential and returned its actual expiry.
    pub fn establish(
        &self,
        token: &str,
        credential_expires_at_unix_ms: u64,
        now_ms: u64,
    ) -> Result<IssuedSession, SessionError> {
        if !valid_token(token) {
            return Err(SessionError::Invalid);
        }
        let expires_ms = now_ms
            .checked_add(self.lifetime_ms)
            .ok_or(SessionError::Unavailable)?
            .min(credential_expires_at_unix_ms);
        let remaining = expires_ms
            .checked_sub(now_ms)
            .ok_or(SessionError::Expired)?;
        if remaining < 1_000 {
            return Err(SessionError::Expired);
        }
        let mut csrf = Zeroizing::new([0_u8; 32]);
        getrandom::fill(&mut *csrf).map_err(|_| SessionError::Unavailable)?;
        let csrf = URL_SAFE_NO_PAD.encode(csrf.as_slice());
        let claims = Claims {
            token: Zeroizing::new(token.to_owned()),
            csrf: Zeroizing::new(csrf.clone()),
            issued_ms: now_ms,
            expires_ms,
        };
        let sealed = self.seal(&claims)?;
        let mut cookie = HeaderValue::from_str(&format!("{COOKIE_NAME}={COOKIE_PREFIX}{sealed}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age={}", remaining / 1_000))
            .map_err(|_| SessionError::Unavailable)?;
        cookie.set_sensitive(true);
        Ok(IssuedSession {
            set_cookie: cookie,
            csrf_token: csrf,
            expires_at_unix_ms: expires_ms,
        })
    }

    /// This recovers a credential, not authority; every request must still call the authenticated SDK.
    pub fn authenticate(&self, headers: &HeaderMap, now_ms: u64) -> Result<Session, SessionError> {
        let value = cookie_value(headers)?;
        let value = value
            .strip_prefix(COOKIE_PREFIX)
            .ok_or(SessionError::Invalid)?;
        let body = self.open_body(value, self.aad.as_bytes())?;
        let claims: Claims =
            serde_json::from_slice(body.as_slice()).map_err(|_| SessionError::Invalid)?;
        self.validate_claims(&claims, now_ms)?;
        Ok(Session { claims })
    }

    /// Callers must bind the opaque payload to its actor, resource, filters, and expiry.
    pub(super) fn seal_cursor(&self, payload: &[u8]) -> Result<String, SessionError> {
        if payload.is_empty() || payload.len() > MAX_CURSOR_PAYLOAD_BYTES {
            return Err(SessionError::Invalid);
        }
        let aad = format!("{}:cursor", self.aad);
        let encoded = self.seal_body(Zeroizing::new(payload.to_vec()), aad.as_bytes())?;
        Ok(format!("{CURSOR_PREFIX}{encoded}"))
    }

    pub(super) fn open_cursor(&self, token: &str) -> Result<Vec<u8>, SessionError> {
        if token.len() > MAX_CURSOR_BYTES {
            return Err(SessionError::Invalid);
        }
        let encoded = token
            .strip_prefix(CURSOR_PREFIX)
            .ok_or(SessionError::Invalid)?;
        let aad = format!("{}:cursor", self.aad);
        let body = self.open_body(encoded, aad.as_bytes())?;
        if body.is_empty() || body.len() > MAX_CURSOR_PAYLOAD_BYTES {
            return Err(SessionError::Invalid);
        }
        Ok(body.to_vec())
    }

    pub fn check_origin(&self, headers: &HeaderMap) -> Result<(), SessionError> {
        let origin = one_header(headers, header::ORIGIN.as_str()).ok_or(SessionError::Origin)?;
        if origin != self.origin.as_bytes()
            || headers.contains_key("sec-fetch-site")
                && one_header(headers, "sec-fetch-site") != Some(b"same-origin")
        {
            return Err(SessionError::Origin);
        }
        Ok(())
    }

    pub fn check_csrf(&self, headers: &HeaderMap, session: &Session) -> Result<(), SessionError> {
        self.check_origin(headers)?;
        let csrf = one_header(headers, "x-csrf-token").ok_or(SessionError::Csrf)?;
        if !bool::from(csrf.ct_eq(session.csrf_token().as_bytes())) {
            return Err(SessionError::Csrf);
        }
        Ok(())
    }

    pub fn clear_cookie() -> HeaderValue {
        let mut cookie = HeaderValue::from_static(
            "__Host-tidemq-session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0",
        );
        cookie.set_sensitive(true);
        cookie
    }

    fn seal(&self, claims: &Claims) -> Result<String, SessionError> {
        let body =
            Zeroizing::new(serde_json::to_vec(claims).map_err(|_| SessionError::Unavailable)?);
        let encoded = self.seal_body(body, self.aad.as_bytes())?;
        if encoded.len() > MAX_COOKIE_BYTES - COOKIE_PREFIX.len() {
            return Err(SessionError::Unavailable);
        }
        Ok(encoded)
    }

    fn seal_body(&self, mut body: Zeroizing<Vec<u8>>, aad: &[u8]) -> Result<String, SessionError> {
        let nonce = self
            .current
            .seal_in_place_append_tag(Aad::from(aad), &mut *body)
            .map_err(|_| SessionError::Unavailable)?;
        let mut envelope = Vec::with_capacity(NONCE_BYTES + body.len());
        envelope.extend_from_slice(nonce.as_ref());
        envelope.extend_from_slice(body.as_slice());
        Ok(URL_SAFE_NO_PAD.encode(envelope))
    }

    fn open_body(&self, encoded: &str, aad: &[u8]) -> Result<Zeroizing<Vec<u8>>, SessionError> {
        let envelope = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| SessionError::Invalid)?;
        let nonce: [u8; NONCE_BYTES] = envelope
            .get(..NONCE_BYTES)
            .ok_or(SessionError::Invalid)?
            .try_into()
            .map_err(|_| SessionError::Invalid)?;
        let ciphertext = envelope.get(NONCE_BYTES..).ok_or(SessionError::Invalid)?;
        for key in std::iter::once(&self.current).chain(&self.previous) {
            let mut body = Zeroizing::new(ciphertext.to_vec());
            if let Ok(plaintext) = key.open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(aad),
                body.as_mut_slice(),
            ) {
                let length = plaintext.len();
                body.truncate(length);
                return Ok(body);
            }
        }
        Err(SessionError::Invalid)
    }

    fn validate_claims(&self, claims: &Claims, now_ms: u64) -> Result<(), SessionError> {
        if !valid_token(&claims.token)
            || claims.csrf.len() != 43
            || !claims
                .csrf
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
            || claims.issued_ms > now_ms.saturating_add(CLOCK_SKEW_MS)
            || claims.expires_ms <= claims.issued_ms
            || claims.expires_ms.saturating_sub(claims.issued_ms) > self.lifetime_ms
        {
            return Err(SessionError::Invalid);
        }
        if claims.expires_ms <= now_ms {
            return Err(SessionError::Expired);
        }
        Ok(())
    }
}

fn valid_token(token: &str) -> bool {
    (32..=256).contains(&token.len()) && token.bytes().all(|byte| byte.is_ascii_graphic())
}

fn one_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a [u8]> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    values.next().is_none().then_some(value.as_bytes())
}

fn cookie_value(headers: &HeaderMap) -> Result<&str, SessionError> {
    let mut found = None;
    let mut bytes = 0_usize;
    for value in headers.get_all(header::COOKIE) {
        bytes = bytes
            .checked_add(value.as_bytes().len())
            .ok_or(SessionError::Invalid)?;
        if bytes > MAX_HEADER_BYTES {
            return Err(SessionError::Invalid);
        }
        for pair in value
            .to_str()
            .map_err(|_| SessionError::Invalid)?
            .split(';')
        {
            if let Some((name, value)) = pair.trim().split_once('=')
                && name == COOKIE_NAME
            {
                if found.is_some() || value.is_empty() || value.len() > MAX_COOKIE_BYTES {
                    return Err(SessionError::Invalid);
                }
                found = Some(value);
            }
        }
    }
    found.ok_or(SessionError::Missing)
}

#[cfg(test)]
mod tests;
