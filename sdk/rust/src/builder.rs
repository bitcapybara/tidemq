use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use bytes::Bytes;
use tokio::io::AsyncReadExt;

use crate::{Client, ClientConfig, Error, RequestContext, Result};

#[derive(Clone, Debug)]
pub enum TlsTrust {
    CaFile(PathBuf),
    CaPem(Bytes),
}

impl TlsTrust {
    pub fn ca_file(path: impl Into<PathBuf>) -> Self {
        Self::CaFile(path.into())
    }
    pub fn ca_pem(pem: impl Into<Bytes>) -> Self {
        Self::CaPem(pem.into())
    }

    async fn load(self) -> Result<Bytes> {
        let pem = match self {
            Self::CaFile(path) => read_material(&path, 65_536, false).await?,
            Self::CaPem(pem) => pem,
        };
        if pem.is_empty() || pem.len() > 65_536 {
            return Err(Error::Configuration {
                reason: "CA material must contain 1 to 65536 bytes",
            });
        }
        Ok(pem)
    }
}

#[derive(Clone)]
pub enum Credentials {
    TokenFile(PathBuf),
    TokenEnv(String),
    Token(String),
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Credentials(<redacted>)")
    }
}

impl Credentials {
    pub fn token_file(path: impl Into<PathBuf>) -> Self {
        Self::TokenFile(path.into())
    }
    pub fn token_env(name: impl Into<String>) -> Self {
        Self::TokenEnv(name.into())
    }
    pub fn token(token: impl Into<String>) -> Self {
        Self::Token(token.into())
    }

    async fn load(self) -> Result<String> {
        let token = match self {
            Self::Token(token) => token,
            Self::TokenEnv(name) => std::env::var(name).map_err(|_| Error::Configuration {
                reason: "access token environment variable is absent or is not Unicode",
            })?,
            Self::TokenFile(path) => {
                let bytes = read_material(&path, 258, true).await?;
                std::str::from_utf8(&bytes)
                    .map_err(|_| Error::Configuration {
                        reason: "access token file must contain UTF-8 text",
                    })?
                    .trim_end_matches(['\r', '\n'])
                    .to_owned()
            }
        };
        if !(32..=256).contains(&token.len()) || !token.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(Error::Configuration {
                reason: "access token must contain 32 to 256 printable ASCII bytes without whitespace",
            });
        }
        Ok(token)
    }
}

#[derive(Clone, Debug)]
pub struct ClientBuilder {
    config: ClientConfig,
    trust: TlsTrust,
    credentials: Credentials,
}

impl ClientBuilder {
    pub(crate) fn new(
        endpoint: impl Into<String>,
        trust: TlsTrust,
        credentials: Credentials,
    ) -> Self {
        Self {
            config: ClientConfig::new(endpoint),
            trust,
            credentials,
        }
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.config.namespace = Some(namespace.into());
        self
    }
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        self
    }
    pub fn close_timeout(mut self, timeout: Duration) -> Self {
        self.config.close_timeout = timeout;
        self
    }
    /// At least three entries preserve admission for publication, receiving, and control work.
    pub fn pending_requests(mut self, maximum: u32) -> Self {
        self.config.max_pending_requests = maximum;
        self
    }
    pub fn pending_bytes(mut self, maximum: u64) -> Self {
        self.config.max_pending_bytes = maximum;
        self
    }

    pub fn request_lanes(mut self, lanes: usize) -> Self {
        self.config.request_lanes = lanes;
        self
    }
    pub fn limits(&self) -> crate::ClientLimits {
        (&self.config).into()
    }

    pub async fn build(self) -> Result<Client> {
        self.config.validate()?;
        validate_namespace(self.config.namespace.as_deref())?;
        let context = RequestContext::with_timeout(self.config.connect_timeout)?;
        context
            .scope(async {
                let trust = self.trust.load().await?;
                let token = self.credentials.load().await?;
                Client::connect(self.config, trust, token).await
            })
            .await
    }
}

impl Client {
    pub fn builder(
        endpoint: impl Into<String>,
        trust: TlsTrust,
        credentials: Credentials,
    ) -> ClientBuilder {
        ClientBuilder::new(endpoint, trust, credentials)
    }

    pub(crate) fn topic_name(&self, name: &str) -> Result<String> {
        if name.split('/').count() == 3 {
            validate_resource_name(name, 3)?;
            return Ok(name.to_owned());
        }
        validate_resource_name(name, 1)?;
        let namespace = self
            .inner
            .config
            .namespace
            .as_deref()
            .ok_or(Error::Configuration {
                reason: "short topic names require a configured tenant/namespace",
            })?;
        Ok(format!("{namespace}/{name}"))
    }
}

fn validate_namespace(namespace: Option<&str>) -> Result<()> {
    if let Some(namespace) = namespace {
        validate_resource_name(namespace, 2)?;
    }
    Ok(())
}

fn validate_resource_name(name: &str, components: usize) -> Result<()> {
    if name.len() > 128
        || name.split('/').count() != components
        || name.split('/').any(|part| {
            part.is_empty()
                || matches!(part, "." | "..")
                || part.len() > 64
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
    {
        return Err(Error::Configuration {
            reason: "resource name must contain bounded tenant, namespace and resource components",
        });
    }
    Ok(())
}

async fn read_material(path: &Path, maximum: u64, private: bool) -> Result<Bytes> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|source| Error::Material { source })?;
    let metadata = file
        .metadata()
        .await
        .map_err(|source| Error::Material { source })?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(Error::Configuration {
            reason: "TLS or credential file must be a bounded regular file",
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if private && metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::Configuration {
                reason: "credential file must have owner-only permissions",
            });
        }
    }
    #[cfg(not(unix))]
    let _ = private;
    let mut bytes = Vec::new();
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| Error::Material { source })?;
    if bytes.len() as u64 > maximum {
        return Err(Error::Configuration {
            reason: "TLS or credential file exceeds its byte limit",
        });
    }
    Ok(bytes.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn builders_do_not_read_materials_before_build_and_reject_bad_configuration_first() {
        let builder = Client::builder(
            "invalid",
            TlsTrust::ca_file("missing-ca.pem"),
            Credentials::token_file("missing-token"),
        )
        .namespace("tenant/namespace")
        .request_timeout(Duration::ZERO);
        assert!(matches!(
            builder.build().await,
            Err(Error::Configuration { .. })
        ));
        assert!(
            !format!("{:?}", Credentials::token("secret-token-value"))
                .contains("secret-token-value")
        );
        assert!(validate_namespace(Some("tenant/namespace")).is_ok());
        for name in [
            "",
            "namespace",
            "tenant/",
            "tenant/namespace/extra",
            "../namespace",
        ] {
            assert!(
                validate_namespace(Some(name)).is_err(),
                "Invalid namespace was accepted: {name}"
            );
        }
    }
}
