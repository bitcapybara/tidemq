use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::config::LoggingConfig;
use crate::observability::tracing::TracingConfig;
use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleRole {
    Console,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleConfig {
    pub role: ConsoleRole,
    pub listen: SocketAddr,
    pub management_listen: SocketAddr,
    pub static_dir: PathBuf,
    pub cluster: ClusterConfig,
    pub session: SessionConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub tracing: TracingConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub address: String,
    pub server_name: String,
    pub trust_roots: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionConfig {
    pub key_file: PathBuf,
    #[serde(default)]
    pub previous_key_files: Vec<PathBuf>,
    pub public_origin: String,
    #[serde(default = "default_max_age")]
    pub max_age_seconds: u16,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfig {
    #[serde(default)]
    pub components: Vec<ComponentProbeConfig>,
    pub alerts_url: Option<String>,
    pub alerts_detail_url: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentProbeConfig {
    pub id: String,
    pub name: String,
    pub probe_url: String,
    pub detail_url: Option<String>,
}

fn default_max_age() -> u16 {
    1_800
}

impl ConsoleConfig {
    pub fn validate(&self) -> Result<()> {
        for (valid, reason) in [
            (
                self.management_listen.ip().is_loopback(),
                "console management_listen must use a loopback IP",
            ),
            (
                self.listen != self.management_listen,
                "console listen must differ from management_listen",
            ),
            (
                !self.static_dir.as_os_str().is_empty(),
                "console static_dir must not be empty",
            ),
            (
                valid_cluster_address(&self.cluster.address),
                "console cluster.address must be a fixed IP or DNS host with a nonzero port",
            ),
            (
                valid_server_name(&self.cluster.server_name),
                "console cluster.server_name must be a bounded DNS name",
            ),
            (
                !self.cluster.trust_roots.as_os_str().is_empty(),
                "console cluster.trust_roots must not be empty",
            ),
        ] {
            if !valid {
                return Err(Error::Configuration { reason });
            }
        }
        self.session.validate()?;
        self.observability.validate()?;
        self.logging.validate()?;
        self.tracing.validate()
    }
}

impl ObservabilityConfig {
    pub fn validate(&self) -> Result<()> {
        let mut identifiers = std::collections::BTreeSet::new();
        if self.components.len() > 32 {
            return Err(Error::Configuration {
                reason: "console observability supports at most 32 configured components",
            });
        }
        for component in &self.components {
            if component.id.is_empty()
                || component.id.len() > 64
                || !component
                    .id
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                || ["console", "metadata", "storage", "broker", "gateway"]
                    .contains(&component.id.as_str())
                || !identifiers.insert(&component.id)
                || component.name.trim().is_empty()
                || component.name.len() > 160
                || component.name.chars().any(char::is_control)
            {
                return Err(Error::Configuration {
                    reason: "console observability components require distinct nonreserved IDs and bounded display names",
                });
            }
            validate_observability_url(&component.probe_url, false)?;
            if let Some(url) = &component.detail_url {
                validate_observability_url(url, true)?;
            }
        }
        if let Some(url) = &self.alerts_url {
            let url = validate_observability_url(url, false)?;
            if !url.path().ends_with("/api/v1/alerts") {
                return Err(Error::Configuration {
                    reason: "console observability.alerts_url must address a fixed Prometheus /api/v1/alerts endpoint",
                });
            }
        }
        if let Some(url) = &self.alerts_detail_url {
            validate_observability_url(url, true)?;
            if self.alerts_url.is_none() {
                return Err(Error::Configuration {
                    reason: "console observability.alerts_detail_url requires alerts_url",
                });
            }
        }
        Ok(())
    }
}

pub(super) fn validate_observability_url(value: &str, detail: bool) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(value).map_err(|_| Error::Configuration {
        reason: "console observability URLs must be absolute HTTP or HTTPS URLs",
    })?;
    if value.len() > 2_048
        || value.chars().any(char::is_control)
        || !matches!(url.scheme(), "https" | "http")
        || detail && url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || !detail && url.query().is_some()
        || url.fragment().is_some()
        || url.port() == Some(0)
    {
        return Err(Error::Configuration {
            reason: "console observability URLs cannot contain credentials, fragments, or probe queries; detail links require HTTPS",
        });
    }
    Ok(url)
}

impl SessionConfig {
    pub fn validate(&self) -> Result<()> {
        if self.key_file.as_os_str().is_empty()
            || self.previous_key_files.len() > 2
            || self
                .previous_key_files
                .iter()
                .any(|path| path.as_os_str().is_empty() || path == &self.key_file)
            || self
                .previous_key_files
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.previous_key_files.len()
        {
            return Err(Error::Configuration {
                reason: "console session keys require one current file and at most two distinct previous files",
            });
        }
        if !(60..=3_600).contains(&self.max_age_seconds) {
            return Err(Error::Configuration {
                reason: "console session.max_age_seconds must be between 60 and 3600",
            });
        }
        validate_origin(&self.public_origin)
    }
}

fn valid_cluster_address(address: &str) -> bool {
    if let Ok(address) = address.parse::<SocketAddr>() {
        return address.port() != 0 && !address.ip().is_unspecified();
    }
    address.rsplit_once(':').is_some_and(|(host, port)| {
        valid_server_name(host) && port.parse::<u16>().is_ok_and(|port| port != 0)
    })
}

fn valid_server_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn validate_origin(origin: &str) -> Result<()> {
    let url = reqwest::Url::parse(origin).map_err(|_| Error::Configuration {
        reason: "console session.public_origin must be an absolute HTTPS origin",
    })?;
    if origin.len() > 512
        || url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::Configuration {
            reason: "console session.public_origin must contain only an HTTPS scheme, host, and optional port",
        });
    }
    Ok(())
}
