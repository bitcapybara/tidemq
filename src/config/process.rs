use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize};
use snafu::ResultExt;

use super::{Config, read_configuration};
use crate::console::ConsoleConfig;
use crate::error::ParseConfigurationSnafu;
use crate::{Error, Result};

/// Process selection is separate from the roles accepted by the MQ registry and Raft peers.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum ProcessConfig {
    Node(Box<Config>),
    Console(Box<ConsoleConfig>),
}

impl<'de> Deserialize<'de> for ProcessConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = toml::Value::deserialize(deserializer)?;
        if value.get("role").and_then(toml::Value::as_str) == Some("console") {
            value
                .try_into()
                .map(|config| Self::Console(Box::new(config)))
                .map_err(serde::de::Error::custom)
        } else {
            value
                .try_into()
                .map(|config| Self::Node(Box::new(config)))
                .map_err(serde::de::Error::custom)
        }
    }
}

impl ProcessConfig {
    pub async fn load(path: &Path) -> Result<Self> {
        let text = read_configuration(path).await?;
        let config: Self = toml::from_str(&text).context(ParseConfigurationSnafu { path })?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Node(config) => config.validate(),
            Self::Console(config) => config.validate(),
        }
    }

    pub fn validate_for_startup(&self) -> Result<()> {
        self.validate()?;
        if let Self::Node(config) = self {
            validate_node_runtime(config)?;
        }
        Ok(())
    }

    pub fn validate_for_diagnosis(&self) -> Result<()> {
        self.validate()?;
        if let Self::Node(config) = self {
            validate_node_runtime(config)?;
        }
        Ok(())
    }
}

fn validate_node_runtime(config: &Config) -> Result<()> {
    config.runtime.validate(config).map_err(|source| Error::Io {
        operation: "validate queue runtime configuration",
        path: "runtime".into(),
        source: std::io::Error::other(source),
    })
}
