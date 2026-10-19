use std::fmt;

use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use uuid::Uuid;

use crate::error::{EntropySnafu, UuidSnafu};
use crate::{Error, Result};

/// A nonzero immutable identity used for topics, operations, sessions, and producers.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "Uuid", into = "Uuid")]
pub struct Id(Uuid);

impl Id {
    pub fn random() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).context(EntropySnafu)?;
        Ok(Self(uuid::Builder::from_random_bytes(bytes).into_uuid()))
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        Self::try_from(Uuid::from_slice(bytes).context(UuidSnafu)?)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl TryFrom<Uuid> for Id {
    type Error = Error;

    fn try_from(value: Uuid) -> Result<Self> {
        if value.is_nil() {
            return Err(Error::Invalid {
                reason: "identity must not be zero",
            });
        }
        Ok(Self(value))
    }
}

impl From<Id> for Uuid {
    fn from(value: Id) -> Self {
        value.0
    }
}

impl std::str::FromStr for Id {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::try_from(Uuid::parse_str(value).context(UuidSnafu)?)
    }
}

impl fmt::Display for Id {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_round_trip_and_reject_zero() {
        let id = Id::random().expect("entropy is available");
        assert_eq!(Id::from_slice(id.as_bytes()).unwrap(), id);
        assert_eq!(id.to_string().parse::<Id>().unwrap(), id);
        assert!(Id::from_slice(&[0; 16]).is_err());
        assert!(Id::from_slice(&[1; 15]).is_err());
        assert_eq!(Uuid::from(id).get_version_num(), 4);
    }
}
