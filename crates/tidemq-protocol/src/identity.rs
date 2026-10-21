use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

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
    /// Generates a time-sortable UUID without promising monotonic or distributed ordering.
    pub fn new_v7() -> Result<Self> {
        let mut bytes = [0_u8; 10];
        getrandom::fill(&mut bytes).context(EntropySnafu)?;
        Self::v7_at(SystemTime::now(), &bytes)
    }

    fn v7_at(time: SystemTime, random: &[u8; 10]) -> Result<Self> {
        let millis = time
            .duration_since(UNIX_EPOCH)
            .map_err(|source| Error::IdentityClock { source })?
            .as_millis();
        if millis > u128::from((1_u64 << 48) - 1) {
            return Err(Error::IdentityTimestamp { millis });
        }
        let millis = u64::try_from(millis).map_err(|_| Error::IdentityTimestamp { millis })?;
        Ok(Self(
            uuid::Builder::from_unix_timestamp_millis(millis, random).into_uuid(),
        ))
    }

    /// Generates a UUID v4 when embedding the creation time is undesirable.
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
        let id = Id::new_v7().expect("identity generation succeeds");
        assert_eq!(Id::from_slice(id.as_bytes()).unwrap(), id);
        assert_eq!(id.to_string().parse::<Id>().unwrap(), id);
        assert!(Id::from_slice(&[0; 16]).is_err());
        assert!(Id::from_slice(&[1; 15]).is_err());
        assert_eq!(Uuid::from(id).get_version_num(), 7);
        assert_eq!(Uuid::from(id).get_variant(), uuid::Variant::RFC4122);
        assert_eq!(Uuid::from(Id::random().unwrap()).get_version_num(), 4);
    }

    #[test]
    fn v7_preserves_millisecond_order_and_random_payload() {
        let first_time = UNIX_EPOCH + std::time::Duration::from_millis(1_700_000_000_000);
        let first = Id::v7_at(first_time, &[0; 10]).unwrap();
        let same_millisecond = Id::v7_at(first_time, &[1; 10]).unwrap();
        let later = Id::v7_at(first_time + std::time::Duration::from_millis(1), &[0; 10]).unwrap();
        assert_ne!(first, same_millisecond);
        assert!(first < later);
        let uuid = Uuid::from(first);
        assert_eq!(uuid.get_timestamp().unwrap().to_unix(), (1_700_000_000, 0));
    }

    #[test]
    fn v7_rejects_unrepresentable_clocks_without_panicking() {
        let before_epoch = UNIX_EPOCH - std::time::Duration::from_millis(1);
        assert!(matches!(
            Id::v7_at(before_epoch, &[0; 10]),
            Err(Error::IdentityClock { .. })
        ));
        let overflow = UNIX_EPOCH + std::time::Duration::from_millis(1_u64 << 48);
        assert!(matches!(
            Id::v7_at(overflow, &[0; 10]),
            Err(Error::IdentityTimestamp { .. })
        ));
        let maximum = overflow - std::time::Duration::from_millis(1);
        assert!(Id::v7_at(maximum, &[0; 10]).is_ok());
    }
}
