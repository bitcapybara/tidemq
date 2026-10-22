//! Stable key hashing and half-open ranges shared by routing and storage.

use serde::{Deserialize, Serialize};

use crate::{Error, Id, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HashRange {
    pub start: u64,
    /// None is the exclusive end 2^64, not u64::MAX.
    pub end: Option<u64>,
}

impl HashRange {
    pub const FULL: Self = Self {
        start: 0,
        end: None,
    };

    pub fn validate(self) -> Result<()> {
        if self.end.is_some_and(|end| end <= self.start) {
            return Err(Error::Invalid {
                reason: "hash range must be nonempty",
            });
        }
        Ok(())
    }

    pub fn contains(self, hash: u64) -> bool {
        hash >= self.start && self.end.is_none_or(|end| hash < end)
    }

    pub fn split(self, at: u64) -> Result<(Self, Self)> {
        self.validate()?;
        if at <= self.start || !self.contains(at) {
            return Err(Error::Invalid {
                reason: "split point must be inside the hash range",
            });
        }
        Ok((
            Self {
                start: self.start,
                end: Some(at),
            },
            Self {
                start: at,
                end: self.end,
            },
        ))
    }

    pub fn merge(self, right: Self) -> Result<Self> {
        self.validate()?;
        right.validate()?;
        if self.end != Some(right.start) {
            return Err(Error::Invalid {
                reason: "merged ranges must be adjacent and ordered",
            });
        }
        Ok(Self {
            start: self.start,
            end: right.end,
        })
    }
}

/// The digest distinguishes an absent key from an explicitly empty key.
pub fn routing_hash(key: Option<&[u8]>, producer: Id, sequence: u64) -> Result<u64> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tidemq/key-range/v1\0");
    match key {
        Some(key) => {
            hash.update(&[1]);
            hash.update(key);
        }
        None => {
            hash.update(&[0]);
            hash.update(producer.as_bytes());
            hash.update(&sequence.to_be_bytes());
        }
    }
    let digest = hash.finalize();
    let bytes = digest.as_bytes().get(..8).ok_or(Error::Invalid {
        reason: "routing digest is incomplete",
    })?;
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
        Error::Invalid {
            reason: "routing digest width is invalid",
        }
    })?))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SubscriptionKind {
    Stream,
    Queue,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum InitialPosition {
    #[default]
    EarliestAvailable,
    Latest,
}

/// A birth identity and position never change when authority moves to another group.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MessagePosition {
    pub topic: Id,
    pub shard: Id,
    pub offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardRoute {
    pub id: Id,
    pub range: HashRange,
    pub group: u64,
    pub authority_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopicLayout {
    pub topic: Id,
    pub revision: u64,
    pub shards: Vec<ShardRoute>,
}

impl TopicLayout {
    pub fn validate(&self, maximum: usize) -> Result<()> {
        if self.revision == 0 || self.shards.is_empty() || self.shards.len() > maximum {
            return Err(Error::Invalid {
                reason: "layout revision or shard count is invalid",
            });
        }
        let mut next = Some(0);
        let mut identities = std::collections::BTreeSet::new();
        for shard in &self.shards {
            shard.range.validate()?;
            if next != Some(shard.range.start)
                || shard.group == 0
                || shard.authority_epoch == 0
                || !identities.insert(shard.id)
            {
                return Err(Error::Invalid {
                    reason: "layout has a gap, overlap or invalid shard identity",
                });
            }
            next = shard.range.end;
        }
        if next.is_some() {
            return Err(Error::Invalid {
                reason: "layout does not cover the complete hash space",
            });
        }
        Ok(())
    }

    pub fn route(&self, hash: u64) -> Result<&ShardRoute> {
        let index = self
            .shards
            .partition_point(|shard| shard.range.start <= hash);
        index
            .checked_sub(1)
            .and_then(|index| self.shards.get(index))
            .filter(|shard| shard.range.contains(hash))
            .ok_or(Error::Invalid {
                reason: "layout does not contain the requested hash",
            })
    }
}
