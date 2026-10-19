use std::fmt;

use openraft::raft::AppendEntriesRequest;
use openraft::{BasicNode, Entry, LogId, SnapshotMeta, Vote};
use serde::de::{DeserializeOwned, Error as _, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::replication::{Application, TypeConfig};

use super::{Error, MAX_APPEND_ENTRIES, MAX_SNAPSHOT_CHUNK_BYTES, Result};

#[derive(Deserialize)]
pub(super) struct Snapshot {
    pub vote: Vote<u64>,
    pub meta: SnapshotMeta<u64, BasicNode>,
    pub offset: u64,
    #[serde(deserialize_with = "bounded_chunk")]
    pub data: Vec<u8>,
    pub done: bool,
}

impl Snapshot {
    pub fn into_request<A: Application>(
        self,
    ) -> openraft::raft::InstallSnapshotRequest<TypeConfig<A>> {
        openraft::raft::InstallSnapshotRequest {
            vote: self.vote,
            meta: self.meta,
            offset: self.offset,
            data: self.data,
            done: self.done,
        }
    }
}

fn bounded_chunk<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<u8>, D::Error> {
    struct Chunk;
    impl<'de> Visitor<'de> for Chunk {
        type Value = Vec<u8>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded snapshot chunk")
        }
        fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> std::result::Result<Vec<u8>, E> {
            if bytes.len() > MAX_SNAPSHOT_CHUNK_BYTES {
                return Err(E::custom("snapshot chunk exceeds the wire limit"));
            }
            let mut result = Vec::new();
            result.try_reserve_exact(bytes.len()).map_err(E::custom)?;
            result.extend_from_slice(bytes);
            Ok(result)
        }
    }
    deserializer.deserialize_bytes(Chunk)
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub(super) struct Append<A: Application> {
    pub vote: Vote<u64>,
    pub prev_log_id: Option<LogId<u64>>,
    #[serde(deserialize_with = "bounded_entries")]
    pub entries: Vec<Entry<TypeConfig<A>>>,
    pub leader_commit: Option<LogId<u64>>,
}

impl<A: Application> From<AppendEntriesRequest<TypeConfig<A>>> for Append<A> {
    fn from(value: AppendEntriesRequest<TypeConfig<A>>) -> Self {
        Self {
            vote: value.vote,
            prev_log_id: value.prev_log_id,
            entries: value.entries,
            leader_commit: value.leader_commit,
        }
    }
}

impl<A: Application> From<Append<A>> for AppendEntriesRequest<TypeConfig<A>> {
    fn from(value: Append<A>) -> Self {
        Self {
            vote: value.vote,
            prev_log_id: value.prev_log_id,
            entries: value.entries,
            leader_commit: value.leader_commit,
        }
    }
}

fn bounded_entries<'de, D, T>(deserializer: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Entries<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> Visitor<'de> for Entries<T> {
        type Value = Vec<T>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded Raft entry sequence")
        }
        fn visit_seq<S: SeqAccess<'de>>(
            self,
            mut sequence: S,
        ) -> std::result::Result<Vec<T>, S::Error> {
            let hint = sequence.size_hint().unwrap_or_default();
            if hint > MAX_APPEND_ENTRIES {
                return Err(S::Error::custom("Raft entry count exceeds the wire limit"));
            }
            let mut entries = Vec::new();
            entries.try_reserve_exact(hint).map_err(S::Error::custom)?;
            while entries.len() < MAX_APPEND_ENTRIES {
                let Some(entry) = sequence.next_element()? else {
                    return Ok(entries);
                };
                entries.try_reserve(1).map_err(S::Error::custom)?;
                entries.push(entry);
            }
            if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(S::Error::custom("Raft entry count exceeds the wire limit"));
            }
            Ok(entries)
        }
    }
    deserializer.deserialize_seq(Entries(std::marker::PhantomData))
}

pub(super) fn encode<T: Serialize>(value: &T, limit: usize) -> Result<bytes::Bytes> {
    crate::wal::encode_value(value, limit)
        .map(bytes::Bytes::from)
        .map_err(|source| Error::Codec { source })
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    crate::wal::decode_value(bytes).map_err(|source| Error::Codec { source })
}

pub(super) fn validate_append<A: Application>(rpc: &Append<A>) -> Result<()> {
    if rpc.entries.len() > MAX_APPEND_ENTRIES {
        return Err(Error::Invalid {
            reason: "Raft entry count exceeds the wire limit",
        });
    }
    let mut previous = rpc.prev_log_id;
    for entry in &rpc.entries {
        let expected = previous.map_or(Some(0), |log| log.index.checked_add(1));
        if Some(entry.log_id.index) != expected
            || entry.log_id.leader_id.term > rpc.vote.leader_id.term
            || previous.is_some_and(|log| entry.log_id < log)
        {
            return Err(Error::Invalid {
                reason: "Raft append log positions are inconsistent",
            });
        }
        previous = Some(entry.log_id);
    }
    if rpc
        .prev_log_id
        .is_some_and(|log| log.leader_id.term > rpc.vote.leader_id.term)
    {
        return Err(Error::Invalid {
            reason: "Raft previous log exceeds the sender term",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct Limited {
        #[serde(deserialize_with = "bounded_entries")]
        values: Vec<u64>,
    }

    #[derive(Deserialize)]
    struct Chunk {
        #[serde(deserialize_with = "bounded_chunk")]
        bytes: Vec<u8>,
    }

    #[test]
    fn snapshot_chunk_is_bounded_before_copying() {
        let valid = encode(
            &vec![7_u8; MAX_SNAPSHOT_CHUNK_BYTES],
            MAX_SNAPSHOT_CHUNK_BYTES + 16,
        )
        .unwrap();
        assert_eq!(
            decode::<Chunk>(&valid).unwrap().bytes.len(),
            MAX_SNAPSHOT_CHUNK_BYTES
        );
        let excessive = encode(
            &vec![7_u8; MAX_SNAPSHOT_CHUNK_BYTES + 1],
            MAX_SNAPSHOT_CHUNK_BYTES + 16,
        )
        .unwrap();
        assert!(decode::<Chunk>(&excessive).is_err());
    }

    #[test]
    fn sequence_budget_is_checked_before_element_allocation() {
        let valid = encode(&vec![7_u64; MAX_APPEND_ENTRIES], 4096).unwrap();
        let decoded: Limited = decode(&valid).unwrap();
        assert_eq!(decoded.values.len(), MAX_APPEND_ENTRIES);
        let excessive = encode(&vec![7_u64; MAX_APPEND_ENTRIES + 1], 4096).unwrap();
        assert!(decode::<Limited>(&excessive).is_err());
        let mut trailing = valid.to_vec();
        trailing.push(0);
        assert!(decode::<Limited>(&trailing).is_err());
        assert!(encode(&vec![7_u64; 100], 2).is_err());
    }
}
