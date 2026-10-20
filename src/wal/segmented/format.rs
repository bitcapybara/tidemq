use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

use super::{Error, Result};

pub const SEGMENTED_MANIFEST_FILE: &str = "manifest.bin";
pub const MAX_SEGMENTED_DIRECTORY_ENTRIES: usize = 256;
pub(super) const MAX_MANIFEST_BYTES: usize = 65_536;
pub(super) const LAYOUT_VERSION: u32 = 2;
pub(super) const SEGMENT_HEADER_PAYLOAD: usize = 68;
pub(super) const SEGMENT_HEADER_BYTES: u64 = 156;
pub(super) const ENVELOPE_BYTES: usize = 9;
pub(super) const MAX_SEGMENTS: usize = 64;
pub(super) const MAX_REFERENCES: usize = 65_536;
pub(super) const MAX_CHECKPOINT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentedFileKind {
    Manifest,
    Identity,
    Lock,
    Segment,
    Checkpoint,
    Snapshot,
    Staging,
    Incoming,
}

pub fn segmented_file_kind(name: &str) -> Option<SegmentedFileKind> {
    match name {
        SEGMENTED_MANIFEST_FILE => Some(SegmentedFileKind::Manifest),
        "identity.bin" => Some(SegmentedFileKind::Identity),
        ".writer-lock" => Some(SegmentedFileKind::Lock),
        _ => {
            let forms = [
                ("segment-", ".wal", SegmentedFileKind::Segment),
                ("checkpoint-", ".bin", SegmentedFileKind::Checkpoint),
                ("snapshot-", ".bin", SegmentedFileKind::Snapshot),
                (".replace-", "", SegmentedFileKind::Staging),
                (".incoming-", "", SegmentedFileKind::Incoming),
            ];
            forms.into_iter().find_map(|(prefix, suffix, kind)| {
                let value = name.strip_prefix(prefix)?.strip_suffix(suffix)?;
                let id = uuid::Uuid::parse_str(value).ok()?;
                (id.to_string() == value).then_some(kind)
            })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RecordId(pub(super) u64);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub file_name: String,
    pub byte_len: u64,
    pub checksum: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct FileRef {
    pub id: [u8; 16],
    pub byte_len: u64,
    pub checksum: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Segment {
    pub id: [u8; 16],
    pub first_sequence: u64,
    pub records: u64,
    pub byte_len: u64,
    pub checksum: [u8; 32],
}
impl Segment {
    pub fn name(&self) -> String {
        segment_name(self.id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Manifest {
    pub version: u32,
    pub identity: [u8; 32],
    #[serde(deserialize_with = "segments")]
    pub sealed: Vec<Segment>,
    pub active: Segment,
    pub checkpoint: Option<FileRef>,
    pub replay_sequence: u64,
    pub next_record_id: u64,
    pub snapshot: Option<SnapshotRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Location {
    pub id: RecordId,
    pub segment: [u8; 16],
    pub offset: u64,
    pub frame_bytes: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Checkpoint {
    pub version: u32,
    pub identity: [u8; 32],
    pub replay_sequence: u64,
    #[serde(deserialize_with = "control")]
    pub control: Vec<u8>,
    #[serde(deserialize_with = "references")]
    pub references: Vec<Location>,
}

fn segments<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Vec<Segment>, D::Error> {
    bounded_vec::<D, Segment, MAX_SEGMENTS>(d)
}
fn references<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Vec<Location>, D::Error> {
    bounded_vec::<D, Location, MAX_REFERENCES>(d)
}
fn control<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Vec<u8>, D::Error> {
    bounded_vec::<D, u8, MAX_CHECKPOINT_BYTES>(d)
}
fn bounded_vec<'de, D, T, const N: usize>(d: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Bounded<T, const N: usize>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for Bounded<T, N> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "at most {N} bounded values")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Vec<T>, A::Error> {
            use serde::de::Error;
            let count = seq.size_hint().unwrap_or(0);
            if count > N {
                return Err(A::Error::custom("Storage vector exceeds its bound"));
            }
            let mut values = Vec::new();
            values.try_reserve(count).map_err(A::Error::custom)?;
            while let Some(value) = seq.next_element()? {
                if values.len() >= N {
                    return Err(A::Error::custom("Storage vector exceeds its bound"));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    d.deserialize_seq(Bounded::<T, N>(std::marker::PhantomData))
}

pub(super) fn new_id() -> Result<[u8; 16]> {
    tidemq_protocol::Id::random()
        .map(|id| *uuid::Uuid::from(id).as_bytes())
        .map_err(|e| Error::Writer {
            reason: e.to_string(),
        })
}
pub(super) fn segment_name(id: [u8; 16]) -> String {
    format!("segment-{}.wal", uuid::Uuid::from_bytes(id))
}
pub(super) fn checkpoint_name(id: [u8; 16]) -> String {
    format!("checkpoint-{}.bin", uuid::Uuid::from_bytes(id))
}
pub(super) fn corrupt(reason: &'static str) -> Error {
    Error::Corrupt { offset: 0, reason }
}
