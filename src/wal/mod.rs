//! Bounded, checksummed segmented journal with serialized writes and durable group commit.

mod codec;
mod error;
mod options;
mod segmented;

pub const FRAME_HEADER_BYTES: usize = 88;

pub use error::{Error, Result};
pub use options::{JournalOptions, JournalStatistics};
pub use segmented::{
    MAX_SEGMENTED_DIRECTORY_ENTRIES, RecordId, RecoveredRecord, Recovery, SEGMENTED_MANIFEST_FILE,
    SegmentedDurability, SegmentedFileKind, SegmentedInspection, SegmentedJournal, SnapshotRef,
    inspect_segmented, segmented_file_kind,
};

pub(crate) use codec::{decode_value, encode_value, sync_directory, write_atomic};
pub(crate) use codec::{frame as codec_frame, unframe};
