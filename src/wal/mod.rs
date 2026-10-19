//! Bounded, checksummed journal with serialized writes and durable group commit.

mod codec;
mod error;
mod journal;

pub const FRAME_HEADER_BYTES: usize = 88;

pub use codec::{Inspection, inspect};
pub use error::{Error, Result};
pub use journal::{Durability, Journal, JournalOptions, JournalStatistics};

pub(crate) use codec::{decode_value, encode_value, sync_directory, write_atomic};
pub(crate) use codec::{frame as codec_frame, unframe};
