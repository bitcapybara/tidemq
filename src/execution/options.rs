use std::sync::Arc;

use crate::observability::metrics::OperationMetrics;
use crate::wal::JournalOptions;

use super::{Error, Result};

// Includes the local envelope, WAL envelope and physical frame at maximum sequence width.
pub(super) const RECORD_RESERVE: usize = 160;
pub(super) const SEGMENT_HEADER: u64 = 157;

#[derive(Clone)]
pub struct LocalOptions {
    pub journal: JournalOptions,
    pub max_snapshot_bytes: usize,
    pub max_snapshot_files: usize,
    pub max_snapshot_readers: usize,
    pub metrics: Option<Arc<OperationMetrics>>,
}

impl Default for LocalOptions {
    fn default() -> Self {
        Self {
            journal: JournalOptions::default(),
            max_snapshot_bytes: 64 * 1024 * 1024,
            max_snapshot_files: 4,
            max_snapshot_readers: 8,
            metrics: None,
        }
    }
}

impl LocalOptions {
    pub fn validate(&self) -> Result<()> {
        self.journal.validate_segmented()?;
        if self.max_snapshot_bytes == 0
            || self.max_snapshot_bytes > u32::MAX as usize
            || !(2..=32).contains(&self.max_snapshot_files)
            || !(1..=64).contains(&self.max_snapshot_readers)
            || self.journal.max_pending_records < 2
            || self.journal.max_segments < 4
            || self.journal.max_retained_records < 2
            || self.journal.max_pending_bytes as usize > tokio::sync::Semaphore::MAX_PERMITS
            || self.journal.max_record_bytes <= RECORD_RESERVE
            || self.batch_bytes() < self.journal.max_record_bytes as u64 + RECORD_RESERVE as u64
        {
            return Err(Error::Configuration {
                reason: "Local execution requires bounded snapshot, completion and batch reserves",
            });
        }
        self.file_limit_bytes()?;
        Ok(())
    }

    /// Includes all snapshot generations, the staging generation, WAL and identity metadata.
    pub fn file_limit_bytes(&self) -> Result<u64> {
        (self.max_snapshot_bytes as u64)
            .checked_add(crate::wal::FRAME_HEADER_BYTES as u64)
            .and_then(|n| n.checked_mul(self.max_snapshot_files as u64))
            .and_then(|n| n.checked_add(self.journal.max_file_bytes))
            .and_then(|n| n.checked_add(16 * 1024))
            .ok_or(Error::Configuration {
                reason: "Local file budget overflow",
            })
    }

    pub(super) fn batch_bytes(&self) -> u64 {
        let journal = &self.journal;
        (journal.max_pending_bytes as u64)
            .min(journal.segment_target_bytes.saturating_sub(SEGMENT_HEADER))
            .min(
                journal
                    .max_file_bytes
                    .saturating_sub(journal.control_reserve_bytes)
                    / 4,
            )
    }

    pub(super) fn batch_records(&self) -> usize {
        self.journal
            .max_pending_records
            .min((self.journal.max_retained_records / 4).max(1))
    }
}
