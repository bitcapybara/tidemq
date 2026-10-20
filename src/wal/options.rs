use super::codec::HEADER_BYTES;
use super::{Error, Result};
use serde::Serialize;
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
pub struct JournalOptions {
    pub max_record_bytes: usize,
    pub max_file_bytes: u64,
    pub max_pending_bytes: u32,
    pub max_pending_records: usize,
    pub group_commit_bytes: usize,
    pub group_commit_delay: Duration,
    pub segment_target_bytes: u64,
    pub max_segments: usize,
    pub max_checkpoint_bytes: usize,
    pub max_retained_records: usize,
    pub control_reserve_bytes: u64,
}

impl Default for JournalOptions {
    fn default() -> Self {
        Self {
            max_record_bytes: 4 * 1024 * 1024,
            max_file_bytes: 128 * 1024 * 1024,
            max_pending_bytes: 16 * 1024 * 1024,
            max_pending_records: 64,
            group_commit_bytes: 1024 * 1024,
            group_commit_delay: Duration::from_millis(1),
            segment_target_bytes: 16 * 1024 * 1024,
            max_segments: 64,
            max_checkpoint_bytes: 8 * 1024 * 1024,
            max_retained_records: 65_536,
            control_reserve_bytes: 32 * 1024 * 1024,
        }
    }
}

impl JournalOptions {
    pub(super) fn validate_basic(&self) -> Result<()> {
        let record_bytes = u64::try_from(self.max_record_bytes).map_err(|_| Error::Limit {
            name: "record length",
        })?;
        if record_bytes == 0
            || record_bytes.saturating_add(HEADER_BYTES) > self.max_file_bytes
            || record_bytes > u64::from(self.max_pending_bytes)
            || self.max_pending_records == 0
            || self.max_pending_records > Semaphore::MAX_PERMITS
            || self.group_commit_bytes == 0
            || self.group_commit_delay > Duration::from_secs(1)
        {
            return Err(Error::Configuration {
                reason: "journal limits must be nonzero, consistent, and bounded",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct JournalStatistics {
    pub written_records: u64,
    pub written_bytes: u64,
    pub sync_operations: u64,
    pub checkpoint_rewrites: u64,
    pub available_pending_bytes: usize,
    pub available_pending_records: usize,
    pub current_file_bytes: u64,
    pub active_bytes: u64,
    pub sealed_bytes: u64,
    pub checkpoint_bytes: u64,
    pub staging_bytes: u64,
    pub retired_bytes: u64,
    pub segment_count: usize,
    pub retained_records: usize,
    pub index_bytes: u64,
    pub rotations: u64,
    pub reclaimed_bytes: u64,
    pub gc_failures: u64,
    pub relocation_operations: u64,
    pub relocated_bytes: u64,
}
