use std::{fmt, sync::Arc};

#[derive(Clone, Debug, Default)]
pub struct CloseReport {
    pub(crate) local_cleanup_complete: bool,
    pub(crate) members_left: usize,
    pub(crate) unresolved_sends: usize,
    pub(crate) unresolved_acks: usize,
    pub(crate) unresolved_reads: usize,
    pub(crate) unresolved_deletions: usize,
    pub(crate) unresolved_administrations: usize,
    pub(crate) unclaimed_results: usize,
}

impl CloseReport {
    pub(crate) fn merge(&mut self, other: &Self) {
        self.local_cleanup_complete &= other.local_cleanup_complete;
        self.members_left = self.members_left.saturating_add(other.members_left);
        self.unresolved_sends = self.unresolved_sends.saturating_add(other.unresolved_sends);
        self.unresolved_acks = self.unresolved_acks.saturating_add(other.unresolved_acks);
        self.unresolved_reads = self.unresolved_reads.saturating_add(other.unresolved_reads);
        self.unresolved_deletions = self
            .unresolved_deletions
            .saturating_add(other.unresolved_deletions);
        self.unresolved_administrations = self
            .unresolved_administrations
            .saturating_add(other.unresolved_administrations);
        self.unclaimed_results = self
            .unclaimed_results
            .saturating_add(other.unclaimed_results);
    }
    pub fn local_cleanup_complete(&self) -> bool {
        self.local_cleanup_complete
    }
    pub fn members_left(&self) -> usize {
        self.members_left
    }
    pub fn unresolved_sends(&self) -> usize {
        self.unresolved_sends
    }
    pub fn unresolved_acks(&self) -> usize {
        self.unresolved_acks
    }
    pub fn unresolved_reads(&self) -> usize {
        self.unresolved_reads
    }
    pub fn unresolved_deletions(&self) -> usize {
        self.unresolved_deletions
    }
    pub fn unresolved_administrations(&self) -> usize {
        self.unresolved_administrations
    }
    pub fn unclaimed_results(&self) -> usize {
        self.unclaimed_results
    }
}

#[derive(Clone, Debug)]
pub struct CloseError {
    pub(crate) report: CloseReport,
    pub(crate) source: Arc<crate::Error>,
}

impl CloseError {
    pub fn report(&self) -> &CloseReport {
        &self.report
    }
}
impl fmt::Display for CloseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SDK cleanup did not finish completely")
    }
}
impl std::error::Error for CloseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

pub(crate) type CloseResult = Result<CloseReport, CloseError>;
