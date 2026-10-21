use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OffsetRange {
    pub start: u64,
    pub end: u64,
}

impl OffsetRange {
    pub fn new(start: u64, end: u64) -> Result<Self> {
        if start > end {
            return Err(Error::Invalid {
                reason: "offset range is reversed",
            });
        }
        Ok(Self { start, end })
    }

    pub fn from_count(start: u64, count: u64) -> Result<Self> {
        let end = start.checked_add(count).ok_or(Error::Overflow {
            name: "offset range",
        })?;
        Self::new(start, end)
    }

    pub fn contains(&self, offset: u64) -> bool {
        self.start <= offset && offset < self.end
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataBoundaries {
    pub committed_end: u64,
    pub object_published_end: u64,
    pub local_reclaim_end: u64,
    pub retained_floor: u64,
}

impl DataBoundaries {
    pub fn validate(&self) -> Result<()> {
        for (valid, reason) in [
            (
                self.local_reclaim_end <= self.object_published_end,
                "local_reclaim_end must not exceed object_published_end",
            ),
            (
                self.object_published_end <= self.committed_end,
                "object_published_end must not exceed committed_end",
            ),
            (
                self.retained_floor <= self.committed_end,
                "retained_floor must not exceed committed_end",
            ),
        ] {
            if !valid {
                return Err(Error::Invalid { reason });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusive_ranges_and_overflow_are_checked() {
        let range = OffsetRange::from_count(5, 2).unwrap();
        assert!(range.contains(5));
        assert!(!range.contains(7));
        assert!(OffsetRange::from_count(u64::MAX, 1).is_err());
        assert!(OffsetRange::new(5, 4).is_err());
    }

    #[test]
    fn publication_and_reclamation_cannot_pass_commit() {
        let mut bounds = DataBoundaries {
            committed_end: 8,
            object_published_end: 6,
            local_reclaim_end: 6,
            retained_floor: 2,
        };
        bounds.validate().unwrap();
        bounds.local_reclaim_end = 7;
        assert!(bounds.validate().is_err());
    }
}
