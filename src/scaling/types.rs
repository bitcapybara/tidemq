use std::collections::BTreeSet;

use openraft::LogId;
use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use crate::metadata::{DataGroup, Rejection};

/// One replacement identity survives retries; generation prevents retired plans returning.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MembershipPlan {
    pub(crate) cluster_id: Id,
    pub(crate) group_id: u64,
    pub(crate) generation: u64,
    pub(crate) previous: BTreeSet<u64>,
    pub(crate) target: BTreeSet<u64>,
    pub(crate) operation_id: Id,
}

impl MembershipPlan {
    pub fn new(
        cluster_id: Id,
        group: &DataGroup,
        target: BTreeSet<u64>,
    ) -> Result<Self, Rejection> {
        let mut plan = Self {
            cluster_id,
            group_id: group.group_id,
            generation: group.generation,
            previous: group
                .placement
                .raft_voters()
                .ok_or(Rejection::Unsupported)?
                .clone(),
            target,
            operation_id: cluster_id,
        };
        plan.operation_id = plan.derived_id(false)?;
        plan.validate()?;
        Ok(plan)
    }

    pub fn operation_id(&self) -> Id {
        self.operation_id
    }
    pub fn group_id(&self) -> u64 {
        self.group_id
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn previous(&self) -> &BTreeSet<u64> {
        &self.previous
    }
    pub fn target(&self) -> &BTreeSet<u64> {
        &self.target
    }

    pub(crate) fn validate(&self) -> Result<(), Rejection> {
        if self.group_id == 0
            || self.generation == 0
            || self.generation == u64::MAX
            || !matches!(self.previous.len(), 1 | 3)
            || self.previous.len() != self.target.len()
            || self.previous.contains(&0)
            || self.target.contains(&0)
            || self.previous == self.target
            || self.previous.difference(&self.target).count() != 1
            || self.target.difference(&self.previous).count() != 1
            || self.operation_id != self.derived_id(false)?
        {
            return Err(Rejection::InvalidGroup);
        }
        Ok(())
    }

    pub(crate) fn derived_id(&self, complete: bool) -> Result<Id, Rejection> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"tidemq:membership:replace:v1");
        hash.update(self.cluster_id.as_bytes());
        hash.update(&self.group_id.to_be_bytes());
        hash.update(&self.generation.to_be_bytes());
        hash.update(&[u8::from(complete)]);
        for voters in [&self.previous, &self.target] {
            hash.update(&(voters.len() as u64).to_be_bytes());
            for voter in voters {
                hash.update(&voter.to_be_bytes());
            }
        }
        let mut bytes = [0u8; 16];
        for (destination, source) in bytes.iter_mut().zip(hash.finalize().as_bytes()) {
            *destination = *source;
        }
        if let Some(version) = bytes.get_mut(6) {
            *version = (*version & 0x0f) | 0x80;
        }
        if let Some(variant) = bytes.get_mut(8) {
            *variant = (*variant & 0x3f) | 0x80;
        }
        Id::from_slice(&bytes).map_err(|_| Rejection::InvalidCommand)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct MembershipEvidence {
    pub plan: MembershipPlan,
    pub membership_log: LogId<u64>,
    pub applied_log: LogId<u64>,
}

/// Fixed one slot per existing group, with two original stage responses.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MigrationRecord {
    pub plan: MembershipPlan,
    pub prepared_revision: u64,
    pub copy_bytes: u64,
    pub(crate) completed: Option<(u64, MembershipEvidence)>,
}

impl MigrationRecord {
    pub fn is_complete(&self) -> bool {
        self.completed.is_some()
    }
    pub fn completed_revision(&self) -> Option<u64> {
        self.completed.as_ref().map(|v| v.0)
    }
}
