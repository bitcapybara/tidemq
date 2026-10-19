use openraft::LogId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tidemq_protocol::{Id, common::TrafficClass};

use crate::metadata::{DataGroup, DeploymentMode, NodeSpec};
use crate::scaling::{
    Capacity, Error, MembershipEvidence, MembershipPlan, MigrationRecord, Result,
};

pub const MAX_WIRE_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Query {
    Capacity {
        group: u64,
        node: u64,
    },
    View {
        plan: MembershipPlan,
    },
    Estimate {
        plan: MembershipPlan,
    },
    Prepare {
        plan: MembershipPlan,
        copy_bytes: u64,
    },
    Change {
        plan: MembershipPlan,
    },
    ObserveStable {
        plan: MembershipPlan,
    },
    Complete {
        plan: MembershipPlan,
    },
    Finish {
        plan: MembershipPlan,
    },
    Group {
        group: u64,
    },
}
impl Query {
    pub fn group_id(&self, metadata_group: u64) -> u64 {
        match self {
            Self::View { .. }
            | Self::Prepare { .. }
            | Self::Complete { .. }
            | Self::Group { .. } => metadata_group,
            Self::Capacity { group, .. } => *group,
            Self::Estimate { plan }
            | Self::Change { plan }
            | Self::ObserveStable { plan }
            | Self::Finish { plan } => plan.group_id,
        }
    }
    pub fn traffic_class(&self) -> TrafficClass {
        TrafficClass::Control
    }
    pub(super) fn plan(&self) -> Option<&MembershipPlan> {
        match self {
            Self::Capacity { .. } | Self::Group { .. } => None,
            Self::View { plan }
            | Self::Estimate { plan }
            | Self::Prepare { plan, .. }
            | Self::Change { plan }
            | Self::ObserveStable { plan }
            | Self::Complete { plan }
            | Self::Finish { plan } => Some(plan),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Reply {
    Capacity(CapacityReport),
    CapacityUnavailable,
    View(Box<View>),
    Estimate {
        copy_bytes: u64,
    },
    Stable(Box<Observation>),
    Done,
    Group {
        group: DataGroup,
        migration: Option<Box<MigrationRecord>>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CapacityReport {
    pub(super) node: u64,
    pub(super) age_ms: u64,
    pub(super) free_bytes: u64,
    pub(super) reserved_bytes: u64,
    pub(super) cpu_per_mille: u16,
    pub(super) bytes_per_second: u64,
    pub(super) connections: u32,
    pub(super) ack_bytes: u64,
}
impl CapacityReport {
    pub(super) fn measured(self, node: u64, elapsed: Duration) -> Result<Capacity> {
        let age = Duration::from_millis(self.age_ms).saturating_add(elapsed);
        if self.node != node || age > Duration::from_secs(5) {
            return Err(Error::Placement);
        }
        Ok(Capacity {
            node_id: node,
            observed_at: std::time::Instant::now()
                .checked_sub(age)
                .ok_or(Error::Placement)?,
            free_bytes: self.free_bytes,
            reserved_bytes: self.reserved_bytes,
            bytes_per_second: self.bytes_per_second,
            cpu_per_mille: self.cpu_per_mille,
            connections: self.connections,
            ack_bytes: self.ack_bytes,
        })
    }

    pub(super) fn from_sample(report: Capacity) -> Result<Self> {
        let now = std::time::Instant::now();
        if report.observed_at > now {
            return Err(Error::Placement);
        }
        let age = now.duration_since(report.observed_at);
        if age > Duration::from_secs(5) {
            return Err(Error::Placement);
        }
        Ok(Self {
            node: report.node_id,
            age_ms: age.as_millis() as u64,
            free_bytes: report.free_bytes,
            reserved_bytes: report.reserved_bytes,
            cpu_per_mille: report.cpu_per_mille,
            bytes_per_second: report.bytes_per_second,
            connections: report.connections,
            ack_bytes: report.ack_bytes,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct View {
    pub(super) cluster: Id,
    pub(super) source: u64,
    pub(super) metadata_group: u64,
    pub(super) mode: DeploymentMode,
    pub(super) metadata_nodes: BTreeMap<u64, NodeSpec>,
    pub(super) metadata_voters: BTreeSet<u64>,
    pub(super) applied: LogId<u64>,
    pub(super) revision: u64,
    pub(super) group: DataGroup,
    pub(super) migration: Option<MigrationRecord>,
    pub(super) nodes: BTreeMap<u64, NodeSpec>,
    pub(super) reserved_copy_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Observation {
    pub(super) source: u64,
    pub(super) voters: BTreeSet<u64>,
    pub(super) evidence: MembershipEvidence,
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let result = postcard::to_stdvec(value).map_err(|_| Error::Configuration)?;
    if result.len() > MAX_WIRE_BYTES {
        return Err(Error::Overloaded);
    }
    Ok(result)
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > MAX_WIRE_BYTES {
        return Err(Error::Overloaded);
    }
    let (value, tail) = postcard::take_from_bytes(bytes).map_err(|_| Error::Configuration)?;
    if !tail.is_empty() {
        return Err(Error::Configuration);
    }
    Ok(value)
}
pub fn encode_query(query: &Query) -> Result<Vec<u8>> {
    encode(query)
}
pub fn decode_query(bytes: &[u8]) -> Result<Query> {
    decode(bytes)
}
pub fn encode_reply(reply: &Reply) -> Result<Vec<u8>> {
    encode(reply)
}
pub fn decode_reply(bytes: &[u8]) -> Result<Reply> {
    decode(bytes)
}
