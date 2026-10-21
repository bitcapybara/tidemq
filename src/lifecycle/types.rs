use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tidemq_protocol::{Id, state::OffsetRange};

use crate::{archive::Descriptor, metadata::PartitionKey, ownership::Grant};

pub(crate) const MAX_OBJECTS: usize = 4_096;
pub(crate) const MAX_PARTITIONS: usize = 64;
pub(crate) const MAX_SEGMENTS: usize = 4_096;
pub(crate) const MAX_PINS: usize = 1_024;
pub(crate) const MAX_PIN_OBJECTS: usize = 64;
pub(crate) const MAX_PIN_TTL: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapacityUsage {
    pub registered_physical_upper_bytes: u64,
    pub reclaim_lag_upper_bytes: u64,
    pub pending_upload_upper_bytes: u64,
    pub objects: usize,
    pub deleted_objects: usize,
    pub read_pins: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectPage {
    pub objects: Vec<(Id, ObjectPhase)>,
    pub next_after: Option<Id>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PinPage {
    pub pins: Vec<Id>,
    pub next_after: Option<Id>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Rejection {
    Identity,
    Capacity,
    Conflict,
    NotFound,
    InvalidRange,
    StaleGeneration,
    CancelRequested,
    Aborted,
    AlreadyPublished,
    NotPublished,
    References,
    Pinned,
    RecoveryDependency,
    CheckpointRequired,
    Membership,
    InvalidSnapshot,
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Identity => "Object identity does not match the registered descriptor",
            Self::Capacity => "Object lifecycle item or byte capacity is exhausted",
            Self::Conflict => "Lifecycle operation conflicts with the committed object state",
            Self::NotFound => "Object or lifecycle intent does not exist",
            Self::InvalidRange => "Object offsets do not match the authorized partition range",
            Self::StaleGeneration => "Object or partition generation changed",
            Self::CancelRequested => "Object cancellation was requested; publication is blocked",
            Self::Aborted => "Object operation was durably aborted",
            Self::AlreadyPublished => "Object has already been published",
            Self::NotPublished => "Object must be published before this transition",
            Self::References => "Published manifest references still protect the object",
            Self::Pinned => "Active read pins still protect the object",
            Self::RecoveryDependency => "Recovery dependencies still protect the object",
            Self::CheckpointRequired => "A durable replacement checkpoint is required",
            Self::Membership => "Replica membership does not match the lifecycle authority",
            Self::InvalidSnapshot => "Lifecycle snapshot violates its state invariants",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectPhase {
    Registered,
    Uploaded,
    Published,
    CancelRequested,
    Aborted,
    Deleting,
    Deleted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum IntentPhase {
    Authorized,
    Published,
    Aborted,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RangeRequest {
    pub partition: PartitionKey,
    pub offsets: OffsetRange,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Plan {
    pub descriptor: Descriptor,
    pub grants: Vec<Grant>,
    pub expected_generations: BTreeMap<PartitionKey, u64>,
    pub replaces: Vec<Id>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRecord {
    pub(crate) plan: Plan,
    pub phase: ObjectPhase,
    pub generation: u64,
    pub(crate) manifest_references: usize,
    pub checkpoint_generation: Option<u64>,
    pub(crate) published_generations: BTreeMap<PartitionKey, u64>,
    pub(crate) recovery_replacement: Option<Id>,
    pub(crate) floor_holds: BTreeMap<PartitionKey, u64>,
}

impl ObjectRecord {
    pub fn descriptor(&self) -> &Descriptor {
        &self.plan.descriptor
    }
    pub fn object_id(&self) -> Id {
        self.plan.descriptor.identity.object_id
    }
    pub fn recovery_replacement(&self) -> Option<Id> {
        self.recovery_replacement
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Segment {
    pub object_id: Id,
    pub offsets: OffsetRange,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub generation: u64,
    pub publication_generation: u64,
    pub published_end: u64,
    pub segments: Vec<Segment>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PinRecord {
    pub id: Id,
    pub group_id: u64,
    pub objects: Vec<Id>,
    pub ttl_millis: u64,
}

/// The guard starts before the committed pin request; no wall-clock lease is assumed.
pub struct ReadPin {
    pub(crate) record: PinRecord,
    pub(crate) expires_at: Instant,
}

impl ReadPin {
    pub fn id(&self) -> Id {
        self.record.id
    }
    pub fn expires_at(&self) -> Instant {
        self.expires_at
    }
    pub fn check(&self) -> super::Result<()> {
        if Instant::now() >= self.expires_at {
            return Err(super::Error::Deadline);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Blocker {
    PendingTask,
    CancelNeedsDataAbort,
    LiveManifest,
    ReadPin,
    RecoverySnapshot,
    Checkpoint,
    DeletionRetry,
    None,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub object_id: Id,
    pub phase: ObjectPhase,
    pub blocker: Blocker,
    pub manifest_references: usize,
    pub read_pins: usize,
    pub checkpoint_generation: Option<u64>,
    pub recovery_replacement: Option<Id>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum CatalogCommand {
    Register(Plan),
    Uploaded {
        object_id: Id,
    },
    Publish {
        object_id: Id,
    },
    RequestCancel {
        object_id: Id,
    },
    FinishCancel {
        object_id: Id,
        data_generation: u64,
    },
    Pin(PinRecord),
    ReleasePin {
        pin: PinRecord,
    },
    Checkpoint {
        object_id: Id,
        generation: u64,
        voters: Vec<u64>,
        snapshots: Vec<(u64, String, u64)>,
    },
    BeginDelete {
        object_id: Id,
    },
    Deleted {
        object_id: Id,
    },
    RetireBefore(FloorEvidence),
    FloorCheckpoint {
        proof: FloorEvidence,
        snapshots: Vec<(u64, String, u64)>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct FloorEvidence {
    pub cluster_id: Id,
    pub group_id: u64,
    pub partition: PartitionKey,
    pub before: u64,
    pub manifest_generation: u64,
    pub published_end: u64,
    pub position: openraft::LogId<u64>,
    pub voters: BTreeSet<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum CatalogOutcome {
    Object(Box<ObjectRecord>),
    Pinned,
    Released,
    Retired,
}

/// Trusted lifecycle operations are constructed by the coordinator, never decoded from clients.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataOperation {
    pub(crate) command: CatalogCommand,
}

impl MetadataOperation {
    pub(crate) fn validate_context(
        &self,
        state: &crate::metadata::MetadataState,
    ) -> Result<(), Rejection> {
        let proof = match &self.command {
            CatalogCommand::RetireBefore(proof) | CatalogCommand::FloorCheckpoint { proof, .. } => {
                proof
            }
            _ => return Ok(()),
        };
        let group = state.group(proof.group_id).ok_or(Rejection::Identity)?;
        if state.cluster_id() != Some(proof.cluster_id)
            || state.partition_group(proof.partition) != Some(proof.group_id)
            || group.voters != proof.voters
            || !proof.voters.contains(&proof.position.leader_id.node_id)
            || !matches!(proof.voters.len(), 1 | 3)
            || proof.position.index == 0
        {
            return Err(Rejection::Identity);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataOperation {
    pub(crate) command: DataCommand,
    pub(crate) grants: Vec<Grant>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataResult {
    pub(crate) outcome: CatalogOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataResult {
    pub(crate) outcome: DataOutcome,
}

/// A validated cold record from a live committed-manifest pin.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VerifiedColdDelivery {
    pub(crate) partition: PartitionKey,
    pub(crate) object_id: Id,
    pub(crate) manifest_generation: u64,
    pub(crate) offset: u64,
    pub(crate) body_bytes: u32,
}

impl VerifiedColdDelivery {
    pub fn partition(&self) -> PartitionKey {
        self.partition
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn body_bytes(&self) -> u32 {
        self.body_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum DataCommand {
    Authorize(Plan),
    Abort(Plan),
    Published {
        plan: Plan,
        generations: BTreeMap<PartitionKey, u64>,
    },
    Checkpoint {
        object_id: Id,
    },
    Reclaim {
        partition: PartitionKey,
        end: u64,
        checkpoint_generation: u64,
    },
    FloorCheckpoint {
        partition: PartitionKey,
        before: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum DataOutcome {
    Authorized,
    Aborted { generation: u64 },
    Published,
    Checkpoint { generation: u64 },
    Reclaimed,
    FloorCheckpoint,
}
