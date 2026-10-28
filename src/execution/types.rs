use std::fmt;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tidemq_protocol::Id;

/// Complete authoritative state, with deterministic commands and bounded business state.
///
/// Commands include their clock, entropy and external inputs; apply performs no IO. Business
/// rejection belongs in Response. Local failures belong in try_apply and stop authoritative
/// service. State and commands must support postcard's non-self-describing serde model.
/// The application bounds deduplication and retirement fences; validate rejects decoded state
/// without changing external state. Dynamic values and untagged or internally tagged enums
/// are not supported by this storage format.
pub trait Application:
    Clone + fmt::Debug + Default + Serialize + DeserializeOwned + Send + Sync + 'static
{
    const NAME: &'static str;
    const VERSION: u32;
    type Command: Clone + fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static;
    type Response: Clone
        + fmt::Debug
        + Default
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static;

    fn apply(&mut self, command: Self::Command) -> Self::Response;
    fn try_apply(&mut self, command: Self::Command) -> std::io::Result<Self::Response> {
        Ok(self.apply(command))
    }
    fn validate(&self) -> std::io::Result<()>;
}

/// Application snapshots belong to exactly one group.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupIdentity {
    pub cluster_id: Id,
    pub group_id: u64,
    pub application: String,
    pub application_version: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageIdentity {
    pub group: GroupIdentity,
    pub node_id: u64,
}

impl StorageIdentity {
    pub fn new<A: Application>(cluster_id: Id, group_id: u64, node_id: u64) -> Self {
        Self {
            group: GroupIdentity {
                cluster_id,
                group_id,
                application: A::NAME.to_owned(),
                application_version: A::VERSION,
            },
            node_id,
        }
    }
}

/// Compare positions only after establishing the same group and execution kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExecutionPosition {
    Local {
        sequence: u64,
    },
    Raft {
        term: u64,
        leader_node: u64,
        index: u64,
    },
}

#[derive(Clone, Debug)]
pub struct Applied<R> {
    pub position: ExecutionPosition,
    pub response: R,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotInfo {
    pub position: ExecutionPosition,
    pub snapshot_id: String,
}
