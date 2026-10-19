use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;

use openraft::{BasicNode, RaftTypeConfig};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tidemq_protocol::Id;

use super::SnapshotFile;

/// An application's complete authoritative state, not a list of previously executed commands.
///
/// Commands must be deterministic: no clocks, entropy, network, or IO in `apply`. Include those
/// inputs in the replicated command instead. Business rejection belongs in `Response`. The
/// application must enforce finite state budgets, including deduplication and retirement fences,
/// and `validate` must reject invalid decoded snapshots without changing external state.
/// State and commands must support postcard's non-self-describing serde model; dynamic values,
/// untagged enums, and internally tagged enums are not supported by this storage format.
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
    fn validate(&self) -> std::io::Result<()>;
}

/// A snapshot is portable between replicas in the same group, never between groups.
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

/// Generic type configuration without imposing ordering or `Copy` on application state.
pub struct TypeConfig<A: Application>(PhantomData<fn() -> A>);

impl<A: Application> fmt::Debug for TypeConfig<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("TypeConfig").field(&A::NAME).finish()
    }
}
impl<A: Application> Clone for TypeConfig<A> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<A: Application> Copy for TypeConfig<A> {}
impl<A: Application> Default for TypeConfig<A> {
    fn default() -> Self {
        Self(PhantomData)
    }
}
impl<A: Application> PartialEq for TypeConfig<A> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl<A: Application> Eq for TypeConfig<A> {}
impl<A: Application> PartialOrd for TypeConfig<A> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<A: Application> Ord for TypeConfig<A> {
    fn cmp(&self, _: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl<A: Application> RaftTypeConfig for TypeConfig<A> {
    type D = A::Command;
    type R = A::Response;
    type NodeId = u64;
    type Node = BasicNode;
    type Entry = openraft::Entry<Self>;
    type SnapshotData = SnapshotFile;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}
