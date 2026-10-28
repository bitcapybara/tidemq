use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;

use super::SnapshotFile;
pub use crate::execution::{Application, GroupIdentity, StorageIdentity};
use openraft::{BasicNode, RaftTypeConfig};

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
