use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Instant,
};

use openraft::LogId;
use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use crate::{
    metadata::{
        Action, Command, DataGroup, DeploymentMode, MetadataState, NodeRecord, NodeRole, Outcome,
        PartitionKey, Response,
    },
    ownership::{ControlLease, ControlPolicy, FenceRecord, Grant, MetadataAuthority},
    replication::{DiskStateMachine, Raft},
};

use super::{
    Error, ObjectRecord, RangeRequest, Rejection, Result,
    coordinator::stable_voters,
    types::{CatalogCommand, CatalogOutcome, MetadataOperation, PinRecord},
};

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum Query {
    Health,
    Capacity,
    Objects { after: Option<Id>, limit: usize },
    Pins { after: Option<Id>, limit: usize },
    DataVoters,
    Object(Id),
    Diagnose(Id),
    Prepare(Vec<RangeRequest>),
    Reclaim { partition: PartitionKey, end: u64 },
    Read { partition: PartitionKey, first: u64 },
    Pin(Id),
    Compaction(Id),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum View {
    Health,
    Capacity(super::CapacityUsage),
    Objects(super::ObjectPage),
    Pins(super::PinPage),
    DataVoters(DataGroup),
    Object(Box<ObjectRecord>),
    Diagnostic(super::Diagnostic),
    Prepared {
        grants: Vec<Grant>,
        generations: BTreeMap<PartitionKey, u64>,
    },
    Reclaim {
        generation: u64,
    },
    Read {
        object: Box<ObjectRecord>,
        generation: u64,
    },
    Pin(Option<PinRecord>),
    Compaction(Option<Vec<RangeRequest>>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct QuorumView {
    pub cluster_id: Id,
    pub group_id: u64,
    pub node_id: u64,
    pub revision: u64,
    pub mode: DeploymentMode,
    pub voters: Vec<u64>,
    pub nodes: Vec<NodeRecord>,
    pub position: LogId<u64>,
    pub view: View,
}

pub(crate) trait MetadataBackend: Send + Sync {
    fn query(&self, data_group: u64, query: Query, deadline: Instant) -> BoxFuture<'_, QuorumView>;
    fn execute(&self, command: CatalogCommand, deadline: Instant) -> BoxFuture<'_, CatalogOutcome>;
    fn lease(&self, fence: FenceRecord, deadline: Instant) -> BoxFuture<'_, ControlLease>;
}

#[derive(Clone)]
pub struct MetadataReplica {
    pub(crate) backend: Arc<dyn MetadataBackend>,
    pub(crate) cluster_id: Id,
    pub(crate) group_id: u64,
}

pub(crate) struct LocalMetadata {
    pub(crate) raft: Raft<MetadataState>,
    pub(crate) state: DiskStateMachine<MetadataState>,
}

impl MetadataReplica {
    /// The data-group selector is fixed when the coordinator is constructed.
    pub fn local(raft: Raft<MetadataState>, state: DiskStateMachine<MetadataState>) -> Self {
        let cluster_id = state.0.identity.group.cluster_id;
        let group_id = state.0.identity.group.group_id;
        Self {
            backend: Arc::new(LocalMetadata { raft, state }),
            cluster_id,
            group_id,
        }
    }
}

impl LocalMetadata {
    pub(crate) async fn read(
        &self,
        data_group: u64,
        query: Query,
        deadline: Instant,
    ) -> Result<QuorumView> {
        let until = deadline.min(Instant::now() + ControlPolicy::default().ttl);
        tokio::time::timeout_at(until.into(), async {
            self.raft.ensure_linearizable().await.map_err(consensus)?;
            let voters = stable_voters(&self.raft)?;
            self.state
                .try_with_applied(|state, position| {
                    let cluster_id = self.state.0.identity.group.cluster_id;
                    let group_id = self.state.0.identity.group.group_id;
                    if state.cluster_id() != Some(cluster_id)
                        || state.metadata_group_id() != Some(group_id)
                        || state
                            .mode
                            .is_none_or(|mode| mode.replication_factor() != voters.len())
                    {
                        return Err(Error::Rejected {
                            reason: Rejection::Membership,
                        });
                    }
                    let mut nodes = Vec::new();
                    let mut zones = BTreeSet::new();
                    for voter in &voters {
                        let node = state
                            .node(*voter)
                            .filter(|node| node.spec.roles.contains(&NodeRole::Metadata))
                            .ok_or(Error::Rejected {
                                reason: Rejection::Membership,
                            })?;
                        zones.insert(&node.spec.zone);
                        nodes.push(node.clone());
                    }
                    if voters.len() == 3 && zones.len() != 3 {
                        return Err(Error::Rejected {
                            reason: Rejection::Membership,
                        });
                    }
                    Ok(QuorumView {
                        cluster_id,
                        group_id,
                        node_id: self.state.0.identity.node_id,
                        revision: state.revision(),
                        mode: state.mode.ok_or(Error::Configuration)?,
                        voters: voters.into_iter().collect(),
                        nodes,
                        position: position.ok_or(Error::Configuration)?,
                        view: project(state, data_group, query)?,
                    })
                })
                .await
                .map_err(|source| Error::Storage { source })?
        })
        .await
        .map_err(|_| Error::Deadline)?
    }
}

impl MetadataBackend for LocalMetadata {
    fn query(&self, data_group: u64, query: Query, deadline: Instant) -> BoxFuture<'_, QuorumView> {
        Box::pin(self.read(data_group, query, deadline))
    }
    fn execute(&self, command: CatalogCommand, deadline: Instant) -> BoxFuture<'_, CatalogOutcome> {
        Box::pin(async move {
            for _ in 0..3 {
                let started = Instant::now();
                let revision = self.read(0, Query::Health, deadline).await?.revision;
                let operation_id = Id::new_v7().map_err(|source| Error::Identity { source })?;
                self.state
                    .try_with_application(|state| {
                        MetadataOperation {
                            command: command.clone(),
                        }
                        .validate_context(state)
                    })
                    .await
                    .map_err(|source| Error::Storage { source })?
                    .map_err(|reason| Error::Rejected { reason })?;
                let request = Command::new(
                    operation_id,
                    revision,
                    Action::Lifecycle(MetadataOperation {
                        command: command.clone(),
                    }),
                );
                let response = tokio::time::timeout_at(
                    deadline.min(started + ControlPolicy::default().ttl).into(),
                    self.raft.client_write(request),
                )
                .await
                .map_err(|_| Error::Unknown)?
                .map_err(consensus)?;
                match response.data {
                    Response::Applied {
                        outcome: Outcome::Lifecycle(result),
                        ..
                    } => return Ok(result.outcome),
                    Response::Rejected {
                        reason: crate::metadata::Rejection::RevisionConflict,
                        ..
                    } => continue,
                    Response::Rejected {
                        reason: crate::metadata::Rejection::Lifecycle(reason),
                        ..
                    } => return Err(Error::Rejected { reason }),
                    _ => {
                        return Err(Error::Rejected {
                            reason: Rejection::Conflict,
                        });
                    }
                }
            }
            Err(Error::Rejected {
                reason: Rejection::StaleGeneration,
            })
        })
    }
    fn lease(&self, fence: FenceRecord, deadline: Instant) -> BoxFuture<'_, ControlLease> {
        Box::pin(async move {
            let authority = MetadataAuthority::new(
                self.raft.clone(),
                self.state.clone(),
                self.state.0.identity.group.cluster_id,
                self.state.0.identity.group.group_id,
                ControlPolicy::default(),
            )
            .map_err(|source| Error::Authority { source })?;
            tokio::time::timeout_at(deadline.into(), authority.active_lease(&fence))
                .await
                .map_err(|_| Error::Deadline)?
                .map_err(|source| Error::Authority { source })
        })
    }
}

pub(crate) fn project(state: &MetadataState, data_group: u64, query: Query) -> Result<View> {
    let catalog = state.lifecycle();
    match query {
        Query::Health => Ok(View::Health),
        Query::Capacity => Ok(View::Capacity(catalog.capacity_usage_for(data_group))),
        Query::Objects { after, limit } => catalog
            .objects_page(data_group, after, limit)
            .map(View::Objects)
            .map_err(|reason| Error::Rejected { reason }),
        Query::Pins { after, limit } => catalog
            .pins_page(data_group, after, limit)
            .map(View::Pins)
            .map_err(|reason| Error::Rejected { reason }),
        Query::DataVoters => state
            .group(data_group)
            .cloned()
            .map(View::DataVoters)
            .ok_or(Error::Rejected {
                reason: Rejection::Membership,
            }),
        Query::Diagnose(id) => catalog
            .diagnose(id)
            .map(View::Diagnostic)
            .ok_or(Error::Rejected {
                reason: Rejection::NotFound,
            }),
        Query::Object(id) => catalog
            .object(id)
            .cloned()
            .map(|object| View::Object(Box::new(object)))
            .ok_or(Error::Rejected {
                reason: Rejection::NotFound,
            }),
        Query::Prepare(ranges) => {
            if ranges.is_empty() || ranges.len() > 64 {
                return Err(Error::Configuration);
            }
            let mut grants = Vec::new();
            let mut generations = BTreeMap::new();
            for range in ranges {
                if state.partition_group(range.partition) != Some(data_group) {
                    return Err(Error::Rejected {
                        reason: Rejection::Identity,
                    });
                }
                let grant = state
                    .grant(range.partition)
                    .filter(|_| state.route(range.partition).is_some())
                    .ok_or(Error::Rejected {
                        reason: Rejection::Identity,
                    })?;
                grants.push(grant.clone());
                if generations
                    .insert(
                        range.partition,
                        catalog
                            .manifest(range.partition)
                            .map_or(0, |m| m.generation),
                    )
                    .is_some()
                {
                    return Err(Error::Configuration);
                }
            }
            Ok(View::Prepared {
                grants,
                generations,
            })
        }
        Query::Reclaim { partition, end } => {
            if end <= catalog.checkpointed_floor(partition) {
                return Ok(View::Reclaim { generation: 0 });
            }
            let manifest = catalog.manifest(partition).ok_or(Error::Rejected {
                reason: Rejection::NotPublished,
            })?;
            if end > manifest.published_end {
                return Err(Error::Rejected {
                    reason: Rejection::InvalidRange,
                });
            }
            let mut generation = 0;
            for segment in manifest.segments.iter().filter(|s| s.offsets.start < end) {
                let object = catalog
                    .object(segment.object_id)
                    .ok_or(Error::Configuration)?;
                generation =
                    generation.max(object.checkpoint_generation.ok_or(Error::Rejected {
                        reason: Rejection::CheckpointRequired,
                    })?);
            }
            Ok(View::Reclaim { generation })
        }

        Query::Read { partition, first } => {
            let manifest = catalog.manifest(partition).ok_or(Error::Rejected {
                reason: Rejection::NotPublished,
            })?;
            let segment = manifest
                .segments
                .iter()
                .find(|s| s.offsets.start <= first && first < s.offsets.end)
                .ok_or(Error::Rejected {
                    reason: Rejection::InvalidRange,
                })?;
            let object = catalog
                .object(segment.object_id)
                .ok_or(Error::Configuration)?;
            if state.partition_group(partition) != Some(object.descriptor().identity.group_id) {
                return Err(Error::Configuration);
            }
            Ok(View::Read {
                object: Box::new(object.clone()),
                generation: manifest.publication_generation,
            })
        }
        Query::Pin(id) => Ok(View::Pin(catalog.pin(id).cloned())),
        Query::Compaction(id) => catalog
            .compaction_ranges(id)
            .map(View::Compaction)
            .map_err(|reason| Error::Rejected { reason }),
    }
}

fn consensus(error: impl std::fmt::Display) -> Error {
    Error::Consensus {
        reason: error.to_string(),
    }
}
