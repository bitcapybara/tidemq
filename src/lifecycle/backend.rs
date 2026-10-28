use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;

use crate::{
    execution::{GroupHandle, GroupState},
    metadata::{
        Action, Command, DataGroup, DeploymentMode, GroupPlacement, MetadataState, NodeRecord,
        NodeRole, Outcome, PartitionKey, Response,
    },
    ownership::{
        CommitPosition, ControlLease, ControlPolicy, FenceRecord, Grant, MetadataAuthority,
    },
};

use super::{
    Error, ObjectRecord, RangeRequest, Rejection, Result,
    coordinator::group_placement,
    types::{CatalogCommand, CatalogOutcome, MetadataOperation, PinRecord},
};

fn authority_scopes(
    state: &MetadataState,
    group: u64,
    object: &ObjectRecord,
) -> Result<Vec<super::CheckpointScope>> {
    let mut scopes = Vec::new();
    for index in &object.descriptor().partitions {
        let partition = super::catalog::key(index);
        if state.partition_group(partition) != Some(group)
            || state.topology().transfer_blocks(partition)
        {
            continue;
        }
        let binding = state
            .topology()
            .binding(partition)
            .filter(|binding| binding.authority_group == group)
            .ok_or(Error::Rejected {
                reason: Rejection::Identity,
            })?;
        scopes.push(super::CheckpointScope {
            partition,
            binding_generation: binding.generation,
        });
    }
    Ok(scopes)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum Query {
    Health,
    Capacity,
    Objects { after: Option<Id>, limit: usize },
    Pins { after: Option<Id>, limit: usize },
    DataPlacement,
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
    DataPlacement(DataGroup),
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
pub(crate) struct AuthorityView {
    pub cluster_id: Id,
    pub group_id: u64,
    pub node_id: u64,
    pub revision: u64,
    pub mode: DeploymentMode,
    pub placement: GroupPlacement,
    pub nodes: Vec<NodeRecord>,
    pub position: CommitPosition,
    pub view: View,
}

pub(crate) enum MetadataBackend {
    Local(LocalMetadata),
    Remote(super::remote::RemoteMetadata),
}

impl MetadataBackend {
    pub(crate) async fn query(
        &self,
        data_group: u64,
        query: Query,
        deadline: Instant,
    ) -> Result<AuthorityView> {
        match self {
            Self::Local(backend) => backend.read(data_group, query, deadline).await,
            Self::Remote(backend) => backend.query(data_group, query, deadline).await,
        }
    }

    pub(crate) async fn execute(
        &self,
        command: CatalogCommand,
        deadline: Instant,
    ) -> Result<CatalogOutcome> {
        match self {
            Self::Local(backend) => backend.execute(command, deadline).await,
            Self::Remote(backend) => backend.execute(command, deadline).await,
        }
    }

    pub(crate) async fn lease(
        &self,
        fence: FenceRecord,
        deadline: Instant,
    ) -> Result<ControlLease> {
        match self {
            Self::Local(backend) => backend.lease(fence, deadline).await,
            Self::Remote(backend) => backend.lease(fence, deadline).await,
        }
    }
}

#[derive(Clone)]
pub struct MetadataReplica {
    pub(crate) backend: Arc<MetadataBackend>,
    pub(crate) cluster_id: Id,
    pub(crate) group_id: u64,
}

pub(crate) struct LocalMetadata {
    pub(crate) group: GroupHandle<MetadataState>,
    pub(crate) state: GroupState<MetadataState>,
}

impl MetadataReplica {
    /// The data-group selector is fixed when the coordinator is constructed.
    pub fn local(group: GroupHandle<MetadataState>) -> Self {
        let cluster_id = group.identity().group.cluster_id;
        let group_id = group.identity().group.group_id;
        let state = group.state();
        Self {
            backend: Arc::new(MetadataBackend::Local(LocalMetadata { group, state })),
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
    ) -> Result<AuthorityView> {
        let until = deadline.min(Instant::now() + ControlPolicy::default().ttl);
        tokio::time::timeout_at(until.into(), async {
            self.group
                .ensure_read(until.into())
                .await
                .map_err(execution)?;
            let placement = group_placement(&self.group)?;
            self.state
                .try_with_applied(|state, position| {
                    let cluster_id = self.state.storage_identity().group.cluster_id;
                    let group_id = self.state.storage_identity().group.group_id;
                    if state.cluster_id() != Some(cluster_id)
                        || state.metadata_group_id() != Some(group_id)
                        || state.mode.is_none_or(|mode| {
                            placement.mode() != Ok(mode)
                                || (mode == DeploymentMode::Local) != self.group.is_local()
                        })
                    {
                        return Err(Error::Rejected {
                            reason: Rejection::Membership,
                        });
                    }
                    let mut nodes = Vec::new();
                    let mut zones = BTreeSet::new();
                    for voter in &placement.hosts() {
                        let node = state
                            .node(*voter)
                            .filter(|node| node.spec.roles.contains(&NodeRole::Metadata))
                            .ok_or(Error::Rejected {
                                reason: Rejection::Membership,
                            })?;
                        zones.insert(&node.spec.zone);
                        nodes.push(node.clone());
                    }
                    if placement.mode() == Ok(DeploymentMode::SeparateRf3) && zones.len() != 3 {
                        return Err(Error::Rejected {
                            reason: Rejection::Membership,
                        });
                    }
                    Ok(AuthorityView {
                        cluster_id,
                        group_id,
                        node_id: self.state.storage_identity().node_id,
                        revision: state.revision(),
                        mode: state.mode.ok_or(Error::Configuration)?,
                        placement,
                        nodes,
                        position: position
                            .map(CommitPosition::from)
                            .ok_or(Error::Configuration)?,
                        view: project(state, data_group, query)?,
                    })
                })
                .await
                .map_err(execution)?
        })
        .await
        .map_err(|_| Error::Deadline)?
    }
}

impl LocalMetadata {
    pub(super) async fn execute(
        &self,
        command: CatalogCommand,
        deadline: Instant,
    ) -> Result<CatalogOutcome> {
        let operation_id = Id::new_v7().map_err(|source| Error::Identity { source })?;
        loop {
            if Instant::now() >= deadline {
                return Err(Error::Deadline);
            }
            let started = Instant::now();
            let revision = self.read(0, Query::Health, deadline).await?.revision;
            self.state
                .try_with_application(|state| {
                    MetadataOperation {
                        command: command.clone(),
                    }
                    .validate_context(state)
                })
                .await
                .map_err(execution)?
                .map_err(|reason| Error::Rejected { reason })?;
            let request = Command::new(
                operation_id,
                revision,
                Action::Lifecycle(MetadataOperation {
                    command: command.clone(),
                }),
            );
            let until = deadline.min(started + ControlPolicy::default().ttl);
            let response = if matches!(
                &command,
                CatalogCommand::Register(_) | CatalogCommand::Pin(_)
            ) {
                self.group.commit(request, until.into()).await
            } else {
                self.group.commit_completion(request, until.into()).await
            }
            .map_err(execution)?;
            match response.response {
                Response::Applied {
                    outcome: Outcome::Lifecycle(result),
                    ..
                } => return Ok(result.outcome),
                Response::Rejected {
                    reason: crate::metadata::Rejection::RevisionConflict,
                    ..
                } => {
                    // Unrelated metadata writes do not change the catalog generation or admit this operation.
                    tokio::task::yield_now().await;
                    continue;
                }
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
    }
    async fn lease(&self, fence: FenceRecord, deadline: Instant) -> Result<ControlLease> {
        let authority = MetadataAuthority::new(
            self.group.clone(),
            self.state.storage_identity().group.cluster_id,
            self.state.storage_identity().group.group_id,
            ControlPolicy::default(),
        )
        .map_err(|source| Error::Authority { source })?;
        tokio::time::timeout_at(deadline.into(), authority.active_lease(&fence))
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|source| Error::Authority { source })
    }
}

pub(crate) fn project(state: &MetadataState, data_group: u64, query: Query) -> Result<View> {
    let catalog = state.lifecycle();
    match query {
        Query::Health => Ok(View::Health),
        Query::Capacity => Ok(View::Capacity(catalog.capacity_usage_for(data_group))),
        Query::Objects { after, limit } => catalog
            .related_objects_page(data_group, after, limit, |partition| {
                state.partition_group(partition)
            })
            .map(View::Objects)
            .map_err(|reason| Error::Rejected { reason }),
        Query::Pins { after, limit } => catalog
            .related_pins_page(data_group, after, limit, |partition| {
                state.partition_group(partition)
            })
            .map(View::Pins)
            .map_err(|reason| Error::Rejected { reason }),
        Query::DataPlacement => state
            .group(data_group)
            .cloned()
            .map(View::DataPlacement)
            .ok_or(Error::Rejected {
                reason: Rejection::Membership,
            }),
        Query::Diagnose(id) => {
            let object = catalog.object(id).ok_or(Error::Rejected {
                reason: Rejection::NotFound,
            })?;
            let scopes = authority_scopes(state, data_group, object)?;
            let mut diagnostic = catalog.diagnose(id).ok_or(Error::Configuration)?;
            if !scopes.is_empty() {
                diagnostic.checkpoint_generation =
                    scopes.iter().try_fold(0, |generation, scope| {
                        object
                            .checkpoints
                            .get(&scope.partition)
                            .filter(|row| {
                                row.group_id == data_group
                                    && row.binding_generation == scope.binding_generation
                            })
                            .map(|row| generation.max(row.generation))
                    });
            }
            diagnostic.authority_scopes = scopes;
            Ok(View::Diagnostic(diagnostic))
        }
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
            let binding = state
                .topology()
                .binding(partition)
                .filter(|binding| {
                    binding.authority_group == data_group
                        && state.partition_group(partition) == Some(data_group)
                })
                .ok_or(Error::Rejected {
                    reason: Rejection::Identity,
                })?;
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
                generation = generation.max(
                    object
                        .checkpoints
                        .get(&partition)
                        .filter(|row| {
                            row.group_id == data_group
                                && row.binding_generation == binding.generation
                        })
                        .ok_or(Error::Rejected {
                            reason: Rejection::CheckpointRequired,
                        })?
                        .generation,
                );
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
            if state.partition_group(partition) != Some(data_group)
                || state.topology().binding(partition).is_none_or(|binding| {
                    !binding.accepts_object(object.descriptor().identity.group_id)
                })
            {
                return Err(Error::Configuration);
            }
            Ok(View::Read {
                object: Box::new(object.clone()),
                generation: manifest.publication_generation,
            })
        }
        Query::Pin(id) => Ok(View::Pin(catalog.pin(id).cloned())),
        Query::Compaction(id) => {
            let object = catalog.object(id).ok_or(Error::Rejected {
                reason: Rejection::NotFound,
            })?;
            if authority_scopes(state, data_group, object)?.len()
                != object.descriptor().partitions.len()
            {
                return Ok(View::Compaction(None));
            }
            catalog
                .compaction_ranges(id)
                .map(View::Compaction)
                .map_err(|reason| Error::Rejected { reason })
        }
    }
}

pub(super) fn execution(source: crate::execution::Error) -> Error {
    match source {
        crate::execution::Error::Unknown { .. } => Error::Unknown,
        source => Error::Execution { source },
    }
}
