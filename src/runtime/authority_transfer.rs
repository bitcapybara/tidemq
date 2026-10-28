use std::collections::BTreeSet;

use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tidemq_protocol::{Id, common::TrafficClass};
use tokio::time::Instant;

use crate::{
    broker::{
        AuthorityDataCommand, DataCommand, DataResponse, TransferCapacity, TransferOutcome,
        TransferPage,
    },
    metadata::{
        AuthorityTransfer, TRANSFER_PAGE_BYTES, TransferCheckpoint, TransferManifest, TransferStage,
    },
    ownership::{CommitPosition, RecoveryEvidence},
};

use super::{
    Error, Result,
    service::Service,
    wire::{self, Request},
};

#[path = "authority_export.rs"]
mod export;
#[path = "authority_orchestrator.rs"]
mod orchestrator;
pub(super) use export::ExportCache;

pub(super) const AUTHORITY_TRANSFER: u32 = 1_004;
const MAX_WIRE_BYTES: usize = TRANSFER_PAGE_BYTES + 12 * 1024;

#[derive(Serialize, Deserialize)]
enum TransferRequest {
    View(Id),
    Reserve(Id),
    Freeze {
        plan: Id,
        capacity: TransferCapacity,
    },
    Export(Id),
    Page {
        plan: Id,
        offset: u64,
    },
    Begin(Id),
    Import(TransferPage),
    Finish(Id),
    Checkpoint(Id),
    Reclaim(Id),
}

impl TransferRequest {
    fn class(&self) -> TrafficClass {
        match self {
            Self::Export(_) | Self::Page { .. } | Self::Import(_) | Self::Checkpoint(_) => {
                TrafficClass::Bulk
            }
            _ => TrafficClass::Control,
        }
    }

    fn plan(&self) -> Id {
        match self {
            Self::View(id)
            | Self::Reserve(id)
            | Self::Export(id)
            | Self::Begin(id)
            | Self::Finish(id)
            | Self::Checkpoint(id)
            | Self::Reclaim(id) => *id,
            Self::Freeze { plan, .. } | Self::Page { plan, .. } => *plan,
            Self::Import(page) => page.plan,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct TransferView {
    plan: AuthorityTransfer,
    pending: bool,
    source_pins: bool,
    voters: BTreeSet<u64>,
}

#[derive(Serialize, Deserialize)]
enum TransferReply {
    View(Box<TransferView>),
    Reserved(TransferCapacity),
    Frozen(CommitPosition),
    Manifest(TransferManifest),
    Page(TransferPage),
    Progress(u64),
    Imported(RecoveryEvidence),
    Checkpoint(TransferCheckpoint),
    Retired(CommitPosition),
}

fn encode(value: &impl Serialize) -> Result<Bytes> {
    let bytes = postcard::to_stdvec(value)
        .map_err(|source| Error::operation("encode authority transfer value", source))?;
    if bytes.len() > MAX_WIRE_BYTES {
        return Err(Error::invalid(
            "Authority transfer value exceeds its finite bound",
        ));
    }
    Ok(bytes.into())
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > MAX_WIRE_BYTES {
        return Err(Error::invalid(
            "Authority transfer value exceeds its finite bound",
        ));
    }
    postcard::from_bytes(bytes)
        .map_err(|source| Error::operation("decode authority transfer value", source))
}

impl Service {
    pub(super) async fn authority_transfer_status(
        &self,
        id: Id,
        deadline: Instant,
    ) -> Result<Option<AuthorityTransfer>> {
        if self.config.role == crate::config::Role::Combined {
            return Err(super::metadata::metadata_rejection(
                crate::metadata::Rejection::Unsupported,
            ));
        }
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        wire::within(deadline, replica.group.ensure_read(deadline))
            .await?
            .map_err(wire::execution_error)?;
        replica
            .state
            .try_with_application(|state| state.topology().transfer(id).cloned())
            .await
            .map_err(|source| Error::operation("read original authority transfer status", source))
    }

    async fn transfer_rpc(
        &self,
        node: u64,
        group: u64,
        request: TransferRequest,
        deadline: Instant,
    ) -> Result<TransferReply> {
        if self.config.role == crate::config::Role::Combined {
            return Err(super::metadata::metadata_rejection(
                crate::metadata::Rejection::Unsupported,
            ));
        }
        let class = request.class();
        let operation = request.plan();
        let request = Request {
            kind: AUTHORITY_TRANSFER,
            body: encode(&request)?,
            principal: String::new(),
            scope: String::new(),
            hops: 0,
            client_session: Bytes::new(),
            tenant: Bytes::new(),
        };
        let reply = self
            .rpc
            .submit(node, group, class, operation, request, deadline, group)
            .await?
            .receive()
            .await?;
        decode(&reply)
    }

    async fn transfer_view(&self, id: Id, deadline: Instant) -> Result<TransferView> {
        let group = self.config.runtime.metadata_group;
        let leader = self.leader(group, deadline).await?;
        match self
            .transfer_rpc(leader, group, TransferRequest::View(id), deadline)
            .await?
        {
            TransferReply::View(view) if view.plan.id == id => Ok(*view),
            _ => Err(Error::invalid(
                "Metadata omitted the original authority transfer",
            )),
        }
    }

    async fn transfer_view_command(&self, id: Id, deadline: Instant) -> Result<TransferReply> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        wire::within(deadline, replica.group.ensure_read(deadline))
            .await?
            .map_err(wire::execution_error)?;
        let view = replica
            .state
            .try_with_application(|state| {
                let plan = state
                    .topology()
                    .transfer(id)
                    .cloned()
                    .ok_or_else(super::metadata::not_found)?;
                Ok(TransferView {
                    pending: state.lifecycle().transfer_has_pending(plan.partition),
                    source_pins: state
                        .lifecycle()
                        .transfer_has_pins(plan.partition, plan.source.group_id),
                    voters: state
                        .group(plan.target.group_id)
                        .ok_or_else(Error::unavailable)?
                        .placement
                        .raft_voters()
                        .cloned()
                        .ok_or_else(Error::unavailable)?,
                    plan,
                })
            })
            .await
            .map_err(|source| Error::operation("read fresh authority transfer", source))??;
        Ok(TransferReply::View(Box::new(view)))
    }

    pub(super) async fn authority_transfer_command(
        &self,
        peer: &crate::transport::NodeGrant,
        group: u64,
        operation: Id,
        class: i32,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        if self.config.role == crate::config::Role::Combined {
            return Err(super::metadata::metadata_rejection(
                crate::metadata::Rejection::Unsupported,
            ));
        }
        let request: TransferRequest = decode(bytes)?;
        if operation != request.plan()
            || class != i32::from(request.class())
            || peer.role == "gateway"
        {
            return Err(super::service::forbidden());
        }
        if let TransferRequest::View(id) = request {
            if group != self.config.runtime.metadata_group {
                return Err(super::service::forbidden());
            }
            return encode(&self.transfer_view_command(id, deadline).await?);
        }
        if !matches!(peer.role.as_str(), "metadata" | "combined") {
            return Err(super::service::forbidden());
        }
        let view = self.transfer_view(request.plan(), deadline).await?;
        let plan = &view.plan;
        let source = matches!(
            request,
            TransferRequest::Freeze { .. }
                | TransferRequest::Export(_)
                | TransferRequest::Page { .. }
                | TransferRequest::Reclaim(_)
        );
        if group
            != if source {
                plan.source.group_id
            } else {
                plan.target.group_id
            }
        {
            return Err(super::service::forbidden());
        }
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        if !matches!(request, TransferRequest::Checkpoint(_)) {
            self.require_leader(replica)?;
        }
        let voters = if source {
            &plan.source_voters
        } else {
            &view.voters
        };
        if crate::lifecycle::stable_voters(&replica.group)
            .map_err(|source| Error::operation("verify transfer group voters", source))?
            != *voters
        {
            return Err(Error::unavailable());
        }
        encode(
            &self
                .transfer_data_command(group, request, view, deadline)
                .await?,
        )
    }

    async fn transfer_data_command(
        &self,
        group: u64,
        request: TransferRequest,
        view: TransferView,
        deadline: Instant,
    ) -> Result<TransferReply> {
        if self.config.role == crate::config::Role::Combined {
            return Err(super::metadata::metadata_rejection(
                crate::metadata::Rejection::Unsupported,
            ));
        }
        let plan = view.plan;
        match request {
            TransferRequest::Reserve(_) => self.reserve_transfer(group, plan, deadline).await,
            TransferRequest::Freeze { capacity, .. } => {
                self.freeze_transfer(group, plan, capacity, view.pending, deadline)
                    .await
            }
            TransferRequest::Export(_) => Ok(TransferReply::Manifest(
                self.export_manifest(group, &plan, deadline).await?,
            )),
            TransferRequest::Page { offset, .. } => Ok(TransferReply::Page(
                self.export_page(group, &plan, offset, deadline).await?,
            )),
            TransferRequest::Begin(_) => {
                match self
                    .transfer_write(
                        group,
                        AuthorityDataCommand::BeginImport {
                            plan: Box::new(plan),
                        },
                        deadline,
                    )
                    .await?
                {
                    TransferOutcome::ImportProgress { received, .. } => {
                        Ok(TransferReply::Progress(received))
                    }
                    _ => Err(Error::invalid("Target did not return import progress")),
                }
            }
            TransferRequest::Import(page) => {
                match self
                    .transfer_write(group, AuthorityDataCommand::ImportPage(page), deadline)
                    .await?
                {
                    TransferOutcome::ImportProgress { received, .. } => {
                        Ok(TransferReply::Progress(received))
                    }
                    _ => Err(Error::invalid("Target did not commit an import page")),
                }
            }
            TransferRequest::Finish(_) => self.finish_transfer(group, &plan, deadline).await,
            TransferRequest::Checkpoint(_) => {
                self.checkpoint_transfer(group, &plan, &view.voters, deadline)
                    .await
            }
            TransferRequest::Reclaim(_) => {
                self.reclaim_transfer(group, plan, view.source_pins, deadline)
                    .await
            }
            TransferRequest::View(_) => Err(super::service::forbidden()),
        }
    }

    async fn transfer_write(
        &self,
        group: u64,
        command: AuthorityDataCommand,
        deadline: Instant,
    ) -> Result<TransferOutcome> {
        match self
            .data_write(group, DataCommand::AuthorityTransfer(command), deadline)
            .await?
        {
            DataResponse::AuthorityTransfer(outcome) => Ok(outcome),
            _ => Err(Error::invalid(
                "Data group omitted the authority transfer result",
            )),
        }
    }

    async fn reserve_transfer(
        &self,
        group: u64,
        plan: AuthorityTransfer,
        deadline: Instant,
    ) -> Result<TransferReply> {
        if self.config.role == crate::config::Role::Combined {
            return Err(super::metadata::metadata_rejection(
                crate::metadata::Rejection::Unsupported,
            ));
        }
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        let id = plan.id;
        let (existing, capacity) = replica
            .state
            .try_with_application(|state| {
                state
                    .transfer_capacity(id)
                    .map(|capacity| (state.transfer_import_progress(id).is_some(), capacity))
            })
            .await
            .map_err(|source| Error::operation("read original target reservation", source))?
            .map_err(transfer_error)?;
        if !existing {
            self.transfer_write(
                group,
                AuthorityDataCommand::Reserve {
                    plan: Box::new(plan),
                    capacity,
                },
                deadline,
            )
            .await?;
        }
        let expanded = replica
            .state
            .try_with_application(|state| state.expanded_transfer_capacity(id))
            .await
            .map_err(|source| {
                Error::operation("read available target reservation expansion", source)
            })?
            .map_err(transfer_error)?;
        if expanded != capacity {
            self.transfer_write(
                group,
                AuthorityDataCommand::ExpandReservation {
                    plan: id,
                    capacity: expanded,
                },
                deadline,
            )
            .await?;
        }
        let capacity = replica
            .state
            .try_with_application(|state| state.transfer_capacity(id))
            .await
            .map_err(|source| Error::operation("read committed target reservation", source))?
            .map_err(transfer_error)?;
        Ok(TransferReply::Reserved(capacity))
    }

    async fn transfer_position(&self, group: u64, deadline: Instant) -> Result<CommitPosition> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        wire::within(deadline, replica.group.ensure_read(deadline))
            .await?
            .map_err(wire::execution_error)?;
        replica
            .state
            .try_with_applied(|_, position| position.map(CommitPosition::from))
            .await
            .map_err(|source| Error::operation("read committed transfer position", source))?
            .ok_or_else(Error::unavailable)
    }

    async fn freeze_transfer(
        &self,
        group: u64,
        plan: AuthorityTransfer,
        capacity: TransferCapacity,
        pending: bool,
        deadline: Instant,
    ) -> Result<TransferReply> {
        if self.config.role == crate::config::Role::Combined {
            return Err(super::metadata::metadata_rejection(
                crate::metadata::Rejection::Unsupported,
            ));
        }
        if let Some(cut) = plan.frozen_cut {
            return Ok(TransferReply::Frozen(cut));
        }
        if pending {
            return Err(Error::unavailable());
        }
        let capacity = self
            .data
            .get(&group)
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state.transfer_source_capacity(plan.id).unwrap_or(capacity)
            })
            .await
            .map_err(|source| Error::operation("read original freeze capacity", source))?;
        let id = plan.id;
        self.transfer_write(
            group,
            AuthorityDataCommand::Freeze {
                plan: Box::new(plan),
                capacity,
            },
            deadline,
        )
        .await?;
        let cut = self.transfer_position(group, deadline).await?;
        match self
            .transfer_write(
                group,
                AuthorityDataCommand::RecordCut { plan: id, cut },
                deadline,
            )
            .await?
        {
            TransferOutcome::Cut(cut) => Ok(TransferReply::Frozen(cut)),
            _ => Err(Error::invalid("Source omitted its original frozen cut")),
        }
    }

    async fn finish_transfer(
        &self,
        group: u64,
        plan: &AuthorityTransfer,
        deadline: Instant,
    ) -> Result<TransferReply> {
        if self.config.role == crate::config::Role::Combined {
            return Err(super::metadata::metadata_rejection(
                crate::metadata::Rejection::Unsupported,
            ));
        }
        let recovered = match self
            .transfer_write(
                group,
                AuthorityDataCommand::FinishImport { plan: plan.id },
                deadline,
            )
            .await?
        {
            TransferOutcome::Imported(recovered) => recovered,
            _ => return Err(Error::invalid("Target did not finish importing the shard")),
        };
        Ok(TransferReply::Imported(RecoveryEvidence {
            placement: crate::metadata::GroupPlacement::Raft {
                voters: plan.target_voters.clone(),
            },
            recovered,
            committed: self.transfer_position(group, deadline).await?,
        }))
    }

    async fn reclaim_transfer(
        &self,
        group: u64,
        plan: AuthorityTransfer,
        pins: bool,
        deadline: Instant,
    ) -> Result<TransferReply> {
        if self.config.role == crate::config::Role::Combined {
            return Err(super::metadata::metadata_rejection(
                crate::metadata::Rejection::Unsupported,
            ));
        }
        if let Some(cut) = plan.retired_cut {
            return Ok(TransferReply::Retired(cut));
        }
        if pins || plan.stage < TransferStage::Checkpointed {
            return Err(Error::unavailable());
        }
        let id = plan.id;
        self.transfer_write(
            group,
            AuthorityDataCommand::ReclaimSource {
                plan: Box::new(plan),
            },
            deadline,
        )
        .await?;
        self.clear_export(id, deadline).await?;
        let cut = self.transfer_position(group, deadline).await?;
        match self
            .transfer_write(
                group,
                AuthorityDataCommand::RecordRetiredCut { plan: id, cut },
                deadline,
            )
            .await?
        {
            TransferOutcome::Cut(cut) => Ok(TransferReply::Retired(cut)),
            _ => Err(Error::invalid("Source omitted its original retirement cut")),
        }
    }

    async fn checkpoint_transfer(
        &self,
        group: u64,
        plan: &AuthorityTransfer,
        voters: &BTreeSet<u64>,
        deadline: Instant,
    ) -> Result<TransferReply> {
        if self.config.role == crate::config::Role::Combined {
            return Err(super::metadata::metadata_rejection(
                crate::metadata::Rejection::Unsupported,
            ));
        }
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        let imported = plan
            .imported
            .as_ref()
            .filter(|_| plan.stage >= TransferStage::Published)
            .ok_or_else(Error::unavailable)?;
        loop {
            let ready = replica
                .state
                .try_with_application(|state| {
                    state
                        .transfer_import_progress(plan.id)
                        .is_some_and(|(_, recovered)| {
                            recovered.as_ref() == Some(&imported.recovery.recovered)
                        })
                })
                .await
                .map_err(|source| {
                    Error::operation("verify local imported recovery state", source)
                })?;
            if ready {
                break;
            }
            wire::within(
                deadline,
                tokio::time::sleep(std::time::Duration::from_millis(5)),
            )
            .await?;
        }
        let snapshot = replica
            .group
            .snapshot(deadline)
            .await
            .map_err(wire::execution_error)?
            .ok_or_else(Error::unavailable)?;
        let position = CommitPosition::from(snapshot.position);
        if !position.at_least(imported.recovery.committed) {
            return Err(Error::unavailable());
        }
        if !voters.contains(&self.config.node_id)
            || crate::lifecycle::stable_voters(&replica.group)
                .map_err(|source| Error::operation("verify target snapshot voters", source))?
                != *voters
        {
            return Err(Error::unavailable());
        }
        Ok(TransferReply::Checkpoint(TransferCheckpoint {
            voter: self.config.node_id,
            position,
            manifest_digest: imported.manifest.digest,
        }))
    }
}

fn transfer_error(source: crate::broker::TransferDataRejection) -> Error {
    Error::operation("apply bounded authority transfer", source)
}
