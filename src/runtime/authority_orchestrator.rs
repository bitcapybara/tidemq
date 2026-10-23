use std::{sync::Arc, time::Duration};

use crate::metadata::{Command, ImportEvidence, TransferCommand, TransferStep};
use tokio::sync::watch;

use super::*;

impl Service {
    pub(in crate::runtime) async fn start_authority_transfer(
        &self,
        operation: Id,
        shard: Id,
        target_group: u64,
        deadline: Instant,
    ) -> Result<AuthorityTransfer> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        {
            let _mutation = wire::within(deadline, self.metadata_mutation.lock()).await?;
            let previous = replica
                .state
                .try_with_application(|state| state.topology().transfer(operation).cloned())
                .await
                .map_err(|source| Error::operation("read original transfer preparation", source))?;
            if let Some(previous) = previous {
                if previous.shard != shard || previous.target.group_id != target_group {
                    return Err(Error::invalid(
                        "Transfer retry differs from its original operation",
                    ));
                }
            } else {
                let birth = replica
                    .state
                    .try_with_application(|state| state.topology().shard(shard).cloned())
                    .await
                    .map_err(|source| Error::operation("read transferable shard", source))?
                    .ok_or_else(super::super::metadata::not_found)?;
                let route = self.ensure_route(birth.partition, deadline).await?;
                let layout_revision = replica
                    .state
                    .try_with_application(|state| {
                        state
                            .topology()
                            .layout(birth.partition.topic_id)
                            .map(|layout| layout.revision)
                    })
                    .await
                    .map_err(|source| Error::operation("read transfer layout revision", source))?
                    .ok_or_else(Error::unavailable)?;
                self.meta_write(
                    Command::transfer(
                        operation,
                        self.metadata_request_revision(operation).await?,
                        TransferCommand::Prepare {
                            shard,
                            layout_revision,
                            target_group,
                            target_owner: route.owner_node,
                        },
                    ),
                    deadline,
                )
                .await?;
            }
        }
        self.resume_authority_transfer(operation, deadline).await
    }

    pub(in crate::runtime) async fn resume_authority_transfers(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut after = None;
        loop {
            tokio::select! { _ = shutdown.changed() => return Ok(()), _ = ticker.tick() => {} }
            let Some(replica) = &self.metadata else {
                continue;
            };
            if self.require_leader(replica).is_err() {
                continue;
            }
            let id = replica
                .state
                .try_with_application(|state| {
                    state
                        .topology()
                        .pending_transfers()
                        .find(|plan| after.is_none_or(|after| plan.id > after))
                        .or_else(|| state.topology().pending_transfers().next())
                        .map(|plan| plan.id)
                })
                .await
                .map_err(|source| Error::operation("read pending authority transfers", source))?;
            let Some(id) = id else {
                continue;
            };
            after = Some(id);
            let deadline = Instant::now() + Duration::from_secs(30);
            let result = tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                result = self.resume_authority_transfer(id, deadline) => result,
            };
            if let Err(source) = result {
                tracing::warn!(event = "authority_transfer_pending", plan = %id, error = %format!("{:#}", anyhow::Error::new(source)), "Authority transfer remains durable for retry");
            }
        }
    }

    async fn resume_authority_transfer(
        &self,
        id: Id,
        deadline: Instant,
    ) -> Result<AuthorityTransfer> {
        // This lock serializes transfers only; consumer liveness and metadata mutations stay independent.
        let _serial = wire::within(deadline, self.transfer_serial.lock()).await?;
        loop {
            let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
            self.require_leader(replica)?;
            let view = self.transfer_view(id, deadline).await?;
            if view.plan.stage == TransferStage::Retired {
                return Ok(view.plan);
            }
            let step = self.authority_transfer_step(&view, deadline).await?;
            let operation = Id::new_v7().map_err(|source| {
                Error::operation("allocate authority transfer observation", source)
            })?;
            let outcome = {
                let _mutation = wire::within(deadline, self.metadata_mutation.lock()).await?;
                self.meta_write(
                    Command::transfer(
                        operation,
                        self.meta_revision().await?,
                        TransferCommand::Advance {
                            plan: id,
                            step: Box::new(step),
                        },
                    ),
                    deadline,
                )
                .await
            };
            if let Err(error) = outcome {
                // An uncertain older observation may already have committed the next stage.
                let current = self.transfer_view(id, deadline).await?;
                if current.plan.stage <= view.plan.stage
                    && error.failure().code
                        != i32::from(tidemq_protocol::common::ErrorCode::StaleGeneration)
                {
                    return Err(error);
                }
            }
        }
    }

    async fn authority_transfer_step(
        &self,
        view: &TransferView,
        deadline: Instant,
    ) -> Result<TransferStep> {
        let plan = &view.plan;
        match plan.stage {
            TransferStage::Prepare => {
                let capacity = match self
                    .transfer_leader_rpc(
                        plan.target.group_id,
                        TransferRequest::Reserve(plan.id),
                        deadline,
                    )
                    .await?
                {
                    TransferReply::Reserved(capacity) => capacity,
                    _ => {
                        return Err(Error::invalid(
                            "Target omitted its durable capacity reservation",
                        ));
                    }
                };
                match self
                    .transfer_leader_rpc(
                        plan.source.group_id,
                        TransferRequest::Freeze {
                            plan: plan.id,
                            capacity,
                        },
                        deadline,
                    )
                    .await?
                {
                    TransferReply::Frozen(cut) => Ok(TransferStep::Frozen(cut)),
                    _ => Err(Error::invalid("Source omitted its durable frozen cut")),
                }
            }
            TransferStage::Frozen => {
                match self
                    .transfer_leader_rpc(
                        plan.source.group_id,
                        TransferRequest::Export(plan.id),
                        deadline,
                    )
                    .await?
                {
                    TransferReply::Manifest(manifest) => Ok(TransferStep::Exported(manifest)),
                    _ => Err(Error::invalid("Source omitted its bounded export manifest")),
                }
            }
            TransferStage::Exported => self.import_authority_transfer(plan, deadline).await,
            TransferStage::Imported => Ok(TransferStep::Published),
            TransferStage::Published => {
                let mut checkpoints = Vec::new();
                for node in &view.voters {
                    match self
                        .transfer_rpc(
                            *node,
                            plan.target.group_id,
                            TransferRequest::Checkpoint(plan.id),
                            deadline,
                        )
                        .await?
                    {
                        TransferReply::Checkpoint(proof) if proof.voter == *node => {
                            checkpoints.push(proof)
                        }
                        _ => {
                            return Err(Error::invalid(
                                "Target voter omitted its own durable snapshot",
                            ));
                        }
                    }
                }
                Ok(TransferStep::Checkpointed(checkpoints))
            }
            TransferStage::Checkpointed => {
                if view.source_pins {
                    return Err(Error::unavailable());
                }
                match self
                    .transfer_leader_rpc(
                        plan.source.group_id,
                        TransferRequest::Reclaim(plan.id),
                        deadline,
                    )
                    .await?
                {
                    TransferReply::Retired(cut) => Ok(TransferStep::Retired(cut)),
                    _ => Err(Error::invalid("Source omitted its durable retirement cut")),
                }
            }
            TransferStage::Retired => Err(Error::invalid("A retired transfer has no pending step")),
        }
    }

    async fn transfer_leader_rpc(
        &self,
        group: u64,
        request: TransferRequest,
        deadline: Instant,
    ) -> Result<TransferReply> {
        let leader = self.leader(group, deadline).await?;
        self.transfer_rpc(leader, group, request, deadline).await
    }

    async fn import_authority_transfer(
        &self,
        plan: &AuthorityTransfer,
        deadline: Instant,
    ) -> Result<TransferStep> {
        let manifest = plan.manifest.as_ref().ok_or_else(Error::unavailable)?;
        let mut offset = match self
            .transfer_leader_rpc(
                plan.target.group_id,
                TransferRequest::Begin(plan.id),
                deadline,
            )
            .await?
        {
            TransferReply::Progress(offset) if offset <= manifest.bytes => offset,
            _ => {
                return Err(Error::invalid(
                    "Target omitted its original import progress",
                ));
            }
        };
        while offset < manifest.bytes {
            let page = match self
                .transfer_leader_rpc(
                    plan.source.group_id,
                    TransferRequest::Page {
                        plan: plan.id,
                        offset,
                    },
                    deadline,
                )
                .await?
            {
                TransferReply::Page(page) if page.plan == plan.id && page.offset == offset => page,
                _ => return Err(Error::invalid("Source returned a different transfer page")),
            };
            let next = offset
                .checked_add(page.bytes.len() as u64)
                .filter(|next| *next > offset && *next <= manifest.bytes)
                .ok_or_else(|| Error::invalid("Transfer page did not advance its manifest"))?;
            match self
                .transfer_leader_rpc(
                    plan.target.group_id,
                    TransferRequest::Import(page),
                    deadline,
                )
                .await?
            {
                TransferReply::Progress(received) if received == next => offset = next,
                _ => {
                    return Err(Error::invalid(
                        "Target did not durably commit the expected page",
                    ));
                }
            }
        }
        match self
            .transfer_leader_rpc(
                plan.target.group_id,
                TransferRequest::Finish(plan.id),
                deadline,
            )
            .await?
        {
            TransferReply::Imported(recovery) => Ok(TransferStep::Imported(ImportEvidence {
                manifest: manifest.clone(),
                recovery,
            })),
            _ => Err(Error::invalid("Target omitted its imported recovery proof")),
        }
    }
}
