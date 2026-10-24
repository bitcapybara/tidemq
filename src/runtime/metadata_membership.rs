use std::{collections::BTreeSet, sync::Arc, time::Duration};

use openraft::{BasicNode, RaftSnapshotBuilder};
use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;
use tokio::{sync::watch, time::Instant};

use super::{
    Error, Result,
    service::Service,
    wire::{self, Request},
};
use crate::{
    metadata::{ClusterAction, Command, MetadataChange, NodeRole},
    scaling::remote::CapacitySource,
};

pub(super) const STATUS: u32 = 1010;
pub(super) const CHECKPOINT: u32 = 1011;

#[derive(Serialize, Deserialize)]
struct Status {
    node: u64,
    cluster: Id,
    available_bytes: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct CheckpointRequest {
    change: Id,
    target: BTreeSet<u64>,
    minimum_index: u64,
}

#[derive(Serialize, Deserialize)]
struct Checkpoint {
    change: Id,
    node: u64,
    cluster: Id,
    voters: BTreeSet<u64>,
    membership_index: u64,
    snapshot_index: u64,
}

impl Service {
    pub(super) async fn metadata_membership_action(
        &self,
        operation: Id,
        removed: u64,
        added: u64,
        deadline: Instant,
    ) -> Result<ClusterAction> {
        if removed == added
            || added == 0
            || removed == 0
            || !self.registry.has_role(added, NodeRole::Metadata)
        {
            return Err(Error::invalid(
                "Metadata replacement requires distinct registered metadata nodes",
            ));
        }
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let (previous, index) = replica
            .state
            .try_with_membership(|_, _, membership| {
                if membership.membership().get_joint_config().len() != 1 {
                    return Err(Error::unavailable());
                }
                let previous: BTreeSet<_> = membership.membership().voter_ids().collect();
                if !previous.contains(&removed) || previous.contains(&added) {
                    return Err(Error::invalid(
                        "Metadata replacement does not match the current voter set",
                    ));
                }
                Ok((previous, membership.log_id().map_or(0, |log| log.index)))
            })
            .await
            .map_err(|source| Error::operation("read metadata replacement source", source))??;
        let copy_bound = replica.log.diagnostics().await.replica_file_limit_bytes;
        let bytes = self
            .rpc
            .call(
                added,
                self.config.runtime.metadata_group,
                Request::control(STATUS, &())?,
                deadline.min(Instant::now() + Duration::from_millis(500)),
            )
            .await?;
        let status: Status = wire::decode_json(&bytes)?;
        if status.node != added
            || status.cluster != self.cluster
            || status
                .available_bytes
                .is_none_or(|bytes| bytes < copy_bound)
        {
            return Err(Error::unavailable());
        }
        let mut target = previous.clone();
        target.remove(&removed);
        target.insert(added);
        Ok(ClusterAction::ReplaceMetadata(MetadataChange {
            id: operation,
            previous,
            target,
            expected_membership_index: index,
            completed_membership_index: None,
        }))
    }

    pub(super) async fn metadata_membership_status(&self) -> Result<bytes::Bytes> {
        if self.metadata.is_none() {
            return Err(Error::unavailable());
        }
        let available_bytes = self
            .scaling
            .capacity
            .snapshot()
            .map(|sample| sample.free_bytes.saturating_sub(sample.reserved_bytes));
        wire::json(&Status {
            node: self.config.node_id,
            cluster: self.cluster,
            available_bytes,
        })
    }

    pub(super) async fn metadata_membership_checkpoint(
        &self,
        body: &[u8],
        deadline: Instant,
    ) -> Result<bytes::Bytes> {
        let request: CheckpointRequest = wire::decode_json(body)?;
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let allowed = replica
            .state
            .try_with_application(|state| {
                state.metadata_change(request.change).is_some_and(|change| {
                    change.target == request.target && request.target.contains(&self.config.node_id)
                })
            })
            .await
            .map_err(|source| Error::operation("verify metadata checkpoint plan", source))?;
        if !allowed {
            return Err(Error::unavailable());
        }
        if !super::maintenance::background_admitted(&self.quota, &self.quota_observed, deadline)
            .await?
        {
            return Err(Error::reject(
                tidemq_protocol::common::ErrorCode::QuotaExceeded,
                tidemq_protocol::common::Outcome::Rejected,
                true,
            ));
        }
        let mut state = replica.state.clone();
        let snapshot = wire::within(deadline, state.build_snapshot())
            .await?
            .map_err(|source| {
                Error::operation("checkpoint replacement metadata replica", source)
            })?;
        let membership = &snapshot.meta.last_membership;
        let voters: BTreeSet<_> = membership.membership().voter_ids().collect();
        let membership_index = membership.log_id().map_or(0, |log| log.index);
        let snapshot_index = snapshot
            .meta
            .last_log_id
            .ok_or_else(Error::unavailable)?
            .index;
        if membership.membership().get_joint_config().len() != 1
            || voters != request.target
            || membership_index < request.minimum_index
            || snapshot_index < membership_index
        {
            return Err(Error::unavailable());
        }
        wire::json(&Checkpoint {
            change: request.change,
            node: self.config.node_id,
            cluster: self.cluster,
            voters,
            membership_index,
            snapshot_index,
        })
    }

    pub(super) async fn follow_metadata_membership(
        self: Arc<Self>,
        mut stopped: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut ticks = tokio::time::interval(Duration::from_secs(1));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = stopped.changed() => return Ok(()), _ = ticks.tick() => {} }
            let deadline = Instant::now() + Duration::from_secs(10);
            let Some(replica) = &self.metadata else {
                continue;
            };
            if self.require_leader(replica).is_err() {
                self.metadata_repair
                    .lock()
                    .map_err(|_| Error::unavailable())?
                    .take();
                continue;
            }
            let change = replica
                .state
                .try_with_application(|state| state.pending_metadata_change().cloned())
                .await
                .map_err(|source| Error::operation("read pending metadata replacement", source))?;
            let Some(change) = change else {
                self.metadata_repair
                    .lock()
                    .map_err(|_| Error::unavailable())?
                    .take();
                continue;
            };
            tokio::select! {
                _ = stopped.changed() => return Ok(()),
                result = self.resume_metadata_membership(&change, deadline) => {
                    if let Err(error) = result { tracing::debug!(event_code = "metadata_membership_wait", error_code = error.failure().code,
                        "The original metadata membership plan will resume after quorum and capacity recover"); }
                }
            }
        }
    }

    async fn resume_metadata_membership(
        &self,
        change: &MetadataChange,
        deadline: Instant,
    ) -> Result<()> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        #[cfg(feature = "test-hooks")]
        if let Some(added) = change.target.difference(&change.previous).next().copied() {
            super::test_hooks::pause_metadata_membership(self.cluster, added, deadline).await;
        }
        self.metadata_repair(change)?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        let configs = replica
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .get_joint_config()
            .clone();
        if !(configs.len() == 1 && configs.contains(&change.target)) {
            if !configs.contains(&change.previous)
                || !(configs.len() == 1 || configs.len() == 2 && configs.contains(&change.target))
            {
                return Err(Error::unavailable());
            }
            if configs.len() == 1 {
                for node in change.target.difference(&change.previous) {
                    let endpoint = replica
                        .state
                        .try_with_application(|state| {
                            state.node(*node).map(|node| node.spec.endpoint)
                        })
                        .await
                        .map_err(|source| {
                            Error::operation("read replacement metadata endpoint", source)
                        })?
                        .ok_or_else(Error::unavailable)?;
                    wire::within(
                        deadline,
                        replica
                            .raft
                            .add_learner(*node, BasicNode::new(endpoint.to_string()), true),
                    )
                    .await?
                    .map_err(|source| {
                        Error::operation("synchronize replacement metadata learner", source)
                    })?;
                }
            }
            wire::within(
                deadline,
                replica.raft.change_membership(change.target.clone(), false),
            )
            .await?
            .map_err(|source| Error::operation("commit joint metadata membership", source))?;
        }
        self.checkpoint_metadata_membership(change, deadline).await
    }

    fn metadata_repair(&self, change: &MetadataChange) -> Result<()> {
        let mut slot = self
            .metadata_repair
            .lock()
            .map_err(|_| Error::unavailable())?;
        if slot.as_ref().is_some_and(|(id, _)| *id != change.id) {
            return Err(Error::unavailable());
        }
        if slot.is_none() {
            let target = change
                .target
                .difference(&change.previous)
                .next()
                .copied()
                .ok_or_else(Error::unavailable)?;
            let repair = self
                .repair
                .try_register(target, self.config.runtime.metadata_group)
                .map_err(|source| Error::operation("reserve metadata repair capacity", source))?;
            *slot = Some((change.id, repair));
        }
        Ok(())
    }

    async fn checkpoint_metadata_membership(
        &self,
        change: &MetadataChange,
        deadline: Instant,
    ) -> Result<()> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        let minimum_index = replica
            .state
            .try_with_membership(|_, _, membership| {
                if membership.membership().get_joint_config().len() != 1
                    || membership.membership().voter_ids().collect::<BTreeSet<_>>() != change.target
                {
                    return Err(Error::unavailable());
                }
                membership
                    .log_id()
                    .map(|log| log.index)
                    .ok_or_else(Error::unavailable)
            })
            .await
            .map_err(|source| Error::operation("read stable metadata replacement", source))??;
        for node in &change.target {
            let request = Request::control(
                CHECKPOINT,
                &CheckpointRequest {
                    change: change.id,
                    target: change.target.clone(),
                    minimum_index,
                },
            )?;
            let bytes = self
                .rpc
                .call(*node, self.config.runtime.metadata_group, request, deadline)
                .await?;
            let proof: Checkpoint = wire::decode_json(&bytes)?;
            if proof.change != change.id
                || proof.node != *node
                || proof.cluster != self.cluster
                || proof.voters != change.target
                || proof.membership_index != minimum_index
                || proof.snapshot_index < minimum_index
            {
                return Err(Error::unavailable());
            }
        }
        let _mutation = wire::within(deadline, self.metadata_mutation.lock()).await?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        self.meta_write(
            Command::complete_metadata_change(
                super::node::random_id()?,
                self.meta_revision().await?,
                change.id,
                minimum_index,
            ),
            deadline,
        )
        .await?;
        self.metadata_repair
            .lock()
            .map_err(|_| Error::unavailable())?
            .take();
        Ok(())
    }
}
