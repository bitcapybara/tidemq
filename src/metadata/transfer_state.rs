use std::collections::BTreeMap;
use tidemq_protocol::Id;

use super::{
    Action, AuthorityTransfer, MAX_AUTHORITY_TRANSFERS, MetadataState, NodeRole, Outcome,
    RecordStatus, Rejection, Response, Route, TransferCommand, TransferStage, TransferStep,
    control::{ControlLocation, ControlStage},
    state::{Decision, PartitionRecord},
};
use crate::ownership::{FenceEvidence, Grant, OwnershipPhase};

impl MetadataState {
    pub(super) fn transfer_command(
        &mut self,
        operation: Id,
        command: &TransferCommand,
        revision: u64,
    ) -> Result<AuthorityTransfer, Rejection> {
        if self.mode == Some(super::DeploymentMode::Local) {
            return Err(Rejection::Unsupported);
        }
        match command {
            TransferCommand::Prepare { .. } => self.prepare_transfer(operation, command, revision),
            TransferCommand::Advance { plan, step } => {
                let original = self
                    .topology
                    .transfers
                    .get(plan)
                    .cloned()
                    .ok_or(Rejection::NotFound)?;
                let mut next = original.clone();
                if matches!(step.as_ref(), TransferStep::Checkpointed(_))
                    && original.stage == TransferStage::Published
                {
                    next.checkpoint_voters = self
                        .group(original.target.group_id)
                        .ok_or(Rejection::NotFound)?
                        .placement
                        .raft_voters()
                        .ok_or(Rejection::Unsupported)?
                        .clone();
                }
                next.advance(step.as_ref().clone())?;
                if next == original {
                    return Ok(next);
                }
                self.check_transfer_membership(&next)?;
                if original.stage < TransferStage::Published
                    && self.grant(original.partition) != Some(&original.source)
                {
                    return Err(Rejection::Superseded);
                }
                if matches!(step.as_ref(), TransferStep::Frozen(_))
                    && self.lifecycle.transfer_has_pending(original.partition)
                {
                    return Err(Rejection::RecoveryRequired);
                }
                if matches!(step.as_ref(), TransferStep::Retired(_))
                    && self
                        .lifecycle
                        .transfer_has_pins(original.partition, original.source.group_id)
                {
                    return Err(Rejection::RecoveryRequired);
                }
                if original.stage < TransferStage::Published
                    && next.stage == TransferStage::Published
                {
                    self.publish_transfer(&next, revision)?;
                }
                self.topology.transfers.insert(*plan, next.clone());
                Ok(next)
            }
        }
    }

    fn prepare_transfer(
        &mut self,
        id: Id,
        command: &TransferCommand,
        revision: u64,
    ) -> Result<AuthorityTransfer, Rejection> {
        let TransferCommand::Prepare {
            shard,
            layout_revision,
            target_group,
            target_owner,
        } = *command
        else {
            return Err(Rejection::InvalidCommand);
        };
        if let Some(previous) = self.topology.transfers.get(&id) {
            return if previous.shard == shard
                && previous.layout_revision == layout_revision
                && previous.target.group_id == target_group
                && previous.target.owner_node == target_owner
            {
                Ok(previous.clone())
            } else {
                Err(Rejection::OperationConflict)
            };
        }
        let source = self.transfer_source(shard, layout_revision, target_group)?;
        let partition = source.partition;
        if self.pending_retention(partition.topic_id) {
            return Err(Rejection::RecoveryRequired);
        }
        let target = self.transfer_target(&source, target_group, target_owner, revision)?;
        let source_voters = self
            .group(source.group_id)
            .ok_or(Rejection::NotFound)?
            .placement
            .raft_voters()
            .ok_or(Rejection::Unsupported)?
            .clone();
        let target_voters = self
            .group(target_group)
            .ok_or(Rejection::NotFound)?
            .placement
            .raft_voters()
            .ok_or(Rejection::Unsupported)?
            .clone();
        let plan = AuthorityTransfer {
            id,
            shard,
            partition,
            layout_revision,
            binding_generation: self
                .topology
                .binding(partition)
                .ok_or(Rejection::NotFound)?
                .generation,
            source,
            target,
            source_voters,
            target_voters,
            checkpoint_voters: self
                .group(target_group)
                .ok_or(Rejection::NotFound)?
                .placement
                .raft_voters()
                .ok_or(Rejection::Unsupported)?
                .clone(),
            stage: TransferStage::Prepare,
            frozen_cut: None,
            manifest: None,
            imported: None,
            checkpoints: BTreeMap::new(),
            retired_cut: None,
        };
        plan.validate()?;
        self.check_transfer_membership(&plan)?;
        self.topology.transfers.insert(id, plan.clone());
        Ok(plan)
    }

    fn transfer_source(&self, shard: Id, revision: u64, target: u64) -> Result<Grant, Rejection> {
        let birth = self.topology.shard(shard).ok_or(Rejection::NotFound)?;
        let partition = birth.partition;
        if self
            .quotas
            .allocation(partition)
            .is_some_and(|record| record.pending.is_some())
        {
            return Err(Rejection::RecoveryRequired);
        }
        let layout = self
            .topology
            .layout(partition.topic_id)
            .ok_or(Rejection::NotFound)?;
        let source = self.grant(partition).ok_or(Rejection::RecoveryRequired)?;
        if self
            .subscriptions
            .values()
            .any(|row| row.topic_id == partition.topic_id && row.status == RecordStatus::Active)
        {
            return Err(Rejection::InvalidSubscription);
        }
        if !self.topic_writable(partition.topic_id)
            || self.route(partition).is_none()
            || source.group_id == target
            || layout.revision != revision
            || birth.route.group != source.group_id
        {
            return Err(Rejection::GenerationConflict);
        }
        if self.topology.transfers.len() >= MAX_AUTHORITY_TRANSFERS {
            return Err(Rejection::Capacity);
        }
        if self
            .topology
            .pending_transfers()
            .any(|plan| plan.partition.topic_id == partition.topic_id)
            || self
                .topology
                .pending()
                .any(|plan| plan.topic == partition.topic_id)
            || self.consumer_subscriptions.values().any(|row| {
                row.topic == partition.topic_id && (!row.members.plan().active || row.is_deleting())
            })
        {
            return Err(Rejection::RecoveryRequired);
        }
        Ok(source.clone())
    }

    fn transfer_target(
        &self,
        source: &Grant,
        group: u64,
        owner: u64,
        revision: u64,
    ) -> Result<Grant, Rejection> {
        self.node(owner)
            .filter(|node| node.spec.schedulable && node.spec.roles.contains(&NodeRole::Broker))
            .ok_or(Rejection::NotSchedulable)?;
        let voters = self
            .group(group)
            .ok_or(Rejection::NotFound)?
            .placement
            .raft_voters()
            .ok_or(Rejection::Unsupported)?;
        if voters.iter().any(|node| {
            self.node(*node).is_none_or(|node| {
                !node.spec.schedulable || !node.spec.roles.contains(&NodeRole::Storage)
            })
        }) {
            return Err(Rejection::NotSchedulable);
        }
        let epoch = source
            .epoch
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let expected_route_generation = source
            .expected_route_generation
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        Ok(Grant {
            cluster_id: source.cluster_id,
            partition: source.partition,
            group_id: group,
            operation_id: super::grant_operation_id(
                source.cluster_id,
                source.partition,
                epoch,
                owner,
                expected_route_generation,
            )?,
            epoch,
            owner_node: owner,
            expected_route_generation,
            metadata_revision: revision,
        })
    }

    fn check_transfer_membership(&self, plan: &AuthorityTransfer) -> Result<(), Rejection> {
        if self
            .group(plan.source.group_id)
            .is_none_or(|group| group.placement.raft_voters() != Some(&plan.source_voters))
            || plan.stage < TransferStage::Published
                && self
                    .group(plan.target.group_id)
                    .is_none_or(|group| group.placement.raft_voters() != Some(&plan.target_voters))
            || plan.stage == TransferStage::Published
                && self.group(plan.target.group_id).is_none_or(|group| {
                    group.placement.raft_voters() != Some(&plan.checkpoint_voters)
                })
            || [plan.source.group_id, plan.target.group_id]
                .iter()
                .any(|group| {
                    (*group == plan.source.group_id || plan.stage < TransferStage::Checkpointed)
                        && self
                            .membership_change(*group)
                            .is_some_and(|record| record.completed.is_none())
                })
        {
            return Err(Rejection::PlacementViolation);
        }
        Ok(())
    }

    fn publish_transfer(
        &mut self,
        plan: &AuthorityTransfer,
        revision: u64,
    ) -> Result<(), Rejection> {
        let mut binding = self
            .topology
            .binding(plan.partition)
            .cloned()
            .ok_or(Rejection::NotFound)?;
        binding.move_to(plan.binding_generation, plan.target.group_id)?;
        let mut birth = self
            .topology
            .shard(plan.shard)
            .cloned()
            .ok_or(Rejection::NotFound)?;
        let mut layout = self
            .topology
            .layout(plan.partition.topic_id)
            .cloned()
            .ok_or(Rejection::NotFound)?;
        if layout.revision != plan.layout_revision {
            return Err(Rejection::GenerationConflict);
        }
        birth.route.group = plan.target.group_id;
        birth.route.authority_epoch = binding.generation;
        if let Some(route) = layout
            .shards
            .iter_mut()
            .find(|shard| shard.id == plan.shard)
        {
            *route = birth.route.clone();
        }
        layout.revision = layout
            .revision
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        let record = transfer_partition_record(plan, revision)?;
        let index: Vec<_> = record
            .control_decisions
            .iter()
            .map(|(stage, decision)| {
                (
                    decision.operation_id,
                    ControlLocation {
                        partition: plan.partition,
                        stage: *stage,
                    },
                )
            })
            .collect();
        self.control_operations
            .retain(|_, location| location.partition != plan.partition);
        self.control_operations.extend(index);
        self.partitions.insert(plan.partition, record);
        self.topology.bindings.insert(plan.partition, binding);
        self.topology.shards.insert(plan.shard, birth);
        self.topology.layouts.insert(layout.topic, layout);
        Ok(())
    }
}

fn transfer_partition_record(
    plan: &AuthorityTransfer,
    revision: u64,
) -> Result<PartitionRecord, Rejection> {
    let imported = plan.imported.as_ref().ok_or(Rejection::RecoveryRequired)?;
    let route = Route {
        partition: plan.partition,
        group_id: plan.target.group_id,
        owner_node: plan.target.owner_node,
        epoch: plan.target.epoch,
        generation: plan
            .target
            .expected_route_generation
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?,
        ownership_operation: plan.target.operation_id,
    };
    let fence = FenceEvidence {
        placement: imported.recovery.placement.clone(),
        fence: imported.recovery.recovered.fence.clone(),
        committed: imported.recovery.committed,
    };
    let slots = transfer_control_slots(plan, &route, &fence, revision)?;
    Ok(PartitionRecord {
        group_id: plan.target.group_id,
        highest_epoch: plan.target.epoch,
        route_generation: route.generation,
        grant: Some(plan.target.clone()),
        phase: OwnershipPhase::Active,
        fence: Some(fence),
        recovery: Some(imported.recovery.clone()),
        route: Some(route),
        control_decisions: slots,
    })
}

fn transfer_control_slots(
    plan: &AuthorityTransfer,
    route: &Route,
    fence: &FenceEvidence,
    revision: u64,
) -> Result<BTreeMap<ControlStage, Decision>, Rejection> {
    let imported = plan.imported.as_ref().ok_or(Rejection::RecoveryRequired)?;
    let grant = &plan.target;
    let actions = [
        (
            ControlStage::Grant,
            Action::GrantOwnership {
                partition: plan.partition,
                owner_node: grant.owner_node,
                expected_epoch: grant
                    .epoch
                    .checked_sub(1)
                    .ok_or(Rejection::CounterExhausted)?,
                expected_route_generation: grant.expected_route_generation,
            },
            Outcome::OwnershipGranted(grant.clone()),
        ),
        (
            ControlStage::Fence,
            Action::ObserveFence(fence.clone()),
            Outcome::FenceObserved,
        ),
        (
            ControlStage::Recovery,
            Action::ObserveRecovery(imported.recovery.clone()),
            Outcome::RecoveryObserved,
        ),
        (
            ControlStage::Activate,
            Action::ActivateRoute(imported.recovery.clone()),
            Outcome::RouteActivated(route.clone()),
        ),
    ];
    actions
        .into_iter()
        .map(|(stage, action, outcome)| {
            let operation_id = action.expected_control_id(grant.cluster_id)?;
            Ok((
                stage,
                Decision {
                    operation_id,
                    digest: action.logical_digest()?,
                    management_digest: None,
                    response: Response::Applied {
                        revision: if stage == ControlStage::Grant {
                            grant.metadata_revision
                        } else {
                            revision
                        },
                        outcome,
                    },
                    proof_position: (stage != ControlStage::Grant)
                        .then_some(imported.recovery.committed),
                },
            ))
        })
        .collect()
}
