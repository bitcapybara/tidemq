use crate::scaling::{MembershipEvidence, MembershipPlan, MigrationRecord};

use super::{Action, Command, MetadataState, Outcome, Rejection, Response};

impl Command {
    pub fn prepare_membership(
        expected_revision: u64,
        plan: MembershipPlan,
    ) -> Result<Self, Rejection> {
        Self::prepare_membership_with_capacity(expected_revision, plan, 0)
    }

    pub fn prepare_membership_with_capacity(
        expected_revision: u64,
        plan: MembershipPlan,
        copy_bytes: u64,
    ) -> Result<Self, Rejection> {
        plan.validate()?;
        Ok(Self::new(
            plan.operation_id(),
            expected_revision,
            Action::PrepareMembership { plan, copy_bytes },
        ))
    }
}

impl MetadataState {
    pub fn membership_change(&self, group: u64) -> Option<&MigrationRecord> {
        self.migrations.get(&group)
    }

    pub(super) fn apply_membership(&mut self, command: &Command) -> Response {
        let (plan, complete) = match &command.action {
            Action::PrepareMembership { plan, .. } => (plan, false),
            Action::CompleteMembership(proof) => (&proof.plan, true),
            _ => return self.reject(Rejection::InvalidCommand),
        };
        if let Err(reason) = plan.validate() {
            return self.reject(reason);
        }
        if self.cluster_id != Some(plan.cluster_id)
            || plan.derived_id(complete) != Ok(command.operation_id)
        {
            return self.reject(Rejection::OperationConflict);
        }
        if let Some(record) = self.migrations.get(&plan.group_id) {
            if record.plan == *plan {
                if !complete {
                    if let Action::PrepareMembership { copy_bytes, .. } = command.action
                        && copy_bytes != record.copy_bytes
                    {
                        return self.reject(Rejection::OperationConflict);
                    }
                    return Response::Applied {
                        revision: record.prepared_revision,
                        outcome: Outcome::MembershipPrepared(plan.clone()),
                    };
                }
                if let (Some((revision, previous)), Action::CompleteMembership(proof)) =
                    (&record.completed, &command.action)
                {
                    if validate_evidence(proof).is_err()
                        || proof.membership_log != previous.membership_log
                        || proof.applied_log.index < previous.applied_log.index
                        || proof.applied_log.leader_id.term < previous.applied_log.leader_id.term
                    {
                        return self.reject(Rejection::InvalidEvidence);
                    }
                    return Response::Applied {
                        revision: *revision,
                        outcome: Outcome::MembershipCompleted(plan.clone()),
                    };
                }
            } else if !record.is_complete() {
                return self.reject(Rejection::OperationConflict);
            }
        }
        if self
            .group(plan.group_id)
            .is_some_and(|group| group.generation > plan.generation)
        {
            return self.reject(Rejection::Superseded);
        }
        self.apply_new(command)
    }

    pub(super) fn prepare_membership(
        &mut self,
        plan: &MembershipPlan,
        copy_bytes: u64,
        revision: u64,
    ) -> Result<Outcome, Rejection> {
        let current = self.groups.get(&plan.group_id).ok_or(Rejection::NotFound)?;
        if current.generation > plan.generation {
            return Err(Rejection::Superseded);
        }
        if current.generation != plan.generation || current.voters != plan.previous {
            return Err(Rejection::GenerationConflict);
        }
        self.validate_placement(&plan.target, false)?;
        for added in plan.target.difference(&plan.previous) {
            if self.node(*added).is_none_or(|node| !node.spec.schedulable) {
                return Err(Rejection::NotSchedulable);
            }
        }
        self.migrations.insert(
            plan.group_id,
            MigrationRecord {
                plan: plan.clone(),
                prepared_revision: revision,
                copy_bytes,
                completed: None,
            },
        );
        Ok(Outcome::MembershipPrepared(plan.clone()))
    }

    pub(super) fn complete_membership(
        &mut self,
        evidence: &MembershipEvidence,
        revision: u64,
    ) -> Result<Outcome, Rejection> {
        let plan = &evidence.plan;
        let record = self
            .migrations
            .get(&plan.group_id)
            .ok_or(Rejection::NotFound)?;
        if record.plan != *plan || record.is_complete() {
            return Err(Rejection::Superseded);
        }
        validate_evidence(evidence)?;
        self.validate_placement(&plan.target, false)?;
        let group = self
            .groups
            .get_mut(&plan.group_id)
            .ok_or(Rejection::NotFound)?;
        if group.generation != plan.generation || group.voters != plan.previous {
            return Err(Rejection::GenerationConflict);
        }
        group.generation = plan
            .generation
            .checked_add(1)
            .ok_or(Rejection::CounterExhausted)?;
        group.voters = plan.target.clone();
        let record = self
            .migrations
            .get_mut(&plan.group_id)
            .ok_or(Rejection::NotFound)?;
        record.completed = Some((revision, evidence.clone()));
        Ok(Outcome::MembershipCompleted(plan.clone()))
    }

    pub(super) fn validate_migrations(&self) -> Result<(), Rejection> {
        if self.migrations.len() > self.groups.len() {
            return Err(Rejection::Capacity);
        }
        for (id, record) in &self.migrations {
            record.plan.validate()?;
            let plan = &record.plan;
            self.validate_placement(&plan.previous, false)?;
            self.validate_placement(&plan.target, false)?;
            let group = self.group(*id).ok_or(Rejection::InvalidGroup)?;
            if *id != plan.group_id
                || self.cluster_id != Some(plan.cluster_id)
                || record.prepared_revision == 0
                || record.prepared_revision > self.revision
            {
                return Err(Rejection::InvalidEvidence);
            }
            let (generation, voters) = if let Some((revision, proof)) = &record.completed {
                validate_evidence(proof)?;
                if proof.plan != *plan
                    || *revision <= record.prepared_revision
                    || *revision > self.revision
                {
                    return Err(Rejection::InvalidEvidence);
                }
                (
                    plan.generation
                        .checked_add(1)
                        .ok_or(Rejection::CounterExhausted)?,
                    &plan.target,
                )
            } else {
                (plan.generation, &plan.previous)
            };
            if group.generation != generation || group.voters != *voters {
                return Err(Rejection::InvalidEvidence);
            }
        }
        Ok(())
    }

    pub(super) fn validate_recorded_evidence_identity(
        &self,
        grant: &crate::ownership::Grant,
        position: crate::ownership::CommitPosition,
    ) -> Result<(), Rejection> {
        if !position.validate()
            || self.cluster_id != Some(grant.cluster_id)
            || !self.groups.contains_key(&grant.group_id)
            || self
                .node(position.leader_node)
                .is_none_or(|node| !node.spec.roles.contains(&super::NodeRole::Storage))
        {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(())
    }
}

fn validate_evidence(proof: &MembershipEvidence) -> Result<(), Rejection> {
    if proof.membership_log.index > proof.applied_log.index
        || proof.membership_log.leader_id.term == 0
        || proof.membership_log.leader_id.node_id == 0
        || !proof
            .plan
            .previous
            .union(&proof.plan.target)
            .any(|id| *id == proof.membership_log.leader_id.node_id)
        || proof.applied_log.leader_id.term < proof.membership_log.leader_id.term
    {
        return Err(Rejection::InvalidEvidence);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DeploymentMode, MetadataLimits, NodeRole, NodeSpec};
    use crate::ownership::{CommitPosition, FenceEvidence, FenceRecord};
    use openraft::{CommittedLeaderId, LogId};
    use std::collections::BTreeSet;
    use tidemq_protocol::Id;

    fn fixture(operations: usize) -> MetadataState {
        let mut state = MetadataState::default();
        let limits = MetadataLimits {
            operations,
            ..MetadataLimits::default()
        };
        assert!(
            state
                .apply(Command::bootstrap(
                    Id::random().unwrap(),
                    Id::random().unwrap(),
                    77,
                    DeploymentMode::ThreeReplica,
                    limits
                ))
                .is_applied()
        );
        for id in 1..=4 {
            let spec = NodeSpec {
                node_id: id,
                zone: format!("zone-{}", if id == 4 { 3 } else { id }),
                endpoint: format!("127.0.0.1:{}", 7400 + id).parse().unwrap(),
                roles: BTreeSet::from([NodeRole::Storage, NodeRole::Metadata, NodeRole::Broker]),
                schedulable: true,
            };
            assert!(
                state
                    .apply(Command::put_node(
                        Id::random().unwrap(),
                        state.revision(),
                        spec,
                        0
                    ))
                    .is_applied()
            );
        }
        assert!(
            state
                .apply(Command::create_group(
                    Id::random().unwrap(),
                    state.revision(),
                    2,
                    BTreeSet::from([1, 2, 3])
                ))
                .is_applied()
        );
        assert!(
            state
                .apply(Command::create_topic(
                    Id::random().unwrap(),
                    state.revision(),
                    Id::random().unwrap(),
                    "orders".into(),
                    vec![2]
                ))
                .is_applied()
        );
        state
    }

    fn proof(plan: MembershipPlan, index: u64) -> MembershipEvidence {
        let position = LogId::new(CommittedLeaderId::new(1, 1), index);
        MembershipEvidence {
            plan,
            membership_log: position,
            applied_log: position,
        }
    }

    #[test]
    fn fixed_migration_slot_retries_after_crud_exhaustion_and_retires_old_generations() {
        let mut state = fixture(7);
        let mut first = None;
        for _ in 0..20 {
            let current = state.group(2).unwrap();
            let target = if current.voters.contains(&3) {
                BTreeSet::from([1, 2, 4])
            } else {
                BTreeSet::from([1, 2, 3])
            };
            let plan = MembershipPlan::new(state.cluster_id().unwrap(), current, target).unwrap();
            let prepare = Command::prepare_membership(state.revision(), plan.clone()).unwrap();
            let original = state.apply(prepare.clone());
            assert!(original.is_applied());
            assert_eq!(state.apply(prepare.clone()), original);
            let complete = Command::new(
                plan.derived_id(true).unwrap(),
                state.revision(),
                Action::CompleteMembership(proof(plan.clone(), 100)),
            );
            let completed = state.apply(complete.clone());
            assert!(completed.is_applied());
            assert_eq!(state.apply(complete), completed);
            state.validate_snapshot().unwrap();
            if first.is_none() {
                first = Some(prepare);
            }
        }
        assert_eq!(state.migrations.len(), 1);
        assert_eq!(state.decisions.len(), 7);
        let old = first.unwrap();
        assert!(matches!(
            state.apply(old),
            Response::Rejected {
                reason: Rejection::Superseded,
                ..
            }
        ));
        let encoded = postcard::to_stdvec(&state).unwrap();
        let reopened: MetadataState = postcard::from_bytes(&encoded).unwrap();
        reopened.validate_snapshot().unwrap();
        assert_eq!(state, reopened);
    }

    #[test]
    fn membership_changes_preserve_historical_fences_and_storage_identity() {
        let mut state = fixture(8);
        let topic = state.topics.keys().next().copied().unwrap();
        let partition = crate::metadata::PartitionKey {
            topic_id: topic,
            partition: 0,
        };
        assert!(
            state
                .apply(
                    Command::grant_ownership(
                        state.revision(),
                        state.cluster_id().unwrap(),
                        partition,
                        1,
                        0,
                        0
                    )
                    .unwrap()
                )
                .is_applied()
        );
        let grant = state.grant(partition).unwrap().clone();
        let evidence = FenceEvidence {
            fence: FenceRecord {
                grant: grant.clone(),
                fence_generation: 1,
            },
            committed: CommitPosition {
                term: 1,
                leader_node: 3,
                index: 10,
            },
        };
        let operation =
            super::super::stage_operation_id(&grant, super::super::ControlStage::Fence).unwrap();
        assert!(
            state
                .apply(Command::new(
                    operation,
                    state.revision(),
                    Action::ObserveFence(evidence)
                ))
                .is_applied()
        );
        let plan = MembershipPlan::new(
            state.cluster_id().unwrap(),
            state.group(2).unwrap(),
            BTreeSet::from([1, 2, 4]),
        )
        .unwrap();
        assert!(
            state
                .apply(Command::prepare_membership(state.revision(), plan.clone()).unwrap())
                .is_applied()
        );
        assert!(
            state
                .apply(Command::new(
                    plan.derived_id(true).unwrap(),
                    state.revision(),
                    Action::CompleteMembership(proof(plan, 20))
                ))
                .is_applied()
        );
        state.validate_snapshot().unwrap();
        let mut retired = state.node(3).unwrap().spec.clone();
        retired.roles.remove(&NodeRole::Storage);
        assert!(matches!(
            state.apply(Command::put_node(
                Id::random().unwrap(),
                state.revision(),
                retired,
                1
            )),
            Response::Rejected {
                reason: Rejection::NodeInUse,
                ..
            }
        ));
        state.validate_snapshot().unwrap();
    }
}
