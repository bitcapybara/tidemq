use super::{ClusterOutcome, MetadataState, Outcome, Rejection, Response};
use crate::scaling::MembershipPlan;

impl MetadataState {
    /// Saved operator intents retain their canonical target before native preparation starts.
    pub fn pending_data_replacements(&self) -> impl Iterator<Item = &MembershipPlan> {
        self.decisions
            .values()
            .filter_map(|decision| match &decision.response {
                Response::Applied {
                    outcome: Outcome::Cluster(result),
                    ..
                } => match &result.outcome {
                    ClusterOutcome::DataChange(plan) => Some(plan),
                    _ => None,
                },
                _ => None,
            })
            .filter(|plan| {
                self.groups
                    .get(&plan.group_id())
                    .is_some_and(|group| group.generation == plan.generation())
            })
    }
    pub fn data_replacement_complete(&self, plan: &MembershipPlan) -> bool {
        self.groups
            .get(&plan.group_id())
            .is_some_and(|group| group.generation > plan.generation())
    }
    pub(super) fn validate_data_replacement(&self, plan: &MembershipPlan) -> Result<(), Rejection> {
        plan.validate()?;
        if self.cluster_id != Some(plan.cluster_id) {
            return Err(Rejection::InvalidGroup);
        }
        let group = self
            .groups
            .get(&plan.group_id())
            .ok_or(Rejection::NotFound)?;
        if group.generation > plan.generation() {
            return Err(Rejection::Superseded);
        }
        if group.generation != plan.generation() || group.voters != *plan.previous() {
            return Err(Rejection::GenerationConflict);
        }
        if self
            .pending_data_replacements()
            .any(|pending| pending.group_id() == plan.group_id() && pending != plan)
            || self
                .membership_change(plan.group_id())
                .is_some_and(|change| !change.is_complete() && change.plan != *plan)
        {
            return Err(Rejection::OperationConflict);
        }
        if self.topology.pending_transfers().any(|transfer| {
            transfer.source.group_id == plan.group_id()
                || transfer.target.group_id == plan.group_id()
                    && transfer.stage < super::TransferStage::Published
        }) {
            return Err(Rejection::RecoveryRequired);
        }
        self.validate_placement(plan.target(), false)?;
        for added in plan.target().difference(plan.previous()) {
            if self.node(*added).is_none_or(|node| !node.spec.schedulable)
                || self
                    .node_identity(*added)
                    .is_some_and(|identity| identity.retired)
            {
                return Err(Rejection::NotSchedulable);
            }
        }
        Ok(())
    }
    pub(super) fn validate_data_replacement_history(
        &self,
        plan: &MembershipPlan,
    ) -> Result<(), Rejection> {
        plan.validate()?;
        let group = self.group(plan.group_id()).ok_or(Rejection::InvalidGroup)?;
        if self.cluster_id != Some(plan.cluster_id)
            || group.generation < plan.generation()
            || group.generation == plan.generation() && group.voters != *plan.previous()
        {
            return Err(Rejection::InvalidEvidence);
        }
        self.validate_placement(plan.previous(), false)?;
        self.validate_placement(plan.target(), false)
    }
}
