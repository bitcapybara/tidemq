use super::{
    CapacitySource, Error, Flight, MembershipApplication, MembershipPlan, Query, Reply, Result,
    Rpc, Service, wire,
};
use crate::scaling::{MembershipEvidence, Stage};
use openraft::RaftSnapshotBuilder;
use std::{collections::BTreeSet, io::SeekFrom, time::Duration};
use tokio::{io::AsyncSeekExt, sync::oneshot, time::Instant};

impl<R: Rpc, C: CapacitySource, A: MembershipApplication> Service<R, C, A> {
    pub(super) async fn estimate(&self, plan: &MembershipPlan, deadline: Instant) -> Result<Reply> {
        let node = self
            .replicas
            .data
            .get(&plan.group_id)
            .cloned()
            .ok_or(Error::Authority)?;
        let permit = self
            .estimates
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let (sender, receiver) = oneshot::channel();
        let plan = plan.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let result = async {
                node.raft
                    .ensure_linearizable()
                    .await
                    .map_err(|source| Error::Quorum {
                        source: Box::new(source),
                    })?;
                node.state
                    .try_with_membership(|application, position, membership| {
                        let voters: BTreeSet<_> = membership.membership().voter_ids().collect();
                        let membership_log = (*membership.log_id()).ok_or(Error::Authority)?;
                        let applied = position.ok_or(Error::Authority)?;
                        if application.membership_identity()
                            != Some((plan.cluster_id, plan.group_id))
                            || membership.membership().get_joint_config().len() != 1
                            || voters != plan.previous
                            || applied.index < membership_log.index
                        {
                            return Err(Error::Authority);
                        }
                        Ok(())
                    })
                    .await
                    .map_err(|source| Error::Storage { source })??;
                let mut state = node.state.clone();
                let mut snapshot =
                    state
                        .build_snapshot()
                        .await
                        .map_err(|source| Error::Snapshot {
                            source: Box::new(source),
                        })?;
                let length = snapshot
                    .snapshot
                    .seek(SeekFrom::End(0))
                    .await
                    .map_err(|source| Error::SnapshotIo { source })?;
                let disk = node.log.diagnostics().await;
                let directory = node.log.0.directory.clone();
                let physical = tokio::task::spawn_blocking(move || {
                    crate::runtime::physical::replica_bytes(&directory, true)
                })
                .await
                .map_err(|source| Error::Worker { source })?
                .map_err(|source| Error::Remote {
                    source: Box::new(source),
                })?;
                // Include real snapshot/replica files and bounded future snapshot, rewrite and tail growth.
                let copy_bytes = length
                    .checked_mul(2)
                    .and_then(|value| value.checked_add(physical))
                    .and_then(|value| {
                        value.checked_add(disk.journal_file_limit_bytes.checked_mul(2)?)
                    })
                    .and_then(|value| {
                        value
                            .checked_add((disk.snapshot_payload_limit_bytes as u64).checked_mul(2)?)
                    })
                    .and_then(|value| value.checked_add(64 * 1024))
                    .ok_or(Error::Placement)?;
                Ok(Reply::Estimate { copy_bytes })
            }
            .await;
            let _ = sender.send(result);
        });
        tokio::time::timeout_at(deadline, receiver)
            .await
            .map_err(|_| Error::Unknown)?
            .map_err(|_| Error::Unknown)?
    }

    pub(super) async fn change(&self, plan: &MembershipPlan, deadline: Instant) -> Result<Reply> {
        let node = self
            .replicas
            .data
            .get(&plan.group_id)
            .ok_or(Error::Authority)?;
        let slot = self.slot(plan.group_id)?;
        let mut flight = slot.try_lock().map_err(|_| Error::Overloaded)?;
        if flight.as_ref().is_some_and(|old| old.plan != *plan) {
            return Err(Error::Authority);
        }
        let view = self.fresh_view(plan, deadline).await?;
        let prepared = view
            .migration
            .as_ref()
            .filter(|record| record.plan == *plan)
            .ok_or(Error::Authority)?;
        if prepared.is_complete() {
            self.observe(plan, deadline).await?;
            *flight = None;
            return Ok(Reply::Done);
        }
        let targets = self.target_nodes(&view, plan)?;
        if flight.is_none() {
            let target = plan
                .target
                .difference(&plan.previous)
                .next()
                .copied()
                .ok_or(Error::Configuration)?;
            let repair = self.budget.try_register(target, plan.group_id)?;
            *flight = Some(Flight {
                plan: plan.clone(),
                repair,
                started: Instant::now(),
            });
        }
        tokio::time::timeout_at(deadline, node.raft.ensure_linearizable())
            .await
            .map_err(|_| Error::Unknown)?
            .map_err(|source| Error::Quorum {
                source: Box::new(source),
            })?;
        let actual = node
            .state
            .try_with_membership(|application, _, membership| {
                if application.membership_identity() != Some((plan.cluster_id, plan.group_id)) {
                    return Err(Error::Authority);
                }
                Ok(membership.clone())
            })
            .await
            .map_err(|source| Error::Storage { source })??;
        let configs = actual.membership().get_joint_config();
        let old = configs.first() == Some(&plan.previous);
        let target = configs.last() == Some(&plan.target);
        if configs.len() == 1 && target {
            flight
                .as_mut()
                .ok_or(Error::Configuration)?
                .repair
                .set_learner(false)?;
            return Ok(Reply::Done);
        }
        if !(configs.len() == 1 && old || configs.len() == 2 && old && target) {
            return Err(Error::Authority);
        }
        if configs.len() == 1 {
            flight
                .as_mut()
                .ok_or(Error::Configuration)?
                .repair
                .set_learner(true)?;
            let started = Instant::now();
            for id in plan.target.difference(&plan.previous) {
                let target = targets.get(id).cloned().ok_or(Error::Authority)?;
                tokio::time::timeout_at(deadline, node.raft.add_learner(*id, target, true))
                    .await
                    .map_err(|_| Error::Unknown)?
                    .map_err(|source| Error::Consensus {
                        source: Box::new(source),
                    })?;
            }
            self.metrics.observe(Stage::Learner, started.elapsed());
        }
        let refreshed = self.fresh_view(plan, deadline).await?;
        self.target_nodes(&refreshed, plan)?;
        flight
            .as_mut()
            .ok_or(Error::Configuration)?
            .repair
            .set_learner(false)?;
        let started = Instant::now();
        tokio::time::timeout_at(
            deadline,
            node.raft.change_membership(plan.target.clone(), false),
        )
        .await
        .map_err(|_| Error::Unknown)?
        .map_err(|source| Error::Consensus {
            source: Box::new(source),
        })?;
        self.metrics.observe(Stage::Membership, started.elapsed());
        Ok(Reply::Done)
    }

    pub(super) async fn observe(
        &self,
        plan: &MembershipPlan,
        deadline: Instant,
    ) -> Result<wire::Observation> {
        let node = self
            .replicas
            .data
            .get(&plan.group_id)
            .ok_or(Error::Authority)?;
        let expires = (Instant::now() + Duration::from_secs(3)).min(deadline);
        tokio::time::timeout_at(expires, async {
            node.raft
                .ensure_linearizable()
                .await
                .map_err(|source| Error::Quorum {
                    source: Box::new(source),
                })?;
            node.state
                .try_with_membership(|application, position, membership| {
                    let voters: BTreeSet<_> = membership.membership().voter_ids().take(4).collect();
                    if application.membership_identity() != Some((plan.cluster_id, plan.group_id))
                        || membership.membership().get_joint_config().len() != 1
                        || voters != plan.target
                    {
                        return Err(Error::Authority);
                    }
                    let membership_log = (*membership.log_id()).ok_or(Error::Authority)?;
                    let applied_log = position.ok_or(Error::Authority)?;
                    if applied_log.index < membership_log.index {
                        return Err(Error::Authority);
                    }
                    Ok(wire::Observation {
                        source: self.config.node_id,
                        voters,
                        evidence: MembershipEvidence {
                            plan: plan.clone(),
                            membership_log,
                            applied_log,
                        },
                    })
                })
                .await
                .map_err(|source| Error::Storage { source })?
        })
        .await
        .map_err(|_| Error::Authority)?
    }

    pub(super) async fn finish(&self, plan: &MembershipPlan, deadline: Instant) -> Result<Reply> {
        let started = Instant::now();
        let (source, reply) = self
            .call_leader(
                self.config.metadata_group,
                Query::View { plan: plan.clone() },
                deadline,
            )
            .await?;
        let Reply::View(view) = reply else {
            return Err(Error::Authority);
        };
        if started.elapsed() >= Duration::from_secs(3) {
            return Err(Error::Authority);
        }
        match self.validate_view(plan, source, &view) {
            Ok(()) => {
                if view
                    .migration
                    .as_ref()
                    .is_none_or(|record| record.plan != *plan || !record.is_complete())
                {
                    return Err(Error::Authority);
                }
            }
            Err(Error::Rejected {
                reason: crate::metadata::Rejection::Superseded,
            }) if view.group.generation > plan.generation => {}
            Err(error) => return Err(error),
        }
        let slot = self.slot(plan.group_id)?;
        let mut flight = slot.try_lock().map_err(|_| Error::Overloaded)?;
        if flight.as_ref().is_some_and(|entry| entry.plan == *plan)
            && let Some(entry) = flight.take()
        {
            self.metrics
                .observe(Stage::ReplicaRepair, entry.started.elapsed());
        }
        Ok(Reply::Done)
    }

    fn slot(&self, group: u64) -> Result<std::sync::Arc<tokio::sync::Mutex<Option<Flight>>>> {
        if !self.replicas.data.contains_key(&group) {
            return Err(Error::Authority);
        }
        let mut slots = self.flights.lock().map_err(|_| Error::Poisoned)?;
        if slots.len() >= 256 && !slots.contains_key(&group) {
            return Err(Error::Overloaded);
        }
        Ok(slots
            .entry(group)
            .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(None)))
            .clone())
    }
}
