use std::collections::{BTreeMap, BTreeSet};

use tidemq_protocol::{Id, topic::TopicLayout};
use tokio::time::Instant;

use super::{Diagnostic, Domain, Evaluation, Scan, Status, TopicScalingPolicy};
use crate::runtime::{Error, Result, service::Service, wire};
use crate::{
    metadata::{self, Command, DataGroup, LayoutChange, NodeRole},
    scaling::{
        Placement, PlacementPolicy,
        remote::{Query, Reply, Rpc},
    },
};

impl Service {
    pub(super) async fn decide_topic_layout(
        &self,
        policy: &TopicScalingPolicy,
        scan: &Scan,
        evaluations: &BTreeMap<Id, Evaluation>,
        deadline: Instant,
    ) -> Result<Option<Diagnostic>> {
        for layout in &scan.layouts {
            if let Some(decision) = self
                .split_topic(policy, scan, evaluations, layout, deadline)
                .await?
            {
                return Ok(Some(decision));
            }
            if let Some(decision) = self
                .merge_topic(policy, scan, evaluations, layout, deadline)
                .await?
            {
                return Ok(Some(decision));
            }
        }
        Ok(None)
    }

    async fn split_topic(
        &self,
        policy: &TopicScalingPolicy,
        scan: &Scan,
        evaluations: &BTreeMap<Id, Evaluation>,
        layout: &TopicLayout,
        deadline: Instant,
    ) -> Result<Option<Diagnostic>> {
        for shard in &layout.shards {
            let Some(evaluation) = evaluations.get(&shard.id).filter(|row| row.split.is_some())
            else {
                continue;
            };
            let diagnostic = Diagnostic {
                topic: Some(layout.topic),
                shard: Some(shard.id),
                observed_domains: scan.domains.len(),
                ..Default::default()
            };
            if layout.shards.len() >= policy.max_shards {
                return Ok(Some(Diagnostic {
                    status: Status::LayoutLimit,
                    ..diagnostic
                }));
            }
            let domain = scan
                .domains
                .iter()
                .find(|domain| domain.shard.route.id == shard.id)
                .ok_or_else(Error::unavailable)?;
            let mut incoming = evaluation.rates;
            for rate in incoming.iter_mut().take(2) {
                *rate = scaled(*rate, evaluation.split_share)?;
            }
            let Some(target) = self
                .split_capacity(policy, scan, evaluations, domain, incoming, deadline)
                .await?
            else {
                return Ok(Some(Diagnostic {
                    status: Status::StorageCapacityRequired,
                    required_independent_groups: 1,
                    ..diagnostic
                }));
            };
            let change = LayoutChange::Split {
                source: shard.id,
                at: evaluation.split.ok_or_else(Error::unavailable)?,
                groups: [domain.group, target.group_id],
            };
            return self
                .apply_topic_decision(layout, change, diagnostic, scan, deadline)
                .await
                .map(Some);
        }
        Ok(None)
    }

    async fn merge_topic(
        &self,
        policy: &TopicScalingPolicy,
        scan: &Scan,
        evaluations: &BTreeMap<Id, Evaluation>,
        layout: &TopicLayout,
        deadline: Instant,
    ) -> Result<Option<Diagnostic>> {
        if layout.shards.len() <= policy.min_shards {
            return Ok(None);
        }
        for pair in layout.shards.windows(2) {
            let Some((left, right)) = pair.first().zip(pair.get(1)) else {
                continue;
            };
            let Some(left_load) = evaluations.get(&left.id).and_then(|row| row.low_peak) else {
                continue;
            };
            let Some(right_load) = evaluations.get(&right.id).and_then(|row| row.low_peak) else {
                continue;
            };
            let left_domain = scan
                .domains
                .iter()
                .find(|domain| domain.shard.route.id == left.id)
                .ok_or_else(Error::unavailable)?;
            let right_domain = scan
                .domains
                .iter()
                .find(|domain| domain.shard.route.id == right.id)
                .ok_or_else(Error::unavailable)?;
            if !policy.low(&combined(&left_load, &right_load)?)
                || !merge_group_low(
                    policy,
                    scan,
                    evaluations,
                    left_domain.group,
                    right_domain.group,
                    &right_load,
                )?
            {
                continue;
            }
            if !self
                .group_capacity(left_domain.group, right_load, deadline)
                .await
                .unwrap_or(false)
            {
                continue;
            }
            let change = LayoutChange::Merge {
                left: left.id,
                right: right.id,
                group: left_domain.group,
            };
            let diagnostic = Diagnostic {
                topic: Some(layout.topic),
                shard: Some(left.id),
                observed_domains: scan.domains.len(),
                ..Default::default()
            };
            return self
                .apply_topic_decision(layout, change, diagnostic, scan, deadline)
                .await
                .map(Some);
        }
        Ok(None)
    }

    async fn split_capacity(
        &self,
        policy: &TopicScalingPolicy,
        scan: &Scan,
        evaluations: &BTreeMap<Id, Evaluation>,
        source: &Domain,
        incoming: [u64; 8],
        deadline: Instant,
    ) -> Result<Option<DataGroup>> {
        let Some(mut remaining) = group_work(scan, evaluations, source.group, false)? else {
            return Ok(None);
        };
        let original = evaluations
            .get(&source.shard.route.id)
            .ok_or_else(Error::unavailable)?;
        for ((remaining, old), new) in remaining
            .iter_mut()
            .zip(original.rates)
            .zip(incoming)
            .take(2)
        {
            *remaining = remaining
                .checked_sub(old)
                .and_then(|rate| rate.checked_add(new))
                .ok_or_else(Error::unavailable)?;
        }
        if policy.high(&remaining) {
            return Ok(None);
        }
        let source_node = self
            .query_shard_loads(source.group, &[], deadline)
            .await?
            .node;
        let groups = self.scaling_groups(deadline).await?;
        for group in groups {
            if group.group_id == source.group {
                continue;
            }
            let Some(current) = group_work(scan, evaluations, group.group_id, false)? else {
                continue;
            };
            if policy.high(&combined(&current, &incoming)?) {
                continue;
            }
            let Ok(target) = self.query_shard_loads(group.group_id, &[], deadline).await else {
                continue;
            };
            if target.node == source_node {
                continue;
            }
            if self
                .group_capacity(group.group_id, incoming, deadline)
                .await
                .unwrap_or(false)
            {
                return Ok(Some(group));
            }
        }
        Ok(None)
    }

    async fn scaling_groups(&self, deadline: Instant) -> Result<Vec<DataGroup>> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        replica
            .group
            .ensure_read(deadline)
            .await
            .map_err(crate::runtime::wire::execution_error)?;
        replica
            .state
            .try_with_application(|state| {
                state
                    .groups
                    .values()
                    .filter(|group| eligible_group(state, group, self.config.replication_factor))
                    .cloned()
                    .collect()
            })
            .await
            .map_err(|source| Error::operation("select stable topic scaling groups", source))
    }

    async fn group_capacity(
        &self,
        group: u64,
        incoming: [u64; 8],
        deadline: Instant,
    ) -> Result<bool> {
        let Some(record) = self
            .scaling_groups(deadline)
            .await?
            .into_iter()
            .find(|record| record.group_id == group)
        else {
            return Ok(false);
        };
        if let Some(voters) = record.placement.raft_voters() {
            self.verify_prepared_group(group, voters, true, deadline)
                .await?;
        } else {
            let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
            if replica.current_placement()? != record.placement {
                return Err(Error::unavailable());
            }
            replica
                .group
                .ensure_read(deadline)
                .await
                .map_err(wire::execution_error)?;
        }
        let policy = PlacementPolicy::default();
        let placement = Placement::new(policy)
            .map_err(|source| Error::operation("validate topic scaling capacity policy", source))?;
        let logical = incoming
            .first()
            .copied()
            .and_then(|publish| {
                incoming
                    .get(2)
                    .copied()
                    .and_then(|delivery| publish.checked_add(delivery))
            })
            .and_then(|bytes| {
                incoming
                    .get(5)
                    .copied()
                    .and_then(|ack| bytes.checked_add(ack))
            })
            .and_then(|bytes| bytes.checked_mul(u64::from(self.config.replication_factor)))
            .ok_or_else(Error::unavailable)?;
        let required = policy
            .reserve_bytes
            .checked_add(logical.checked_mul(10).ok_or_else(Error::unavailable)?)
            .ok_or_else(Error::unavailable)?;
        for node in record.placement.hosts() {
            let started = Instant::now();
            let report = if self.rpc.is_local() {
                use crate::scaling::remote::CapacitySource;
                if node != self.config.node_id {
                    return Ok(false);
                }
                self.scaling
                    .capacity
                    .snapshot()
                    .and_then(|sample| {
                        crate::scaling::remote::CapacityReport::from_sample(sample).ok()
                    })
                    .ok_or_else(Error::unavailable)?
            } else {
                let Reply::Capacity(report) = self
                    .scaling
                    .rpc
                    .call(node, Query::Capacity { group, node }, deadline)
                    .await
                    .map_err(|source| {
                        Error::operation("inspect every destination replica capacity", source)
                    })?
                else {
                    return Ok(false);
                };
                report
            };
            let mut capacity = report.measured(node, started.elapsed()).map_err(|source| {
                Error::operation("verify destination capacity freshness", source)
            })?;
            capacity.bytes_per_second = capacity
                .bytes_per_second
                .checked_add(logical)
                .ok_or_else(Error::unavailable)?;
            if placement
                .capacity_block(&capacity, required, std::time::Instant::now())
                .is_some()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn apply_topic_decision(
        &self,
        layout: &TopicLayout,
        change: LayoutChange,
        diagnostic: Diagnostic,
        scan: &Scan,
        deadline: Instant,
    ) -> Result<Diagnostic> {
        let _mutation = wire::within(deadline, self.metadata_mutation.lock()).await?;
        let target = match &change {
            LayoutChange::Split { groups, .. } => groups.get(1).copied(),
            LayoutChange::Merge { group, .. } => Some(*group),
        }
        .ok_or_else(Error::unavailable)?;
        self.query_shard_loads(target, &[], deadline).await?;
        let Ok(_repair) = self.repair.try_acquire() else {
            return Ok(Diagnostic {
                status: Status::RepairCapacityRequired,
                ..diagnostic
            });
        };
        let expected = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                validate_decision(state, scan, layout, &change, self.config.replication_factor)
            })
            .await
            .map_err(|source| {
                Error::operation("revalidate observed layout and capacity ownership", source)
            })??;
        let status = if matches!(change, LayoutChange::Split { .. }) {
            Status::Split
        } else {
            Status::Merge
        };
        let outcome = self
            .meta_write(
                Command::prepare_layout(
                    super::super::node::random_id()?,
                    expected,
                    layout.topic,
                    layout.revision,
                    change,
                ),
                deadline,
            )
            .await?;
        let metadata::Outcome::LayoutPrepared(plan) = outcome else {
            return Err(Error::unavailable());
        };
        self.resume_layout(plan, deadline)
            .await
            .map_err(Error::admitted)?;
        Ok(Diagnostic {
            status,
            ..diagnostic
        })
    }
}

fn scaled(rate: u64, fraction: u16) -> Result<u64> {
    u64::try_from((u128::from(rate) * u128::from(fraction)).div_ceil(1000))
        .map_err(|_| Error::unavailable())
}

pub(super) fn combined(left: &[u64; 8], right: &[u64; 8]) -> Result<[u64; 8]> {
    let mut total = [0; 8];
    for ((out, left), right) in total.iter_mut().zip(left).zip(right) {
        *out = left.checked_add(*right).ok_or_else(Error::unavailable)?;
    }
    Ok(total)
}

fn group_work(
    scan: &Scan,
    evaluations: &BTreeMap<Id, Evaluation>,
    group: u64,
    low: bool,
) -> Result<Option<[u64; 8]>> {
    let mut total = [0; 8];
    for domain in scan.domains.iter().filter(|domain| domain.group == group) {
        let Some(row) = evaluations.get(&domain.shard.route.id) else {
            return Ok(None);
        };
        let rates = if low {
            let Some(peak) = row.low_peak else {
                return Ok(None);
            };
            peak
        } else {
            row.rates
        };
        total = combined(&total, &rates)?;
    }
    Ok(Some(total))
}

pub(super) fn merge_group_low(
    policy: &TopicScalingPolicy,
    scan: &Scan,
    evaluations: &BTreeMap<Id, Evaluation>,
    target: u64,
    source: u64,
    incoming: &[u64; 8],
) -> Result<bool> {
    let Some(current) = group_work(scan, evaluations, target, true)? else {
        return Ok(false);
    };
    Ok(policy.low(&if target == source {
        current
    } else {
        combined(&current, incoming)?
    }))
}

fn eligible_group(state: &metadata::MetadataState, group: &DataGroup, rf: u8) -> bool {
    let hosts = group.placement.hosts();
    group
        .placement
        .mode()
        .is_ok_and(|mode| mode.replication_factor() == usize::from(rf))
        && !state.topology.pending_transfers().any(|plan| {
            plan.source.group_id == group.group_id || plan.target.group_id == group.group_id
        })
        && !state
            .pending_data_replacements()
            .any(|plan| plan.group_id() == group.group_id)
        && !state
            .membership_change(group.group_id)
            .is_some_and(|change| !change.is_complete())
        && !state.topology.pending().any(|plan| {
            plan.successors
                .iter()
                .any(|shard| shard.route.group == group.group_id)
        })
        && hosts.iter().all(|node| {
            state.node(*node).is_some_and(|node| {
                node.spec.schedulable && node.spec.roles.contains(&NodeRole::Storage)
            })
        })
        && hosts
            .iter()
            .filter_map(|node| state.node(*node).map(|node| node.spec.zone.as_str()))
            .collect::<BTreeSet<_>>()
            .len()
            == hosts.len()
}

pub(super) fn validate_decision(
    state: &metadata::MetadataState,
    scan: &Scan,
    layout: &TopicLayout,
    change: &LayoutChange,
    rf: u8,
) -> Result<u64> {
    if state.registry_revision() != scan.registry_revision
        || state.topology.layout(layout.topic) != Some(layout)
    {
        return Err(Error::unavailable());
    }
    let (sources, mut groups) = match change {
        LayoutChange::Split { source, groups, .. } => (
            vec![*source],
            groups.iter().copied().collect::<BTreeSet<_>>(),
        ),
        LayoutChange::Merge { left, right, group } => {
            (vec![*left, *right], BTreeSet::from([*group]))
        }
    };
    for source in sources {
        let domain = scan
            .domains
            .iter()
            .find(|domain| domain.shard.route.id == source)
            .ok_or_else(Error::unavailable)?;
        if !domain.active
            || state
                .route(domain.shard.partition)
                .is_none_or(|route| route.epoch != domain.owner_epoch)
        {
            return Err(Error::unavailable());
        }
        groups.insert(domain.group);
    }
    for group in groups {
        let original = scan.groups.get(&group).ok_or_else(Error::unavailable)?;
        if state.group(group) != Some(original) || !eligible_group(state, original, rf) {
            return Err(Error::unavailable());
        }
    }
    Ok(state.revision())
}
