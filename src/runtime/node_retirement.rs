use std::{sync::Mutex, time::Duration};

use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;
use tokio::{io::AsyncWriteExt, sync::Notify, time::Instant};

use super::{Error, Result, service::Service};

#[derive(Default)]
struct Activity {
    stopped: bool,
    active: usize,
}

#[derive(Default)]
pub(super) struct Admission {
    state: Mutex<Activity>,
    changed: Notify,
}

pub(super) struct Active<'a>(&'a Admission);
impl Admission {
    pub fn enter(&self) -> Result<Active<'_>> {
        let mut state = self.state.lock().map_err(|_| Error::unavailable())?;
        if state.stopped {
            return Err(Error::unavailable());
        }
        state.active = state.active.checked_add(1).ok_or_else(Error::unavailable)?;
        Ok(Active(self))
    }
    pub fn stop(&self) -> Result<()> {
        self.state.lock().map_err(|_| Error::unavailable())?.stopped = true;
        Ok(())
    }
    async fn drained(&self, deadline: Instant) -> Result<()> {
        loop {
            let changed = self.changed.notified();
            if self.state.lock().map_err(|_| Error::unavailable())?.active == 0 {
                return Ok(());
            }
            super::wire::within(deadline, changed).await?;
        }
    }
}
impl Drop for Active<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.active = state.active.saturating_sub(1);
        }
        self.0.changed.notify_waiters();
    }
}

/// A durable local admission fence survives a crash before Metadata records retirement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Fence {
    version: u32,
    cluster: Id,
    node: u64,
    generation: u64,
    drain: Id,
}

pub(super) async fn load(config: &crate::config::Config, cluster: Id) -> Result<Option<Fence>> {
    let path = config.data_dir.join("node-retirement.json");
    if !tokio::fs::try_exists(&path)
        .await
        .map_err(|source| Error::operation("inspect node retirement fence", source))?
    {
        return Ok(None);
    }
    let bytes = super::read_secret(&path, 4096).await?;
    let fence: Fence = serde_json::from_slice(&bytes)
        .map_err(|source| Error::operation("decode node retirement fence", source))?;
    if fence.version != 1
        || fence.cluster != cluster
        || fence.node != config.node_id
        || fence.generation == 0
        || fence.drain.as_bytes().iter().all(|byte| *byte == 0)
    {
        return Err(Error::invalid(
            "Node retirement fence identity or format is invalid",
        ));
    }
    Ok(Some(fence))
}

impl Service {
    pub(super) async fn retirement_proof(
        &self,
        generation: u64,
        drain: Id,
        deadline: Instant,
    ) -> Result<Fence> {
        self.refresh_registry(deadline).await?;
        let local = self
            .registry
            .nodes()?
            .into_iter()
            .find(|node| node.record.spec.node_id == self.config.node_id)
            .ok_or_else(Error::unavailable)?;
        if local.record.spec.schedulable
            || local.record.generation != generation
            || local.identity.drain_operation != Some(drain)
        {
            return Err(Error::unavailable());
        }
        let fence = Fence {
            version: 1,
            cluster: self.cluster,
            node: self.config.node_id,
            generation,
            drain,
        };
        let mut saved = super::wire::within(deadline, self.retirement.lock()).await?;
        if let Some(previous) = &*saved {
            return if previous == &fence {
                Ok(previous.clone())
            } else {
                Err(Error::invalid(
                    "Node retirement fence changed its original drain",
                ))
            };
        }
        self.stop_public_admission(deadline).await?;
        self.save_retirement_fence(&fence).await?;
        *saved = Some(fence.clone());
        #[cfg(feature = "test-hooks")]
        super::test_hooks::pause_node_retirement(self.cluster, self.config.node_id, deadline).await;
        Ok(fence)
    }
    async fn stop_public_admission(&self, deadline: Instant) -> Result<()> {
        self.public_admission.stop()?;
        self.draining
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = self.external_stop.send(true);
        loop {
            match self
                .external_drained
                .load(std::sync::atomic::Ordering::Acquire)
            {
                1 => break,
                2 => return Err(Error::invalid("External listener cleanup did not complete")),
                _ => {
                    super::wire::within(deadline, tokio::time::sleep(Duration::from_millis(20)))
                        .await?
                }
            }
        }
        self.public_admission.drained(deadline).await
    }
    async fn save_retirement_fence(&self, fence: &Fence) -> Result<()> {
        let bytes = serde_json::to_vec(fence)
            .map_err(|source| Error::operation("encode node retirement fence", source))?;
        let temporary = self.config.data_dir.join("node-retirement.next");
        let result = async {
            let mut options = tokio::fs::OpenOptions::new();
            options.write(true).create(true).truncate(true);
            #[cfg(unix)]
            options.mode(0o600);
            let mut file = options.open(&temporary).await?;
            file.write_all(&bytes).await?;
            file.sync_all().await?;
            tokio::fs::rename(
                &temporary,
                self.config.data_dir.join("node-retirement.json"),
            )
            .await?;
            tokio::fs::File::open(&self.config.data_dir)
                .await?
                .sync_all()
                .await
        }
        .await;
        result.map_err(|source| Error::operation("persist node retirement admission fence", source))
    }
    pub(super) fn verify_retirement_proof(
        &self,
        proof: &Fence,
        node: u64,
        generation: u64,
        drain: Id,
    ) -> Result<()> {
        if proof
            != &(Fence {
                version: 1,
                cluster: self.cluster,
                node,
                generation,
                drain,
            })
        {
            return Err(Error::unavailable());
        }
        Ok(())
    }
}

impl Service {
    pub(super) async fn follow_node_drains(
        self: std::sync::Arc<Self>,
        mut stopped: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let mut ticks = tokio::time::interval(Duration::from_secs(1));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = stopped.changed() => return Ok(()), _ = ticks.tick() => {} }
            let Some(replica) = &self.metadata else {
                continue;
            };
            if self.require_leader(replica).is_err() {
                continue;
            }
            let deadline = Instant::now() + Duration::from_secs(10);
            let result = tokio::select! { _ = stopped.changed() => return Ok(()), result = self.move_draining_owners(deadline) => result };
            if let Err(error) = result {
                tracing::debug!(
                    event_code = "node_drain_wait",
                    error_code = error.failure().code,
                    "Node drain will resume after a surviving broker becomes available"
                );
            }
        }
    }
    async fn move_draining_owners(&self, deadline: Instant) -> Result<()> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        super::wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        let (routes, mut candidates) = replica
            .state
            .try_with_application(drain_targets)
            .await
            .map_err(|source| Error::operation("read persisted node drain ownership", source))?;
        candidates.sort_unstable();
        for route in routes {
            let mut target = None;
            for (_, node) in candidates.iter().take(32) {
                if self
                    .probe_broker(
                        *node,
                        deadline.min(Instant::now() + Duration::from_millis(300)),
                    )
                    .await
                    == super::failover::Probe::Eligible
                {
                    target = Some(*node);
                    break;
                }
            }
            let Some(target) = target else {
                return Err(Error::unavailable());
            };
            let handoff = crate::scaling::Handoff {
                partition: route.partition,
                target_owner: target,
                expected_epoch: route.epoch,
                expected_route_generation: route.generation,
            };
            crate::scaling::DrainPlan::handoff(self, &handoff, &self.scaling.metrics, deadline)
                .await
                .map_err(|source| Error::operation("handoff draining broker ownership", source))?;
            if let Some(candidate) = candidates.iter_mut().find(|(_, node)| *node == target) {
                candidate.0 = candidate.0.saturating_add(1);
            }
            candidates.sort_unstable();
        }
        Ok(())
    }
}

type DrainTargets = (Vec<crate::metadata::Route>, Vec<(usize, u64)>);

fn drain_targets(state: &crate::metadata::MetadataState) -> DrainTargets {
    let draining: std::collections::BTreeSet<_> = state
        .nodes
        .keys()
        .filter(|node| {
            state
                .node_identity(**node)
                .is_some_and(|identity| identity.drain_operation.is_some() && !identity.retired)
        })
        .copied()
        .collect();
    let routes = state
        .partitions
        .keys()
        .filter_map(|key| state.route(*key))
        .filter(|route| draining.contains(&route.owner_node))
        .take(4)
        .cloned()
        .collect();
    let mut counts = std::collections::BTreeMap::<u64, usize>::new();
    for record in state.partitions.values() {
        if let Some(grant) = &record.grant {
            let count = counts.entry(grant.owner_node).or_default();
            *count = count.saturating_add(1);
        }
    }
    let candidates = state
        .nodes
        .values()
        .filter(|node| {
            node.spec.schedulable && node.spec.roles.contains(&crate::metadata::NodeRole::Broker)
        })
        .map(|node| {
            (
                counts.remove(&node.spec.node_id).unwrap_or_default(),
                node.spec.node_id,
            )
        })
        .collect();
    (routes, candidates)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn closing_admission_fences_new_work_and_waits_for_the_existing_guard() {
        let admission = Admission::default();
        let active = admission.enter().unwrap();
        admission.stop().unwrap();
        assert!(admission.enter().is_err());
        assert!(
            admission
                .drained(Instant::now() + Duration::from_millis(1))
                .await
                .is_err()
        );
        drop(active);
        admission
            .drained(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }
}
