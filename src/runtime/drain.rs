use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::common::{ErrorCode, FrameKind, Outcome, TrafficClass};
use tokio::time::Instant;

use crate::metadata::{PartitionKey, Route};
use crate::scaling::{DrainActions, Handoff};

use super::service::{Service, forbidden};
use super::wire::{self, Request};
use super::{Error, Result};

pub(crate) const KIND: u32 = 9;

#[derive(Clone, Serialize, Deserialize)]
struct Move {
    partition: PartitionKey,
    target_owner: u64,
    expected_epoch: u64,
    expected_route_generation: u64,
}
impl From<&Handoff> for Move {
    fn from(value: &Handoff) -> Self {
        Self {
            partition: value.partition,
            target_owner: value.target_owner,
            expected_epoch: value.expected_epoch,
            expected_route_generation: value.expected_route_generation,
        }
    }
}
#[derive(Serialize, Deserialize)]
enum Command {
    Prewarm(Move),
    Drain(Move),
    Takeover(Move),
    Reconnect { node: u64 },
}

impl Service {
    pub(crate) fn admit_partition(
        &self,
        partition: PartitionKey,
        epoch: u64,
        kind: FrameKind,
    ) -> Result<()> {
        let mut drains = self
            .partition_drains
            .lock()
            .map_err(|_| Error::unavailable())?;
        if let Some(expected) = drains.get(&partition) {
            if *expected != epoch {
                drains.remove(&partition);
            } else if matches!(
                kind,
                FrameKind::ProduceBatch
                    | FrameKind::OpenProducer
                    | FrameKind::Read
                    | FrameKind::ClaimAssignment
            ) {
                return Err(Error::unavailable());
            }
        }
        Ok(())
    }

    async fn drain_rpc(
        &self,
        node: u64,
        group: u64,
        command: Command,
        deadline: Instant,
    ) -> Result<Bytes> {
        self.rpc
            .call(node, group, Request::control(KIND, &command)?, deadline)
            .await
    }

    pub(crate) async fn drain_command(
        &self,
        group: u64,
        body: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        match wire::decode_json(body)? {
            Command::Prewarm(handoff) => {
                if !self.config.role.serves_broker() || handoff.target_owner != self.config.node_id
                {
                    return Err(forbidden());
                }
                let route = self.route(handoff.partition, deadline).await?;
                check_route(&handoff, &route)?;
                if route.group_id != group {
                    return Err(forbidden());
                }
                self.authoritative_group(group, deadline).await?;
                let leader = self.leader(group, deadline).await?;
                for class in [
                    TrafficClass::Control,
                    TrafficClass::Produce,
                    TrafficClass::Delivery,
                    TrafficClass::Bulk,
                ] {
                    self.rpc
                        .pool
                        .connection(leader, class, deadline)
                        .await
                        .map_err(|source| {
                            Error::operation("prewarm authenticated broker data connection", source)
                        })?;
                }
                wire::json(&())
            }
            Command::Drain(handoff) => {
                if !self.config.role.serves_broker() {
                    return Err(forbidden());
                }
                let route = self.route(handoff.partition, deadline).await?;
                check_route(&handoff, &route)?;
                if route.group_id != group {
                    return Err(forbidden());
                }
                if route.owner_node == handoff.target_owner && route.epoch > handoff.expected_epoch
                {
                    return wire::json(&());
                }
                if route.owner_node != self.config.node_id {
                    return Err(forbidden());
                }
                let mut drains = self
                    .partition_drains
                    .lock()
                    .map_err(|_| Error::unavailable())?;
                if drains.len() >= 2048 && !drains.contains_key(&handoff.partition) {
                    return Err(Error::unavailable());
                }
                drains.insert(handoff.partition, handoff.expected_epoch);
                wire::json(&())
            }
            Command::Takeover(handoff) => {
                if group != self.config.runtime.metadata_group {
                    return Err(forbidden());
                }
                let route = self
                    .execute_takeover(
                        handoff.partition,
                        handoff.target_owner,
                        handoff.expected_epoch,
                        handoff.expected_route_generation,
                        deadline,
                    )
                    .await?;
                wire::json(&route)
            }
            Command::Reconnect { node } => {
                if group != self.config.runtime.metadata_group
                    || node != self.config.node_id
                    || self.config.role != crate::config::Role::Gateway
                {
                    return Err(forbidden());
                }
                self.draining
                    .store(true, std::sync::atomic::Ordering::Release);
                let _ = self.shutdown_requested.send(true);
                wire::json(&())
            }
        }
    }
}

fn check_route(handoff: &Move, route: &Route) -> Result<()> {
    let previous = route.epoch == handoff.expected_epoch
        && route.generation == handoff.expected_route_generation;
    let completed = handoff.expected_epoch.checked_add(1) == Some(route.epoch)
        && handoff.expected_route_generation.checked_add(1) == Some(route.generation)
        && route.owner_node == handoff.target_owner;
    if route.partition != handoff.partition || !(previous || completed) {
        return Err(Error::reject(ErrorCode::Fenced, Outcome::Rejected, false));
    }
    Ok(())
}

fn scaling_error(source: Error) -> crate::scaling::Error {
    crate::scaling::Error::Remote {
        source: Box::new(source),
    }
}

impl DrainActions for Service {
    async fn prewarm(&self, handoff: &Handoff, deadline: Instant) -> crate::scaling::Result<()> {
        let route = self
            .route(handoff.partition, deadline)
            .await
            .map_err(scaling_error)?;
        self.drain_rpc(
            handoff.target_owner,
            route.group_id,
            Command::Prewarm(handoff.into()),
            deadline,
        )
        .await
        .map_err(scaling_error)?;
        Ok(())
    }
    async fn drain_owner(
        &self,
        handoff: &Handoff,
        deadline: Instant,
    ) -> crate::scaling::Result<()> {
        let route = self
            .route(handoff.partition, deadline)
            .await
            .map_err(scaling_error)?;
        if route.owner_node == handoff.target_owner && route.epoch > handoff.expected_epoch {
            return Ok(());
        }
        self.drain_rpc(
            route.owner_node,
            route.group_id,
            Command::Drain(handoff.into()),
            deadline,
        )
        .await
        .map_err(scaling_error)?;
        Ok(())
    }
    async fn takeover(
        &self,
        handoff: &Handoff,
        deadline: Instant,
    ) -> crate::scaling::Result<Route> {
        let leader = self
            .leader(self.config.runtime.metadata_group, deadline)
            .await
            .map_err(scaling_error)?;
        let bytes = self
            .drain_rpc(
                leader,
                self.config.runtime.metadata_group,
                Command::Takeover(handoff.into()),
                deadline,
            )
            .await
            .map_err(scaling_error)?;
        wire::decode_json(&bytes).map_err(scaling_error)
    }
    async fn reconnect_gateway(&self, node: u64, deadline: Instant) -> crate::scaling::Result<()> {
        self.drain_rpc(
            node,
            self.config.runtime.metadata_group,
            Command::Reconnect { node },
            deadline,
        )
        .await
        .map_err(scaling_error)?;
        Ok(())
    }
}
