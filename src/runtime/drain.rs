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
    Unschedule { node: u64 },
    Inspect { node: u64 },
    Stop { node: u64 },
    Probe { node: u64 },
}

#[derive(Serialize, Deserialize)]
struct Probe {
    cluster: tidemq_protocol::Id,
    node: u64,
    role: crate::config::Role,
    accepting: bool,
}

pub(super) fn allows_completion(
    frame: &tidemq_protocol::common::Frame,
    limits: &tidemq_protocol::ProtocolLimits,
) -> Result<bool> {
    use tidemq_protocol::{framing::decode_payload, public};
    Ok(match super::service::frame_kind(frame)? {
        FrameKind::GroupAck
        | FrameKind::GroupRetireRead
        | FrameKind::GroupRetireAck
        | FrameKind::RetirePublishes
        | FrameKind::GetTopic
        | FrameKind::GetTopicLayout
        | FrameKind::Cancel => true,
        FrameKind::ConsumerControl => {
            let request: public::ConsumerControl = decode_payload(frame.payload.clone(), limits)
                .map_err(|source| Error::operation("decode draining consumer action", source))?;
            matches!(
                public::ConsumerAction::try_from(request.action),
                Ok(public::ConsumerAction::LeaveConsumer
                    | public::ConsumerAction::RetireConsumer
                    | public::ConsumerAction::HeartbeatConsumer
                    | public::ConsumerAction::ConsumerStatus)
            )
        }
        FrameKind::PublisherControl => {
            let request: public::PublisherControl = decode_payload(frame.payload.clone(), limits)
                .map_err(|source| {
                Error::operation("decode draining publisher action", source)
            })?;
            matches!(
                public::PublisherAction::try_from(request.action),
                Ok(public::PublisherAction::ClosePublisher
                    | public::PublisherAction::HeartbeatPublisher
                    | public::PublisherAction::PublisherStatus)
            )
        }
        _ => false,
    })
}

impl Service {
    pub(super) async fn probe_broker(
        &self,
        node: u64,
        deadline: Instant,
    ) -> super::failover::Probe {
        use super::failover::Probe as Observation;
        let rpc = match self
            .rpc
            .pool
            .connection(node, TrafficClass::Control, deadline)
            .await
        {
            Ok(rpc) => rpc,
            Err(error) => {
                return if connection_unreachable(&error) {
                    Observation::Unreachable
                } else {
                    Observation::Responsive
                };
            }
        };
        let result = self
            .drain_rpc(
                node,
                self.config.runtime.metadata_group,
                Command::Probe { node },
                deadline,
            )
            .await;
        match result {
            Ok(bytes) => match wire::decode_json::<Probe>(&bytes) {
                Ok(probe)
                    if probe.cluster == self.cluster
                        && probe.node == node
                        && self
                            .config
                            .runtime
                            .peer(node)
                            .is_ok_and(|peer| probe.role == peer.role)
                        && probe.role.serves_broker()
                        && probe.accepting =>
                {
                    Observation::Eligible
                }
                _ => Observation::Responsive,
            },
            Err(Error::Rejected { .. }) => Observation::Responsive,
            Err(_) if rpc.is_closed() => Observation::Unreachable,
            Err(_) => Observation::Responsive,
        }
    }

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
            } else if matches!(kind, FrameKind::Publish | FrameKind::GroupRead) {
                return Err(Error::unavailable());
            }
        }
        Ok(())
    }

    pub(crate) fn shutdown_response(
        &self,
        peer: &crate::transport::Authorization,
        frame: &tidemq_protocol::common::Frame,
    ) -> bool {
        let crate::transport::Authorization::Internal(peer) = peer else {
            return false;
        };
        if peer.cluster_id != self.cluster
            || !matches!(peer.role.as_str(), "metadata" | "combined")
            || frame.kind != i32::from(FrameKind::InternalCommand)
            || frame.traffic_class != i32::from(TrafficClass::Control)
        {
            return false;
        }
        let Ok(envelope) = tidemq_protocol::framing::decode_payload::<
            tidemq_protocol::internal::InternalCommand,
        >(frame.payload.clone(), &self.config.protocol) else {
            return false;
        };
        if envelope.group_id != self.config.runtime.metadata_group
            || envelope.operation_id != frame.operation_id
        {
            return false;
        }
        let Ok(request) = tidemq_protocol::framing::decode_payload::<Request>(
            envelope.encoded_command,
            &self.config.protocol,
        ) else {
            return false;
        };
        if request.kind != KIND {
            return false;
        }
        matches!(wire::decode_json::<Command>(&request.body), Ok(Command::Stop { node } | Command::Reconnect { node }) if node == self.config.node_id)
    }

    pub(crate) async fn planned_node_drain(
        &self,
        node: u64,
        handoffs: &[Handoff],
        deadline: Instant,
    ) -> Result<crate::scaling::DrainProgress> {
        if !self.config.role.stores_metadata() || handoffs.len() > 64 {
            return Err(forbidden());
        }
        let leader = self
            .leader(self.config.runtime.metadata_group, deadline)
            .await?;
        self.drain_rpc(
            leader,
            self.config.runtime.metadata_group,
            Command::Unschedule { node },
            deadline,
        )
        .await?;
        for handoff in handoffs {
            let route = self.route(handoff.partition, deadline).await?;
            check_route(&handoff.into(), &route)?;
            if handoff.target_owner == node
                || route.owner_node != node && route.owner_node != handoff.target_owner
            {
                return Err(forbidden());
            }
            crate::scaling::DrainPlan::handoff(self, handoff, &self.scaling.metrics, deadline)
                .await
                .map_err(|source| Error::operation("execute bounded node handoff", source))?;
        }
        let progress = self.remote_drain_progress(node, deadline).await?;
        if progress.durable_state_drained() {
            self.drain_rpc(
                node,
                self.config.runtime.metadata_group,
                Command::Stop { node },
                deadline,
            )
            .await?;
        }
        Ok(progress)
    }

    async fn unschedule(&self, node: u64, deadline: Instant) -> Result<()> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        let _mutation = wire::within(deadline, self.metadata_mutation.lock()).await?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        let mut record = replica
            .state
            .try_with_application(|state| state.node(node).cloned())
            .await
            .map_err(|source| Error::operation("read node drain identity", source))?
            .ok_or_else(Error::unavailable)?;
        if record.spec.schedulable {
            record.spec.schedulable = false;
            self.meta_write(
                crate::metadata::Command::put_node(
                    super::node::random_id()?,
                    self.meta_revision().await?,
                    record.spec,
                    record.generation,
                ),
                deadline,
            )
            .await?;
        }
        Ok(())
    }

    async fn inspect_node_drain(
        &self,
        node: u64,
        deadline: Instant,
    ) -> Result<crate::scaling::DrainProgress> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        replica
            .state
            .try_with_application(|state| crate::scaling::DrainPlan::inspect(state, node))
            .await
            .map_err(|source| Error::operation("inspect durable drain blockers", source))?
            .map_err(|source| Error::operation("validate node drain state", source))
    }

    async fn remote_drain_progress(
        &self,
        node: u64,
        deadline: Instant,
    ) -> Result<crate::scaling::DrainProgress> {
        let leader = self
            .leader(self.config.runtime.metadata_group, deadline)
            .await?;
        let bytes = self
            .drain_rpc(
                leader,
                self.config.runtime.metadata_group,
                Command::Inspect { node },
                deadline,
            )
            .await?;
        let progress: crate::scaling::DrainProgress = wire::decode_json(&bytes)?;
        if progress.node_id != node {
            return Err(forbidden());
        }
        Ok(progress)
    }

    async fn stop_drained_node(&self, deadline: Instant) -> Result<()> {
        let progress = self
            .remote_drain_progress(self.config.node_id, deadline)
            .await?;
        if !progress.durable_state_drained() {
            return Err(Error::unavailable());
        }
        self.draining
            .store(true, std::sync::atomic::Ordering::Release);
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
        peer: &crate::transport::NodeGrant,
        group: u64,
        body: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let command: Command = wire::decode_json(body)?;
        let controller = matches!(peer.role.as_str(), "metadata" | "combined");
        if !controller && !matches!(command, Command::Inspect { node } if node == peer.node_id) {
            return Err(forbidden());
        }
        match command {
            Command::Probe { node } => {
                if node != self.config.node_id || group != self.config.runtime.metadata_group {
                    return Err(forbidden());
                }
                wire::json(&Probe {
                    cluster: self.cluster,
                    node: self.config.node_id,
                    role: self.config.role,
                    accepting: !self.draining.load(std::sync::atomic::Ordering::Acquire),
                })
            }
            Command::Unschedule { node } => {
                if group != self.config.runtime.metadata_group {
                    return Err(forbidden());
                }
                self.unschedule(node, deadline).await?;
                wire::json(&self.inspect_node_drain(node, deadline).await?)
            }
            Command::Inspect { node } => {
                if group != self.config.runtime.metadata_group {
                    return Err(forbidden());
                }
                wire::json(&self.inspect_node_drain(node, deadline).await?)
            }
            Command::Stop { node } => {
                if node != self.config.node_id || group != self.config.runtime.metadata_group {
                    return Err(forbidden());
                }
                self.stop_drained_node(deadline).await?;
                wire::json(&())
            }
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
                self.stop_drained_node(deadline).await?;
                wire::json(&())
            }
        }
    }
}

fn connection_unreachable(error: &crate::replication::rpc::Error) -> bool {
    use crate::replication::rpc::Error;
    use crate::transport::Error as Transport;
    matches!(
        error,
        Error::Transport {
            source: Transport::Deadline { .. } | Transport::Closed
        } | Error::Rpc {
            source: crate::transport::rpc::Error::Closed
        } | Error::Rpc {
            source: crate::transport::rpc::Error::Transport {
                source: Transport::Deadline { .. } | Transport::Closed
            }
        }
    )
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
        let leader = self
            .leader(self.config.runtime.metadata_group, deadline)
            .await
            .map_err(scaling_error)?;
        self.drain_rpc(
            leader,
            self.config.runtime.metadata_group,
            Command::Unschedule { node },
            deadline,
        )
        .await
        .map_err(scaling_error)?;
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

#[cfg(test)]
mod admission_tests {
    use super::*;
    use tidemq_protocol::{ProtocolLimits, common::Frame, framing::encode_payload, public};

    #[test]
    fn draining_keeps_member_and_publisher_cleanup_live_without_admitting_new_work() {
        let limits = ProtocolLimits::default();
        for (action, allowed) in [
            (public::ConsumerAction::JoinConsumer, false),
            (public::ConsumerAction::EnsureConsumerSubscription, false),
            (public::ConsumerAction::HeartbeatConsumer, true),
            (public::ConsumerAction::LeaveConsumer, true),
            (public::ConsumerAction::RetireConsumer, true),
            (public::ConsumerAction::ConsumerStatus, true),
        ] {
            let frame = Frame {
                kind: FrameKind::ConsumerControl.into(),
                payload: encode_payload(
                    &public::ConsumerControl {
                        action: action.into(),
                        ..Default::default()
                    },
                    limits.max_control_frame_bytes,
                )
                .unwrap(),
                ..Default::default()
            };
            assert_eq!(allows_completion(&frame, &limits).unwrap(), allowed);
        }
        for (action, allowed) in [
            (public::PublisherAction::OpenPublisher, false),
            (public::PublisherAction::HeartbeatPublisher, true),
            (public::PublisherAction::ClosePublisher, true),
            (public::PublisherAction::PublisherStatus, true),
        ] {
            let frame = Frame {
                kind: FrameKind::PublisherControl.into(),
                payload: encode_payload(
                    &public::PublisherControl {
                        action: action.into(),
                        ..Default::default()
                    },
                    limits.max_control_frame_bytes,
                )
                .unwrap(),
                ..Default::default()
            };
            assert_eq!(allows_completion(&frame, &limits).unwrap(), allowed);
        }
        for (kind, allowed) in [
            (FrameKind::Publish, false),
            (FrameKind::GroupRead, false),
            (FrameKind::CreateLogicalTopic, false),
            (FrameKind::GroupAck, true),
            (FrameKind::GroupRetireRead, true),
            (FrameKind::GroupRetireAck, true),
            (FrameKind::RetirePublishes, true),
            (FrameKind::GetTopicLayout, true),
        ] {
            assert_eq!(
                allows_completion(
                    &Frame {
                        kind: kind.into(),
                        ..Default::default()
                    },
                    &limits
                )
                .unwrap(),
                allowed
            );
        }
    }
}
