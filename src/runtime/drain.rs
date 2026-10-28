use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::common::{ErrorCode, FrameKind, Outcome, TrafficClass};
use tokio::time::Instant;

use crate::metadata::{PartitionKey, Route};
use crate::scaling::remote::CapacitySource;
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
#[derive(Clone, Serialize, Deserialize)]
enum Command {
    Prewarm(Move),
    Drain(Move),
    Takeover(Move),
    Reconnect {
        node: u64,
    },
    Unschedule {
        node: u64,
    },
    Inspect {
        node: u64,
    },
    Quiesce {
        node: u64,
        generation: u64,
        drain: tidemq_protocol::Id,
    },
    Probe {
        node: u64,
    },
}

#[derive(Serialize, Deserialize)]
struct Probe {
    cluster: tidemq_protocol::Id,
    node: u64,
    role: crate::config::Role,
    accepting: bool,
    capacity: Option<crate::scaling::remote::CapacityReport>,
}

fn broker_capacity_available(
    report: Option<crate::scaling::remote::CapacityReport>,
    node: u64,
    elapsed: std::time::Duration,
) -> bool {
    let Some(capacity) = report.and_then(|report| report.measured(node, elapsed).ok()) else {
        return false;
    };
    let policy = crate::scaling::PlacementPolicy::default();
    let Ok(placement) = crate::scaling::Placement::new(policy) else {
        return false;
    };
    placement
        .capacity_block(&capacity, policy.reserve_bytes, std::time::Instant::now())
        .is_none()
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
        | FrameKind::GetConsoleOperation
        | FrameKind::Cancel => true,
        FrameKind::GroupWatchCancel => true,
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
        if self.rpc.is_local() {
            return if node == self.config.node_id && self.local_broker_eligible(deadline).await {
                Observation::Eligible
            } else {
                Observation::Responsive
            };
        }
        let Ok(pool) = self.rpc.pool() else {
            return Observation::Responsive;
        };
        let rpc = match pool.connection(node, TrafficClass::Control, deadline).await {
            Ok(rpc) => rpc,
            Err(error) => {
                return if connection_unreachable(&error) {
                    Observation::Unreachable
                } else {
                    Observation::Responsive
                };
            }
        };
        let started = Instant::now();
        let result = self
            .drain_rpc(
                node,
                self.config.runtime.metadata_group,
                Command::Probe { node },
                deadline,
            )
            .await;
        let elapsed = started.elapsed();
        match result {
            Ok(bytes) => match wire::decode_json::<Probe>(&bytes) {
                Ok(probe)
                    if probe.cluster == self.cluster
                        && probe.node == node
                        && self
                            .registry
                            .has_role(node, crate::metadata::NodeRole::Broker)
                        && probe.role.serves_broker()
                        && probe.accepting
                        && broker_capacity_available(probe.capacity.clone(), node, elapsed) =>
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

    async fn local_broker_eligible(&self, deadline: Instant) -> bool {
        if !self.config.role.serves_broker()
            || self.draining.load(std::sync::atomic::Ordering::Acquire)
            || self.admit_produce().is_err()
            || self
                .metadata
                .as_ref()
                .is_none_or(|replica| replica.authority_node() != Some(self.config.node_id))
        {
            return false;
        }
        let capacity =
            self.scaling.capacity.snapshot().and_then(|sample| {
                crate::scaling::remote::CapacityReport::from_sample(sample).ok()
            });
        if !broker_capacity_available(capacity, self.config.node_id, std::time::Duration::ZERO) {
            return false;
        }
        let Some(metadata) = &self.metadata else {
            return false;
        };
        if metadata.group.ensure_read(deadline).await.is_err() {
            return false;
        }
        for replica in self.data.values() {
            if replica.group.ensure_read(deadline).await.is_err() {
                return false;
            }
        }
        true
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
        matches!(wire::decode_json::<Command>(&request.body), Ok(Command::Reconnect { node }) if node == self.config.node_id)
    }

    pub(super) async fn retirement_action(
        &self,
        node: u64,
        deadline: Instant,
    ) -> Result<crate::metadata::ClusterAction> {
        self.require_cluster()?;
        let progress = self.inspect_node_drain(node, deadline).await?;
        if !progress.durable_state_drained() {
            return Err(Error::invalid("Node drain still has durable blockers"));
        }
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let (generation, drain, voters) = replica
            .state
            .raft_state()
            .ok_or_else(super::cluster::unsupported)?
            .try_with_membership(|state, _, membership| {
                let record = state.node(node).ok_or_else(Error::unavailable)?;
                let drain = state
                    .node_identity(node)
                    .and_then(|identity| identity.drain_operation)
                    .ok_or_else(|| Error::invalid("Begin node drain before retirement"))?;
                Ok::<_, Error>((
                    record.generation,
                    drain,
                    membership.membership().voter_ids().collect(),
                ))
            })
            .await
            .map_err(|source| Error::operation("read original node drain", source))??;
        let bytes = self
            .drain_rpc(
                node,
                self.config.runtime.metadata_group,
                Command::Quiesce {
                    node,
                    generation,
                    drain,
                },
                deadline,
            )
            .await?;
        let proof = wire::decode_json(&bytes)?;
        self.verify_retirement_proof(&proof, node, generation, drain)?;
        Ok(crate::metadata::ClusterAction::RetireNode {
            node,
            metadata_voters: voters,
        })
    }

    async fn unschedule(&self, node: u64, deadline: Instant) -> Result<()> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        let _mutation = wire::within(deadline, self.metadata_mutation.lock()).await?;
        replica
            .group
            .ensure_read(deadline)
            .await
            .map_err(crate::runtime::wire::execution_error)?;
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

    pub(super) async fn inspect_node_drain(
        &self,
        node: u64,
        deadline: Instant,
    ) -> Result<crate::scaling::DrainProgress> {
        self.require_cluster()?;
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        replica
            .group
            .ensure_read(deadline)
            .await
            .map_err(crate::runtime::wire::execution_error)?;
        replica
            .state
            .raft_state()
            .ok_or_else(super::cluster::unsupported)?
            .try_with_membership(|state, _, membership| {
                let mut progress = crate::scaling::DrainPlan::inspect(state, node)?;
                progress.metadata_role = membership
                    .membership()
                    .voter_ids()
                    .any(|voter| voter == node);
                let pending = state.pending_metadata_change().is_some_and(|change| {
                    change.previous.contains(&node) || change.target.contains(&node)
                });
                progress.pending_membership_changes = progress
                    .pending_membership_changes
                    .saturating_add(usize::from(pending));
                Ok::<_, crate::scaling::Error>(progress)
            })
            .await
            .map_err(|source| Error::operation("inspect durable drain blockers", source))?
            .map_err(|source| Error::operation("validate node drain state", source))
    }

    async fn remote_drain_progress(
        &self,
        node: u64,
        deadline: Instant,
    ) -> Result<crate::scaling::DrainProgress> {
        let bytes = self
            .metadata_drain_rpc(Command::Inspect { node }, deadline)
            .await?;
        let progress: crate::scaling::DrainProgress = wire::decode_json(&bytes)?;
        if progress.node_id != node {
            return Err(forbidden());
        }
        Ok(progress)
    }

    async fn metadata_drain_rpc(&self, command: Command, deadline: Instant) -> Result<Bytes> {
        loop {
            let group = self.config.runtime.metadata_group;
            let leader = self.leader(group, deadline).await?;
            match self
                .drain_rpc(leader, group, command.clone(), deadline)
                .await
            {
                Ok(result) => return Ok(result),
                Err(error) if error.failure().retryable && Instant::now() < deadline => {
                    self.leaders.write().await.remove(&group);
                    wire::within(
                        deadline,
                        tokio::time::sleep(std::time::Duration::from_millis(20)),
                    )
                    .await?;
                }
                Err(error) => return Err(error),
            }
        }
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
        if !matches!(command, Command::Probe { .. }) {
            self.require_cluster()?;
        }
        match command {
            Command::Quiesce {
                node,
                generation,
                drain,
            } => {
                if node != self.config.node_id || group != self.config.runtime.metadata_group {
                    return Err(forbidden());
                }
                wire::json(&self.retirement_proof(generation, drain, deadline).await?)
            }
            Command::Probe { node } => {
                if node != self.config.node_id || group != self.config.runtime.metadata_group {
                    return Err(forbidden());
                }
                wire::json(&Probe {
                    cluster: self.cluster,
                    node: self.config.node_id,
                    role: self.config.role,
                    accepting: !self.draining.load(std::sync::atomic::Ordering::Acquire),
                    capacity: self.scaling.capacity.snapshot().and_then(|sample| {
                        crate::scaling::remote::CapacityReport::from_sample(sample).ok()
                    }),
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
                    if self.rpc.is_local() {
                        break;
                    }
                    self.rpc
                        .pool()?
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
        self.metadata_drain_rpc(Command::Unschedule { node }, deadline)
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn draining_combined_and_separate_ingress_keep_original_console_receipts_queryable() {
        use crate::config::Role;
        use crate::runtime::data::consumer_tests::fixture::Fixture;
        use tidemq_protocol::console_operation::{Status, WriteCommand, WriteRequest};

        for local in [true, false] {
            let mut fixture = if local {
                Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await
            } else {
                Fixture::new(
                    &[
                        (1, Role::Metadata),
                        (2, Role::Storage),
                        (3, Role::Broker),
                        (4, Role::Gateway),
                    ],
                    &[1],
                    &[2],
                )
                .await
            };
            fixture.start_all().await;
            let ingress = if local { 1 } else { 4 };
            let client = fixture.client(ingress).await;
            let tenant = client.admin().create_tenant("drain").await.unwrap();
            let namespace = client
                .admin()
                .create_namespace(tenant, "app")
                .await
                .unwrap();
            let request = WriteRequest {
                operation_id: tidemq_protocol::Id::new_v7().unwrap(),
                command: WriteCommand::CreateTopic {
                    tenant: tenant.into(),
                    namespace: namespace.into(),
                    name: "original".into(),
                },
            };
            let receipt = client.console_write(&request).await.unwrap();
            assert_eq!(receipt.status, Status::Succeeded);
            let service = fixture.running.get(&ingress).unwrap().service.clone();
            service.process.drain();
            let recovered = client
                .console_operation(request.operation_id)
                .await
                .unwrap();
            assert_eq!(recovered.id, receipt.id);
            assert_eq!(recovered.actor, receipt.actor);
            assert_eq!(recovered.resource_id, receipt.resource_id);
            assert_eq!(recovered.status, receipt.status);
            let new = WriteRequest {
                operation_id: tidemq_protocol::Id::new_v7().unwrap(),
                command: WriteCommand::CreateTopic {
                    tenant: tenant.into(),
                    namespace: namespace.into(),
                    name: "must-not-submit".into(),
                },
            };
            assert!(client.console_write(&new).await.is_err());
            assert_eq!(
                client
                    .console_operation(new.operation_id)
                    .await
                    .unwrap()
                    .status,
                Status::Unknown
            );
            client.close().await.unwrap();
            fixture.close().await;
        }
    }

    #[test]
    fn broker_targets_need_fresh_capacity_with_network_time_and_reserve_headroom() {
        let sample = crate::scaling::Capacity {
            node_id: 9,
            observed_at: std::time::Instant::now(),
            free_bytes: 2 * 1024 * 1024 * 1024,
            reserved_bytes: 0,
            bytes_per_second: 0,
            cpu_per_mille: 0,
            connections: 0,
            ack_bytes: 0,
        };
        let report =
            |sample| Some(crate::scaling::remote::CapacityReport::from_sample(sample).unwrap());
        assert!(broker_capacity_available(
            report(sample),
            9,
            std::time::Duration::ZERO
        ));
        assert!(!broker_capacity_available(
            None,
            9,
            std::time::Duration::ZERO
        ));
        assert!(!broker_capacity_available(
            report(sample),
            8,
            std::time::Duration::ZERO
        ));
        assert!(!broker_capacity_available(
            report(sample),
            9,
            std::time::Duration::from_secs(6)
        ));
        for blocked in [
            crate::scaling::Capacity {
                cpu_per_mille: 900,
                ..sample
            },
            crate::scaling::Capacity {
                reserved_bytes: sample.free_bytes,
                ..sample
            },
            crate::scaling::Capacity {
                bytes_per_second: 256 * 1024 * 1024,
                ..sample
            },
        ] {
            assert!(!broker_capacity_available(
                report(blocked),
                9,
                std::time::Duration::ZERO
            ));
        }
    }

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
            (FrameKind::ConsoleWrite, false),
            (FrameKind::GetConsoleOperation, true),
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
