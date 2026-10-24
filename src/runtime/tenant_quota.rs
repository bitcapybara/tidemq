use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::{
    Id,
    common::{ErrorCode, Outcome},
};
use tokio::time::Instant;

use super::{
    Error, Result,
    service::Service,
    wire::{self, Request},
};
use crate::{
    broker::{DataCommand, DataResponse, WriterFence},
    metadata::{
        self, AllocationRequest, Command, PartitionKey, QuotaAllocation, QuotaCapacity,
        QuotaCommand, QuotaGrant,
    },
};

pub(super) const CONTROL: u32 = 1_005;
pub(super) const DATA: u32 = 1_006;

#[derive(Clone, Serialize, Deserialize)]
enum ControlRequest {
    Status(PartitionKey),
    Prepare(AllocationRequest),
    Installed(QuotaGrant),
}

#[derive(Serialize, Deserialize)]
struct Status {
    partition: PartitionKey,
    tenant: Id,
    namespace: Id,
    group: u64,
    allocation: Option<QuotaAllocation>,
    available: QuotaCapacity,
    suspended: bool,
    now_ms: u64,
}

#[derive(Serialize, Deserialize)]
enum ControlReply {
    Status(Box<Status>),
    Grant(QuotaGrant),
}

#[derive(Serialize, Deserialize)]
enum DataRequest {
    Status(PartitionKey),
    Install(QuotaGrant),
}

#[derive(Serialize, Deserialize)]
struct DataStatus {
    partition: PartitionKey,
    grant: Option<QuotaGrant>,
    usage: QuotaCapacity,
    last_activity_ms: Option<u64>,
}

impl Service {
    pub(super) async fn maintain_tenant_quotas(
        &self,
        group: u64,
        after: &mut Option<PartitionKey>,
        deadline: Instant,
    ) -> Result<()> {
        let partitions = self
            .data
            .get(&group)
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| state.quota_partitions(*after))
            .await
            .map_err(|source| Error::operation("page tenant quota recovery", source))?;
        for partition in &partitions {
            *after = Some(*partition);
            if let Err(error) = self
                .reconcile_tenant_quota(group, *partition, deadline)
                .await
            {
                let error = anyhow::Error::new(error);
                tracing::debug!(event_code = "tenant_quota_recovery_wait", group_id = group,
                    error = %format_args!("{error:#}"), "Tenant quota reconciliation will resume");
            }
            if Instant::now() >= deadline {
                break;
            }
        }
        if partitions.len() < 4 {
            *after = None;
        }
        Ok(())
    }

    async fn reconcile_tenant_quota(
        &self,
        group: u64,
        partition: PartitionKey,
        deadline: Instant,
    ) -> Result<()> {
        for _ in 0..3 {
            let status = self.quota_status(partition, deadline).await?;
            if status.group != group {
                return Err(Error::unavailable());
            }
            let Some(allocation) = &status.allocation else {
                return Ok(());
            };
            if let Some(pending) = &allocation.pending {
                self.install_quota_allocation(group, pending, deadline)
                    .await?;
                continue;
            }
            let Some(installed) = &allocation.installed else {
                return Ok(());
            };
            let data = self.query_data_quota(group, partition, deadline).await?;
            if data.grant.as_ref() != Some(installed) {
                return Err(Error::unavailable());
            }
            let idle = data
                .last_activity_ms
                .is_some_and(|last| status.now_ms.saturating_sub(last) >= 5_000);
            let (capacity, frozen) =
                if installed.frozen && (installed.capacity != data.usage || !status.suspended) {
                    (data.usage, status.suspended)
                } else if status.suspended && !installed.frozen {
                    (installed.capacity, true)
                } else if idle && installed.capacity != data.usage {
                    // Stop new use before measuring and returning credit to another authority.
                    (installed.capacity, true)
                } else {
                    return Ok(());
                };
            let ControlReply::Grant(grant) = self
                .quota_control(
                    &ControlRequest::Prepare(AllocationRequest {
                        partition,
                        tenant: status.tenant,
                        namespace: status.namespace,
                        expected_version: installed.version,
                        capacity,
                        frozen,
                    }),
                    deadline,
                )
                .await?
            else {
                return Err(Error::invalid("Quota reclamation did not return a grant"));
            };
            self.install_quota_allocation(group, &grant, deadline)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn quota_control_command(
        &self,
        sender: u64,
        body: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let request: ControlRequest = wire::decode_json(body)?;
        let partition = match &request {
            ControlRequest::Status(partition) => *partition,
            ControlRequest::Prepare(request) => request.partition,
            ControlRequest::Installed(grant) => grant.partition,
        };
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let receiver = replica.raft.metrics();
        let metadata_voter = receiver
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .any(|node| node == sender);
        let data_voter = replica
            .state
            .try_with_application(|state| {
                state
                    .partition_group(partition)
                    .and_then(|group| state.group(group))
                    .is_some_and(|group| group.voters.contains(&sender))
            })
            .await
            .map_err(|source| Error::operation("check quota controller authority", source))?;
        if !metadata_voter && !data_voter {
            return Err(super::service::forbidden());
        }
        wire::json(&self.execute_quota_control(request, deadline).await?)
    }

    async fn execute_quota_control(
        &self,
        request: ControlRequest,
        deadline: Instant,
    ) -> Result<ControlReply> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        let reply = match request {
            ControlRequest::Status(partition) => {
                ControlReply::Status(Box::new(self.read_quota_status(partition).await?))
            }
            ControlRequest::Prepare(request) => {
                let grant = self
                    .write_quota(
                        QuotaCommand::Prepare {
                            request,
                            now_ms: super::data::unix_millis()?,
                        },
                        deadline,
                    )
                    .await?;
                ControlReply::Grant(grant)
            }
            ControlRequest::Installed(grant) => {
                let status = self.read_quota_status(grant.partition).await?;
                if status
                    .allocation
                    .as_ref()
                    .and_then(|record| record.installed.as_ref())
                    == Some(&grant)
                {
                    return Ok(ControlReply::Grant(grant));
                }
                if status
                    .allocation
                    .as_ref()
                    .and_then(|record| record.pending.as_ref())
                    != Some(&grant)
                {
                    return Err(Error::unavailable());
                }
                let installed = self
                    .query_data_quota(status.group, grant.partition, deadline)
                    .await?;
                if installed.grant.as_ref() != Some(&grant) {
                    return Err(Error::unavailable());
                }
                let grant = self
                    .write_quota(
                        QuotaCommand::Installed {
                            grant,
                            now_ms: super::data::unix_millis()?,
                        },
                        deadline,
                    )
                    .await?;
                ControlReply::Grant(grant)
            }
        };
        Ok(reply)
    }

    async fn read_quota_status(&self, partition: PartitionKey) -> Result<Status> {
        let now_ms = super::data::unix_millis()?;
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                let topic = state
                    .topic(partition.topic_id)
                    .ok_or_else(super::metadata::not_found)?;
                let tenant = topic.tenant.ok_or_else(super::service::forbidden)?;
                let namespace = topic.namespace.ok_or_else(super::service::forbidden)?;
                let (parent, child) = state
                    .auth()
                    .limits(tenant, namespace)
                    .map_err(|_| super::service::forbidden())?;
                let available = parent
                    .capacity
                    .saturating_sub(
                        state
                            .quotas()
                            .reserved(tenant, None, now_ms)
                            .map_err(super::metadata::metadata_rejection)?,
                    )
                    .minimum(
                        child.capacity.saturating_sub(
                            state
                                .quotas()
                                .reserved(tenant, Some(namespace), now_ms)
                                .map_err(super::metadata::metadata_rejection)?,
                        ),
                    );
                Ok::<_, Error>(Status {
                    partition,
                    tenant,
                    namespace,
                    group: state
                        .partition_group(partition)
                        .ok_or_else(super::metadata::not_found)?,
                    allocation: state.quotas().allocation(partition).cloned(),
                    available,
                    suspended: state
                        .quota_suspended(tenant, namespace, now_ms)
                        .map_err(super::metadata::metadata_rejection)?,
                    now_ms,
                })
            })
            .await
            .map_err(|source| Error::operation("read shared tenant quota allocation", source))?
    }

    // Quota transitions use their own monotone versions; CAS retries never wait on a caller's metadata lock.
    async fn write_quota(&self, command: QuotaCommand, deadline: Instant) -> Result<QuotaGrant> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        for _ in 0..16 {
            self.require_leader(replica)?;
            let operation = Id::new_v7()
                .map_err(|source| Error::operation("allocate quota transition", source))?;
            let command = Command::quota(operation, self.meta_revision().await?, command.clone());
            let response = wire::within(deadline, replica.raft.client_write(command))
                .await?
                .map_err(|source| Error::operation("persist tenant quota allocation", source))?
                .data;
            match response {
                metadata::Response::Applied {
                    outcome: metadata::Outcome::Quota(grant),
                    ..
                } => return Ok(grant),
                metadata::Response::Rejected {
                    reason: metadata::Rejection::RevisionConflict,
                    ..
                } => {
                    wire::within(
                        deadline,
                        tokio::time::sleep(std::time::Duration::from_millis(2)),
                    )
                    .await?;
                }
                metadata::Response::Rejected { reason, .. } => {
                    if reason == metadata::Rejection::Superseded {
                        return Err(Error::unavailable());
                    }
                    return Err(super::metadata::metadata_rejection(reason));
                }
                _ => {
                    return Err(Error::invalid(
                        "Quota transition returned an invalid metadata outcome",
                    ));
                }
            }
        }
        Err(Error::unavailable())
    }

    async fn quota_control(
        &self,
        request: &ControlRequest,
        deadline: Instant,
    ) -> Result<ControlReply> {
        if self
            .metadata
            .as_ref()
            .is_some_and(|replica| self.require_leader(replica).is_ok())
        {
            return self.execute_quota_control(request.clone(), deadline).await;
        }
        let group = self.config.runtime.metadata_group;
        let leader = self.leader(group, deadline).await?;
        let bytes = self
            .rpc
            .call(leader, group, Request::control(CONTROL, request)?, deadline)
            .await?;
        wire::decode_json(&bytes)
    }

    async fn quota_status(&self, partition: PartitionKey, deadline: Instant) -> Result<Status> {
        match self
            .quota_control(&ControlRequest::Status(partition), deadline)
            .await?
        {
            ControlReply::Status(status) if status.partition == partition => Ok(*status),
            _ => Err(Error::invalid(
                "Quota status changed its original data domain",
            )),
        }
    }

    async fn query_data_quota(
        &self,
        group: u64,
        partition: PartitionKey,
        deadline: Instant,
    ) -> Result<DataStatus> {
        if self
            .data
            .get(&group)
            .is_some_and(|replica| self.require_leader(replica).is_ok())
        {
            return self.read_data_quota(group, partition, deadline).await;
        }
        let node = self.leader(group, deadline).await?;
        let bytes = self
            .rpc
            .call(
                node,
                group,
                Request::control(DATA, &DataRequest::Status(partition))?,
                deadline,
            )
            .await?;
        let status: DataStatus = wire::decode_json(&bytes)?;
        if status.partition != partition {
            return Err(Error::invalid(
                "Data quota status changed its original domain",
            ));
        }
        Ok(status)
    }

    pub(super) async fn quota_data_command(
        &self,
        group: u64,
        body: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let request: DataRequest = wire::decode_json(body)?;
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        match request {
            DataRequest::Status(partition) => {
                wire::json(&self.read_data_quota(group, partition, deadline).await?)
            }
            DataRequest::Install(grant) => {
                wire::json(&self.apply_quota_allocation(group, grant, deadline).await?)
            }
        }
    }

    async fn read_data_quota(
        &self,
        group: u64,
        partition: PartitionKey,
        deadline: Instant,
    ) -> Result<DataStatus> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        wire::within(deadline, replica.raft.ensure_linearizable())
            .await?
            .map_err(|_| Error::unavailable())?;
        let status = replica
            .state
            .try_with_application(|state| {
                Ok::<_, Error>(DataStatus {
                    partition,
                    grant: state.tenant_quota(partition).cloned(),
                    usage: state
                        .tenant_quota_usage(partition)
                        .map_err(super::data::data_rejection)?,
                    last_activity_ms: state.quota_last_activity(partition),
                })
            })
            .await
            .map_err(|source| Error::operation("read durable data quota", source))??;
        Ok(status)
    }

    async fn apply_quota_allocation(
        &self,
        group: u64,
        grant: QuotaGrant,
        deadline: Instant,
    ) -> Result<QuotaGrant> {
        let status = self.quota_status(grant.partition, deadline).await?;
        if status
            .allocation
            .as_ref()
            .and_then(|record| record.installed.as_ref())
            .is_some_and(|installed| installed.version > grant.version)
        {
            return Err(Error::unavailable());
        }
        if status.group != group
            || status.allocation.as_ref().is_none_or(|record| {
                record.installed.as_ref() != Some(&grant) && record.pending.as_ref() != Some(&grant)
            })
        {
            return Err(super::service::forbidden());
        }
        let lease = self.data_lease(group, grant.partition, deadline).await?;
        let writer = WriterFence {
            node_id: lease.grant().owner_node,
            owner_epoch: lease.grant().epoch,
        };
        match self
            .data_write(
                group,
                DataCommand::InstallQuota {
                    partition: grant.partition,
                    writer,
                    grant: grant.clone(),
                    now_ms: super::data::unix_millis()?,
                },
                deadline.min(lease.expires_at().into()),
            )
            .await?
        {
            DataResponse::QuotaInstalled(installed) if installed == grant => Ok(grant),
            _ => Err(Error::invalid(
                "Quota installation returned a different grant",
            )),
        }
    }

    async fn install_quota_allocation(
        &self,
        group: u64,
        grant: &QuotaGrant,
        deadline: Instant,
    ) -> Result<()> {
        let installed = if self
            .data
            .get(&group)
            .is_some_and(|replica| self.require_leader(replica).is_ok())
        {
            self.apply_quota_allocation(group, grant.clone(), deadline)
                .await?
        } else {
            let node = self.leader(group, deadline).await?;
            let bytes = self
                .rpc
                .call(
                    node,
                    group,
                    Request::control(DATA, &DataRequest::Install(grant.clone()))?,
                    deadline,
                )
                .await?;
            wire::decode_json::<QuotaGrant>(&bytes)?
        };
        if installed != *grant {
            return Err(Error::invalid(
                "Quota installation changed the pending grant",
            ));
        }
        match self
            .quota_control(&ControlRequest::Installed(grant.clone()), deadline)
            .await?
        {
            ControlReply::Grant(installed) if installed == *grant => Ok(()),
            _ => Err(Error::invalid(
                "Quota installation confirmation changed the pending grant",
            )),
        }
    }

    pub(super) async fn ensure_tenant_quota(
        &self,
        group: u64,
        partition: PartitionKey,
        demand: QuotaCapacity,
        deadline: Instant,
    ) -> Result<()> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        let local = replica
            .state
            .try_with_application(|state| state.tenant_quota(partition).cloned())
            .await
            .map_err(|source| Error::operation("read local tenant quota", source))?;
        if local
            .as_ref()
            .is_some_and(|grant| !grant.frozen && demand.fits(grant.capacity))
        {
            return Ok(());
        }
        for _ in 0..4 {
            let status = self.quota_status(partition, deadline).await?;
            if status.group != group {
                return Err(Error::unavailable());
            }
            if let Some(grant) = status
                .allocation
                .as_ref()
                .and_then(|record| record.pending.as_ref())
            {
                self.install_quota_allocation(group, grant, deadline)
                    .await?;
                continue;
            }
            let installed = status
                .allocation
                .as_ref()
                .and_then(|record| record.installed.as_ref());
            if installed.is_some_and(|grant| grant.frozen) {
                return Err(exhausted());
            }
            if installed.is_some_and(|grant| demand.fits(grant.capacity)) {
                return Ok(());
            }
            let previous = installed.map_or_default(|grant| grant.capacity);
            let reserved = status
                .allocation
                .as_ref()
                .map_or_default(|record| record.reserved(status.now_ms));
            let maximum = reserved
                .checked_add(status.available)
                .map_err(super::metadata::metadata_rejection)?;
            let capacity = grow(previous, demand, maximum)?;
            let request = AllocationRequest {
                partition,
                tenant: status.tenant,
                namespace: status.namespace,
                expected_version: installed.map_or(0, |grant| grant.version),
                capacity,
                frozen: false,
            };
            let ControlReply::Grant(grant) = self
                .quota_control(&ControlRequest::Prepare(request), deadline)
                .await?
            else {
                return Err(Error::invalid(
                    "Quota preparation did not return its pending grant",
                ));
            };
            self.install_quota_allocation(group, &grant, deadline)
                .await?;
            return Ok(());
        }
        Err(Error::unavailable())
    }
}

fn exhausted() -> Error {
    Error::reject(ErrorCode::QuotaExceeded, Outcome::Rejected, true)
}

fn grow(
    previous: QuotaCapacity,
    demand: QuotaCapacity,
    maximum: QuotaCapacity,
) -> Result<QuotaCapacity> {
    if !demand.fits(maximum) {
        return Err(exhausted());
    }
    let next = |current: u64, required: u64, limit: u64, quantum: u64| {
        if current >= required {
            current
        } else {
            required
                .div_ceil(quantum)
                .saturating_mul(quantum)
                .min(limit)
        }
    };
    Ok(QuotaCapacity {
        backlog_bytes: next(
            previous.backlog_bytes,
            demand.backlog_bytes,
            maximum.backlog_bytes,
            1024 * 1024,
        ),
        inflight_bytes: next(
            previous.inflight_bytes,
            demand.inflight_bytes,
            maximum.inflight_bytes,
            256 * 1024,
        ),
        inflight_messages: next(
            previous.inflight_messages,
            demand.inflight_messages,
            maximum.inflight_messages,
            64,
        ),
        publish_bytes_per_second: next(
            previous.publish_bytes_per_second,
            demand.publish_bytes_per_second,
            maximum.publish_bytes_per_second,
            1024 * 1024,
        ),
        consume_bytes_per_second: next(
            previous.consume_bytes_per_second,
            demand.consume_bytes_per_second,
            maximum.consume_bytes_per_second,
            1024 * 1024,
        ),
        cold_reads: next(
            previous.cold_reads,
            demand.cold_reads,
            maximum.cold_reads,
            1,
        ),
    })
}

pub(super) fn is_core(kind: u32, body: &[u8]) -> bool {
    kind == CONTROL
        || kind == DATA
            && matches!(
                wire::decode_json::<DataRequest>(body),
                Ok(DataRequest::Status(_))
            )
}
