use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;
use tokio::time::Instant;

use super::{
    Error, Result,
    service::Service,
    wire::{self, Request},
};
use crate::metadata::{self, PartitionKey, RetentionControl, RetentionPlan};

pub(super) const CONTROL: u32 = 1_012;

#[cfg(test)]
#[path = "../../tests/runtime_support/retention_semantic_local.rs"]
mod local_tests;

#[derive(Serialize, Deserialize)]
pub(super) enum Control {
    Inspect(PartitionKey),
    Apply(RetentionControl),
}

impl Control {
    fn partition(&self) -> PartitionKey {
        match self {
            Self::Inspect(partition)
            | Self::Apply(
                RetentionControl::Prepare { partition, .. }
                | RetentionControl::Finish { partition, .. },
            ) => *partition,
        }
    }
}

impl Service {
    pub(super) async fn reserve_and_advance_retention(
        &self,
        group: u64,
        partition: PartitionKey,
        status: crate::retention::Status,
        now_ms: u64,
        writer: crate::broker::WriterFence,
        deadline: Instant,
    ) -> Result<()> {
        let previous = self
            .retention_control(Control::Inspect(partition), deadline)
            .await?;
        let plan = if let Some(plan) = previous
            .as_ref()
            .filter(|plan| plan.completed_revision.is_none())
        {
            plan.clone()
        } else if status.maximum_floor > status.floor {
            let Some(plan) = self
                .retention_control(
                    Control::Apply(RetentionControl::Prepare {
                        partition,
                        generation: previous.map_or(0, |plan| plan.generation),
                        command: crate::retention::Command {
                            expected_revision: status.revision,
                            before: status.maximum_floor,
                            now_ms,
                        },
                    }),
                    deadline,
                )
                .await?
            else {
                return Ok(());
            };
            plan
        } else {
            return Ok(());
        };
        self.apply_reserved_retention(group, plan, writer, deadline)
            .await
    }

    async fn retention_revision(
        &self,
        group: u64,
        partition: PartitionKey,
        deadline: Instant,
    ) -> Result<u64> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        replica
            .group
            .ensure_read(deadline)
            .await
            .map_err(crate::runtime::wire::execution_error)?;
        replica
            .state
            .try_with_application(|state| {
                state
                    .retention_status(partition, 0)
                    .map(|status| status.revision)
            })
            .await
            .map_err(|source| Error::operation("verify durable retention revision", source))?
            .ok_or_else(Error::unavailable)
    }

    async fn apply_reserved_retention(
        &self,
        group: u64,
        plan: RetentionPlan,
        writer: crate::broker::WriterFence,
        deadline: Instant,
    ) -> Result<()> {
        let revision = self
            .retention_revision(group, plan.partition, deadline)
            .await?;
        if revision < plan.command.expected_revision {
            return Err(Error::unavailable());
        }
        if revision == plan.command.expected_revision {
            let response = self
                .data_write(
                    group,
                    crate::broker::DataCommand::Retention {
                        partition: plan.partition,
                        writer,
                        command: plan.command,
                    },
                    deadline,
                )
                .await?;
            if !matches!(response, crate::broker::DataResponse::Retention(_)) {
                return Err(Error::invalid("Unexpected retention command outcome"));
            }
        }
        // A later append or an applied retention command permanently fences this original CAS.
        let revision = self
            .retention_revision(group, plan.partition, deadline)
            .await?;
        self.retention_control(
            Control::Apply(RetentionControl::Finish {
                partition: plan.partition,
                generation: plan.generation,
                revision,
            }),
            deadline,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn retention_control(
        &self,
        request: Control,
        deadline: Instant,
    ) -> Result<Option<RetentionPlan>> {
        let group = self.config.runtime.metadata_group;
        let leader = self.leader(group, deadline).await?;
        if leader == self.config.node_id {
            use tracing::Instrument;
            return wire::within(
                deadline,
                Box::pin(
                    self.execute_retention_control(self.config.node_id, request, deadline)
                        .instrument(
                            tracing::info_span!(target: "tidemq_trace", "retention_control_typed",
                    node_id = self.config.node_id, group_id = group),
                        ),
                ),
            )
            .await?;
        }
        let bytes = self
            .rpc
            .call(leader, group, Request::typed(CONTROL, &request)?, deadline)
            .await?;
        wire::decode_binary(&bytes)
    }

    pub(super) async fn retention_control_command(
        &self,
        sender: u64,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        let request: Control = wire::decode_binary(bytes)?;
        let reply = self
            .execute_retention_control(sender, request, deadline)
            .await?;
        wire::binary(&reply)
    }

    async fn execute_retention_control(
        &self,
        sender: u64,
        request: Control,
        deadline: Instant,
    ) -> Result<Option<RetentionPlan>> {
        let _admission = self.rpc.admit_local(
            self.config.runtime.metadata_group,
            crate::transport::StreamPurpose::Core,
            32 * 1024,
            deadline,
        )?;
        wire::remaining(deadline)?;
        let partition = request.partition();
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        replica
            .group
            .ensure_read(deadline)
            .await
            .map_err(crate::runtime::wire::execution_error)?;
        let metadata_voter = replica.current_placement()?.hosts().contains(&sender);
        let data_voter = replica
            .state
            .try_with_application(|state| {
                state
                    .partition_group(partition)
                    .and_then(|group| state.group(group))
                    .is_some_and(|group| group.placement.hosts().contains(&sender))
            })
            .await
            .map_err(|source| Error::operation("validate retention controller", source))?;
        if !metadata_voter && !data_voter {
            return Err(super::service::forbidden());
        }
        let plan = match request {
            Control::Inspect(partition) => replica
                .state
                .try_with_application(|state| state.retention_plan(partition).cloned())
                .await
                .map_err(|source| Error::operation("read retention reservation", source))?,
            Control::Apply(command) => self.write_retention_control(command, deadline).await?,
        };
        Ok(plan)
    }

    async fn write_retention_control(
        &self,
        control: RetentionControl,
        deadline: Instant,
    ) -> Result<Option<RetentionPlan>> {
        let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        for _ in 0..16 {
            self.require_leader(replica)?;
            let operation = Id::new_v7()
                .map_err(|source| Error::operation("allocate retention transition", source))?;
            let command = metadata::Command::new(
                operation,
                self.meta_revision().await?,
                metadata::Action::Retention(control.clone()),
            );
            let applied = if matches!(control, RetentionControl::Finish { .. }) {
                replica.group.commit_completion(command, deadline).await
            } else {
                replica.group.commit(command, deadline).await
            }
            .map_err(wire::execution_error)?;
            let response = applied.response;
            match response {
                metadata::Response::Applied {
                    outcome: metadata::Outcome::Retention(plan),
                    ..
                } => return Ok(Some(plan)),
                metadata::Response::Rejected {
                    reason: metadata::Rejection::RecoveryRequired,
                    ..
                } if matches!(control, RetentionControl::Prepare { .. }) => {
                    let _ = self.retention_deferrals.try_update(
                        std::sync::atomic::Ordering::Relaxed,
                        std::sync::atomic::Ordering::Relaxed,
                        |count| Some(count.saturating_add(1)),
                    );
                    return Ok(None);
                }
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
                    return Err(super::metadata::metadata_rejection(reason));
                }
                _ => {
                    return Err(Error::invalid(
                        "Retention transition returned an invalid result",
                    ));
                }
            }
        }
        Err(Error::unavailable())
    }
}
