use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tidemq_protocol::Id;
use tokio::time::Instant;

use super::{
    Error, Result,
    service::Service,
    wire::{self, Request},
};
use crate::{
    lifecycle::GuardedPage,
    metadata::{self, Command, ResourceLease, ResourceLeaseCommand, ResourceLeaseTarget, Route},
};

pub(super) const RESOURCE_LEASE: u32 = 1_007;

#[derive(Serialize, Deserialize)]
pub(super) enum LeaseRequest {
    Acquire { id: Id, target: ResourceLeaseTarget },
    Renew { id: Id },
    Release { id: Id },
}

#[derive(Serialize, Deserialize)]
pub(super) struct LeaseReply {
    pub lease: Option<ResourceLease>,
    now_ms: u64,
}

impl LeaseReply {
    pub(super) fn valid_until(&self, started: Instant) -> Result<Instant> {
        let remaining = self
            .lease
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .expires_ms
            .saturating_sub(self.now_ms);
        started
            .checked_add(std::time::Duration::from_millis(remaining))
            .filter(|expires| *expires > Instant::now())
            .ok_or_else(Error::unavailable)
    }
}

impl Service {
    pub(super) async fn connection_quota_lease(
        self: &std::sync::Arc<Self>,
        credential: Id,
        deadline: Instant,
    ) -> Result<Option<tokio::sync::watch::Receiver<Instant>>> {
        let id = Id::new_v7().map_err(|source| {
            Error::operation("allocate physical connection quota identity", source)
        })?;
        let started = Instant::now();
        let reply = self
            .resource_lease(
                LeaseRequest::Acquire {
                    id,
                    target: ResourceLeaseTarget::Connection(credential),
                },
                deadline,
            )
            .await?;
        if reply.lease.is_none() {
            return Ok(None);
        }
        let expires = reply.valid_until(started)?;
        let (validity, receiver) = tokio::sync::watch::channel(expires);
        let service = std::sync::Arc::downgrade(self);
        tokio::spawn(maintain_connection_lease(service, id, validity, expires));
        Ok(Some(receiver))
    }

    pub(super) async fn resource_lease_command(
        &self,
        owner: u64,
        body: &[u8],
        deadline: Instant,
    ) -> Result<Bytes> {
        wire::json(
            &self
                .execute_resource_lease(owner, wire::decode_json(body)?, deadline)
                .await?,
        )
    }

    async fn execute_resource_lease(
        &self,
        owner: u64,
        request: LeaseRequest,
        deadline: Instant,
    ) -> Result<LeaseReply> {
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let now_ms = super::data::unix_millis()?;
        let action = match request {
            LeaseRequest::Acquire { id, target } => ResourceLeaseCommand::Acquire {
                id,
                target,
                owner,
                now_ms,
            },
            LeaseRequest::Renew { id } => ResourceLeaseCommand::Renew { id, owner, now_ms },
            LeaseRequest::Release { id } => ResourceLeaseCommand::Release { id, owner, now_ms },
        };
        for _ in 0..16 {
            self.require_leader(replica)?;
            let operation = Id::new_v7()
                .map_err(|source| Error::operation("allocate resource lease mutation", source))?;
            let command =
                Command::resource_lease(operation, self.meta_revision().await?, action.clone());
            let applied = if matches!(
                action,
                ResourceLeaseCommand::Renew { .. } | ResourceLeaseCommand::Release { .. }
            ) {
                replica.group.commit_completion(command, deadline).await
            } else {
                replica.group.commit(command, deadline).await
            }
            .map_err(wire::execution_error)?;
            let reply = applied.response;
            match reply {
                metadata::Response::Applied {
                    outcome: metadata::Outcome::ResourceLease(lease),
                    ..
                } => {
                    return Ok(LeaseReply {
                        lease,
                        now_ms: super::data::unix_millis()?,
                    });
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
                        "Resource lease mutation returned an invalid result",
                    ));
                }
            }
        }
        Err(Error::unavailable())
    }

    pub(super) async fn resource_lease(
        &self,
        request: LeaseRequest,
        deadline: Instant,
    ) -> Result<LeaseReply> {
        if self
            .metadata
            .as_ref()
            .is_some_and(|replica| self.require_leader(replica).is_ok())
        {
            return self
                .execute_resource_lease(self.config.node_id, request, deadline)
                .await;
        }
        let group = self.config.runtime.metadata_group;
        let node = self.leader(group, deadline).await?;
        let reply = self
            .rpc
            .call(
                node,
                group,
                Request::control(RESOURCE_LEASE, &request)?,
                deadline,
            )
            .await?;
        wire::decode_json(&reply)
    }

    pub(super) async fn read_tenant_archive(
        &self,
        route: &Route,
        offset: u64,
        messages: usize,
        bytes: u64,
        deadline: Instant,
    ) -> Result<GuardedPage> {
        let id = Id::new_v7()
            .map_err(|source| Error::operation("allocate tenant cold read lease", source))?;
        let started = Instant::now();
        let lease = self
            .resource_lease(
                LeaseRequest::Acquire {
                    id,
                    target: ResourceLeaseTarget::ColdRead(route.partition),
                },
                deadline,
            )
            .await?;
        let expires = deadline.min(lease.valid_until(started)?);
        #[cfg(feature = "test-hooks")]
        super::test_hooks::pause_cold_read(route.partition, expires).await;
        let coordinator = self
            .objects
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?;
        let result = wire::within(
            expires,
            coordinator.read(route.partition, offset, messages, bytes, expires.into_std()),
        )
        .await
        .and_then(|result| {
            result.map_err(|source| Error::operation("read tenant archive page", source))
        });
        // A lost release retains credit until expiry; it never allows another overlapping I/O lease.
        let release_deadline = deadline.min(Instant::now() + std::time::Duration::from_millis(100));
        let _ = self
            .resource_lease(LeaseRequest::Release { id }, release_deadline)
            .await;
        result
    }
}

async fn maintain_connection_lease(
    service: std::sync::Weak<Service>,
    id: Id,
    validity: tokio::sync::watch::Sender<Instant>,
    mut expires: Instant,
) {
    loop {
        let renewal = Instant::now() + expires.saturating_duration_since(Instant::now()) / 2;
        tokio::select! {
            _ = validity.closed() => break,
            _ = tokio::time::sleep_until(renewal) => {}
        }
        let Some(service) = service.upgrade() else {
            return;
        };
        let started = Instant::now();
        let result = service
            .resource_lease(
                LeaseRequest::Renew { id },
                expires.min(started + std::time::Duration::from_secs(5)),
            )
            .await
            .and_then(|reply| reply.valid_until(started));
        match result {
            Ok(next) => {
                expires = next;
                if validity.send(next).is_err() {
                    break;
                }
            }
            Err(_) => {
                let _ = validity.send(Instant::now());
                break;
            }
        }
    }
    if let Some(service) = service.upgrade() {
        let _ = service
            .resource_lease(
                LeaseRequest::Release { id },
                Instant::now() + std::time::Duration::from_secs(1),
            )
            .await;
    }
}

pub(super) fn is_release(kind: u32, body: &[u8]) -> bool {
    kind == RESOURCE_LEASE
        && matches!(
            wire::decode_json::<LeaseRequest>(body),
            Ok(LeaseRequest::Release { .. })
        )
}
