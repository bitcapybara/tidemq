use super::{
    Error, Result,
    consumer_watch_authority::Permit,
    service::{Service, forbidden},
    wire::{self, Request},
};
use crate::{
    broker::{WatchIncarnation, WatchKey, WatchSample},
    gateway::Access,
    metadata::Route,
    server::{
        DeferredReply,
        group_watch::{Charge, Key, Refusal},
    },
    transport::Authorization,
};
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, task::Poll, time::Duration};
use tidemq_protocol::{
    Id, ProtocolLimits,
    common::{ErrorCode, Frame, FrameKind, Outcome, TrafficClass},
    framing::{decode_frame, decode_payload, encode_payload},
    internal::InternalCommand,
    public, watch as protocol,
};
use tokio::time::Instant;
use tracing::Instrument;

tokio::task_local! {
    static ADMISSION: std::sync::Mutex<Option<crate::server::group_watch::Admission>>;
    pub(super) static DELEGATION: (u64, u64);
}

fn release_admission() {
    let _ = ADMISSION.try_with(|admission| {
        if let Ok(mut admission) = admission.lock() {
            admission.take();
        }
    });
}

type WatchFuture<'a> = Pin<Box<dyn Future<Output = Result<public::GroupWatchResult>> + Send + 'a>>;

struct WatchScope {
    request: public::GroupWatch,
    permit: Permit,
}

fn watch_scopes(request: &public::GroupWatch, permit: &Permit) -> Vec<WatchScope> {
    let mut groups =
        BTreeMap::<(u64, u64, u64, u64), Vec<(public::WatchTarget, (Id, Route))>>::new();
    for (target, (shard, route)) in request.targets.iter().zip(&permit.targets) {
        groups
            .entry((
                route.owner_node,
                route.group_id,
                route.epoch,
                route.generation,
            ))
            .or_default()
            .push((target.clone(), (*shard, route.clone())));
    }
    groups
        .into_values()
        .map(|targets| {
            let mut watch = request.clone();
            watch.targets = targets.iter().map(|(target, _)| target.clone()).collect();
            WatchScope {
                request: watch,
                permit: Permit {
                    targets: targets.into_iter().map(|(_, route)| route).collect(),
                    installed_version: permit.installed_version,
                    remaining_ms: permit.remaining_ms,
                    tenant: permit.tenant,
                },
            }
        })
        .collect()
}

struct ReplyCharge {
    bytes: Bytes,
    _charge: Charge,
}
impl AsRef<[u8]> for ReplyCharge {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

pub(super) fn charge_reply(mut response: Frame, charge: Charge) -> Frame {
    response.payload = Bytes::from_owner(ReplyCharge {
        bytes: response.payload,
        _charge: charge,
    });
    response
}

struct Branch {
    target: u64,
    group: u64,
    hops: u32,
    operation: Id,
    frame: Frame,
    request: public::GroupWatch,
    cleanup: Option<BranchCleanup>,
    sent: AtomicBool,
    finished: AtomicBool,
}
struct BranchCleanup {
    registry: Arc<crate::server::group_watch::Registry>,
    rpc: wire::PeerRpc,
    access: Access,
    limits: ProtocolLimits,
    budget: crate::server::group_watch::Budget,
    deadline: Instant,
    charge: Charge,
}
struct CancelBranch {
    target: u64,
    group: u64,
    hops: u32,
    root: Bytes,
    operation: Id,
    attempt: u64,
}
impl Drop for Branch {
    fn drop(&mut self) {
        let Some(cleanup) = self.cleanup.take() else {
            return;
        };
        if !self.sent.load(Ordering::Acquire) || self.finished.load(Ordering::Acquire) {
            return;
        }
        let route = CancelBranch {
            target: self.target,
            group: self.group,
            hops: self.hops,
            root: Bytes::copy_from_slice(&self.request.root_operation_id),
            operation: self.operation,
            attempt: self.request.attempt,
        };
        let budget = cleanup.budget.clone();
        let span = watch_span(
            self.operation,
            &self.request.root_operation_id,
            &self.request.parent_operation_id,
            self.request.attempt,
        );
        let cleanup_span = span.clone();
        let future = async move {
            if let Err(error) = cancel_branch(
                cleanup.rpc,
                cleanup.access,
                route,
                cleanup.limits,
                cleanup.deadline,
            )
            .await
            {
                cleanup.budget.metrics().cleanup_unknown.inc();
                log_watch_cleanup(&cleanup_span, error);
            }
        }
        .instrument(span.clone());
        if let Err(reason) = cleanup.registry.cleanup(future, cleanup.charge) {
            budget.metrics().cleanup_unknown.inc();
            log_watch_cleanup(&span, refusal(reason));
        }
    }
}

impl Service {
    pub(super) fn reserve_watch_forward(
        &self,
        access: &Access,
        frame: &Frame,
    ) -> Result<Option<Charge>> {
        if frame.kind != i32::from(FrameKind::GroupWatch) {
            return Ok(None);
        }
        let request: public::GroupWatch =
            protocol::decode(frame.payload.clone(), &self.config.protocol)
                .map_err(|_| Error::invalid("Group watch payload exceeds its bounded encoding"))?;
        let member = wire::id(&request.member_id)?;
        self.watch_budget
            .reserve_forward(
                access.tenant,
                member,
                16_384_usize.saturating_add(frame.payload.len().saturating_mul(2)),
            )
            .map(Some)
            .ok_or_else(|| self.watch_overloaded())
    }
    pub(super) fn defer_watch(
        self: &Arc<Self>,
        peer: Authorization,
        frame: &Frame,
        deadline: Instant,
    ) -> Option<DeferredReply<Error>> {
        let watch = watch_request(frame, &self.config.protocol)?;
        let operation = wire::id(&frame.operation_id).ok()?;
        let member = wire::id(&watch.member_id).unwrap_or(operation);
        let span = watch_span(
            operation,
            &watch.root_operation_id,
            &watch.parent_operation_id,
            watch.attempt,
        );
        let tenant = self.scheduling_watch_tenant(&peer, frame);
        let charge = self.watch_budget.reserve(
            tenant,
            member,
            watch.targets.len().clamp(1, 32),
            16_384_usize.saturating_add(frame.payload.len().saturating_mul(2)),
            watch.wait_ms > 0,
        )?;
        let compact = Frame {
            request_id: Bytes::copy_from_slice(&frame.request_id),
            operation_id: Bytes::copy_from_slice(&frame.operation_id),
            payload: Bytes::copy_from_slice(&frame.payload),
            traceparent: frame.traceparent.clone(),
            ..frame.clone()
        };
        let service = self.clone();
        Some(Box::pin(
            async move {
                let started = Instant::now();
                service.watch_budget.metrics().admitted.inc();
                let admission = tokio::select! {
                    _ = service.watch_stopped() => None,
                    admission = service.watch_budget.admit(tenant, member, deadline) => admission,
                };
                let Some(admission) = admission else {
                    return wire::error_response(
                        &compact,
                        &service.watch_overloaded(),
                        &service.config.protocol,
                    )
                    .map(|response| Some(charge_reply(response, charge)));
                };
                let response = ADMISSION
                    .scope(
                        std::sync::Mutex::new(Some(admission)),
                        crate::server::WireService::handle(
                            service.as_ref(),
                            peer,
                            &compact,
                            deadline,
                            None,
                        ),
                    )
                    .await?;
                service
                    .watch_budget
                    .metrics()
                    .elapsed(started.elapsed().as_secs_f64());
                Ok(response.map(|response| charge_reply(response, charge)))
            }
            .instrument(span),
        ))
    }

    fn scheduling_watch_tenant(&self, peer: &Authorization, frame: &Frame) -> Option<Id> {
        super::auth::scheduling_key(peer, frame, &self.config.protocol)
            .and_then(|key| Id::from_slice(&key).ok())
    }

    pub(super) fn group_watch_cancel(&self, access: &Access, frame: &Frame) -> Result<Frame> {
        let cancel: public::GroupWatchCancel =
            decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode watch cancellation", source))?;
        let root = wire::id(&cancel.root_operation_id)?;
        let operation = wire::id(&cancel.operation_id)?;
        let session = access.session.ok_or_else(forbidden)?;
        if cancel.attempt == 0 || frame.operation_id == cancel.operation_id {
            return Err(Error::invalid(
                "Watch cancellation requires an attempt and an independent frame operation",
            ));
        }
        let matched = self
            .watch_registry
            .cancel(&access.principal, session, root, operation, cancel.attempt)
            .map_err(|reason| self.watch_refusal(reason))?;
        log_watch_cancel(
            &watch_span(operation, &cancel.root_operation_id, &[], cancel.attempt),
            matched,
        );
        #[cfg(feature = "test-hooks")]
        super::watch_test_hooks::event(super::watch_test_hooks::WatchEvent {
            cluster: self.cluster,
            node: self.config.node_id,
            group: 0,
            root,
            parent: None,
            operation,
            attempt: cancel.attempt,
            targets: 0,
            phase: if matched {
                super::watch_test_hooks::WatchPhase::Cancelled
            } else {
                super::watch_test_hooks::WatchPhase::CancelUnknown
            },
        });
        if !matched {
            let charge = self
                .watch_budget
                .reserve(access.tenant, operation, 0, 1_024, false)
                .ok_or_else(|| self.watch_overloaded())?;
            self.watch_registry
                .remember_cancel(
                    Key {
                        principal: access.principal.clone(),
                        session,
                        member: operation,
                        epoch: 0,
                        root,
                        operation,
                    },
                    cancel.attempt,
                    Instant::now()
                        .checked_add(Duration::from_millis(
                            tidemq_protocol::framing::MAX_TIMEOUT_MS,
                        ))
                        .ok_or_else(Error::unavailable)?,
                    charge,
                )
                .map_err(|reason| self.watch_refusal(reason))?;
        }
        wire::response(
            frame,
            FrameKind::GroupWatchCancelled,
            &public::GroupWatchCancelled {},
            &self.config.protocol,
        )
    }

    pub(super) async fn group_watch_public(
        &self,
        access: &Access,
        frame: &Frame,
        deadline: Instant,
        hops: u32,
    ) -> Result<Frame> {
        // Generic credential authorization is bounded separately from long-lived waiting.
        release_admission();
        let request: public::GroupWatch =
            protocol::decode(frame.payload.clone(), &self.config.protocol)
                .map_err(|_| Error::invalid("Group watch payload exceeds its bounded encoding"))?;
        let operation = wire::id(&frame.operation_id)?;
        protocol::validate(&request, operation, &self.config.protocol).map_err(|_| {
            Error::invalid(
                "Group watch identities, targets, tokens, wait or frame bounds are invalid",
            )
        })?;
        if ADMISSION.try_with(|_| ()).is_err() {
            // Rejections use the dispatcher's bounded reply ownership instead of an uncharged task.
            return Err(self.watch_overloaded());
        }
        if (hops == 0) != request.parent_operation_id.is_empty() {
            return Err(forbidden());
        }
        if !self.watch_accepting() {
            return Err(Error::unavailable());
        }
        let span = watch_span(
            operation,
            &request.root_operation_id,
            &request.parent_operation_id,
            request.attempt,
        );
        let result = async {
            tracing::debug!(
                event_code = "watch_received",
                targets = request.targets.len(),
                "Observe consumer readiness without allocating deliveries"
            );
            let charge = self
                .watch_budget
                .reserve(
                    access.tenant,
                    wire::id(&request.member_id)?,
                    0,
                    2_048,
                    false,
                )
                .ok_or_else(|| self.watch_overloaded())?;
            let registration = self
                .watch_registry
                .register(
                    Key {
                        principal: access.principal.clone(),
                        session: access.session.ok_or_else(forbidden)?,
                        member: wire::id(&request.member_id)?,
                        epoch: request.member_epoch,
                        root: wire::id(&request.root_operation_id)?,
                        operation,
                    },
                    protocol::digest(&request),
                    request.attempt,
                    deadline,
                    charge,
                )
                .map_err(|reason| self.watch_refusal(reason))?;
            let deadline = registration.deadline.min(deadline);
            let member = wire::id(&request.member_id)?;
            let admission = tokio::select! {
                _ = registration.cancelled.cancelled() => return Err(Error::unavailable()),
                _ = self.watch_stopped() => return Err(Error::unavailable()),
                admission = self.watch_budget.admit(access.tenant, member, deadline) => {
                    admission.ok_or_else(|| self.watch_overloaded())?
                }
            };
            let (permit, expires) = tokio::select! {
                _ = registration.cancelled.cancelled() => return Err(Error::unavailable()),
                _ = self.watch_stopped() => return Err(Error::unavailable()),
                authorization = self.watch_authorize(access, &request, deadline) => authorization.map_err(|error| {
                    self.watch_scope_failure(&request, (operation, 0), "entry_authorize", deadline, error)
                })?,
            };
            if hops == 2
                && DELEGATION
                    .try_with(|(owner, group)| {
                        permit.targets.iter().any(|(_, route)| {
                            route.owner_node != *owner || route.group_id != *group
                        })
                    })
                    .unwrap_or(true)
            {
                return Err(forbidden());
            }
            drop(admission);
            #[cfg(feature = "test-hooks")]
            for group in permit
                .targets
                .iter()
                .map(|(_, route)| route.group_id)
                .collect::<std::collections::BTreeSet<_>>()
            {
                super::watch_test_hooks::record(
                    self.cluster,
                    self.config.node_id,
                    group,
                    operation,
                    &request,
                    super::watch_test_hooks::WatchPhase::Registered,
                );
            }
            let scopes = watch_scopes(&request, &permit);
            let mut pending: Vec<Option<WatchFuture<'_>>> = Vec::with_capacity(scopes.len());
            for scope in &scopes {
                pending.push(Some(Box::pin(self.watch_scope(
                    access,
                    scope,
                    operation,
                    (hops, expires, deadline),
                ))));
            }
            let mut results = BTreeMap::new();
            loop {
                if registration.cancelled.is_cancelled() {
                    break;
                }
                if pending.iter().all(Option::is_none) {
                    break;
                }
                let next = tokio::select! {
                    result=next_watch(&mut pending)=>result,
                    _=registration.cancelled.cancelled()=>None,
                    _=self.watch_stopped()=>None,
                    _=tokio::time::sleep_until(deadline)=>None,
                };
                let Some((index, result)) = next else {
                    break;
                };
                if let Ok(result) = result {
                    let changed = result.targets.iter().any(|target| {
                        matches!(
                            public::WatchStatus::try_from(target.status),
                            Ok(public::WatchStatus::WatchSync | public::WatchStatus::WatchChanged)
                        )
                    });
                    for target in result.targets {
                        results.insert(target.shard_id.clone(), target);
                    }
                    if changed {
                        break;
                    }
                } else if let Err(error) = result {
                    let scope = scopes.get(index).map(|scope| &scope.request);
                    if let Some(scope) = scope {
                        for target in &scope.targets {
                            results.insert(target.shard_id.clone(), watch_error(target, &error));
                        }
                    }
                }
            }
            // Drop local attempts before sending explicit, attempt-sensitive downstream cancellation.
            drop(pending);
            if registration.cancelled.is_cancelled()
                || !self.watch_accepting()
                || Instant::now() >= deadline
            {
                return Err(Error::unavailable());
            }
            let targets: Vec<_> = request
                .targets
                .iter()
                .map(|target| {
                    results
                        .remove(&target.shard_id)
                        .unwrap_or_else(|| retry(target, ErrorCode::Unavailable))
                })
                .collect();
            let (current, permission_expires) = tokio::select! {
                _ = registration.cancelled.cancelled() => return Err(Error::unavailable()),
                _ = self.watch_stopped() => return Err(Error::unavailable()),
                _ = tokio::time::sleep_until(deadline) => return Err(Error::unavailable()),
                authorization = self.watch_authorize(access, &request, deadline) => authorization.map_err(|error| {
                    self.watch_scope_failure(&request, (operation, 0), "reply_authorize", deadline, error)
                })?,
            };
            if registration.cancelled.is_cancelled()
                || !self.watch_accepting()
                || permission_expires <= Instant::now()
                || current.targets != permit.targets
                || current.installed_version != permit.installed_version
            {
                return Err(Error::unavailable());
            }
            for target in &targets {
                if let Ok(status) = public::WatchStatus::try_from(target.status) {
                    self.watch_budget.metrics().result(status);
                }
            }
            log_watch_completion(&span, &targets);
            #[cfg(feature = "test-hooks")]
            for group in permit
                .targets
                .iter()
                .map(|(_, route)| route.group_id)
                .collect::<std::collections::BTreeSet<_>>()
            {
                super::watch_test_hooks::record(
                    self.cluster,
                    self.config.node_id,
                    group,
                    operation,
                    &request,
                    super::watch_test_hooks::WatchPhase::Complete,
                );
            }
            wire::response(
                frame,
                FrameKind::GroupWatchResult,
                &public::GroupWatchResult { targets },
                &self.config.protocol,
            )
        }
        .instrument(span.clone())
        .await;
        if let Err(error) = &result {
            log_watch_failure(&span, error);
        }
        result
    }

    async fn watch_scope(
        &self,
        access: &Access,
        scope: &WatchScope,
        operation: Id,
        context: (u32, Instant, Instant),
    ) -> Result<public::GroupWatchResult> {
        let (hops, expires, deadline) = context;
        let identity = (
            operation,
            scope
                .permit
                .targets
                .first()
                .map_or(0, |(_, route)| route.group_id),
        );
        let (branches, local) = self
            .watch_branches(
                &scope.request,
                &scope.permit,
                operation,
                hops,
                deadline,
                access,
            )
            .await
            .map_err(|error| {
                self.watch_scope_failure(&scope.request, identity, "route", deadline, error)
            })?;
        if let Some(branch) = branches.first() {
            branch.sent.store(true, Ordering::Release);
            let response = self
                .forward(
                    branch.target,
                    branch.group,
                    access.clone(),
                    &branch.frame,
                    deadline,
                    None,
                    branch.hops,
                )
                .await;
            if response.as_ref().map_or_else(
                |error| error.failure().outcome == i32::from(Outcome::Rejected),
                |_| true,
            ) {
                branch.finished.store(true, Ordering::Release);
            }
            let response = response.map_err(|error| {
                self.watch_scope_failure(
                    &branch.request,
                    (branch.operation, branch.group),
                    "forward",
                    deadline,
                    error,
                )
            })?;
            return decode_watch_result(&response, &branch.request, &self.config.protocol).map_err(
                |error| {
                    self.watch_scope_failure(
                        &branch.request,
                        (branch.operation, branch.group),
                        "forward_result",
                        deadline,
                        error,
                    )
                },
            );
        }
        if let Some((watch, permit)) = local.first() {
            return self
                .watch_local(access, watch, permit, (operation, expires, deadline))
                .await;
        }
        Err(self.watch_scope_failure(
            &scope.request,
            identity,
            "missing_route",
            deadline,
            Error::unavailable(),
        ))
    }

    fn watch_scope_failure(
        &self,
        request: &public::GroupWatch,
        identity: (Id, u64),
        stage: &'static str,
        deadline: Instant,
        error: Error,
    ) -> Error {
        let (operation, group) = identity;
        let failure = error.failure();
        let remaining_ms = u64::try_from(
            deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let span = watch_span(
            operation,
            &request.root_operation_id,
            &request.parent_operation_id,
            request.attempt,
        );
        tracing::debug!(parent: &span,
            event_code = "watch_scope_failed",
            group,
            stage,
            error_code = failure.code,
            outcome = failure.outcome,
            retryable = failure.retryable,
            remaining_ms,
            "Consumer watch scope check failed"
        );
        #[cfg(feature = "test-hooks")]
        super::watch_test_hooks::record(
            self.cluster,
            self.config.node_id,
            group,
            operation,
            request,
            super::watch_test_hooks::WatchPhase::Failed {
                stage,
                code: failure.code,
                outcome: failure.outcome,
                remaining_ms,
            },
        );
        error
    }

    async fn watch_branches(
        &self,
        request: &public::GroupWatch,
        permit: &Permit,
        operation: Id,
        hops: u32,
        deadline: Instant,
        access: &Access,
    ) -> Result<(Vec<Branch>, Vec<(public::GroupWatch, Permit)>)> {
        let mut groups =
            BTreeMap::<(u64, u64, u32, u64, u64), Vec<(public::WatchTarget, (Id, Route))>>::new();
        let discovery = if hops < 2
            && permit
                .targets
                .first()
                .is_some_and(|(_, route)| route.owner_node == self.config.node_id)
        {
            Some(
                self.watch_budget
                    .reserve_forward(access.tenant, wire::id(&request.member_id)?, 2_048)
                    .ok_or_else(|| self.watch_overloaded())?,
            )
        } else {
            None
        };
        let leader = if discovery.is_some() {
            let group = permit
                .targets
                .first()
                .map(|(_, route)| route.group_id)
                .ok_or_else(forbidden)?;
            Some(
                self.watch_leader(
                    group,
                    deadline.min(
                        Instant::now()
                            .checked_add(Duration::from_millis(500))
                            .ok_or_else(Error::unavailable)?,
                    ),
                )
                .await
                .map_err(|error| {
                    self.watch_scope_failure(
                        request,
                        (operation, group),
                        "leader_discovery",
                        deadline,
                        error,
                    )
                })?,
            )
        } else {
            None
        };
        drop(discovery);
        for (target, (shard, route)) in request.targets.iter().zip(&permit.targets) {
            let (node, stage) = if hops == 0 && route.owner_node != self.config.node_id {
                (route.owner_node, 1)
            } else if hops < 2 && route.owner_node == self.config.node_id {
                (leader.ok_or_else(Error::unavailable)?, 2)
            } else if hops == 2 {
                (self.config.node_id, 2)
            } else {
                return Err(Error::unavailable());
            };
            groups
                .entry((node, route.group_id, stage, route.epoch, route.generation))
                .or_default()
                .push((target.clone(), (*shard, route.clone())));
        }
        let mut branches = Vec::new();
        let mut local = Vec::new();
        for ((node, group, stage, epoch, generation), targets) in groups {
            let mut watch = request.clone();
            watch.targets = targets.iter().map(|(target, _)| target.clone()).collect();
            if node == self.config.node_id {
                local.push((
                    watch,
                    Permit {
                        targets: targets.into_iter().map(|(_, route)| route).collect(),
                        installed_version: permit.installed_version,
                        remaining_ms: permit.remaining_ms,
                        tenant: permit.tenant,
                    },
                ));
            } else {
                watch.parent_operation_id = Bytes::copy_from_slice(operation.as_bytes());
                let child = protocol::child_operation(
                    operation,
                    stage as u8,
                    group,
                    epoch,
                    generation,
                    &watch,
                )
                .map_err(|source| Error::operation("derive watch branch identity", source))?;
                let frame = tidemq_protocol::framing::new_frame(
                    FrameKind::GroupWatch,
                    TrafficClass::Control,
                    Id::new_v7().map_err(|source| {
                        Error::operation("allocate watch branch request", source)
                    })?,
                    child,
                    &watch,
                    wire::remaining(deadline)?,
                    &self.config.protocol,
                )
                .map_err(|source| Error::operation("frame watch branch", source))?;
                branches.push(Branch {
                    target: node,
                    group,
                    hops: stage,
                    operation: child,
                    frame,
                    request: watch,
                    cleanup: Some(BranchCleanup {
                        registry: self.watch_registry.clone(),
                        rpc: self.rpc.clone(),
                        access: access.clone(),
                        limits: self.config.protocol.clone(),
                        budget: self.watch_budget.clone(),
                        deadline,
                        charge: self
                            .watch_budget
                            .reserve(
                                access.tenant,
                                wire::id(&request.member_id)?,
                                0,
                                (self.config.protocol.max_control_frame_bytes as usize)
                                    .saturating_mul(2)
                                    .saturating_add(2_048),
                                false,
                            )
                            .ok_or_else(|| self.watch_overloaded())?,
                    }),
                    sent: AtomicBool::new(false),
                    finished: AtomicBool::new(false),
                });
                #[cfg(feature = "test-hooks")]
                if let Some(branch) = branches.last() {
                    super::watch_test_hooks::record(
                        self.cluster,
                        self.config.node_id,
                        group,
                        child,
                        &branch.request,
                        super::watch_test_hooks::WatchPhase::Forward,
                    );
                }
            }
        }
        Ok((branches, local))
    }

    async fn watch_leader(&self, group: u64, deadline: Instant) -> Result<u64> {
        wire::remaining(deadline)?;
        let voters = self
            .registry
            .candidates(group, self.config.runtime.metadata_group)?;
        if let Some((leader, _)) = self
            .leaders
            .read()
            .await
            .get(&group)
            .filter(|(leader, expires)| *expires > Instant::now() && voters.contains(leader))
        {
            return Ok(*leader);
        }
        for (index, node) in voters.iter().enumerate() {
            let remaining = u32::try_from(voters.len().saturating_sub(index))
                .map_err(|_| Error::unavailable())?;
            let now = Instant::now();
            let share = deadline
                .saturating_duration_since(now)
                .checked_div(remaining)
                .ok_or_else(Error::unavailable)?;
            if share.is_zero() {
                break;
            }
            let attempt = now
                .checked_add(share)
                .ok_or_else(Error::unavailable)?
                .min(deadline);
            let request = Request::control(wire::LEADER, &())?;
            if let Ok(bytes) = self.rpc.call(*node, group, request, attempt).await {
                let leader: Option<u64> = wire::decode_json(&bytes)?;
                if let Some(leader) = leader.filter(|leader| voters.contains(leader)) {
                    let expires = Instant::now()
                        .checked_add(Duration::from_millis(200))
                        .ok_or_else(Error::unavailable)?;
                    self.leaders.write().await.insert(group, (leader, expires));
                    return Ok(leader);
                }
            }
        }
        Err(Error::unavailable())
    }

    pub(super) fn watch_overloaded(&self) -> Error {
        self.watch_budget.metrics().admission_rejected.inc();
        overloaded()
    }

    fn watch_accepting(&self) -> bool {
        !self.draining.load(Ordering::Acquire) && self.registry.schedulable(self.config.node_id)
    }

    async fn watch_stopped(&self) {
        let mut changes = self.registry.subscribe();
        while self.watch_accepting() {
            if changes.changed().await.is_err() {
                break;
            }
        }
    }

    fn watch_refusal(&self, reason: Refusal) -> Error {
        self.watch_budget.metrics().admission_rejected.inc();
        refusal(reason)
    }

    async fn watch_local(
        &self,
        access: &Access,
        request: &public::GroupWatch,
        permit: &Permit,
        context: (Id, Instant, Instant),
    ) -> Result<public::GroupWatchResult> {
        let (operation, expires, deadline) = context;
        let identity = (
            operation,
            permit
                .targets
                .first()
                .map_or(0, |(_, route)| route.group_id),
        );
        let failure =
            |stage, error| self.watch_scope_failure(request, identity, stage, deadline, error);
        let started = Instant::now();
        let wait_end = started
            .checked_add(Duration::from_millis(u64::from(request.wait_ms)))
            .ok_or_else(Error::unavailable)?
            .min(
                expires
                    .checked_sub(Duration::from_millis(20))
                    .unwrap_or(started),
            )
            .min(
                deadline
                    .checked_sub(Duration::from_millis(20))
                    .unwrap_or(started),
            );
        self.watch_barrier(permit, deadline)
            .await
            .map_err(|error| failure("initial_barrier", error))?;
        loop {
            let samples = self
                .watch_samples(access, request, permit, deadline)
                .await
                .map_err(|error| failure("initial_samples", error))?;
            let changed = samples
                .iter()
                .any(|sample| sample.status != public::WatchStatus::WatchUnchanged);
            if changed || Instant::now() >= wait_end || request.wait_ms == 0 {
                let (current, current_expires) = self
                    .watch_authorize(access, request, deadline)
                    .await
                    .map_err(|error| failure("final_authorize", error))?;
                let targets_changed = current.targets != permit.targets;
                let version_changed = current.installed_version != permit.installed_version;
                let permission_expired = current_expires <= Instant::now();
                let draining = self.draining.load(std::sync::atomic::Ordering::Acquire);
                if targets_changed || version_changed || permission_expired || draining {
                    let stage = if targets_changed {
                        "local_targets_changed"
                    } else if version_changed {
                        "local_version_changed"
                    } else if permission_expired {
                        "local_permission_expired"
                    } else {
                        "local_draining"
                    };
                    return Err(failure(stage, Error::unavailable()));
                }
                self.watch_barrier(permit, deadline)
                    .await
                    .map_err(|error| failure("final_barrier", error))?;
                let checked = self
                    .watch_samples(access, request, permit, deadline)
                    .await
                    .map_err(|error| failure("final_samples", error))?;
                return Ok(public::GroupWatchResult {
                    targets: request
                        .targets
                        .iter()
                        .zip(checked)
                        .map(|(target, sample)| public::WatchResult {
                            shard_id: target.shard_id.clone(),
                            status: sample.status.into(),
                            observed_token: sample.token,
                            error_code: ErrorCode::Unspecified.into(),
                            retryable: false,
                        })
                        .collect(),
                });
            }
            let now_ms = super::data::unix_millis()?;
            #[cfg(feature = "test-hooks")]
            if let Some((_, route)) = permit.targets.first() {
                super::watch_test_hooks::before_wait(
                    self.cluster,
                    route.group_id,
                    route.partition,
                    deadline,
                )
                .await;
            }
            let wake = samples
                .iter()
                .filter_map(|sample| sample.due_ms)
                .min()
                .map(|due| {
                    Instant::now().checked_add(Duration::from_millis(due.saturating_sub(now_ms)))
                })
                .unwrap_or(Some(wait_end))
                .ok_or_else(Error::unavailable)?
                .min(wait_end);
            let mut notices: Vec<Pin<Box<dyn Future<Output = ()> + Send + '_>>> = samples
                .iter()
                .map(|sample| {
                    Box::pin(sample.signal.notified()) as Pin<Box<dyn Future<Output = ()> + Send>>
                })
                .collect();
            // Arm notifications before the second applied-state comparison to close registration races.
            if arm_notices(&mut notices).await {
                continue;
            }
            let registered = self
                .watch_samples(access, request, permit, deadline)
                .await
                .map_err(|error| failure("registered_samples", error))?;
            if registered
                .iter()
                .any(|sample| sample.status != public::WatchStatus::WatchUnchanged)
            {
                continue;
            }
            tokio::select! { _=first_notice(&mut notices)=>{}, _=tokio::time::sleep_until(wake)=>{} }
        }
    }

    async fn watch_barrier(&self, permit: &Permit, deadline: Instant) -> Result<()> {
        let group = permit
            .targets
            .first()
            .map(|(_, route)| route.group_id)
            .ok_or_else(forbidden)?;
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        replica
            .group
            .ensure_read(deadline)
            .await
            .map_err(crate::runtime::wire::execution_error)?;
        Ok(())
    }

    async fn watch_samples(
        &self,
        access: &Access,
        request: &public::GroupWatch,
        permit: &Permit,
        deadline: Instant,
    ) -> Result<Vec<WatchSample>> {
        wire::remaining(deadline)?;
        let (client, credential) = super::consumer_control::actor(access)?;
        let member = wire::id(&request.member_id)?;
        let _recheck = self
            .watch_budget
            .recheck(access.tenant, member, deadline)
            .await
            .ok_or_else(|| self.watch_overloaded())?;
        let subscription = wire::id(&request.subscription_id)?;
        let group = permit
            .targets
            .first()
            .map(|(_, route)| route.group_id)
            .ok_or_else(forbidden)?;
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        let incarnation = match replica.group.raft() {
            Some(raft) => WatchIncarnation::Raft {
                term: raft.metrics().borrow().current_term,
            },
            None => WatchIncarnation::Local {
                process: self.instance,
            },
        };
        let now = super::data::unix_millis()?;
        replica
            .state
            .try_with_application(|state| {
                request
                    .targets
                    .iter()
                    .zip(&permit.targets)
                    .map(|(target, (shard, route))| {
                        let fence = state
                            .ownership()
                            .and_then(|ownership| ownership.recovered(route.partition))
                            .ok_or_else(Error::unavailable)?
                            .fence;
                        if fence.grant.group_id != group
                            || fence.grant.epoch != route.epoch
                            || fence.grant.operation_id != route.ownership_operation
                            || fence.grant.owner_node != route.owner_node
                            || fence.grant.expected_route_generation.checked_add(1)
                                != Some(route.generation)
                        {
                            return Err(Error::unavailable());
                        }
                        state
                            .watch_sample(
                                WatchKey {
                                    partition: route.partition,
                                    shard: *shard,
                                    subscription,
                                    member,
                                    epoch: request.member_epoch,
                                    plan: request.assignment_version,
                                    installation: permit.installed_version,
                                    route_epoch: route.epoch,
                                    route_generation: route.generation,
                                    incarnation,
                                },
                                (client, credential),
                                &target.observed_token,
                                now,
                                &self.watch_budget,
                                access.tenant,
                            )
                            .map_err(super::data::data_rejection)
                    })
                    .collect()
            })
            .await
            .map_err(|source| Error::operation("observe committed consumer readiness", source))?
    }
}

async fn cancel_branch(
    rpc: wire::PeerRpc,
    access: Access,
    branch: CancelBranch,
    limits: ProtocolLimits,
    deadline: Instant,
) -> Result<()> {
    let operation =
        Id::new_v7().map_err(|source| Error::operation("allocate watch cancellation", source))?;
    let frame = tidemq_protocol::framing::new_frame(
        FrameKind::GroupWatchCancel,
        TrafficClass::Control,
        operation,
        operation,
        &public::GroupWatchCancel {
            root_operation_id: branch.root,
            operation_id: Bytes::copy_from_slice(branch.operation.as_bytes()),
            attempt: branch.attempt,
        },
        wire::remaining(deadline)?,
        &limits,
    )
    .map_err(|source| Error::operation("frame watch branch cancellation", source))?;
    let request = Request {
        kind: wire::FORWARD,
        body: encode_payload(&frame, limits.max_control_frame_bytes)
            .map_err(|source| Error::operation("encode watch cleanup", source))?,
        principal: access.principal.to_string(),
        scope: access.scope.to_string(),
        hops: branch.hops,
        client_session: access
            .session
            .map(super::metadata::bytes_id)
            .unwrap_or_default(),
        tenant: access
            .tenant
            .map(super::metadata::bytes_id)
            .unwrap_or_default(),
    };
    let bytes = rpc
        .submit(
            branch.target,
            branch.group,
            TrafficClass::Control,
            operation,
            request,
            deadline,
            0,
        )
        .await?
        .receive()
        .await?;
    let reply = decode_frame(bytes, TrafficClass::Control, &limits)
        .map_err(|source| Error::operation("decode watch cleanup confirmation", source))?;
    if reply.kind != i32::from(FrameKind::GroupWatchCancelled)
        || reply.request_id != frame.request_id
        || reply.operation_id != frame.operation_id
    {
        return Err(Error::unavailable());
    }
    Ok(())
}

fn watch_span(operation: Id, root: &[u8], parent: &[u8], attempt: u64) -> tracing::Span {
    tracing::debug_span!("consumer_watch",operation_id=%operation,root_operation_id=?Id::from_slice(root).ok(),parent_operation_id=?Id::from_slice(parent).ok(),watch_attempt=attempt)
}
fn log_watch_failure(span: &tracing::Span, error: &Error) {
    let failure = error.failure();
    span.in_scope(|| {
        tracing::debug!(
            event_code = "watch_failed",
            error_code = failure.code,
            outcome = failure.outcome,
            retryable = failure.retryable,
            "Consumer observation failed; retain the original branch identity when retrying"
        )
    });
}
fn log_watch_cancel(span: &tracing::Span, matched: bool) {
    span.in_scope(|| {
        tracing::debug!(
            event_code = "watch_cancelled",
            matched,
            "Applied attempt-sensitive observation cancellation"
        )
    });
}
fn log_watch_cleanup(span: &tracing::Span, error: Error) {
    let error = anyhow::Error::new(error);
    span.in_scope(|| {
        tracing::debug!(
            event_code = "watch_cleanup_unknown",
            error = %format_args!("{error:#}"),
            "Watch branch cleanup remains unconfirmed until its original deadline"
        )
    });
}
fn log_watch_completion(span: &tracing::Span, targets: &[public::WatchResult]) {
    span.in_scope(|| {
        tracing::debug!(
            event_code = "watch_completed",
            retry_targets = targets
                .iter()
                .filter(|target| target.status == i32::from(public::WatchStatus::WatchRetry))
                .count(),
            reset_targets = targets
                .iter()
                .filter(|target| target.status == i32::from(public::WatchStatus::WatchReset))
                .count(),
            "Completed bounded consumer observation"
        )
    });
}

fn watch_request(frame: &Frame, limits: &ProtocolLimits) -> Option<public::GroupWatch> {
    if frame.kind == i32::from(FrameKind::GroupWatch) {
        return protocol::decode(frame.payload.clone(), limits).ok();
    }
    if frame.kind != i32::from(FrameKind::InternalCommand) {
        return None;
    }
    let envelope: InternalCommand = decode_payload(frame.payload.clone(), limits).ok()?;
    let request: Request = decode_payload(envelope.encoded_command, limits).ok()?;
    if request.kind != wire::FORWARD {
        return None;
    }
    let inner: Frame = decode_payload(request.body, limits).ok()?;
    (inner.kind == i32::from(FrameKind::GroupWatch))
        .then(|| protocol::decode(inner.payload, limits).ok())
        .flatten()
}

fn decode_watch_result(
    frame: &Frame,
    request: &public::GroupWatch,
    limits: &ProtocolLimits,
) -> Result<public::GroupWatchResult> {
    if frame.kind != i32::from(FrameKind::GroupWatchResult) {
        return Err(Error::unavailable());
    }
    let result: public::GroupWatchResult =
        protocol::decode_result(frame.payload.clone(), limits)
            .map_err(|source| Error::operation("decode watch branch results", source))?;
    let mut expected: std::collections::BTreeSet<_> = request
        .targets
        .iter()
        .map(|target| target.shard_id.clone())
        .collect();
    if result.targets.len() != expected.len()
        || result.targets.iter().any(|target| {
            !expected.remove(&target.shard_id)
                || public::WatchStatus::try_from(target.status).is_err()
                || target.status == 0
                || target.observed_token.len() > protocol::MAX_TOKEN_BYTES
        })
    {
        return Err(Error::invalid(
            "Watch response must identify every branch target exactly once",
        ));
    }
    Ok(result)
}

fn retry(target: &public::WatchTarget, code: ErrorCode) -> public::WatchResult {
    public::WatchResult {
        shard_id: target.shard_id.clone(),
        status: public::WatchStatus::WatchRetry.into(),
        observed_token: target.observed_token.clone(),
        error_code: code.into(),
        retryable: true,
    }
}
fn overloaded() -> Error {
    let mut failure = Error::reject(ErrorCode::QuotaExceeded, Outcome::Rejected, true).failure();
    failure.retry_after_ms = Some(50);
    Error::Rejected { failure }
}
fn watch_error(target: &public::WatchTarget, error: &Error) -> public::WatchResult {
    let failure = error.failure();
    let code = ErrorCode::try_from(failure.code).unwrap_or(ErrorCode::Unavailable);
    let status = match code {
        ErrorCode::StaleGeneration | ErrorCode::Fenced | ErrorCode::AssignmentBusy => {
            public::WatchStatus::WatchReset
        }
        ErrorCode::Forbidden
        | ErrorCode::Unauthenticated
        | ErrorCode::InvalidRequest
        | ErrorCode::NotFound => public::WatchStatus::WatchRejected,
        _ => public::WatchStatus::WatchRetry,
    };
    public::WatchResult {
        shard_id: target.shard_id.clone(),
        status: status.into(),
        observed_token: target.observed_token.clone(),
        error_code: code.into(),
        retryable: status != public::WatchStatus::WatchRejected,
    }
}
fn refusal(reason: Refusal) -> Error {
    match reason {
        Refusal::Conflict => Error::reject(ErrorCode::ContentConflict, Outcome::Rejected, false),
        Refusal::Expired => Error::reject(ErrorCode::DeadlineExceeded, Outcome::Rejected, true),
        _ => overloaded(),
    }
}

async fn next_watch(
    pending: &mut [Option<WatchFuture<'_>>],
) -> Option<(usize, Result<public::GroupWatchResult>)> {
    std::future::poll_fn(|context| {
        let mut active = false;
        for (index, slot) in pending.iter_mut().enumerate() {
            if let Some(future) = slot {
                active = true;
                if let Poll::Ready(result) = future.as_mut().poll(context) {
                    *slot = None;
                    return Poll::Ready(Some((index, result)));
                }
            }
        }
        if active {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    })
    .await
}
async fn first_notice(notices: &mut [Pin<Box<dyn Future<Output = ()> + Send + '_>>]) {
    std::future::poll_fn(|context| {
        for notice in notices.iter_mut() {
            if notice.as_mut().poll(context).is_ready() {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    })
    .await
}

async fn arm_notices(notices: &mut [Pin<Box<dyn Future<Output = ()> + Send + '_>>]) -> bool {
    std::future::poll_fn(|context| {
        Poll::Ready(
            notices
                .iter_mut()
                .any(|notice| notice.as_mut().poll(context).is_ready()),
        )
    })
    .await
}

#[cfg(test)]
mod telemetry_tests {
    use super::*;
    use std::{io, sync::Mutex};
    #[derive(Clone)]
    struct Capture(Arc<Mutex<Vec<u8>>>);
    impl io::Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| io::Error::other("Trace capture is poisoned"))?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }
    #[test]
    fn watch_failure_and_cancel_trace_context_contains_identity_without_tokens_or_payloads() {
        let root = Id::new_v7().unwrap();
        let parent = Id::new_v7().unwrap();
        let operation = Id::new_v7().unwrap();
        let capture = Capture(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .with_span_events(
                tracing_subscriber::fmt::format::FmtSpan::NEW
                    | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
            )
            .with_writer(capture.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let span = watch_span(operation, root.as_bytes(), parent.as_bytes(), 7);
            log_watch_failure(&span, &Error::unknown());
            log_watch_cancel(&span, true);
            log_watch_completion(
                &span,
                &[public::WatchResult {
                    status: public::WatchStatus::WatchRetry.into(),
                    ..Default::default()
                }],
            );
            log_watch_cleanup(&span, Error::unknown());
        });
        let encoded = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        for required in [
            root.to_string(),
            parent.to_string(),
            operation.to_string(),
            "watch_attempt=7".into(),
            "watch_failed".into(),
            "watch_cancelled".into(),
            "retry_targets=1".into(),
            "watch_cleanup_unknown".into(),
        ] {
            assert!(
                encoded.contains(&required),
                "Missing Watch trace context {required}: {encoded}"
            );
        }
        for forbidden in [
            "observed_token",
            "access_token",
            "credential=",
            "proof=",
            "payload=",
            "message_body=",
        ] {
            assert!(!encoded.contains(forbidden));
        }
    }
}
