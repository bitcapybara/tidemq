//! Authenticated Console access to the process-owned business handlers.

use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

use bytes::Bytes;
use serde_json::{Value, json};
use tidemq_protocol::{
    Id,
    common::{Frame, Outcome},
    console::{Query, Reply},
    console_operation::{self as operation, Operation, Status, WriteRequest},
    public,
};
use tokio::time::Instant;
use tracing::Instrument;

use super::{Error, Result, console_operation, process::Phase, service::Service, wire};
use crate::metadata::{MetadataState, ResourceScope, TokenDigest};
use crate::observability::metrics::{
    Operation as MetricOperation, OperationMetrics, Outcome as MetricOutcome, Stage,
};

#[derive(Clone)]
pub(crate) struct LocalConsole {
    service: Arc<Service>,
}

/// Only credential authentication can establish this context; cookies carry no authority.
#[derive(Clone)]
pub(crate) struct LocalContext {
    backend: LocalConsole,
    digest: TokenDigest,
    actor: Id,
    credential: Id,
    tenant: Option<Id>,
    namespace: Option<Id>,
    scope: ResourceScope,
    request: Id,
    operation: Id,
    deadline: Instant,
    traceparent: String,
}

impl LocalConsole {
    pub(crate) fn new(service: Arc<Service>) -> Result<Self> {
        if service.config.role != crate::config::Role::Combined
            || !service
                .metadata
                .as_ref()
                .is_some_and(|replica| replica.group.is_local())
            || service.data.is_empty()
            || service
                .data
                .values()
                .any(|replica| !replica.group.is_local())
        {
            return Err(Error::invalid(
                "Local Console requires the Combined metadata authority",
            ));
        }
        Ok(Self { service })
    }

    pub(crate) async fn authenticate(&self, token: &str) -> Result<LocalContext> {
        let mut metrics = LocalMetrics::new(&self.service, MetricOperation::Control);
        let result = self
            .authenticate_token(token)
            .instrument(tracing::info_span!(target: "tidemq_trace", "console_local_authenticate"))
            .await;
        metrics.finish(&result);
        tracing::info!(event_code = "console_local_authentication", outcome = ?metrics.outcome,
            actor = ?result.as_ref().ok().map(|context| context.actor),
            credential = ?result.as_ref().ok().map(|context| context.credential),
            code = result.as_ref().err().map(|error| error.failure().code),
            "Local Console credential authentication completed");
        result
    }

    async fn authenticate_token(&self, token: &str) -> Result<LocalContext> {
        let digest = TokenDigest::of(token).map_err(|_| unauthenticated())?;
        let deadline = Instant::now() + Duration::from_secs(8);
        self.ensure_read(deadline).await?;
        let (actor, credential, tenant, namespace, scope) = wire::within(
            deadline,
            self.service
                .metadata
                .as_ref()
                .ok_or_else(Error::unavailable)?
                .state
                .try_with_application(|state| {
                    let now = super::data::unix_millis()?;
                    let credential = state
                        .auth()
                        .authenticate(digest, now)
                        .map_err(|_| unauthenticated())?;
                    let actor = state
                        .auth()
                        .current_principal(credential.id, now)
                        .map_err(|_| unauthenticated())?;
                    Ok::<_, Error>((
                        actor.id,
                        credential.id,
                        actor.tenant,
                        actor.namespace,
                        actor.resource_scope(),
                    ))
                }),
        )
        .await?
        .map_err(|source| Error::operation("authenticate local Console credential", source))??;
        let request = Id::new_v7()
            .map_err(|source| Error::operation("allocate local Console request", source))?;
        Ok(LocalContext {
            backend: self.clone(),
            digest,
            actor,
            credential,
            tenant,
            namespace,
            scope,
            request,
            operation: request,
            deadline,
            traceparent: crate::observability::tracing::current_traceparent(),
        })
    }

    async fn ensure_read(&self, deadline: Instant) -> Result<()> {
        self.service
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .group
            .ensure_read(deadline)
            .await
            .map_err(|source| Error::operation("establish local Console authority read", source))
    }
}

impl LocalContext {
    async fn project<T>(
        &self,
        project: impl FnOnce(&MetadataState, Id, u64) -> Result<T>,
    ) -> Result<T> {
        wire::remaining(self.deadline)?;
        self.backend.ensure_read(self.deadline).await?;
        let replica = self
            .backend
            .service
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?;
        wire::within(
            self.deadline,
            replica.state.try_with_application(|state| {
                let now = super::data::unix_millis()?;
                let credential = state
                    .auth()
                    .authenticate(self.digest, now)
                    .map_err(|_| unauthenticated())?;
                let actor = state
                    .auth()
                    .current_principal(credential.id, now)
                    .map_err(|_| unauthenticated())?;
                if credential.id != self.credential
                    || actor.id != self.actor
                    || actor.tenant != self.tenant
                    || actor.namespace != self.namespace
                    || actor.resource_scope() != self.scope
                {
                    return Err(unauthenticated());
                }
                project(state, credential.id, now)
            }),
        )
        .await?
        .map_err(|source| Error::operation("read authenticated local Console state", source))?
    }

    pub(crate) async fn identities(
        &self,
        request: &public::ListIdentities,
    ) -> Result<public::IdentityPage> {
        let mut metrics = LocalMetrics::new(&self.backend.service, MetricOperation::Control);
        let result = self
            .project(|state, credential, now| {
                let mut page = state
                    .auth()
                    .identity_page(credential, now, request)
                    .map_err(super::metadata::metadata_rejection)?;
                if let Some(scope) = &request.resource_scope {
                    let scope = crate::metadata::ResourceScope::from_wire(scope)
                        .map_err(|_| super::service::forbidden())?;
                    state
                        .validate_authorization_scope(scope, false)
                        .map_err(super::metadata::metadata_rejection)?;
                }
                page.cluster_id = Bytes::copy_from_slice(self.backend.service.cluster.as_bytes());
                Ok(page)
            })
            .instrument(self.span("identity", None, None))
            .await;
        metrics.finish(&result);
        self.observe("identity", None, &result, metrics.outcome);
        result
    }

    pub(crate) async fn read(&self, query: Query) -> Result<Reply> {
        let mut metrics = LocalMetrics::new(&self.backend.service, MetricOperation::Read);
        let scope = query_scope(&query);
        let span = self.span("read", None, scope);
        let result = async {
            self.project(|_, _, _| Ok(())).await?;
            if matches!(query, Query::Cluster { .. }) {
                return Err(unsupported());
            }
            self.backend
                .service
                .console_read_query(self.credential, &query, self.deadline)
                .await
        }
        .instrument(span)
        .await;
        metrics.finish(&result);
        self.observe("read", None, &result, metrics.outcome);
        result
    }

    pub(crate) async fn write(&self, request: &WriteRequest) -> Result<Operation> {
        let mut metrics = LocalMetrics::new(&self.backend.service, MetricOperation::Control);
        let span = self.span("write", Some(request.operation_id), None);
        let mut context = self.clone();
        context.operation = request.operation_id;
        let result = context.write_admitted(request).instrument(span).await;
        metrics.finish_operation(&result);
        self.observe(
            "write",
            Some(request.operation_id),
            &result,
            metrics.outcome,
        );
        result
    }

    async fn write_admitted(&self, request: &WriteRequest) -> Result<Operation> {
        self.project(|_, _, _| Ok(())).await?;
        if self.backend.service.process.phase() != Phase::Ready
            || self.backend.service.draining.load(Ordering::Acquire)
        {
            return Err(Error::unavailable());
        }
        let _admission = self.backend.service.public_admission.enter()?;
        let _serial =
            wire::within(self.deadline, self.backend.service.console_mutation.lock()).await?;
        if self.backend.service.process.phase() != Phase::Ready {
            return Err(Error::unavailable());
        }
        let bytes = serde_json::to_vec(request)
            .map_err(|source| Error::operation("bind local Console operation", source))?;
        if bytes.len() > 1_500_000 {
            return Err(Error::invalid("Console request exceeds its byte limit"));
        }
        self.execute_write(request, *blake3::hash(&bytes).as_bytes())
            .await
    }

    async fn execute_write(&self, request: &WriteRequest, digest: [u8; 32]) -> Result<Operation> {
        let now = super::data::unix_millis()?;
        let service = &self.backend.service;
        let record = service
            .console_begin(self.credential, request, digest, now, self.deadline)
            .await?;
        if record.expires_ms <= now {
            return Ok(console_operation::expired(&record, now));
        }
        if let Some(result) = service
            .recover_console_record(&record, now, self.deadline)
            .await?
        {
            return Ok(result);
        }
        // Native handlers use this metadata; no outer transport request is encoded locally.
        let original = Frame {
            request_id: Bytes::copy_from_slice(self.request.as_bytes()),
            operation_id: Bytes::copy_from_slice(self.operation.as_bytes()),
            traceparent: self.traceparent.clone(),
            ..Default::default()
        };
        let mut result = service
            .execute_console_record(&record, &request.command, &original, self.deadline)
            .await?;
        service
            .complete_console_record(&record, &mut result, self.deadline)
            .await?;
        Ok(result)
    }

    pub(crate) async fn operation(&self, id: Id) -> Result<Operation> {
        let mut metrics = LocalMetrics::new(&self.backend.service, MetricOperation::Recover);
        let result = async {
            self.project(|_, _, _| Ok(())).await?;
            let now = super::data::unix_millis()?;
            let record = self
                .backend
                .service
                .console_authorized_record(self.credential, id, now)
                .await?;
            match record {
                Some(record) if record.expires_ms <= now => {
                    Ok(console_operation::expired(&record, now))
                }
                Some(record) => Ok(self
                    .backend
                    .service
                    .recover_console_record(&record, now, self.deadline)
                    .await?
                    .unwrap_or_else(|| console_operation::pending(&record, now))),
                None => Ok(Operation {
                    id,
                    actor: String::new(),
                    target: String::new(),
                    scope: String::new(),
                    status: if operation::created_at_ms(id).is_none_or(|created| {
                        created.saturating_add(operation::FIRST_SUBMISSION_MS) <= now
                    }) {
                        Status::Expired
                    } else {
                        Status::Unknown
                    },
                    receipt: None,
                    reason: Some("no_authoritative_receipt".into()),
                    resource_id: None,
                    secret: None,
                    secret_unavailable: false,
                    observation: console_operation::observation(now),
                }),
            }
        }
        .instrument(self.span("operation", Some(id), None))
        .await;
        metrics.finish_operation(&result);
        self.observe("operation", Some(id), &result, metrics.outcome);
        result
    }

    pub(crate) async fn observation(&self) -> Result<Value> {
        let mut metrics = LocalMetrics::new(&self.backend.service, MetricOperation::Control);
        let result = self.project(|state, credential, now| {
            if !state.console_operator(credential, now) {
                return Err(super::service::forbidden());
            }
            let service = &self.backend.service;
            let sample = service.runtime_metrics.0.lock().map_err(|_| Error::unavailable())?.clone();
            Ok(json!({
                "items": [{"id": service.config.node_id.to_string(), "instance": service.instance,
                    "roles": ["metadata", "storage", "broker", "gateway", "console"],
                    "version": env!("CARGO_PKG_VERSION"), "execution": "local",
                    "ready": self.backend.service.process.phase() == Phase::Ready,
                    "phase": self.backend.service.process.phase(), "sampledAtMs": sample.sampled_at_ms.map(|value| value.to_string()),
                    "metadataGroups": "1", "dataGroups": service.data.len().to_string(),
                    "logDroppedEvents": sample.log_dropped_events.map(|value| value.to_string()),
                    "logSinkErrors": sample.log_sink_errors.map(|value| value.to_string())}],
                "nextCursor": null, "revision": state.revision().to_string(),
                "observedAtMs": now.to_string(), "source": "local_process_authority", "partial": false,
            }))
        }).instrument(self.span("observation", None, None)).await;
        metrics.finish(&result);
        self.observe("observation", None, &result, metrics.outcome);
        result
    }

    fn span(
        &self,
        action: &'static str,
        operation: Option<Id>,
        scope: Option<Id>,
    ) -> tracing::Span {
        tracing::info_span!(target: "tidemq_trace", "console_local_request", action,
            request_id = %self.request, actor = %self.actor, credential = %self.credential,
            tenant = ?self.tenant, namespace = ?self.namespace, operation_id = %operation.unwrap_or(self.operation),
            scope = ?self.scope, topic = ?scope, deadline_ms = self.deadline.saturating_duration_since(Instant::now()).as_millis() as u64)
    }

    fn observe<T>(
        &self,
        action: &'static str,
        operation: Option<Id>,
        result: &Result<T>,
        outcome: MetricOutcome,
    ) {
        tracing::info!(event_code = "console_local_result", action, actor = %self.actor,
            credential = %self.credential, request_id = %self.request, operation_id = ?operation,
            outcome = ?outcome, code = result.as_ref().err().map(|error| error.failure().code),
            "Local Console request completed");
    }
}

struct LocalMetrics {
    metrics: Option<Arc<OperationMetrics>>,
    started: std::time::Instant,
    outcome: MetricOutcome,
}

impl LocalMetrics {
    fn new(service: &Service, kind: MetricOperation) -> Self {
        Self {
            metrics: service.metrics.operation(kind),
            started: std::time::Instant::now(),
            outcome: MetricOutcome::Unknown,
        }
    }

    fn finish<T>(&mut self, result: &Result<T>) {
        self.outcome = match result {
            Ok(_) => MetricOutcome::Committed,
            Err(error) if error.failure().outcome == i32::from(Outcome::Rejected) => {
                MetricOutcome::Rejected
            }
            Err(_) => MetricOutcome::Unknown,
        };
    }

    fn finish_operation(&mut self, result: &Result<Operation>) {
        self.finish(result);
        if let Ok(operation) = result {
            self.outcome = match operation.status {
                Status::Succeeded => MetricOutcome::Committed,
                Status::Rejected | Status::Expired => MetricOutcome::Rejected,
                Status::Unknown => MetricOutcome::Unknown,
            };
        }
    }
}

impl Drop for LocalMetrics {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.result(self.outcome);
            metrics.observe(Stage::Total, self.started.elapsed().as_secs_f64());
        }
    }
}

fn query_scope(query: &Query) -> Option<Id> {
    match query {
        Query::Topic { topic }
        | Query::Shards { topic, .. }
        | Query::Messages { topic, .. }
        | Query::Members { topic, .. }
        | Query::Progress { topic, .. }
        | Query::Producers { topic, .. } => Some(*topic),
        Query::Subscriptions { topic, .. } => *topic,
        Query::Topics {
            namespace, tenant, ..
        } => namespace.or(*tenant),
        Query::Cluster { .. } => None,
    }
}

fn unauthenticated() -> Error {
    Error::reject(
        tidemq_protocol::common::ErrorCode::Unauthenticated,
        Outcome::Rejected,
        false,
    )
}

fn unsupported() -> Error {
    Error::invalid("Cluster administration is unavailable for local execution")
}

#[cfg(test)]
impl LocalConsole {
    pub(crate) async fn fixture() -> tests::Fixture {
        tests::Fixture::new().await
    }
}

#[cfg(test)]
#[path = "../../tests/runtime_support/console_local.rs"]
mod tests;
