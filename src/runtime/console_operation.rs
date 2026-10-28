#[path = "console_write_prepare.rs"]
mod prepare;

#[cfg(test)]
#[path = "console_operation_tests.rs"]
pub(super) mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use prost::Message;
use tidemq_protocol::{
    Id,
    common::{ErrorCode, Frame, FrameKind, Outcome, TrafficClass},
    console::{Decimal, Observation},
    console_operation::{self as api, Operation, Status, WriteCommand, WriteRequest},
    public,
};
use tokio::time::Instant;

use super::{
    Error, Result,
    service::{Service, forbidden},
    wire,
};
use crate::{
    gateway::Access,
    metadata::{self, ConsoleBinding, ConsoleCommand, ConsoleRecord, Permission},
};

struct Executed {
    resource: Id,
    receipt: String,
    secret: Option<String>,
    secret_unavailable: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
pub(super) struct DelegatedConsoleSend {
    pub credential: Id,
    client: Id,
    operation: Id,
    topic: Id,
    shard: Id,
    partition: u32,
    registration: u64,
    digest: [u8; 32],
}

impl DelegatedConsoleSend {
    pub fn from_frame(
        credential: Id,
        client: Id,
        frame: &Frame,
        limits: &tidemq_protocol::ProtocolLimits,
    ) -> Result<Self> {
        if frame.kind != i32::from(FrameKind::Publish)
            || frame.traffic_class != i32::from(TrafficClass::Produce)
        {
            return Err(forbidden());
        }
        let request: public::Publish =
            tidemq_protocol::framing::decode_payload(frame.payload.clone(), limits)
                .map_err(|source| Error::operation("decode delegated Console send", source))?;
        let operation = wire::id(&frame.operation_id)?;
        if wire::id(&request.producer_id)? != operation
            || request.sequence != 1
            || request.retired_before != 1
        {
            return Err(forbidden());
        }
        Ok(Self {
            credential,
            client,
            operation,
            topic: wire::id(&request.topic_id)?,
            shard: wire::id(&request.shard_id)?,
            partition: request.partition_id,
            registration: request.registration,
            digest: request
                .digest
                .as_ref()
                .try_into()
                .map_err(|_| forbidden())?,
        })
    }

    pub fn authorize(
        &self,
        state: &metadata::MetadataState,
        now: u64,
    ) -> Result<(Option<Id>, u64)> {
        let record = state
            .console_record(self.operation)
            .filter(|record| {
                record.credential == self.credential
                    && record.id == self.client
                    && record.expires_ms > now
            })
            .ok_or_else(forbidden)?;
        let auth = state.auth();
        let actor = auth
            .current_principal(self.credential, now)
            .map_err(|_| forbidden())?;
        if actor.id != record.actor {
            return Err(forbidden());
        }
        let ConsoleBinding::Send {
            topic,
            shard,
            partition,
            permit,
            digest,
            ..
        } = &record.binding
        else {
            return Err(forbidden());
        };
        if *topic != self.topic
            || *shard != self.shard
            || partition.partition != self.partition
            || permit.registration != self.registration
            || permit.client != self.client
            || permit.credential != self.credential
            || permit.producer != self.operation
            || *digest != self.digest
        {
            return Err(forbidden());
        }
        authorize_binding(state, self.credential, now, record.id, &record.binding)?;
        if record.receipt.is_some() && record_receipt(record, now)?.status != Status::Succeeded {
            return Err(forbidden());
        }
        let tenant = auth
            .credential_scope(self.credential, now)
            .map_err(|_| forbidden())?
            .0;
        let expires = auth
            .credential(self.credential)
            .ok_or_else(forbidden)?
            .expires_ms;
        Ok((tenant, expires.min(record.expires_ms)))
    }
}

impl Executed {
    fn receipt(resource: Id, receipt: String) -> Self {
        Self {
            resource,
            receipt,
            secret: None,
            secret_unavailable: false,
        }
    }
}

fn console_access(record: &ConsoleRecord) -> Access {
    Access {
        principal: Arc::from(format!("credential:{}", record.credential)),
        scope: Arc::from("*"),
        session: Some(record.id),
        tenant: None,
    }
}

fn execution_result(record: &ConsoleRecord, now: u64, executed: Result<Executed>) -> Operation {
    let mut result = pending(record, now);
    match executed {
        Ok(executed) => {
            result.status = Status::Succeeded;
            result.resource_id = Some(executed.resource);
            result.receipt = Some(executed.receipt);
            result.reason = None;
            result.secret = executed.secret;
            result.secret_unavailable = executed.secret_unavailable;
        }
        Err(error) => {
            let failure = error.failure();
            result.reason = Some(
                ErrorCode::try_from(failure.code)
                    .map(|code| code.as_str_name().to_ascii_lowercase())
                    .unwrap_or_else(|_| "upstream_error".into()),
            );
            observe_console_failure(record, "native_result", &error);
            if failure.outcome == i32::from(Outcome::Rejected) {
                result.status = Status::Rejected;
            }
        }
    }
    result
}

fn retry_console_refusal(error: &Error) -> bool {
    let failure = error.failure();
    failure.outcome == i32::from(Outcome::Rejected)
        && failure.code == i32::from(ErrorCode::Unavailable)
        && failure.retryable
        && !failure.original_outcome_known
}

fn observe_console_failure(record: &ConsoleRecord, stage: &'static str, error: &Error) {
    let failure = error.failure();
    let native_kind = match &record.binding {
        ConsoleBinding::Identity { .. } => "identity",
        ConsoleBinding::CreateTopic(_) => "create_topic",
        ConsoleBinding::DeleteTopic(_) => "delete_topic",
        ConsoleBinding::Send { .. } => "send",
    };
    let error_kind = match error {
        Error::Admitted { .. } => "admitted",
        Error::Invalid { .. } => "invalid",
        Error::Operation { .. } => "operation",
        Error::LocalNotSubmitted { .. } => "local_not_submitted",
        Error::GroupNotSubmitted { .. } => "group_not_submitted",
        Error::CommittedConsumerWriter { .. } => "committed_writer",
        Error::Rejected { .. } | Error::TenantGrantExhausted { .. } => "rejected",
    };
    tracing::warn!(event_code = "console_native_operation_failed", operation_id = %record.id,
        stage, native_kind, error_kind, error_code = failure.code, outcome = failure.outcome,
        retryable = failure.retryable, original_outcome_known = failure.original_outcome_known,
        "Bound Console command did not yield an authoritative receipt");
}

fn console_publish(record: &ConsoleRecord, command: &WriteCommand) -> Result<public::Publish> {
    let ConsoleBinding::Send {
        topic,
        shard,
        partition,
        permit,
        digest,
        ..
    } = &record.binding
    else {
        return Err(Error::invalid("Console send requires its original binding"));
    };
    let WriteCommand::Send {
        payload_base64,
        key_base64,
        properties_base64,
        ..
    } = command
    else {
        return Err(Error::invalid("Send binding requires the original message"));
    };
    let message = decode_message(payload_base64, key_base64.as_deref(), properties_base64)?;
    Ok(public::Publish {
        topic_id: id_bytes(*topic),
        shard_id: id_bytes(*shard),
        partition_id: partition.partition,
        producer_id: id_bytes(record.id),
        registration: permit.registration,
        sequence: 1,
        digest: Bytes::copy_from_slice(digest),
        message: Some(message),
        retired_before: 1,
    })
}

impl Service {
    pub(super) async fn console_write_public(
        &self,
        access: &Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let (request, digest) = decode_console_write(frame, &self.config.protocol)?;
        let _serial = wire::within(deadline, self.console_mutation.lock()).await?;
        let now = super::data::unix_millis()?;
        let record = self
            .console_begin(credential(access)?, &request, digest, now, deadline)
            .await?;
        if record.expires_ms <= now {
            return operation_response(self, frame, expired(&record, now));
        }
        if let Some(result) = self.recover_console_record(&record, now, deadline).await? {
            return operation_response(self, frame, result);
        }
        let mut result = self
            .execute_console_record(&record, &request.command, frame, deadline)
            .await?;
        self.complete_console_record(&record, &mut result, deadline)
            .await?;
        operation_response(self, frame, result)
    }

    pub(super) async fn console_begin(
        &self,
        credential: Id,
        request: &WriteRequest,
        digest: [u8; 32],
        now: u64,
        deadline: Instant,
    ) -> Result<ConsoleRecord> {
        if let Some(record) = self
            .console_authorized_record(credential, request.operation_id, now)
            .await?
        {
            if record.digest != digest {
                return Err(Error::reject(
                    ErrorCode::ContentConflict,
                    Outcome::Rejected,
                    false,
                ));
            }
            return Ok(record);
        }
        let record = self
            .prepare_console_record(credential, request, digest, now)
            .await?;
        self.console_commit(
            ConsoleCommand::Begin {
                now_ms: now,
                record: Box::new(record.clone()),
            },
            deadline,
        )
        .await?;
        Ok(record)
    }

    pub(super) async fn complete_console_record(
        &self,
        record: &ConsoleRecord,
        operation: &mut Operation,
        deadline: Instant,
    ) -> Result<()> {
        if !matches!(operation.status, Status::Succeeded | Status::Rejected) {
            return Ok(());
        }
        let mut stored = operation.clone();
        stored.forget_secret();
        let receipt = serde_json::to_vec(&stored)
            .map_err(|source| Error::operation("encode console receipt", source))?;
        if self
            .console_commit(
                ConsoleCommand::Complete {
                    id: record.id,
                    digest: record.digest,
                    receipt,
                },
                deadline,
            )
            .await
            .is_err()
        {
            operation.forget_secret();
            operation.status = Status::Unknown;
            operation.reason = Some("receipt_persistence_unknown".into());
        }
        Ok(())
    }

    pub(super) async fn console_operation_public(
        &self,
        access: &Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let request: public::GetConsoleOperation =
            tidemq_protocol::framing::decode_payload(frame.payload.clone(), &self.config.protocol)
                .map_err(|source| Error::operation("decode console result query", source))?;
        let id = wire::id(&request.operation_id)?;
        let now = super::data::unix_millis()?;
        let record = self
            .console_authorized_record(credential(access)?, id, now)
            .await?;
        let result = match record {
            Some(record) if record.expires_ms <= now => expired(&record, now),
            Some(record) => self
                .recover_console_record(&record, now, deadline)
                .await?
                .unwrap_or_else(|| pending(&record, now)),
            None => Operation {
                id,
                actor: String::new(),
                target: String::new(),
                scope: String::new(),
                status: if api::created_at_ms(id)
                    .is_none_or(|created| created.saturating_add(api::FIRST_SUBMISSION_MS) <= now)
                {
                    Status::Expired
                } else {
                    Status::Unknown
                },
                receipt: None,
                reason: Some("no_authoritative_receipt".into()),
                resource_id: None,
                secret: None,
                secret_unavailable: false,
                observation: observation(now),
            },
        };
        operation_response(self, frame, result)
    }

    pub(super) async fn console_authorized_record(
        &self,
        credential: Id,
        id: Id,
        now: u64,
    ) -> Result<Option<ConsoleRecord>> {
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                let actor = state
                    .auth()
                    .current_principal(credential, now)
                    .map_err(|_| forbidden())?;
                let record = state.console_record(id);
                if let Some(record) = record {
                    if record.actor != actor.id {
                        return Err(forbidden());
                    }
                    authorize_binding(state, credential, now, record.id, &record.binding)?;
                }
                Ok(record.cloned())
            })
            .await
            .map_err(|source| Error::operation("authorize console receipt", source))?
    }

    async fn prepare_console_record(
        &self,
        credential: Id,
        request: &WriteRequest,
        digest: [u8; 32],
        now: u64,
    ) -> Result<ConsoleRecord> {
        let id = request.operation_id;
        let created = api::created_at_ms(id)
            .ok_or_else(|| Error::invalid("Console operations require UUID v7"))?;
        if created > now.saturating_add(30_000)
            || created.saturating_add(api::FIRST_SUBMISSION_MS) <= now
        {
            return Err(Error::reject(
                ErrorCode::DeduplicationExpired,
                Outcome::Rejected,
                false,
            ));
        }
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                let actor = state
                    .auth()
                    .current_principal(credential, now)
                    .map_err(|_| forbidden())?
                    .id;
                let binding = prepare::scoped_binding(state, credential, request, now)?;
                authorize_binding(state, credential, now, id, &binding)?;
                let (target, scope) = binding
                    .labels()
                    .map_err(super::metadata::metadata_rejection)?;
                Ok(ConsoleRecord {
                    id,
                    actor,
                    credential,
                    digest,
                    created_ms: created,
                    expires_ms: created.saturating_add(api::RESULT_WINDOW_MS),
                    binding,
                    target,
                    scope,
                    receipt: None,
                })
            })
            .await
            .map_err(|source| Error::operation("prepare durable console operation", source))?
    }

    async fn console_commit(&self, command: ConsoleCommand, deadline: Instant) -> Result<()> {
        let _guard = wire::within(deadline, self.metadata_mutation.lock()).await?;
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        let operation =
            Id::new_v7().map_err(|source| Error::operation("allocate receipt command", source))?;
        for _ in 0..16 {
            self.require_leader(replica)?;
            let request =
                metadata::Command::console(operation, self.meta_revision().await?, command.clone());
            #[cfg(test)]
            tests::before_metadata_write(&request).await;
            let applied = if matches!(command, ConsoleCommand::Complete { .. }) {
                replica.group.commit_completion(request, deadline).await
            } else {
                replica.group.commit(request, deadline).await
            }
            .map_err(wire::execution_error)?;
            let response = applied.response;
            match response {
                metadata::Response::Applied {
                    outcome: metadata::Outcome::ConsoleOperation(_),
                    ..
                } => return Ok(()),
                metadata::Response::Rejected {
                    reason: metadata::Rejection::RevisionConflict,
                    ..
                } => {
                    // Connection leases can advance the shared CAS while this mutex is held.
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
                        "Metadata returned an invalid Console receipt outcome",
                    ));
                }
            }
        }
        Err(Error::unavailable())
    }

    pub(super) async fn execute_console_record(
        &self,
        record: &ConsoleRecord,
        command: &WriteCommand,
        original: &Frame,
        deadline: Instant,
    ) -> Result<Operation> {
        let mut delay = std::time::Duration::from_millis(2);
        // Keep native command futures off the shared query handler's stack.
        let mut executed =
            Box::pin(self.execute_console_once(record, command, original, deadline)).await;
        for attempt in 1..16 {
            let Err(error) = &executed else {
                break;
            };
            if !retry_console_refusal(error) {
                break;
            }
            observe_console_failure(record, "native_retry", error);
            #[cfg(test)]
            tests::after_native_refusal(record, attempt, error).await;
            #[cfg(not(test))]
            let _ = attempt;
            if wire::within(deadline, tokio::time::sleep(delay))
                .await
                .is_err()
            {
                break;
            }
            delay = delay
                .saturating_mul(2)
                .min(std::time::Duration::from_millis(50));
            executed =
                Box::pin(self.execute_console_once(record, command, original, deadline)).await;
        }
        let result = execution_result(record, super::data::unix_millis()?, executed);
        tracing::info!(event_code = "console_operation_result", actor = %record.actor, operation_id = %record.id, status = ?result.status, "Console business operation observed");
        Ok(result)
    }

    async fn execute_console_once(
        &self,
        record: &ConsoleRecord,
        command: &WriteCommand,
        original: &Frame,
        deadline: Instant,
    ) -> Result<Executed> {
        match &record.binding {
            ConsoleBinding::Identity { payload, .. } => {
                self.execute_console_identity(record, original, payload, deadline)
                    .await
            }
            ConsoleBinding::CreateTopic(name) => {
                self.execute_console_create(record, original, name, deadline)
                    .await
            }
            ConsoleBinding::DeleteTopic(topic) => {
                self.execute_console_delete(record, original, *topic, deadline)
                    .await
            }
            ConsoleBinding::Send { .. } => {
                self.execute_console_send(record, original, command, deadline)
                    .await
            }
        }
    }

    async fn execute_console_identity(
        &self,
        record: &ConsoleRecord,
        original: &Frame,
        payload: &[u8],
        deadline: Instant,
    ) -> Result<Executed> {
        let request = public::ManageIdentity::decode(payload)
            .map_err(|source| Error::operation("decode bound identity command", source))?;
        let frame = self.console_native_frame(
            record,
            original,
            FrameKind::ManageIdentity,
            TrafficClass::Control,
            &request,
            deadline,
        )?;
        let reply = self
            .identity_public(&console_access(record), &frame, deadline)
            .await?;
        let reply: public::IdentityResult =
            tidemq_protocol::framing::decode_payload(reply.payload, &self.config.protocol)
                .map_err(|source| Error::operation("decode identity receipt", source))?;
        let resource = wire::id(&reply.id)?;
        Ok(Executed {
            resource,
            receipt: format!("identity:{resource}"),
            secret: reply.access_token,
            secret_unavailable: reply.secret_unavailable,
        })
    }

    async fn execute_console_create(
        &self,
        record: &ConsoleRecord,
        original: &Frame,
        name: &str,
        deadline: Instant,
    ) -> Result<Executed> {
        let request = public::LogicalTopic {
            name: name.to_owned(),
        };
        let frame = self.console_native_frame(
            record,
            original,
            FrameKind::CreateLogicalTopic,
            TrafficClass::Control,
            &request,
            deadline,
        )?;
        self.topology_public(&console_access(record), &frame, deadline)
            .await?;
        Ok(Executed::receipt(record.id, format!("topic:{}", record.id)))
    }

    async fn execute_console_delete(
        &self,
        record: &ConsoleRecord,
        original: &Frame,
        topic: Id,
        deadline: Instant,
    ) -> Result<Executed> {
        let request = public::RemoveTopic {
            topic_id: id_bytes(topic),
        };
        let frame = self.console_native_frame(
            record,
            original,
            FrameKind::RemoveTopic,
            TrafficClass::Control,
            &request,
            deadline,
        )?;
        let reply = self
            .remove_topic_public(&console_access(record), &frame, deadline)
            .await?;
        let reply: public::TopicRemoved =
            tidemq_protocol::framing::decode_payload(reply.payload, &self.config.protocol)
                .map_err(|source| Error::operation("decode topic removal receipt", source))?;
        if !reply.complete {
            return Err(Error::unknown());
        }
        Ok(Executed::receipt(topic, format!("topic_deleted:{topic}")))
    }

    async fn execute_console_send(
        &self,
        record: &ConsoleRecord,
        original: &Frame,
        command: &WriteCommand,
        deadline: Instant,
    ) -> Result<Executed> {
        let ConsoleBinding::Send { topic, shard, .. } = record.binding else {
            return Err(Error::invalid("Console send requires its original binding"));
        };
        let request = console_publish(record, command)?;
        let frame = self.console_native_frame(
            record,
            original,
            FrameKind::Publish,
            TrafficClass::Produce,
            &request,
            deadline,
        )?;
        let (access, expires) = self
            .dynamic_access(record.credential, record.id, &frame, deadline)
            .await?;
        let deadline = super::auth::admit_deadline(deadline, expires)?;
        let reply = Box::pin(self.public(access, &frame, deadline, None, 0)).await?;
        let reply: public::PublishResult =
            tidemq_protocol::framing::decode_payload(reply.payload, &self.config.protocol)
                .map_err(|source| Error::operation("decode console send receipt", source))?;
        let offset = reply
            .offset
            .ok_or_else(|| Error::reject(ErrorCode::StaleGeneration, Outcome::Rejected, false))?;
        Ok(Executed::receipt(
            topic,
            format!("{topic}:{shard}:{offset}"),
        ))
    }

    fn console_native_frame<M: Message>(
        &self,
        record: &ConsoleRecord,
        original: &Frame,
        kind: FrameKind,
        class: TrafficClass,
        payload: &M,
        deadline: Instant,
    ) -> Result<Frame> {
        // Native handlers also frame their replies; the outer Bulk class is not a native lane.
        let mut frame = tidemq_protocol::framing::new_frame(
            kind,
            class,
            wire::id(&original.request_id)?,
            record.id,
            payload,
            wire::remaining(deadline)?,
            &self.config.protocol,
        )
        .map_err(|source| Error::operation("frame native console command", source))?;
        frame.traceparent = original.traceparent.clone();
        Ok(frame)
    }

    pub(super) async fn recover_console_record(
        &self,
        record: &ConsoleRecord,
        now: u64,
        deadline: Instant,
    ) -> Result<Option<Operation>> {
        if record.receipt.is_some() {
            return record_receipt(record, now).map(Some);
        }
        if let ConsoleBinding::Send {
            topic,
            shard,
            partition,
            group,
            permit,
            digest,
        } = &record.binding
        {
            let identity = tidemq_protocol::topic::SendIdentity {
                topic: *topic,
                producer: record.id,
                registration: permit.registration,
                sequence: 1,
            };
            let reply = self
                .console_data_query(
                    *group,
                    super::console_read::DataRead::PublishReceipt {
                        partition: *partition,
                        identity,
                        digest: *digest,
                    },
                    deadline,
                )
                .await;
            if let Ok(super::console_read::DataReply::PublishReceipt(Some(receipt))) = reply {
                let mut result = pending(record, now);
                result.status = Status::Succeeded;
                result.receipt = Some(format!("{topic}:{shard}:{}", receipt.offset));
                result.resource_id = Some(*topic);
                result.reason = None;
                return Ok(Some(result));
            }
            if let Err(error) = reply {
                let failure = error.failure();
                tracing::debug!(event_code = "console_receipt_query_unavailable", operation_id = %record.id,
                    error_code = failure.code, outcome = failure.outcome,
                    "Original send receipt is not currently observable");
            }
            return Ok(None);
        }
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| recover_metadata_record(state, record, now))
            .await
            .map_err(|source| Error::operation("recover original console decision", source))
    }
}

fn credential(access: &Access) -> Result<Id> {
    access
        .principal
        .strip_prefix("credential:")
        .ok_or_else(forbidden)?
        .parse()
        .map_err(|_| forbidden())
}

fn decode_console_write(
    frame: &Frame,
    limits: &tidemq_protocol::ProtocolLimits,
) -> Result<(WriteRequest, [u8; 32])> {
    let request: public::ConsoleWrite =
        tidemq_protocol::framing::decode_payload(frame.payload.clone(), limits)
            .map_err(|source| Error::operation("decode console write", source))?;
    if request.json.len() > 1_500_000 {
        return Err(Error::invalid("Console request exceeds its byte limit"));
    }
    let request: WriteRequest = serde_json::from_slice(&request.json)
        .map_err(|_| Error::invalid("Console write is invalid"))?;
    if request.operation_id != wire::id(&frame.operation_id)? {
        return Err(Error::invalid("Console operation identities differ"));
    }
    let bytes = serde_json::to_vec(&request)
        .map_err(|source| Error::operation("encode operation binding", source))?;
    Ok((request, *blake3::hash(&bytes).as_bytes()))
}

fn recover_metadata_record(
    state: &metadata::MetadataState,
    record: &ConsoleRecord,
    now: u64,
) -> Option<Operation> {
    let mut result = pending(record, now);
    if let ConsoleBinding::DeleteTopic(topic) = record.binding
        && state
            .topic_deletion(topic)
            .is_some_and(|plan| plan.id == record.id && plan.complete)
    {
        result.status = Status::Succeeded;
        result.reason = None;
        result.resource_id = Some(topic);
        result.receipt = Some(format!("topic_deleted:{topic}"));
        return Some(result);
    }
    let decision = if let Some(digest) = native_decision_binding(record) {
        match state.management_result(record.id, digest) {
            Ok(decision) => decision,
            Err(_) => return Some(conflicting_native_result(record, now)),
        }
    } else {
        state.lookup_operation(record.id)
    };
    match decision {
        metadata::OperationResult::Known(metadata::Response::Rejected { reason, .. }) => {
            result.status = Status::Rejected;
            result.reason = Some(
                super::metadata::metadata_rejection(reason)
                    .failure()
                    .code
                    .to_string(),
            );
        }
        metadata::OperationResult::Known(metadata::Response::Applied { outcome, .. }) => {
            if !matches!(
                (&record.binding, &outcome),
                (
                    ConsoleBinding::Identity { .. },
                    metadata::Outcome::Identity(_)
                ) | (
                    ConsoleBinding::CreateTopic(_),
                    metadata::Outcome::TopicConfigured(_)
                )
            ) {
                return Some(conflicting_native_result(record, now));
            }
            let (resource, receipt) = recovered_resource(&outcome)?;
            result.status = Status::Succeeded;
            result.reason = None;
            result.resource_id = Some(resource);
            result.receipt = Some(receipt);
            result.secret_unavailable = matches!(&record.binding, ConsoleBinding::Identity { payload, .. } if public::ManageIdentity::decode(payload.as_slice()).is_ok_and(|request| {
                matches!(
                    public::IdentityAction::try_from(request.action),
                    Ok(public::IdentityAction::IssueCredential | public::IdentityAction::RevokeCredential)
                )
            }));
        }
        _ => return None,
    }
    Some(result)
}

fn conflicting_native_result(record: &ConsoleRecord, now: u64) -> Operation {
    let mut result = pending(record, now);
    result.status = Status::Rejected;
    result.reason = Some("content_conflict".into());
    result
}

fn native_decision_binding(record: &ConsoleRecord) -> Option<[u8; 32]> {
    let mut hash = blake3::Hasher::new();
    match &record.binding {
        ConsoleBinding::Identity { payload, .. } => {
            hash.update(b"tidemq/identity-request/v1\0");
            hash.update(record.credential.as_bytes());
            hash.update(payload);
        }
        ConsoleBinding::CreateTopic(name) => {
            hash.update(b"tidemq/management-request/v1\0");
            hash.update(record.credential.as_bytes());
            hash.update(&i32::from(FrameKind::CreateLogicalTopic).to_le_bytes());
            hash.update(&public::LogicalTopic { name: name.clone() }.encode_to_vec());
        }
        _ => return None,
    }
    Some(*hash.finalize().as_bytes())
}

fn recovered_resource(outcome: &metadata::Outcome) -> Option<(Id, String)> {
    match outcome {
        metadata::Outcome::Identity(saved) => {
            let id = match saved.outcome {
                metadata::AuthOutcome::Tenant(id)
                | metadata::AuthOutcome::Namespace(id)
                | metadata::AuthOutcome::Principal(id)
                | metadata::AuthOutcome::Credential(id)
                | metadata::AuthOutcome::Policy(id) => id,
            };
            Some((id, format!("identity:{id}")))
        }
        metadata::Outcome::TopicConfigured(topic) => {
            Some((topic.topic_id, format!("topic:{}", topic.topic_id)))
        }
        _ => None,
    }
}
pub(super) fn observation(now: u64) -> Observation {
    Observation {
        observed_at_ms: Decimal(now),
        source: "tidemq_core_receipt".into(),
        revision: None,
        partial: false,
    }
}
pub(super) fn pending(record: &ConsoleRecord, now: u64) -> Operation {
    Operation {
        id: record.id,
        actor: record.actor.to_string(),
        target: record.target.clone(),
        scope: record.scope.clone(),
        status: Status::Unknown,
        receipt: None,
        reason: Some("outcome_unknown".into()),
        resource_id: None,
        secret: None,
        secret_unavailable: false,
        observation: observation(now),
    }
}
pub(super) fn expired(record: &ConsoleRecord, now: u64) -> Operation {
    let mut result = pending(record, now);
    result.status = Status::Expired;
    result.reason = Some("result_window_expired".into());
    result
}
fn record_receipt(record: &ConsoleRecord, now: u64) -> Result<Operation> {
    let mut result: Operation =
        serde_json::from_slice(record.receipt.as_deref().ok_or_else(Error::unavailable)?)
            .map_err(|source| Error::operation("decode durable receipt", source))?;
    result.forget_secret();
    result.observation = observation(now);
    Ok(result)
}
fn operation_response(service: &Service, frame: &Frame, result: Operation) -> Result<Frame> {
    wire::response(
        frame,
        FrameKind::ConsoleOperationResult,
        &public::ConsoleOperationResult {
            json: Bytes::from(
                serde_json::to_vec(&result)
                    .map_err(|source| Error::operation("encode console result", source))?,
            ),
        },
        &service.config.protocol,
    )
}

fn authorize_binding(
    state: &metadata::MetadataState,
    credential: Id,
    now: u64,
    operation: Id,
    binding: &ConsoleBinding,
) -> Result<()> {
    let auth = state.auth();
    match binding {
        ConsoleBinding::Identity { payload, scope } => {
            return prepare::authorize_identity(state, credential, now, operation, payload, *scope);
        }
        ConsoleBinding::CreateTopic(name) => {
            auth.authorize(credential, now, Permission::ManageTopics, Some(name))
        }
        ConsoleBinding::DeleteTopic(topic) | ConsoleBinding::Send { topic, .. } => auth
            .authorize_topic(
                credential,
                now,
                if matches!(binding, ConsoleBinding::Send { .. }) {
                    Permission::Produce
                } else {
                    Permission::ManageTopics
                },
                state.topic(*topic).ok_or_else(forbidden)?,
            ),
    }
    .map(|_| ())
    .map_err(|_| forbidden())
}

fn prepare_binding(
    state: &metadata::MetadataState,
    command: &WriteCommand,
) -> Result<Option<ConsoleBinding>> {
    let request = match command {
        WriteCommand::CreateTopic { name, .. } => {
            return Ok(Some(ConsoleBinding::CreateTopic(name.clone())));
        }
        WriteCommand::DeleteTopic { topic } => {
            return Ok(Some(ConsoleBinding::DeleteTopic(*topic)));
        }
        WriteCommand::Send { .. } | WriteCommand::ReplaceScope { .. } => return Ok(None),
        WriteCommand::SetPermission { .. } | WriteCommand::ReplaceGrants { .. } => {
            resource_request(command)?
        }
        command => principal_request(command)?,
    };
    if request.encoded_len() > 4096 {
        return Err(Error::invalid(
            "Identity request exceeds its finite byte limit",
        ));
    }
    Ok(Some(ConsoleBinding::Identity {
        payload: request.encode_to_vec(),
        scope: prepare::identity_scope(state, command)?,
    }))
}

fn principal_request(command: &WriteCommand) -> Result<public::ManageIdentity> {
    use public::IdentityAction as A;
    let mut request = public::ManageIdentity::default();
    match command {
        WriteCommand::CreatePrincipal {
            name,
            tenant,
            namespace,
            permissions,
        } => {
            request.action = A::PutPrincipalIdentity.into();
            request.name = name.clone();
            request.parent_id = id_bytes(*tenant);
            request.namespace_id = namespace.map(id_bytes).unwrap_or_default();
            request.permissions = permissions.clone();
            request.enabled = true;
        }
        WriteCommand::SetPrincipalEnabled { principal, enabled } => {
            request.action = A::SetPrincipalEnabled.into();
            request.target_id = id_bytes(*principal);
            request.enabled = *enabled;
        }
        WriteCommand::IssueCredential {
            principal,
            lifetime_ms,
        } => {
            if lifetime_ms.0 == 0 || lifetime_ms.0 > 31_536_000_000 {
                return Err(Error::invalid(
                    "Credential lifetime must be between one millisecond and one year",
                ));
            }
            request.action = A::IssueCredential.into();
            request.parent_id = id_bytes(*principal);
            request.lifetime_ms = lifetime_ms.0;
        }
        WriteCommand::RevokeCredential { credential } => {
            request.action = A::RevokeCredential.into();
            request.target_id = id_bytes(*credential);
        }
        _ => return Err(Error::invalid("Console identity command is invalid")),
    }
    Ok(request)
}

fn resource_request(command: &WriteCommand) -> Result<public::ManageIdentity> {
    use public::IdentityAction as A;
    let mut request = public::ManageIdentity::default();
    match command {
        WriteCommand::SetPermission {
            principal,
            scope,
            permission,
            allow,
        } => {
            request.action = if *allow {
                A::GrantResourcePermission
            } else {
                A::RevokeResourcePermission
            }
            .into();
            request.target_id = id_bytes(*principal);
            request.resource_scope = Some(scope_wire(scope));
            request.permissions = vec![permission.clone()];
        }
        WriteCommand::ReplaceGrants {
            principal,
            expected_revision,
            grants,
        } => {
            request.action = A::ReplaceResourceGrants.into();
            request.target_id = id_bytes(*principal);
            request.expected_auth_revision = expected_revision.0;
            request.resource_grants = grants
                .iter()
                .map(|grant| public::ResourceGrant {
                    scope: Some(scope_wire(&grant.scope)),
                    permissions: grant.permissions.clone(),
                })
                .collect();
        }
        _ => return Err(Error::invalid("Console resource command is invalid")),
    }
    Ok(request)
}

fn scope_wire(scope: &api::Scope) -> public::ResourceScope {
    public::ResourceScope {
        tenant_id: id_bytes(scope.tenant),
        namespace_id: scope.namespace.map(id_bytes).unwrap_or_default(),
        topic_id: scope.topic.map(id_bytes).unwrap_or_default(),
    }
}
fn id_bytes(id: Id) -> Bytes {
    Bytes::copy_from_slice(id.as_bytes())
}
fn decode_message(
    payload: &str,
    key: Option<&str>,
    properties: &BTreeMap<String, String>,
) -> Result<public::PublishMessage> {
    if payload.len() > 1_398_104 || key.is_some_and(|key| key.len() > 344) || properties.len() > 64
    {
        return Err(Error::invalid(
            "Message exceeds its byte or property limits",
        ));
    }
    let decode = |value: &str| {
        STANDARD
            .decode(value)
            .map(Bytes::from)
            .map_err(|_| Error::invalid("Message binary fields require canonical Base64"))
    };
    let properties = properties
        .iter()
        .map(|(name, value)| {
            if name.is_empty() || name.len() > 128 || value.len() > 16_384 {
                return Err(Error::invalid("Message property exceeds its finite limits"));
            }
            Ok((name.clone(), decode(value)?))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(public::PublishMessage {
        payload: decode(payload)?,
        key: key.map(decode).transpose()?,
        properties,
    })
}
