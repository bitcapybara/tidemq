use std::collections::BTreeMap;

use prost::Message;

use serde::{Deserialize, Serialize};
use tidemq_protocol::{
    Id,
    console_operation::{FIRST_SUBMISSION_MS, RESULT_WINDOW_MS, created_at_ms},
};

use super::{Command, MetadataState, Outcome, PartitionKey, Permission, Rejection, ResourceScope};
use crate::producer::PublisherPermit;

pub(crate) const MAX_OPERATIONS: usize = 4096;

/// No message, key, property value, or credential secret is retained here.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum ConsoleBinding {
    Identity {
        payload: Vec<u8>,
        scope: ResourceScope,
    },
    CreateTopic(String),
    DeleteTopic(Id),
    Send {
        topic: Id,
        shard: Id,
        partition: PartitionKey,
        group: u64,
        permit: PublisherPermit,
        digest: [u8; 32],
    },
}

impl ConsoleBinding {
    pub(crate) fn labels(&self) -> Result<(String, String), Rejection> {
        binding_labels(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ConsoleRecord {
    pub id: Id,
    pub actor: Id,
    pub credential: Id,
    pub digest: [u8; 32],
    pub created_ms: u64,
    pub expires_ms: u64,
    pub binding: ConsoleBinding,
    pub target: String,
    pub scope: String,
    pub receipt: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum ConsoleCommand {
    Begin {
        now_ms: u64,
        record: Box<ConsoleRecord>,
    },
    Complete {
        id: Id,
        digest: [u8; 32],
        receipt: Vec<u8>,
    },
}

impl Command {
    pub(crate) fn console(operation: Id, revision: u64, command: ConsoleCommand) -> Self {
        Self::new(operation, revision, super::Action::Console(command))
    }
}

impl MetadataState {
    pub(crate) fn console_record(&self, id: Id) -> Option<&ConsoleRecord> {
        self.console_operations.get(&id)
    }

    pub(super) fn console_command(
        &mut self,
        command: &ConsoleCommand,
    ) -> Result<Outcome, Rejection> {
        match command {
            ConsoleCommand::Begin { now_ms, record } => {
                self.begin_console_operation(*now_ms, record)
            }
            ConsoleCommand::Complete {
                id,
                digest,
                receipt,
            } => self.complete_console_operation(*id, *digest, receipt),
        }
    }

    fn begin_console_operation(
        &mut self,
        now: u64,
        record: &ConsoleRecord,
    ) -> Result<Outcome, Rejection> {
        let actor = self
            .auth
            .current_principal(record.credential, now)
            .map_err(Rejection::Authorization)?;
        if actor.id != record.actor {
            return Err(Rejection::InvalidEvidence);
        }
        if let Some(previous) = self.console_operations.get(&record.id) {
            return if previous.actor != record.actor || previous.digest != record.digest {
                Err(Rejection::OperationConflict)
            } else if previous.expires_ms <= now {
                Err(Rejection::InvalidCommand)
            } else {
                Ok(Outcome::ConsoleOperation(record.id))
            };
        }
        self.validate_console_record(record)?;
        if record.receipt.is_some()
            || record.created_ms > now.saturating_add(30_000)
            || record.created_ms.saturating_add(FIRST_SUBMISSION_MS) <= now
        {
            return Err(Rejection::InvalidCommand);
        }
        if self
            .console_operations
            .values()
            .filter(|item| item.expires_ms > now)
            .count()
            >= MAX_OPERATIONS
        {
            return Err(Rejection::Capacity);
        }
        let publishers = self.prepare_console_publisher(record, now)?;
        // No state changes occur until every binding and publisher check has succeeded.
        self.console_operations
            .retain(|_, item| item.expires_ms > now);
        if let Some(publishers) = publishers {
            self.publishers = publishers;
        }
        self.console_operations.insert(record.id, record.clone());
        Ok(Outcome::ConsoleOperation(record.id))
    }

    fn prepare_console_publisher(
        &self,
        record: &ConsoleRecord,
        now: u64,
    ) -> Result<Option<super::PublishersBook>, Rejection> {
        let ConsoleBinding::Send { topic, permit, .. } = &record.binding else {
            return Ok(None);
        };
        let topic_record = self.topic(*topic).ok_or(Rejection::NotFound)?;
        self.auth
            .authorize_topic(record.credential, now, Permission::Produce, topic_record)
            .map_err(Rejection::Authorization)?;
        let command = super::PublisherCommand::Open {
            topic: *topic,
            producer: record.id,
            registration: permit.registration,
            client: record.id,
            credential: record.credential,
            now_ms: now,
            credential_expires_ms: self
                .auth
                .credential(record.credential)
                .ok_or(Rejection::InvalidEvidence)?
                .expires_ms,
            digest: record.digest,
        };
        let mut publishers = self.publishers.clone();
        if publishers.apply(record.id, &command)? != *permit {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(Some(publishers))
    }

    fn complete_console_operation(
        &mut self,
        id: Id,
        digest: [u8; 32],
        receipt: &[u8],
    ) -> Result<Outcome, Rejection> {
        let record = self
            .console_operations
            .get(&id)
            .ok_or(Rejection::NotFound)?;
        if record.digest != digest {
            return Err(Rejection::OperationConflict);
        }
        if let Some(previous) = &record.receipt {
            return if previous == receipt {
                Ok(Outcome::ConsoleOperation(id))
            } else {
                Err(Rejection::OperationConflict)
            };
        }
        validate_receipt(record, receipt)?;
        self.console_operations
            .get_mut(&id)
            .ok_or(Rejection::NotFound)?
            .receipt = Some(receipt.to_vec());
        Ok(Outcome::ConsoleOperation(id))
    }

    fn validate_console_record(&self, record: &ConsoleRecord) -> Result<(), Rejection> {
        if created_at_ms(record.id) != Some(record.created_ms)
            || record.expires_ms != record.created_ms.saturating_add(RESULT_WINDOW_MS)
            || record.digest == [0; 32]
            || record.target.len() > 256
            || record.scope.len() > 256
            || self.auth.principal(record.actor).is_none()
            || self
                .auth
                .credential(record.credential)
                .is_none_or(|credential| credential.principal != record.actor)
        {
            return Err(Rejection::InvalidEvidence);
        }
        let labels = binding_labels(&record.binding)?;
        if labels != (record.target.clone(), record.scope.clone()) {
            return Err(Rejection::InvalidEvidence);
        }
        self.validate_console_send(record)
    }

    fn validate_console_send(&self, record: &ConsoleRecord) -> Result<(), Rejection> {
        let ConsoleBinding::Send {
            topic,
            shard,
            partition,
            group,
            permit,
            digest,
        } = &record.binding
        else {
            return Ok(());
        };
        let domain = self
            .topology
            .shard(*shard)
            .ok_or(Rejection::InvalidEvidence)?;
        if partition.topic_id != *topic
            || domain.partition != *partition
            || *group == 0
            || permit.topic != *topic
            || permit.producer != record.id
            || permit.client != record.id
            || permit.credential != record.credential
            || permit.registration == 0
            || !permit.live
            || permit.expires_ms <= record.created_ms
            || *digest == [0; 32]
        {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(())
    }

    pub(crate) fn validate_console_operations(&self) -> Result<(), Rejection> {
        if self.console_operations.len() > MAX_OPERATIONS {
            return Err(Rejection::Capacity);
        }
        for (id, record) in &self.console_operations {
            if *id != record.id {
                return Err(Rejection::InvalidEvidence);
            }
            self.validate_console_record(record)?;
            if let Some(receipt) = &record.receipt {
                validate_receipt(record, receipt)?;
            }
        }
        Ok(())
    }
}

fn binding_labels(binding: &ConsoleBinding) -> Result<(String, String), Rejection> {
    match binding {
        ConsoleBinding::Identity { payload, scope } => {
            let request = identity_request(payload)?;
            validate_identity_scope(&request, *scope)?;
            let target = if request.target_id.is_empty() {
                if request.action
                    == i32::from(tidemq_protocol::public::IdentityAction::IssueCredential)
                {
                    parse_id(&request.parent_id)?.to_string()
                } else {
                    request.name
                }
            } else {
                parse_id(&request.target_id)?.to_string()
            };
            Ok((target, scope_label(*scope)))
        }
        ConsoleBinding::CreateTopic(name) => {
            if !super::types::valid_topic_name(name) || name.split('/').count() != 3 {
                return Err(Rejection::InvalidEvidence);
            }
            Ok((name.clone(), name.clone()))
        }
        ConsoleBinding::DeleteTopic(topic) | ConsoleBinding::Send { topic, .. } => {
            Ok((topic.to_string(), topic.to_string()))
        }
    }
}

fn scope_label(scope: ResourceScope) -> String {
    match scope {
        ResourceScope::Cluster => "cluster".into(),
        ResourceScope::Tenant(tenant) => format!("tenant:{tenant}"),
        ResourceScope::Namespace { tenant, namespace } => format!("namespace:{tenant}/{namespace}"),
        ResourceScope::Topic {
            tenant,
            namespace,
            topic,
        } => format!("topic:{tenant}/{namespace}/{topic}"),
    }
}

fn validate_identity_scope(
    request: &tidemq_protocol::public::ManageIdentity,
    scope: ResourceScope,
) -> Result<(), Rejection> {
    use tidemq_protocol::public::IdentityAction as A;
    let action = A::try_from(request.action).map_err(|_| Rejection::InvalidEvidence)?;
    let expected = match action {
        A::PutPrincipalIdentity => Some(
            ResourceScope::from_wire(&tidemq_protocol::public::ResourceScope {
                tenant_id: request.parent_id.clone(),
                namespace_id: request.namespace_id.clone(),
                topic_id: Default::default(),
            })
            .map_err(|_| Rejection::InvalidEvidence)?,
        ),
        A::GrantResourcePermission | A::RevokeResourcePermission => Some(
            ResourceScope::from_wire(
                request
                    .resource_scope
                    .as_ref()
                    .ok_or(Rejection::InvalidEvidence)?,
            )
            .map_err(|_| Rejection::InvalidEvidence)?,
        ),
        A::IssueCredential | A::RevokeCredential | A::SetPrincipalEnabled
            if matches!(scope, ResourceScope::Topic { .. }) =>
        {
            return Err(Rejection::InvalidEvidence);
        }
        _ => None,
    };
    if expected.is_some_and(|expected| scope != expected) {
        return Err(Rejection::InvalidEvidence);
    }
    Ok(())
}

fn identity_request(bytes: &[u8]) -> Result<tidemq_protocol::public::ManageIdentity, Rejection> {
    use tidemq_protocol::public::{IdentityAction as A, ManageIdentity};
    if bytes.len() > 4096 {
        return Err(Rejection::Capacity);
    }
    let request = ManageIdentity::decode(bytes).map_err(|_| Rejection::InvalidEvidence)?;
    let action = A::try_from(request.action).map_err(|_| Rejection::InvalidEvidence)?;
    if request.encode_to_vec() != bytes
        || !matches!(
            action,
            A::PutPrincipalIdentity
                | A::SetPrincipalEnabled
                | A::IssueCredential
                | A::RevokeCredential
                | A::GrantResourcePermission
                | A::RevokeResourcePermission
                | A::ReplaceResourceGrants
        )
    {
        return Err(Rejection::InvalidEvidence);
    }
    Ok(request)
}

fn parse_id(bytes: &[u8]) -> Result<Id, Rejection> {
    Id::from_slice(bytes).map_err(|_| Rejection::InvalidEvidence)
}

fn validate_receipt(record: &ConsoleRecord, receipt: &[u8]) -> Result<(), Rejection> {
    use tidemq_protocol::console_operation::{Operation, Status};
    if receipt.len() > 16_384 {
        return Err(Rejection::Capacity);
    }
    let parsed: Operation =
        serde_json::from_slice(receipt).map_err(|_| Rejection::InvalidCommand)?;
    if parsed.id != record.id
        || parsed.actor != record.actor.to_string()
        || parsed.target != record.target
        || parsed.scope != record.scope
        || parsed.secret.is_some()
        || parsed.observation.source != "tidemq_core_receipt"
        || serde_json::to_vec(&parsed).map_err(|_| Rejection::InvalidCommand)? != receipt
    {
        return Err(Rejection::InvalidCommand);
    }
    match parsed.status {
        Status::Succeeded => validate_success(record, &parsed),
        Status::Rejected
            if parsed.receipt.is_none()
                && parsed.resource_id.is_none()
                && !parsed.secret_unavailable
                && parsed.reason.as_ref().is_some_and(|reason| {
                    !reason.is_empty()
                        && reason.len() <= 128
                        && reason.bytes().all(|byte| {
                            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
                        })
                }) =>
        {
            Ok(())
        }
        _ => Err(Rejection::InvalidCommand),
    }
}

fn validate_success(
    record: &ConsoleRecord,
    parsed: &tidemq_protocol::console_operation::Operation,
) -> Result<(), Rejection> {
    let resource = parsed.resource_id.ok_or(Rejection::InvalidCommand)?;
    let (expected, secret_unavailable) = match &record.binding {
        ConsoleBinding::CreateTopic(_) if resource == record.id => {
            (format!("topic:{resource}"), false)
        }
        ConsoleBinding::DeleteTopic(topic) if resource == *topic => {
            (format!("topic_deleted:{topic}"), false)
        }
        ConsoleBinding::Send { topic, shard, .. } if resource == *topic => {
            let prefix = format!("{topic}:{shard}:");
            let offset = parsed
                .receipt
                .as_deref()
                .and_then(|receipt| receipt.strip_prefix(&prefix))
                .and_then(|offset| offset.parse::<u64>().ok())
                .ok_or(Rejection::InvalidCommand)?;
            (format!("{prefix}{offset}"), false)
        }
        ConsoleBinding::Identity { payload, .. } => {
            let request = identity_request(payload)?;
            let action = tidemq_protocol::public::IdentityAction::try_from(request.action)
                .map_err(|_| Rejection::InvalidCommand)?;
            let creates = matches!(
                action,
                tidemq_protocol::public::IdentityAction::IssueCredential
                    | tidemq_protocol::public::IdentityAction::PutPrincipalIdentity
            );
            let expected = if creates {
                record.id
            } else {
                parse_id(&request.target_id)?
            };
            if resource != expected {
                return Err(Rejection::InvalidCommand);
            }
            (
                format!("identity:{resource}"),
                matches!(
                    action,
                    tidemq_protocol::public::IdentityAction::IssueCredential
                        | tidemq_protocol::public::IdentityAction::RevokeCredential
                ),
            )
        }
        _ => return Err(Rejection::InvalidCommand),
    };
    if parsed.reason.is_some()
        || parsed.receipt.as_deref() != Some(expected.as_str())
        || parsed.secret_unavailable != secret_unavailable
    {
        return Err(Rejection::InvalidCommand);
    }
    Ok(())
}

pub(crate) type ConsoleOperations = BTreeMap<Id, ConsoleRecord>;

#[cfg(test)]
mod tests {
    use tidemq_protocol::console::{Decimal, Observation};
    use tidemq_protocol::console_operation::{Operation, Status};

    use super::*;
    use crate::metadata::{
        AuthAction, AuthCommand, DeploymentMode, MetadataLimits, Namespace, NodeRole, NodeSpec,
        Response, Tenant, TokenDigest,
    };

    struct Fixture {
        state: MetadataState,
        actor: Id,
        credential: Id,
        tenant: Id,
        namespace: Id,
        topic: Id,
        now: u64,
    }

    fn id() -> Id {
        Id::new_v7().unwrap()
    }

    fn id_at(timestamp: u64) -> Id {
        let mut bytes = *id().as_bytes();
        bytes[..6].copy_from_slice(&timestamp.to_be_bytes()[2..]);
        Id::from_slice(&bytes).unwrap()
    }

    impl Fixture {
        fn new() -> Self {
            let now = created_at_ms(id()).unwrap();
            let mut state = MetadataState::default();
            assert!(
                state
                    .apply(Command::bootstrap(
                        id(),
                        id(),
                        1,
                        DeploymentMode::SeparateRf1,
                        MetadataLimits::default()
                    ))
                    .is_applied()
            );
            assert!(
                state
                    .apply(Command::put_node(
                        id(),
                        state.revision(),
                        NodeSpec {
                            node_id: 1,
                            zone: "test".into(),
                            endpoint: "127.0.0.1:7443".parse().unwrap(),
                            roles: [NodeRole::Storage, NodeRole::Broker].into(),
                            schedulable: true
                        },
                        0
                    ))
                    .is_applied()
            );
            assert!(
                state
                    .apply(Command::create_group(
                        id(),
                        state.revision(),
                        2,
                        crate::metadata::GroupPlacement::Raft { voters: [1].into() }
                    ))
                    .is_applied()
            );
            let actor = id();
            let credential = id();
            state
                .auth
                .apply(&AuthCommand {
                    actor: None,
                    now_ms: now,
                    request_digest: [1; 32],
                    action: AuthAction::Bootstrap {
                        principal: actor,
                        credential,
                        digest: TokenDigest::of("metadata-console-private-bootstrap-token")
                            .unwrap(),
                        expires_ms: now + 2 * RESULT_WINDOW_MS,
                    },
                })
                .unwrap();
            let tenant = id();
            let namespace = id();
            for action in [
                AuthAction::CreateTenant(Tenant {
                    id: tenant,
                    name: "acme".into(),
                    enabled: true,
                    limits: Default::default(),
                }),
                AuthAction::CreateNamespace(Namespace {
                    id: namespace,
                    tenant,
                    name: "app".into(),
                    enabled: true,
                    limits: None,
                }),
            ] {
                state
                    .auth
                    .apply(&AuthCommand {
                        actor: Some(credential),
                        now_ms: now,
                        request_digest: [2; 32],
                        action,
                    })
                    .unwrap();
            }
            let topic = id();
            assert!(
                state
                    .apply(Command::create_topic(
                        id(),
                        state.revision(),
                        topic,
                        "acme/app/orders".into(),
                        vec![2]
                    ))
                    .is_applied()
            );
            state.validate_snapshot().unwrap();
            Self {
                state,
                actor,
                credential,
                tenant,
                namespace,
                topic,
                now,
            }
        }

        fn record(&self, binding: ConsoleBinding) -> ConsoleRecord {
            let operation = id();
            let created_ms = created_at_ms(operation).unwrap();
            let (target, scope) = binding_labels(&binding).unwrap();
            ConsoleRecord {
                id: operation,
                actor: self.actor,
                credential: self.credential,
                digest: [3; 32],
                created_ms,
                expires_ms: created_ms + RESULT_WINDOW_MS,
                binding,
                target,
                scope,
                receipt: None,
            }
        }

        fn send(&self) -> ConsoleRecord {
            let route = self
                .state
                .topology
                .layout(self.topic)
                .unwrap()
                .shards
                .first()
                .unwrap();
            let domain = self.state.topology.shard(route.id).unwrap();
            let mut record = self.record(ConsoleBinding::Send {
                topic: self.topic,
                shard: route.id,
                partition: domain.partition,
                group: route.group,
                permit: PublisherPermit {
                    topic: self.topic,
                    producer: self.actor,
                    client: self.actor,
                    credential: self.credential,
                    registration: self.state.publishers.next_registration(),
                    expires_ms: self.now + super::super::PUBLISHER_LEASE_MS,
                    live: true,
                },
                digest: [9; 32],
            });
            if let ConsoleBinding::Send { permit, .. } = &mut record.binding {
                permit.producer = record.id;
                permit.client = record.id;
            }
            record
        }

        fn apply(&mut self, command: ConsoleCommand) -> Response {
            self.state
                .apply(Command::console(id(), self.state.revision(), command))
        }

        fn begin(&mut self, record: &ConsoleRecord) -> Response {
            self.apply(ConsoleCommand::Begin {
                now_ms: self.now,
                record: Box::new(record.clone()),
            })
        }

        fn complete(&mut self, record: &ConsoleRecord, receipt: Vec<u8>) -> Response {
            self.apply(ConsoleCommand::Complete {
                id: record.id,
                digest: record.digest,
                receipt,
            })
        }
    }

    fn receipt(record: &ConsoleRecord) -> Operation {
        Operation {
            id: record.id,
            actor: record.actor.to_string(),
            target: record.target.clone(),
            scope: record.scope.clone(),
            status: Status::Succeeded,
            receipt: Some(format!("topic:{}", record.id)),
            reason: None,
            resource_id: Some(record.id),
            secret_unavailable: false,
            secret: None,
            observation: Observation {
                observed_at_ms: Decimal(record.created_ms + 1),
                source: "tidemq_core_receipt".into(),
                revision: None,
                partial: false,
            },
        }
    }

    fn rejected(response: Response, expected: Rejection) {
        assert!(
            matches!(response, Response::Rejected { reason, .. } if reason == expected),
            "Unexpected metadata refusal"
        );
    }

    #[test]
    fn console_intent_conflicts_expiry_and_snapshot_roundtrip_preserve_original_binding() {
        let mut fixture = Fixture::new();
        let record = fixture.record(ConsoleBinding::CreateTopic("acme/app/new".into()));
        assert!(fixture.begin(&record).is_applied());
        assert!(fixture.begin(&record).is_applied());
        assert_eq!(fixture.state.console_operations.len(), 1);
        let mut changed = record.clone();
        changed.digest = [4; 32];
        rejected(fixture.begin(&changed), Rejection::OperationConflict);
        let mut expired = record.clone();
        expired.id = id_at(fixture.now - RESULT_WINDOW_MS - 1);
        expired.created_ms = created_at_ms(expired.id).unwrap();
        expired.expires_ms = expired.created_ms + RESULT_WINDOW_MS;
        rejected(fixture.begin(&expired), Rejection::InvalidCommand);
        let mut restored: MetadataState =
            postcard::from_bytes(&postcard::to_stdvec(&fixture.state).unwrap()).unwrap();
        restored.validate_snapshot().unwrap();
        assert_eq!(restored.console_record(record.id), Some(&record));
        let result = restored.apply(Command::console(
            id(),
            restored.revision(),
            ConsoleCommand::Begin {
                now_ms: fixture.now,
                record: Box::new(changed),
            },
        ));
        rejected(result, Rejection::OperationConflict);
    }

    #[test]
    fn rejected_publisher_open_changes_neither_publishers_nor_expired_console_records() {
        let mut fixture = Fixture::new();
        let mut expired = fixture.record(ConsoleBinding::CreateTopic("acme/app/old".into()));
        expired.id = id_at(fixture.now - RESULT_WINDOW_MS - 1);
        expired.created_ms = created_at_ms(expired.id).unwrap();
        expired.expires_ms = expired.created_ms + RESULT_WINDOW_MS;
        fixture
            .state
            .console_operations
            .insert(expired.id, expired.clone());
        let mut send = fixture.send();
        if let ConsoleBinding::Send { permit, .. } = &mut send.binding {
            permit.expires_ms += 1;
        }
        let before = fixture.state.clone();
        rejected(fixture.begin(&send), Rejection::InvalidEvidence);
        assert_eq!(
            fixture.state, before,
            "A rejected Begin partially changed durable state"
        );
        if let ConsoleBinding::Send { permit, .. } = &mut send.binding {
            permit.expires_ms -= 1;
        }
        assert!(fixture.begin(&send).is_applied());
        assert!(fixture.state.console_record(expired.id).is_none());
        assert_eq!(fixture.state.publishers.records().count(), 1);
        fixture.state.validate_snapshot().unwrap();
    }

    #[test]
    fn console_completion_rejects_secrets_unknown_fields_and_changed_receipts() {
        let mut fixture = Fixture::new();
        let record = fixture.record(ConsoleBinding::CreateTopic("acme/app/new".into()));
        assert!(fixture.begin(&record).is_applied());
        let valid = receipt(&record);
        let mut with_secret = valid.clone();
        with_secret.secret = Some("credential-secret-must-not-be-persisted".into());
        let mut wrong_target = valid.clone();
        wrong_target.target = "other".into();
        let mut pending = valid.clone();
        pending.status = Status::Unknown;
        let mut arbitrary_receipt = valid.clone();
        arbitrary_receipt.receipt = Some("private-payload-must-not-be-persisted".into());
        for invalid in [with_secret, wrong_target, pending, arbitrary_receipt] {
            rejected(
                fixture.complete(&record, serde_json::to_vec(&invalid).unwrap()),
                Rejection::InvalidCommand,
            );
            assert!(
                fixture
                    .state
                    .console_record(record.id)
                    .unwrap()
                    .receipt
                    .is_none()
            );
        }
        let mut unknown = serde_json::to_value(&valid).unwrap();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("payload".into(), "secret-in-an-unknown-field".into());
        rejected(
            fixture.complete(&record, serde_json::to_vec(&unknown).unwrap()),
            Rejection::InvalidCommand,
        );
        let bytes = serde_json::to_vec(&valid).unwrap();
        assert!(fixture.complete(&record, bytes.clone()).is_applied());
        assert!(fixture.complete(&record, bytes.clone()).is_applied());
        let mut changed = valid;
        changed.observation.observed_at_ms.0 += 1;
        rejected(
            fixture.complete(&record, serde_json::to_vec(&changed).unwrap()),
            Rejection::OperationConflict,
        );
        assert_eq!(
            fixture
                .state
                .console_record(record.id)
                .unwrap()
                .receipt
                .as_ref(),
            Some(&bytes)
        );
        fixture.state.validate_snapshot().unwrap();
    }

    #[test]
    fn console_snapshot_validation_rejects_bad_bindings_and_persisted_secrets() {
        let mut fixture = Fixture::new();
        let record = fixture.record(ConsoleBinding::CreateTopic("acme/app/new".into()));
        assert!(fixture.begin(&record).is_applied());
        assert!(
            fixture
                .complete(&record, serde_json::to_vec(&receipt(&record)).unwrap())
                .is_applied()
        );
        let restored: MetadataState =
            postcard::from_bytes(&postcard::to_stdvec(&fixture.state).unwrap()).unwrap();
        restored.validate_snapshot().unwrap();
        assert_eq!(
            restored.console_record(record.id),
            fixture.state.console_record(record.id)
        );
        let mut corrupt = restored.clone();
        corrupt
            .console_operations
            .get_mut(&record.id)
            .unwrap()
            .binding = ConsoleBinding::Identity {
            payload: b"invalid-bound-command".to_vec(),
            scope: ResourceScope::Cluster,
        };
        assert!(corrupt.validate_snapshot().is_err());
        let mut corrupt = restored;
        let mut secret = receipt(&record);
        secret.secret = Some("not-a-snapshot-field".into());
        corrupt
            .console_operations
            .get_mut(&record.id)
            .unwrap()
            .receipt = Some(serde_json::to_vec(&secret).unwrap());
        assert!(corrupt.validate_snapshot().is_err());
        let send = fixture.send();
        assert!(fixture.begin(&send).is_applied());
        if let ConsoleBinding::Send { partition, .. } = &mut fixture
            .state
            .console_operations
            .get_mut(&send.id)
            .unwrap()
            .binding
        {
            partition.topic_id = id();
        }
        assert!(fixture.state.validate_snapshot().is_err());
    }

    #[test]
    fn credential_receipts_keep_only_the_one_time_secret_unavailable_fact() {
        let mut fixture = Fixture::new();
        let request = tidemq_protocol::public::ManageIdentity {
            action: tidemq_protocol::public::IdentityAction::IssueCredential.into(),
            parent_id: bytes::Bytes::copy_from_slice(fixture.actor.as_bytes()),
            lifetime_ms: 300_000,
            ..Default::default()
        };
        let record = fixture.record(ConsoleBinding::Identity {
            payload: request.encode_to_vec(),
            scope: ResourceScope::Cluster,
        });
        assert_eq!(record.target, fixture.actor.to_string());
        assert_eq!(record.scope, "cluster");
        assert!(fixture.begin(&record).is_applied());
        let mut result = receipt(&record);
        result.receipt = Some(format!("identity:{}", record.id));
        result.secret = Some("one-time-credential-secret-is-returned-once".into());
        rejected(
            fixture.complete(&record, serde_json::to_vec(&result).unwrap()),
            Rejection::InvalidCommand,
        );
        result.forget_secret();
        assert!(
            fixture
                .complete(&record, serde_json::to_vec(&result).unwrap())
                .is_applied()
        );
        let restored: MetadataState =
            postcard::from_bytes(&postcard::to_stdvec(&fixture.state).unwrap()).unwrap();
        restored.validate_snapshot().unwrap();
        let parsed: Operation = serde_json::from_slice(
            restored
                .console_record(record.id)
                .unwrap()
                .receipt
                .as_ref()
                .unwrap(),
        )
        .unwrap();
        assert!(parsed.secret.is_none());
        assert!(parsed.secret_unavailable);
    }

    #[test]
    fn revoked_credential_receipts_retain_no_secret_and_match_the_native_result() {
        let mut fixture = Fixture::new();
        let request = tidemq_protocol::public::ManageIdentity {
            action: tidemq_protocol::public::IdentityAction::RevokeCredential.into(),
            target_id: bytes::Bytes::copy_from_slice(fixture.credential.as_bytes()),
            ..Default::default()
        };
        let record = fixture.record(ConsoleBinding::Identity {
            payload: request.encode_to_vec(),
            scope: ResourceScope::Cluster,
        });
        assert!(fixture.begin(&record).is_applied());
        let mut result = receipt(&record);
        result.resource_id = Some(fixture.credential);
        result.receipt = Some(format!("identity:{}", fixture.credential));
        rejected(
            fixture.complete(&record, serde_json::to_vec(&result).unwrap()),
            Rejection::InvalidCommand,
        );
        result.secret_unavailable = true;
        let mut with_secret = result.clone();
        with_secret.secret = Some("a-revoked-token-must-not-enter-a-receipt".into());
        rejected(
            fixture.complete(&record, serde_json::to_vec(&with_secret).unwrap()),
            Rejection::InvalidCommand,
        );
        assert!(
            fixture
                .complete(&record, serde_json::to_vec(&result).unwrap())
                .is_applied()
        );
        let restored: MetadataState =
            postcard::from_bytes(&postcard::to_stdvec(&fixture.state).unwrap()).unwrap();
        restored.validate_snapshot().unwrap();
    }

    #[test]
    fn cleared_scope_receipts_preserve_the_begin_scope_after_snapshot_restore() {
        let mut fixture = Fixture::new();
        let scope = ResourceScope::Topic {
            tenant: fixture.tenant,
            namespace: fixture.namespace,
            topic: fixture.topic,
        };
        let request = tidemq_protocol::public::ManageIdentity {
            action: tidemq_protocol::public::IdentityAction::ReplaceResourceGrants.into(),
            target_id: bytes::Bytes::copy_from_slice(fixture.actor.as_bytes()),
            expected_auth_revision: 1,
            resource_grants: Vec::new(),
            ..Default::default()
        };
        let record = fixture.record(ConsoleBinding::Identity {
            payload: request.encode_to_vec(),
            scope,
        });
        let expected = format!(
            "topic:{}/{}/{}",
            fixture.tenant, fixture.namespace, fixture.topic
        );
        assert_eq!(record.scope, expected);
        assert!(fixture.begin(&record).is_applied());
        let mut result = receipt(&record);
        result.resource_id = Some(fixture.actor);
        result.receipt = Some(format!("identity:{}", fixture.actor));
        result.scope.clear();
        rejected(
            fixture.complete(&record, serde_json::to_vec(&result).unwrap()),
            Rejection::InvalidCommand,
        );
        let mut broken = record.clone();
        broken.scope.clear();
        assert_eq!(
            fixture.state.validate_console_record(&broken),
            Err(Rejection::InvalidEvidence)
        );
        result.scope = expected.clone();
        assert!(
            fixture
                .complete(&record, serde_json::to_vec(&result).unwrap())
                .is_applied()
        );
        let restored: MetadataState =
            postcard::from_bytes(&postcard::to_stdvec(&fixture.state).unwrap()).unwrap();
        restored.validate_snapshot().unwrap();
        let restored = restored.console_record(record.id).unwrap();
        let parsed: Operation = serde_json::from_slice(restored.receipt.as_ref().unwrap()).unwrap();
        assert_eq!(restored.scope, expected);
        assert_eq!(parsed.scope, expected);
    }

    #[test]
    fn explicit_identity_scopes_must_match_the_command_and_cannot_use_empty_labels() {
        let fixture = Fixture::new();
        let scope = ResourceScope::Namespace {
            tenant: fixture.tenant,
            namespace: fixture.namespace,
        };
        let mut request = tidemq_protocol::public::ManageIdentity {
            action: tidemq_protocol::public::IdentityAction::PutPrincipalIdentity.into(),
            parent_id: bytes::Bytes::copy_from_slice(fixture.tenant.as_bytes()),
            namespace_id: bytes::Bytes::copy_from_slice(fixture.namespace.as_bytes()),
            name: "new-identity".into(),
            ..Default::default()
        };
        let correct = ConsoleBinding::Identity {
            payload: request.encode_to_vec(),
            scope,
        };
        let record = fixture.record(correct);
        assert_eq!(record.target, "new-identity");
        assert_eq!(
            record.scope,
            format!("namespace:{}/{}", fixture.tenant, fixture.namespace)
        );
        assert!(
            ConsoleBinding::Identity {
                payload: request.encode_to_vec(),
                scope: ResourceScope::Cluster
            }
            .labels()
            .is_err()
        );
        for action in [
            tidemq_protocol::public::IdentityAction::GrantResourcePermission,
            tidemq_protocol::public::IdentityAction::RevokeResourcePermission,
        ] {
            request.action = action.into();
            request.target_id = bytes::Bytes::copy_from_slice(fixture.actor.as_bytes());
            request.resource_scope = Some(scope.to_wire());
            assert!(
                ConsoleBinding::Identity {
                    payload: request.encode_to_vec(),
                    scope
                }
                .labels()
                .is_ok()
            );
            assert!(
                ConsoleBinding::Identity {
                    payload: request.encode_to_vec(),
                    scope: ResourceScope::Tenant(fixture.tenant)
                }
                .labels()
                .is_err()
            );
        }
    }
}
