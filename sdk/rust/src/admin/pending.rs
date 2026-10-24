use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

use prost::Message;
use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    public,
};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

use crate::{Admin, Client, Error, Result, TopicInfo};

// Cover a complete bounded layout, request, result, and task while recovery handles retain it.
const WORK_BYTES: u32 = 128 * 1024;
const MAX_PENDING: usize = 16;

#[derive(Clone, Debug)]
pub enum AdminReceipt {
    Topic(TopicInfo),
    TopicDeleted(crate::TopicId),
    TenantCreated(crate::TenantId),
    NamespaceCreated(crate::NamespaceId),
    PrincipalConfigured(crate::PrincipalId),
    CredentialIssued(crate::AccessToken),
    CredentialSecretUnavailable {
        id: crate::CredentialId,
        expires_at_unix_ms: u64,
    },
    CredentialRevoked(crate::CredentialId),
    TenantStatusChanged(crate::TenantId),
}

pub(crate) enum Request {
    Identity(public::ManageIdentity),
    CreateTopic(String),
    DeleteTopic(Id),
    Layout {
        name: String,
        request: public::ChangeTopicLayout,
    },
}

impl Request {
    fn validate(&self) -> Result<()> {
        let bytes = match self {
            Self::Identity(request) => request.encoded_len(),
            Self::CreateTopic(name) => name.len(),
            Self::DeleteTopic(_) => 16,
            Self::Layout { name, request } => name.len().saturating_add(request.encoded_len()),
        };
        if bytes > 4096 {
            return Err(Error::Configuration {
                reason: "administration request exceeds its recovery bound",
            });
        }
        Ok(())
    }

    // Keep each wire request paired with its typed reply and original resource validation.
    async fn execute(
        &self,
        client: &Client,
        operation: Id,
        deadline: Instant,
    ) -> Result<AdminReceipt> {
        let (name, wire): (&String, public::TopicLayoutResult) = match self {
            Self::DeleteTopic(topic) => {
                return delete_topic(client, *topic, operation, deadline).await;
            }
            Self::Identity(request) => {
                let reply = client
                    .request(
                        FrameKind::ManageIdentity,
                        FrameKind::IdentityResult,
                        TrafficClass::Control,
                        operation,
                        request,
                        deadline,
                    )
                    .await?;
                return crate::identity::receipt(request, reply, operation)
                    .map_err(|error| error.preserve_uncertainty(true));
            }
            Self::CreateTopic(name) => {
                let reply = client
                    .request(
                        FrameKind::CreateLogicalTopic,
                        FrameKind::TopicLayoutResult,
                        TrafficClass::Control,
                        operation,
                        &public::LogicalTopic { name: name.clone() },
                        deadline,
                    )
                    .await?;
                (name, reply)
            }
            Self::Layout { name, request } => {
                let reply = client
                    .request(
                        FrameKind::ChangeTopicLayout,
                        FrameKind::TopicLayoutResult,
                        TrafficClass::Control,
                        operation,
                        request,
                        deadline,
                    )
                    .await?;
                (name, reply)
            }
        };
        let expected_topic = match self {
            Self::CreateTopic(_) => crate::admin::bytes(operation),
            Self::Layout { request, .. } => request.topic_id.clone(),
            Self::Identity(_) | Self::DeleteTopic(_) => {
                return Err(Error::ResponseMismatch {
                    reason: "identity operation returned a topic layout".into(),
                }
                .preserve_uncertainty(true));
            }
        };
        if wire.topic_id != expected_topic {
            return Err(Error::ResponseMismatch {
                reason: "administration response changed its original topic identity".into(),
            }
            .preserve_uncertainty(true));
        }
        let wire = crate::topic::complete_layout(client, wire, deadline)
            .await
            .map_err(|error| error.preserve_uncertainty(true))?;
        TopicInfo::from_wire(name.clone(), wire)
            .map(AdminReceipt::Topic)
            .map_err(|error| error.preserve_uncertainty(true))
    }
}

async fn delete_topic(
    client: &Client,
    topic: Id,
    operation: Id,
    deadline: Instant,
) -> Result<AdminReceipt> {
    loop {
        let result: public::TopicRemoved = client
            .request(
                FrameKind::RemoveTopic,
                FrameKind::TopicRemoved,
                TrafficClass::Control,
                operation,
                &public::RemoveTopic {
                    topic_id: crate::admin::bytes(topic),
                },
                deadline,
            )
            .await?;
        if result.topic_id != crate::admin::bytes(topic)
            || Id::from_slice(&result.deletion_operation).is_err()
            || !(1..=256).contains(&result.total_domains)
            || result.sealed_domains > result.total_domains
            || result.complete && result.sealed_domains != result.total_domains
        {
            return Err(Error::ResponseMismatch {
                reason: "topic removal response changed its identity or bounded progress".into(),
            }
            .preserve_uncertainty(true));
        }
        if result.complete {
            return Ok(AdminReceipt::TopicDeleted(crate::TopicId(topic)));
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

type Completion = std::result::Result<AdminReceipt, Arc<Error>>;

#[derive(Default)]
struct State {
    result: Option<Completion>,
    complete: bool,
    running: bool,
    uncertain: bool,
    task: Option<JoinHandle<()>>,
}

pub(crate) struct Work {
    client: Weak<crate::client::Inner>,
    _memory: OwnedSemaphorePermit,
    operation: Id,
    request: Request,
    state: Mutex<State>,
    changed: Notify,
}

pub(crate) type Operations = Mutex<BTreeMap<Id, Arc<Work>>>;

#[derive(Clone)]
pub struct PendingAdmin {
    work: Arc<Work>,
}

impl std::fmt::Debug for PendingAdmin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingAdmin")
            .field("operation", &self.work.operation)
            .finish_non_exhaustive()
    }
}

impl PendingAdmin {
    pub async fn retry(&self) -> Result<AdminReceipt> {
        if let Some(result) = self.completed()? {
            return self.claim(result);
        }
        let inner = self.work.client.upgrade().ok_or(Error::Closed)?;
        let client = Client::background_from(inner);
        let client_ref = &client;
        client
            .operation(|deadline| async move {
                crate::request::retain(crate::request::Recovery::Administration(self.clone()));
                self.launch(client_ref, deadline)?;
                self.wait(deadline).await
            })
            .await
            .map_err(|source| {
                if matches!(source, Error::Administration { .. }) {
                    return source;
                }
                let unresolved = self
                    .work
                    .state
                    .lock()
                    .map(|state| !state.complete)
                    .unwrap_or(true);
                if unresolved {
                    Error::Administration {
                        pending: self.clone(),
                        source: Arc::new(source.preserve_uncertainty(true)),
                    }
                } else {
                    source
                }
            })
    }

    fn completed(&self) -> Result<Option<Completion>> {
        let state = self.work.state.lock().map_err(|_| Error::Closed)?;
        Ok(state.complete.then(|| state.result.clone()).flatten())
    }

    fn claim(&self, result: Completion) -> Result<AdminReceipt> {
        if let Some(inner) = self.work.client.upgrade() {
            inner
                .administrations
                .lock()
                .map_err(|_| Error::Closed)?
                .remove(&self.work.operation);
        }
        result.map_err(|source| Error::Background { source })
    }

    fn launch(&self, client: &Client, deadline: Instant) -> Result<()> {
        let mut state = self.work.state.lock().map_err(|_| Error::Closed)?;
        if state.complete || state.running {
            return Ok(());
        }
        state.result = None;
        state.running = true;
        let work = self.work.clone();
        let client = client.background();
        state.task = Some(tokio::spawn(crate::context::inherit_trace(async move {
            let result = timeout_at(
                deadline,
                work.request.execute(&client, work.operation, deadline),
            )
            .await
            .unwrap_or_else(|_| {
                Err(Error::RequestUnknown {
                    source: Box::new(Error::Deadline),
                })
            });
            if let Ok(mut state) = work.state.lock() {
                let result = result.map_err(|error| error.preserve_uncertainty(state.uncertain));
                state.complete = match &result {
                    Ok(_) => true,
                    Err(error) => !error.outcome_unknown(),
                };
                state.uncertain |= !state.complete;
                state.result = Some(result.map_err(Arc::new));
                state.running = false;
            }
            work.changed.notify_waiters();
        })));
        Ok(())
    }

    async fn wait(&self, deadline: Instant) -> Result<AdminReceipt> {
        loop {
            let notified = self.work.changed.notified();
            let result = {
                let state = self.work.state.lock().map_err(|_| Error::Closed)?;
                state.result.clone().map(|result| (result, state.complete))
            };
            if let Some((result, complete)) = result {
                if complete {
                    return self.claim(result);
                }
                return result.map_err(|source| Error::Administration {
                    pending: self.clone(),
                    source,
                });
            }
            timeout_at(deadline, notified)
                .await
                .map_err(|_| Error::Administration {
                    pending: self.clone(),
                    source: Arc::new(Error::RequestUnknown {
                        source: Box::new(Error::Deadline),
                    }),
                })?;
        }
    }
}

impl Admin {
    pub(crate) async fn execute(&self, request: Request) -> Result<AdminReceipt> {
        request.validate()?;
        self.client
            .operation(|deadline| async move {
                let pending = self.admit(request)?;
                crate::request::retain(crate::request::Recovery::Administration(pending.clone()));
                crate::request::mark_sent();
                pending.launch(&self.client, deadline)?;
                pending.wait(deadline).await
            })
            .await
    }

    fn admit(&self, request: Request) -> Result<PendingAdmin> {
        let memory = self
            .client
            .inner
            .resource_memory
            .clone()
            .try_acquire_many_owned(WORK_BYTES)
            .map_err(|_| Error::Admission {
                resource: "administration recovery memory",
            })?;
        let operation = Id::new_v7()?;
        let work = Arc::new(Work {
            client: Arc::downgrade(&self.client.inner),
            _memory: memory,
            operation,
            request,
            state: Mutex::new(State::default()),
            changed: Notify::new(),
        });
        let mut pending = self
            .client
            .inner
            .administrations
            .lock()
            .map_err(|_| Error::Closed)?;
        if self.client.is_closing() {
            return Err(Error::Closed);
        }
        if pending.len() >= MAX_PENDING {
            return Err(Error::Admission {
                resource: "pending administration operations",
            });
        }
        pending.insert(operation, work.clone());
        Ok(PendingAdmin { work })
    }

    pub fn pending_operations(&self) -> Result<Vec<PendingAdmin>> {
        Ok(self
            .client
            .inner
            .administrations
            .lock()
            .map_err(|_| Error::Closed)?
            .values()
            .cloned()
            .map(|work| PendingAdmin { work })
            .collect())
    }
}

impl Client {
    pub(crate) async fn close_administrations(
        &self,
        deadline: Instant,
    ) -> crate::close::CloseResult {
        let mut report = crate::CloseReport {
            local_cleanup_complete: true,
            ..Default::default()
        };
        let pending = self
            .admin()
            .pending_operations()
            .map_err(|source| crate::CloseError {
                report: report.clone(),
                source: Arc::new(source),
            })?;
        let mut failure = None;
        for pending in pending {
            let task = pending
                .work
                .state
                .lock()
                .map_err(|_| crate::CloseError {
                    report: report.clone(),
                    source: Arc::new(Error::Closed),
                })?
                .task
                .take();
            if let Some(mut task) = task {
                match timeout_at(deadline, &mut task).await {
                    Ok(Ok(())) => {}
                    Ok(Err(source)) => {
                        report.local_cleanup_complete = false;
                        failure.get_or_insert_with(|| Arc::new(Error::Task { source }));
                    }
                    Err(_) => {
                        task.abort();
                        let _ = task.await;
                        report.local_cleanup_complete = false;
                        failure.get_or_insert_with(|| Arc::new(Error::Deadline));
                    }
                }
            }
            let state = pending.work.state.lock().map_err(|_| crate::CloseError {
                report: report.clone(),
                source: Arc::new(Error::Closed),
            })?;
            if state.complete {
                report.unclaimed_results += 1;
            } else {
                report.unresolved_administrations += 1;
            }
        }
        match failure {
            Some(source) => Err(crate::CloseError { report, source }),
            None => Ok(report),
        }
    }
}
