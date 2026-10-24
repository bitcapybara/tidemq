use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    public,
    topic::InitialPosition,
};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

use crate::{Admin, Client, Error, Result, admin::bytes};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SubscriptionId(Id);

impl std::fmt::Display for SubscriptionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug)]
pub struct SubscriptionInfo {
    id: SubscriptionId,
    name: String,
    initial: InitialPosition,
    deleting: bool,
    deleted: bool,
}

impl SubscriptionInfo {
    pub fn id(&self) -> SubscriptionId {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn initial_position(&self) -> InitialPosition {
        self.initial
    }
    pub fn is_deleting(&self) -> bool {
        self.deleting
    }
    pub fn is_deleted(&self) -> bool {
        self.deleted
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionDeleted {
    subscription: SubscriptionId,
}

impl SubscriptionDeleted {
    pub fn subscription_id(&self) -> SubscriptionId {
        self.subscription
    }
}

type Completion = std::result::Result<SubscriptionDeleted, Arc<Error>>;

#[derive(Default)]
struct State {
    result: Option<Completion>,
    complete: bool,
    running: bool,
    uncertain: bool,
    task: Option<JoinHandle<()>>,
}

pub(crate) struct DeleteWork {
    client: Weak<crate::client::Inner>,
    _memory: OwnedSemaphorePermit,
    operation: Id,
    subscription: SubscriptionId,
    request: public::SubscriptionAdmin,
    state: Mutex<State>,
    changed: Notify,
}

pub(crate) type Deletions = Mutex<BTreeMap<Id, Arc<DeleteWork>>>;

#[derive(Clone)]
pub struct PendingSubscriptionDeletion {
    work: Arc<DeleteWork>,
}

impl std::fmt::Debug for PendingSubscriptionDeletion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingSubscriptionDeletion")
            .field("operation", &self.work.operation)
            .finish_non_exhaustive()
    }
}

impl PendingSubscriptionDeletion {
    pub fn subscription_id(&self) -> SubscriptionId {
        self.work.subscription
    }

    pub async fn retry(&self) -> Result<SubscriptionDeleted> {
        let known = {
            let state = self.work.state.lock().map_err(|_| Error::Closed)?;
            state.complete.then(|| state.result.clone()).flatten()
        };
        if let Some(result) = known {
            if let Some(inner) = self.work.client.upgrade() {
                inner
                    .deletions
                    .lock()
                    .map_err(|_| Error::Closed)?
                    .remove(&self.work.operation);
            }
            return result.map_err(|source| Error::Background { source });
        }
        let inner = self.work.client.upgrade().ok_or(Error::Closed)?;
        let client = Client::background_from(inner);
        let client_ref = &client;
        client
            .operation(|deadline| async move {
                crate::request::retain(crate::request::Recovery::SubscriptionDeletion(
                    self.clone(),
                ));
                self.launch(client_ref, deadline)?;
                self.wait(client_ref, deadline).await
            })
            .await
            .map_err(|source| self.unresolved(source))
    }

    fn unresolved(&self, source: Error) -> Error {
        if matches!(source, Error::SubscriptionDeletion { .. }) {
            return source;
        }
        let complete = self
            .work
            .state
            .lock()
            .map(|state| state.complete)
            .unwrap_or(false);
        Error::SubscriptionDeletion {
            pending: self.clone(),
            source: Arc::new(source.preserve_uncertainty(!complete)),
        }
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
            let (result, complete) = drive(&client, &work, deadline).await;
            if let Ok(mut state) = work.state.lock() {
                state.result = Some(result);
                state.complete = complete;
                state.uncertain |= !complete;
                state.running = false;
            }
            work.changed.notify_waiters();
        })));
        Ok(())
    }

    async fn wait(&self, client: &Client, deadline: Instant) -> Result<SubscriptionDeleted> {
        loop {
            let notified = self.work.changed.notified();
            let result = {
                let state = self.work.state.lock().map_err(|_| Error::Closed)?;
                state.result.clone().map(|result| (result, state.complete))
            };
            if let Some((result, complete)) = result {
                if complete {
                    client
                        .inner
                        .deletions
                        .lock()
                        .map_err(|_| Error::Closed)?
                        .remove(&self.work.operation);
                    return result.map_err(|source| Error::Background { source });
                }
                return result.map_err(|source| Error::SubscriptionDeletion {
                    pending: self.clone(),
                    source,
                });
            }
            timeout_at(deadline, notified)
                .await
                .map_err(|_| Error::Deadline)?;
        }
    }
}

async fn drive(client: &Client, work: &DeleteWork, deadline: Instant) -> (Completion, bool) {
    let mut accepted = work
        .state
        .lock()
        .map(|state| state.uncertain)
        .unwrap_or(true);
    loop {
        let result: Result<public::SubscriptionAdminResult> = client
            .request(
                FrameKind::SubscriptionAdmin,
                FrameKind::SubscriptionAdminResult,
                TrafficClass::Control,
                work.operation,
                &work.request,
                deadline,
            )
            .await;
        let reply = match result {
            Ok(reply) => reply,
            Err(error) => {
                let complete = !accepted && !error.outcome_unknown();
                return (
                    Err(Arc::new(error.preserve_uncertainty(accepted))),
                    complete,
                );
            }
        };
        if reply.topic_id != work.request.topic_id
            || reply.subscription_id != work.request.subscription_id
            || reply.model != work.request.model
            || reply.deletion_operation.is_empty()
            || reply.total_domains == 0
            || reply.total_domains > 256
            || reply.deleted_domains > reply.total_domains
            || Id::from_slice(&reply.deletion_operation).is_err()
        {
            return (
                Err(Arc::new(
                    Error::ResponseMismatch {
                        reason: "subscription deletion response differs from its original resource"
                            .into(),
                    }
                    .preserve_uncertainty(true),
                )),
                false,
            );
        }
        accepted = true;
        if reply.deletion_complete {
            if reply.deleted_domains != reply.total_domains {
                return (
                    Err(Arc::new(
                        Error::ResponseMismatch {
                            reason: "subscription deletion completed without every domain".into(),
                        }
                        .preserve_uncertainty(true),
                    )),
                    false,
                );
            }
            return (
                Ok(SubscriptionDeleted {
                    subscription: work.subscription,
                }),
                true,
            );
        }
        if timeout_at(deadline, tokio::time::sleep(Duration::from_millis(25)))
            .await
            .is_err()
        {
            return (
                Err(Arc::new(Error::Deadline.preserve_uncertainty(true))),
                false,
            );
        }
    }
}

impl Admin {
    pub async fn stream_subscription(&self, topic: &str, name: &str) -> Result<SubscriptionInfo> {
        self.inspect_subscription(topic, name, public::ConsumerModel::StreamConsumer)
            .await
            .map(|(_, info)| info)
    }
    pub async fn queue_subscription(&self, topic: &str, name: &str) -> Result<SubscriptionInfo> {
        self.inspect_subscription(topic, name, public::ConsumerModel::QueueConsumer)
            .await
            .map(|(_, info)| info)
    }
    pub async fn delete_stream_subscription(
        &self,
        topic: &str,
        name: &str,
    ) -> Result<SubscriptionDeleted> {
        self.delete_subscription_by_name(topic, name, public::ConsumerModel::StreamConsumer)
            .await
    }
    pub async fn delete_queue_subscription(
        &self,
        topic: &str,
        name: &str,
    ) -> Result<SubscriptionDeleted> {
        self.delete_subscription_by_name(topic, name, public::ConsumerModel::QueueConsumer)
            .await
    }

    async fn inspect_subscription(
        &self,
        topic: &str,
        name: &str,
        model: public::ConsumerModel,
    ) -> Result<(public::SubscriptionAdmin, SubscriptionInfo)> {
        crate::admin::validate_name(name)?;
        let topic = self.topic_info(topic).await?;
        let request = public::SubscriptionAdmin {
            topic_id: bytes(topic.id),
            name: name.into(),
            model: model.into(),
            action: public::SubscriptionAdminAction::InspectSubscription.into(),
            ..Default::default()
        };
        let request_ref = &request;
        let reply: public::SubscriptionAdminResult = self
            .client
            .operation(|deadline| async move {
                self.client
                    .request(
                        FrameKind::SubscriptionAdmin,
                        FrameKind::SubscriptionAdminResult,
                        TrafficClass::Control,
                        Id::new_v7()?,
                        request_ref,
                        deadline,
                    )
                    .await
            })
            .await?;
        if reply.topic_id != request.topic_id || reply.name != name || reply.model != request.model
        {
            return Err(Error::ResponseMismatch {
                reason: "subscription lookup returned another resource".into(),
            });
        }
        let id = Id::from_slice(&reply.subscription_id)?;
        let info = SubscriptionInfo {
            id: SubscriptionId(id),
            name: reply.name,
            initial: if reply.start_at_latest {
                InitialPosition::Latest
            } else {
                InitialPosition::EarliestAvailable
            },
            deleting: !reply.deletion_operation.is_empty() && !reply.deletion_complete,
            deleted: reply.deletion_complete,
        };
        Ok((
            public::SubscriptionAdmin {
                subscription_id: bytes(id),
                name: String::new(),
                action: public::SubscriptionAdminAction::RemoveSubscription.into(),
                ..request
            },
            info,
        ))
    }

    async fn delete_subscription_by_name(
        &self,
        topic: &str,
        name: &str,
        model: public::ConsumerModel,
    ) -> Result<SubscriptionDeleted> {
        let admitted = Mutex::new(None::<PendingSubscriptionDeletion>);
        let admitted_ref = &admitted;
        let result = self
            .client
            .operation(|deadline| async move {
                let (request, info) = self.inspect_subscription(topic, name, model).await?;
                let memory = timeout_at(
                    deadline,
                    self.client
                        .inner
                        .resource_memory
                        .clone()
                        .acquire_many_owned(4096),
                )
                .await
                .map_err(|_| Error::Deadline)?
                .map_err(|_| Error::Closed)?;
                let operation = Id::new_v7()?;
                let work = Arc::new(DeleteWork {
                    client: Arc::downgrade(&self.client.inner),
                    _memory: memory,
                    operation,
                    subscription: info.id,
                    request,
                    state: Mutex::new(State::default()),
                    changed: Notify::new(),
                });
                {
                    let mut ledger = self
                        .client
                        .inner
                        .deletions
                        .lock()
                        .map_err(|_| Error::Closed)?;
                    if self.client.is_closing() {
                        return Err(Error::Closed);
                    }
                    if ledger.len() >= 64 {
                        return Err(Error::Admission {
                            resource: "pending subscription deletions",
                        });
                    }
                    ledger.insert(operation, work.clone());
                }
                let pending = PendingSubscriptionDeletion { work };
                *admitted_ref.lock().map_err(|_| Error::Closed)? = Some(pending.clone());
                crate::request::retain(crate::request::Recovery::SubscriptionDeletion(
                    pending.clone(),
                ));
                pending.launch(&self.client, deadline)?;
                pending.wait(&self.client, deadline).await
            })
            .await;
        result.map_err(
            |source| match admitted.lock().ok().and_then(|pending| pending.clone()) {
                Some(pending) => pending.unresolved(source),
                None => source,
            },
        )
    }

    pub fn pending_subscription_deletions(&self) -> Result<Vec<PendingSubscriptionDeletion>> {
        Ok(self
            .client
            .inner
            .deletions
            .lock()
            .map_err(|_| Error::Closed)?
            .values()
            .cloned()
            .map(|work| PendingSubscriptionDeletion { work })
            .collect())
    }
}

impl Client {
    pub(crate) async fn close_subscription_deletions(
        &self,
        deadline: Instant,
    ) -> crate::close::CloseResult {
        let pending = self
            .admin()
            .pending_subscription_deletions()
            .map_err(|source| crate::CloseError {
                report: crate::CloseReport::default(),
                source: Arc::new(source),
            })?;
        let mut report = crate::CloseReport {
            local_cleanup_complete: true,
            ..Default::default()
        };
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
                    result => {
                        if result.is_err() {
                            task.abort();
                            let _ = task.await;
                        }
                        report.local_cleanup_complete = false;
                        failure.get_or_insert_with(|| Arc::new(Error::Closed));
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
                report.unresolved_deletions += 1;
            }
        }
        match failure {
            Some(source) => Err(crate::CloseError { report, source }),
            None => Ok(report),
        }
    }
}
