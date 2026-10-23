mod driver;
mod send;

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    public,
};
use tokio::{
    sync::{Notify, Semaphore},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{
    Client, Error, Message, MessageId, Result, TopicInfo,
    admin::bytes,
    close::{CloseError, CloseReport, CloseResult},
};
pub use send::PendingSend;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SendReceipt {
    pub(crate) id: MessageId,
}
impl SendReceipt {
    pub fn message_id(&self) -> MessageId {
        self.id
    }
}

#[derive(Debug)]
pub struct SendBatchResult {
    pub outcomes: Vec<Result<SendReceipt>>,
}

pub struct ProducerBuilder {
    client: Client,
    topic: String,
    sends: u32,
    bytes: u32,
}

impl Client {
    pub fn new_producer(&self, topic: impl Into<String>) -> ProducerBuilder {
        ProducerBuilder {
            client: self.clone(),
            topic: topic.into(),
            sends: 64,
            bytes: 8 * 1024 * 1024,
        }
    }
}

struct State {
    layout: TopicInfo,
    next_sequence: u64,
    pending: BTreeMap<u64, PendingSend>,
    touched: BTreeMap<Id, public::ShardRoute>,
    closed: Option<CloseResult>,
}

pub(crate) struct Core {
    client: Client,
    topic: String,
    topic_id: Id,
    producer: Id,
    registration: AtomicU64,
    state: Mutex<State>,
    slots: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    max_sends: u32,
    max_bytes: u32,
    lease_until: Mutex<Instant>,
    stop: CancellationToken,
    changed: Notify,
    close_deadline: Mutex<Option<Instant>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    resource_memory: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
}

struct Owner {
    core: Arc<Core>,
    _client: Client,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.core.stop.cancel();
    }
}

#[derive(Clone)]
pub struct TopicProducer {
    owner: Arc<Owner>,
}

impl ProducerBuilder {
    pub fn max_pending_sends(mut self, sends: u32) -> Self {
        self.sends = sends;
        self
    }
    pub fn max_pending_bytes(mut self, bytes: u32) -> Self {
        self.bytes = bytes;
        self
    }
    pub async fn build(self) -> Result<TopicProducer> {
        if self.sends == 0 || self.sends > 256 || self.bytes < 4096 || self.bytes > 8 * 1024 * 1024
        {
            return Err(Error::Configuration {
                reason: "producer pending budgets are invalid",
            });
        }
        let client = self.client.clone();
        client
            .operation(|deadline| self.build_until(deadline))
            .await
    }

    async fn build_until(self, deadline: Instant) -> Result<TopicProducer> {
        let resource_memory = self.client.reserve_resource(deadline).await?;
        let layout = self.client.admin().topic_info(&self.topic).await?;
        let producer = Id::new_v7()?;
        let mut request = public::PublisherControl {
            topic_id: bytes(layout.id),
            producer_id: bytes(producer),
            action: public::PublisherAction::PublisherStatus.into(),
            ..Default::default()
        };
        let status = control(&self.client, Id::new_v7()?, &request, deadline).await?;
        request.registration = status.next_registration;
        request.action = public::PublisherAction::OpenPublisher.into();
        let core = Arc::new(Core {
            client: self.client.background(),
            topic: self.topic,
            topic_id: layout.id,
            producer,
            registration: AtomicU64::new(request.registration),
            state: Mutex::new(State {
                layout,
                next_sequence: 1,
                pending: BTreeMap::new(),
                touched: BTreeMap::new(),
                closed: None,
            }),
            slots: Arc::new(Semaphore::new(self.sends as usize)),
            memory: Arc::new(Semaphore::new(self.bytes as usize)),
            max_sends: self.sends,
            max_bytes: self.bytes,
            lease_until: Mutex::new(Instant::now()),
            stop: CancellationToken::new(),
            changed: Notify::new(),
            close_deadline: Mutex::new(None),
            task: Mutex::new(None),
            resource_memory: Mutex::new(Some(resource_memory)),
        });
        self.client.register_producer(&core)?;
        let owner = Arc::new(Owner {
            core: core.clone(),
            _client: self.client,
        });
        let (ready, result) = tokio::sync::oneshot::channel();
        let worker = core.clone();
        let task = tokio::spawn(async move {
            driver::start(worker, request, deadline, ready).await;
        });
        *core.task.lock().map_err(|_| Error::Closed)? = Some(task);
        result
            .await
            .map_err(|_| Error::Closed)?
            .map_err(|source| Error::Background { source })?;
        Ok(TopicProducer { owner })
    }
}

async fn control(
    client: &Client,
    operation: Id,
    request: &public::PublisherControl,
    deadline: Instant,
) -> Result<public::PublisherControlResult> {
    client
        .request(
            FrameKind::PublisherControl,
            FrameKind::PublisherControlResult,
            TrafficClass::Control,
            operation,
            request,
            deadline,
        )
        .await
}

impl Core {
    fn state(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state.lock().map_err(|_| Error::Closed)
    }
    pub(crate) fn retains_work(&self) -> bool {
        self.state()
            .is_ok_and(|state| state.closed.is_none() || !state.pending.is_empty())
    }
    fn request(&self, action: public::PublisherAction) -> public::PublisherControl {
        public::PublisherControl {
            topic_id: bytes(self.topic_id),
            producer_id: bytes(self.producer),
            registration: self.registration.load(Ordering::Acquire),
            action: action.into(),
            heartbeat_sequence: 0,
        }
    }
    fn update_lease(
        &self,
        status: &public::PublisherControlResult,
        started: Instant,
    ) -> Result<()> {
        if !status.live
            || status.registration != self.registration.load(Ordering::Acquire)
            || status.remaining_lease_ms > 300_000
        {
            return Err(Error::SessionExpired);
        }
        *self.lease_until.lock().map_err(|_| Error::Closed)? = started
            .checked_add(Duration::from_millis(status.remaining_lease_ms))
            .ok_or(Error::Deadline)?;
        Ok(())
    }
    pub(crate) async fn close_until(&self, deadline: Instant) -> CloseResult {
        if let Ok(mut original) = self.close_deadline.lock()
            && original.is_none()
        {
            *original = Some(deadline);
        }
        self.stop.cancel();
        loop {
            let notified = self.changed.notified();
            let result = self.state().ok().and_then(|state| state.closed.clone());
            if let Some(result) = result {
                let task = self.task.lock().ok().and_then(|mut task| task.take());
                if let Some(mut task) = task {
                    match tokio::time::timeout_at(deadline, &mut task).await {
                        Ok(Ok(())) => {}
                        state => {
                            let mut report = match result {
                                Ok(report) => report,
                                Err(error) => error.report,
                            };
                            report.local_cleanup_complete = false;
                            let source = match state {
                                Ok(Err(source)) => Error::Task { source },
                                _ => {
                                    if let Ok(mut original) = self.task.lock() {
                                        *original = Some(task);
                                    }
                                    Error::Deadline
                                }
                            };
                            return Err(CloseError {
                                report,
                                source: Arc::new(source),
                            });
                        }
                    }
                }
                return result;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(CloseError {
                    report: CloseReport::default(),
                    source: Arc::new(Error::Deadline),
                });
            }
        }
    }
}

impl TopicProducer {
    pub async fn send(&self, message: Message) -> Result<SendReceipt> {
        send::one(&self.owner.core, message).await
    }
    pub async fn send_batch(&self, messages: Vec<Message>) -> Result<SendBatchResult> {
        send::batch(&self.owner.core, messages).await
    }
    pub async fn pending_sends(&self) -> Result<Vec<PendingSend>> {
        Ok(self.owner.core.state()?.pending.values().cloned().collect())
    }
    pub async fn close(&self) -> std::result::Result<CloseReport, CloseError> {
        self.owner
            .core
            .close_until(Instant::now() + self.owner.core.client.inner.config.close_timeout)
            .await
    }
}
