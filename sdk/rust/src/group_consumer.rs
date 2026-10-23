mod ack;
mod driver;

use bytes::Bytes;
use std::{
    collections::{BTreeMap, VecDeque},
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
    topic::SubscriptionKind,
};
use tokio::{
    sync::{Notify, Semaphore},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{
    Client, Error, InitialPosition, ReceivedMessage, Result,
    admin::bytes,
    close::{CloseError, CloseReport, CloseResult},
};
pub use ack::{AckReceipt, PendingAck};

struct Builder {
    client: Client,
    topic: String,
    name: String,
    model: SubscriptionKind,
    initial: InitialPosition,
    messages: u32,
    bytes: u32,
}

pub struct StreamConsumerBuilder(Builder);
pub struct QueueConsumerBuilder(Builder);

macro_rules! builder {
    ($builder:ident, $consumer:ident) => {
        impl $builder {
            pub fn initial_position(mut self, initial: InitialPosition) -> Self {
                self.0.initial = initial;
                self
            }
            pub fn prefetch_messages(mut self, messages: u32) -> Self {
                self.0.messages = messages;
                self
            }
            pub fn prefetch_bytes(mut self, bytes: u32) -> Self {
                self.0.bytes = bytes;
                self
            }
            pub async fn build(self) -> Result<$consumer> {
                Ok($consumer {
                    owner: self.0.build().await?,
                })
            }
        }
    };
}
builder!(StreamConsumerBuilder, StreamConsumer);
builder!(QueueConsumerBuilder, QueueConsumer);

#[derive(Clone)]
pub struct StreamConsumer {
    owner: Arc<Owner>,
}
#[derive(Clone)]
pub struct QueueConsumer {
    owner: Arc<Owner>,
}

struct Owner {
    core: Arc<Core>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.core.stop.cancel();
    }
}

struct State {
    queue: VecDeque<ReceivedMessage>,
    ledger: BTreeMap<u64, ReceivedMessage>,
    next_delivery: u64,
    domains: BTreeMap<Id, driver::Domain>,
    read_after: Option<Id>,
    pending: BTreeMap<Id, PendingAck>,
    claims: BTreeMap<u64, std::sync::Weak<ack::Work>>,
    ack_sequences: BTreeMap<(Id, u64), u64>,
    error: Option<Arc<Error>>,
    closed: Option<CloseResult>,
}

struct Core {
    client: Client,
    topic: String,
    topic_id: Id,
    subscription: Id,
    model: SubscriptionKind,
    member: Id,
    epoch: AtomicU64,
    max_messages: u32,
    max_bytes: u32,
    info: Mutex<public::ConsumerControlResult>,
    state: Mutex<State>,
    changed: Notify,
    slots: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    stop: CancellationToken,
    ack_gate: tokio::sync::Mutex<()>,
}

impl Client {
    pub fn new_stream_consumer(
        &self,
        topic: impl Into<String>,
        subscription: impl Into<String>,
    ) -> StreamConsumerBuilder {
        StreamConsumerBuilder(Builder::new(
            self.clone(),
            topic.into(),
            subscription.into(),
            SubscriptionKind::Stream,
        ))
    }
    pub fn new_queue_consumer(
        &self,
        topic: impl Into<String>,
        subscription: impl Into<String>,
    ) -> QueueConsumerBuilder {
        QueueConsumerBuilder(Builder::new(
            self.clone(),
            topic.into(),
            subscription.into(),
            SubscriptionKind::Queue,
        ))
    }
}

impl Builder {
    fn new(client: Client, topic: String, name: String, model: SubscriptionKind) -> Self {
        Self {
            client,
            topic,
            name,
            model,
            initial: InitialPosition::EarliestAvailable,
            messages: 256,
            bytes: 8 * 1024 * 1024,
        }
    }

    async fn build(self) -> Result<Arc<Owner>> {
        if self.messages == 0
            || self.messages > 256
            || self.bytes < 4096
            || self.bytes > 8 * 1024 * 1024
            || self.name.is_empty()
            || self.name.len() > 128
        {
            return Err(Error::Configuration {
                reason: "consumer name or prefetch budget is invalid",
            });
        }
        let client = self.client.clone();
        client
            .operation(|deadline| self.build_until(deadline))
            .await
    }

    async fn build_until(self, deadline: Instant) -> Result<Arc<Owner>> {
        let info = self.client.admin().topic_info(&self.topic).await?;
        let mut request = public::ConsumerControl {
            topic_id: bytes(info.id),
            name: self.name,
            subscription_id: Bytes::new(),
            model: match self.model {
                SubscriptionKind::Stream => public::ConsumerModel::StreamConsumer,
                SubscriptionKind::Queue => public::ConsumerModel::QueueConsumer,
            }
            .into(),
            start_at_latest: self.initial == InitialPosition::Latest,
            action: public::ConsumerAction::EnsureConsumerSubscription.into(),
            member_id: Bytes::new(),
            member_epoch: 0,
            heartbeat_sequence: 0,
            max_messages: self.messages,
            max_bytes: u64::from(self.bytes),
        };
        let status = control(&self.client, Id::new_v7()?, &request, deadline).await?;
        request.subscription_id = status.subscription_id.clone();
        request.member_id = bytes(Id::new_v7()?);
        request.member_epoch = status.next_member_epoch;
        request.action = public::ConsumerAction::JoinConsumer.into();
        let member = Id::from_slice(&request.member_id)?;
        let core = Arc::new(Core {
            client: self.client,
            topic: self.topic,
            topic_id: info.id,
            subscription: Id::from_slice(&status.subscription_id)?,
            model: self.model,
            member,
            epoch: AtomicU64::new(request.member_epoch),
            max_messages: self.messages,
            max_bytes: self.bytes,
            info: Mutex::new(status),
            state: Mutex::new(State {
                queue: VecDeque::new(),
                ledger: BTreeMap::new(),
                next_delivery: 1,
                domains: BTreeMap::new(),
                read_after: None,
                pending: BTreeMap::new(),
                claims: BTreeMap::new(),
                ack_sequences: BTreeMap::new(),
                error: None,
                closed: None,
            }),
            changed: Notify::new(),
            slots: Arc::new(Semaphore::new(self.messages as usize)),
            memory: Arc::new(Semaphore::new(self.bytes as usize)),
            stop: CancellationToken::new(),
            ack_gate: tokio::sync::Mutex::new(()),
        });
        let owner = Arc::new(Owner { core: core.clone() });
        let operation = Id::new_v7()?;
        let (ready, result) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            match control(&core.client, operation, &request, deadline).await {
                Ok(status) => {
                    if let Ok(mut info) = core.info.lock() {
                        *info = status;
                    }
                    let _ = ready.send(Ok(()));
                    driver::run(core).await;
                }
                Err(error) => {
                    let error = Arc::new(error);
                    let _ = ready.send(Err(error.clone()));
                    driver::finish(core, Some(error)).await;
                }
            }
        });
        result
            .await
            .map_err(|_| Error::Closed)?
            .map_err(|source| Error::Background { source })?;
        Ok(owner)
    }
}

async fn control(
    client: &Client,
    operation: Id,
    request: &public::ConsumerControl,
    deadline: Instant,
) -> Result<public::ConsumerControlResult> {
    client
        .request(
            FrameKind::ConsumerControl,
            FrameKind::ConsumerControlResult,
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
    fn request(&self, action: public::ConsumerAction) -> public::ConsumerControl {
        public::ConsumerControl {
            topic_id: bytes(self.topic_id),
            subscription_id: bytes(self.subscription),
            name: String::new(),
            model: match self.model {
                SubscriptionKind::Stream => public::ConsumerModel::StreamConsumer,
                SubscriptionKind::Queue => public::ConsumerModel::QueueConsumer,
            }
            .into(),
            start_at_latest: false,
            action: action.into(),
            member_id: bytes(self.member),
            member_epoch: self.epoch.load(Ordering::Acquire),
            heartbeat_sequence: 0,
            max_messages: self.max_messages,
            max_bytes: u64::from(self.max_bytes),
        }
    }

    fn try_receive(&self) -> Result<Option<ReceivedMessage>> {
        if self.stop.is_cancelled() {
            return Err(Error::Closed);
        }
        let mut state = self.state()?;
        if let Some(source) = state.error.take() {
            return Err(Error::Background { source });
        }
        let Some(mut message) = state.queue.pop_front() else {
            return Ok(None);
        };
        if self.model == SubscriptionKind::Stream {
            let info = self.info.lock().map_err(|_| Error::Closed)?;
            if !info.lanes.iter().any(|lane| {
                message.data.hash >= lane.hash_start
                    && lane.hash_end.is_none_or(|end| message.data.hash < end)
            }) {
                drop(info);
                drop(state);
                return self.try_receive();
            }
        }
        let sequence = state.next_delivery;
        state.next_delivery = sequence.checked_add(1).ok_or(Error::SessionExpired)?;
        message.delivery_sequence = sequence;
        state.ledger.insert(sequence, message.clone());
        Ok(Some(message))
    }

    async fn receive(&self) -> Result<ReceivedMessage> {
        loop {
            let notified = self.changed.notified();
            if let Some(message) = self.try_receive()? {
                return Ok(message);
            }
            notified.await;
        }
    }

    async fn close(&self) -> CloseResult {
        self.stop.cancel();
        self.changed.notify_waiters();
        loop {
            let notified = self.changed.notified();
            match self.state() {
                Ok(state) => {
                    if let Some(result) = &state.closed {
                        return result.clone();
                    }
                }
                Err(source) => {
                    return Err(CloseError {
                        report: CloseReport::default(),
                        source: Arc::new(source),
                    });
                }
            }
            notified.await;
        }
    }

    fn record_error(&self, error: Error) {
        if let Ok(mut state) = self.state.lock() {
            state.error = Some(Arc::new(error));
        }
        self.changed.notify_waiters();
    }
}

macro_rules! consumer {
    ($ty:ident) => {
        impl $ty {
            pub async fn receive(&self) -> Result<ReceivedMessage> {
                self.owner.core.receive().await
            }
            pub fn try_receive(&self) -> Result<Option<ReceivedMessage>> {
                self.owner.core.try_receive()
            }
            pub async fn pending_acks(&self) -> Result<Vec<PendingAck>> {
                Ok(self.owner.core.state()?.pending.values().cloned().collect())
            }
            pub async fn close(&self) -> Result<CloseReport, CloseError> {
                self.owner.core.close().await
            }
        }
    };
}
consumer!(StreamConsumer);
consumer!(QueueConsumer);

impl StreamConsumer {
    pub async fn ack_cumulative(&self, message: &ReceivedMessage) -> Result<AckReceipt> {
        ack::start(&self.owner.core, message, None, true).await
    }
}
impl QueueConsumer {
    pub async fn ack(&self, message: &ReceivedMessage) -> Result<AckReceipt> {
        ack::start(&self.owner.core, message, None, false).await
    }
    pub async fn nack(&self, message: &ReceivedMessage, delay: Duration) -> Result<AckReceipt> {
        let delay = u64::try_from(delay.as_millis()).map_err(|_| Error::Configuration {
            reason: "redelivery delay is too large",
        })?;
        if delay > 300_000 {
            return Err(Error::Configuration {
                reason: "redelivery delay must be at most five minutes",
            });
        }
        ack::start(&self.owner.core, message, Some(delay), false).await
    }
}
