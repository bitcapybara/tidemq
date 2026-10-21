use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use openraft::Vote;
use openraft::storage::RaftLogStorage;
use tracing::instrument::WithSubscriber;
use tracing::{Dispatch, Instrument, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

use super::{DiskStore, GATE, Ledger, StorageIdentity, StorageOptions, hold};

const REQUEST: &str = "cancelled_storage_request";

#[derive(Default)]
struct Evidence {
    request_alive: AtomicBool,
    durable: Mutex<Vec<bool>>,
}
struct Capture(Arc<Evidence>);
impl<S> tracing_subscriber::Layer<S> for Capture
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        _: &tracing::Id,
        _: Context<'_, S>,
    ) {
        if attributes.metadata().name() == REQUEST {
            self.0.request_alive.store(true, Ordering::Relaxed);
        }
    }

    fn on_close(&self, id: tracing::Id, context: Context<'_, S>) {
        if context.span(&id).is_some_and(|span| span.name() == REQUEST) {
            self.0.request_alive.store(false, Ordering::Relaxed);
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, context: Context<'_, S>) {
        let mut durable = DurableEvent(false);
        event.record(&mut durable);
        if durable.0 {
            let linked = context
                .event_scope(event)
                .is_some_and(|mut spans| spans.any(|span| span.name() == REQUEST));
            self.0.durable.lock().unwrap().push(linked);
        }
    }
}
struct DurableEvent(bool);
impl tracing::field::Visit for DurableEvent {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0 |= field.name() == "event" && value == "wal_record_durable";
    }
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_span_survives_caller_cancellation_and_reaches_wal_durability() {
    let path = std::env::temp_dir().join(format!(
        "tidemq-owned-tracing-{}",
        tidemq_protocol::Id::new_v7().unwrap()
    ));
    let identity = StorageIdentity::new::<Ledger>(tidemq_protocol::Id::new_v7().unwrap(), 1, 1);
    let storage = DiskStore::<Ledger>::open(&path, identity, StorageOptions::default(), None)
        .await
        .unwrap();
    let evidence = Arc::new(Evidence::default());
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(Capture(evidence.clone())));
    let request = tracing::dispatcher::with_default(&dispatch, || {
        tracing::info_span!("cancelled_storage_request")
    });
    let (reached, release) = hold(&path, "persist_started");
    let mut log = storage.log.clone();
    let caller = tokio::spawn(
        async move { log.save_vote(&Vote::new(2, 1)).await }
            .instrument(request)
            .with_subscriber(dispatch),
    );
    reached.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let retained_after_cancel = evidence.request_alive.load(Ordering::Relaxed);
    release.notify_one();
    let mut observer = storage.log.clone();
    assert_eq!(observer.read_vote().await.unwrap(), Some(Vote::new(2, 1)));
    storage.log.flush().await.unwrap();
    storage.log.close().await.unwrap();
    GATE.lock().unwrap().remove(&path);
    tokio::fs::remove_dir_all(&path).await.unwrap();
    assert!(
        retained_after_cancel,
        "The owned worker must retain the cancelled request span"
    );
    assert_eq!(
        *evidence.durable.lock().unwrap(),
        vec![true],
        "The durable event must retain the request scope and subscriber"
    );
}
