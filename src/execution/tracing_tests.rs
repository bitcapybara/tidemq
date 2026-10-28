use std::sync::{Arc, Mutex};

use tracing::instrument::WithSubscriber;
use tracing::{Dispatch, Instrument, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

const REQUEST: &str = "local_durability_trace_request";
const CHILD: &str = "execution::tracing_tests::scoped_durability_trace_child";

struct Capture(Arc<Mutex<Vec<bool>>>);
impl<S> tracing_subscriber::Layer<S> for Capture
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_event(&self, event: &tracing::Event<'_>, context: Context<'_, S>) {
        let mut durable = Durable(false);
        event.record(&mut durable);
        if durable.0 {
            let linked = context
                .event_scope(event)
                .is_some_and(|mut spans| spans.any(|span| span.name() == REQUEST));
            self.0.lock().unwrap().push(linked);
        }
    }
}

struct Durable(bool);
impl tracing::field::Visit for Durable {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0 |= field.name() == "event" && value == "wal_record_durable";
    }
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scoped_durability_trace_child() {
    if std::env::var_os("TIDEMQ_SCOPED_TRACE_CHILD").is_none() {
        return;
    }
    let directory = std::env::temp_dir().join(format!(
        "tidemq-scoped-wal-trace-{}",
        tidemq_protocol::Id::new_v7().unwrap()
    ));
    let evidence = Arc::new(Mutex::new(Vec::new()));
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(Capture(evidence.clone())));
    let global = std::env::var("TIDEMQ_SCOPED_TRACE_CHILD").unwrap() == "global";
    if global {
        tracing::dispatcher::set_global_default(dispatch.clone()).unwrap();
    }
    let (journal, _, recovery) = crate::wal::SegmentedJournal::open(
        &directory,
        crate::wal::JournalOptions::default(),
        [13; 32],
    )
    .await
    .unwrap();
    drop(recovery);
    // A fresh process makes the first durable callsite registration happen without a subscriber.
    journal
        .append(vec![1])
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    let request = tracing::dispatcher::with_default(&dispatch, || {
        tracing::info_span!("local_durability_trace_request")
    });
    journal
        .append(vec![2])
        .instrument(request)
        .with_subscriber(dispatch)
        .await
        .unwrap()
        .durable()
        .await
        .unwrap();
    journal.close().await.unwrap();
    tokio::fs::remove_dir_all(&directory).await.unwrap();
    assert_eq!(
        *evidence.lock().unwrap(),
        if global {
            vec![false, true]
        } else {
            vec![true]
        },
        "An unscoped writer must not disable a later scoped durability event"
    );
}

#[test]
fn unscoped_wal_durability_cannot_disable_a_later_scoped_trace() {
    for subscriber in ["scoped", "global"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD, "--nocapture"])
            .env("TIDEMQ_SCOPED_TRACE_CHILD", subscriber)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "Fresh-process {subscriber} tracing regression failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
