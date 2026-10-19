use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use snafu::ResultExt;
use tracing_appender::non_blocking::{ErrorCounter, NonBlocking, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry, reload};

use crate::config::LoggingConfig;
use crate::error::{LogFilterSnafu, LogReloadSnafu, LoggingSnafu};
use crate::{Error, Result};

#[derive(Clone)]
pub struct LogHealth {
    queue_drops: ErrorCounter,
    pub oversized_events: Arc<AtomicU64>,
    pub sink_errors: Arc<AtomicU64>,
}

impl LogHealth {
    pub fn dropped_events(&self) -> u64 {
        self.queue_drops.dropped_lines() as u64
    }
}

pub struct Logging {
    _worker: WorkerGuard,
    pub health: LogHealth,
    pub control: LogControl,
}

#[derive(Clone)]
pub struct LogControl {
    handle: reload::Handle<EnvFilter, Registry>,
    state: Arc<Mutex<FilterState>>,
    baseline: String,
}

struct FilterState {
    generation: u64,
    expiry: Option<tokio::task::AbortHandle>,
}

impl LogControl {
    pub fn temporary_filter(&self, value: &str, ttl: Duration) -> Result<()> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| Error::Configuration {
            reason: "temporary logging requires an active Tokio runtime",
        })?;
        if value.len() > 1_024 || ttl.is_zero() || ttl > Duration::from_secs(300) {
            return Err(Error::Configuration {
                reason: "diagnostic log filter budget is invalid",
            });
        }
        let filter = parse_filter(value)?;
        let baseline = parse_filter(&self.baseline)?;
        let mut state = self.state.lock().map_err(|_| Error::Configuration {
            reason: "log filter control is poisoned",
        })?;
        let generation = state
            .generation
            .checked_add(1)
            .ok_or(Error::Configuration {
                reason: "log filter generation is exhausted",
            })?;
        self.handle.reload(filter).context(LogReloadSnafu)?;
        state.generation = generation;
        if let Some(expiry) = state.expiry.take() {
            expiry.abort();
        }
        let control = self.clone();
        let expiry = runtime.spawn(async move {
            tokio::time::sleep(ttl).await;
            if let Ok(state) = control.state.lock()
                && state.generation == generation
                && let Err(error) = control.handle.reload(baseline)
            {
                let error = anyhow::Error::new(error);
                tracing::warn!(event_code = "log_filter_restore_failed", error = %format_args!("{error:#}"));
            }
        });
        state.expiry = Some(expiry.abort_handle());
        drop(state);
        tracing::info!(
            event_code = "log_filter_changed",
            ttl_ms = ttl.as_millis() as u64
        );
        Ok(())
    }
}

fn parse_filter(value: &str) -> Result<EnvFilter> {
    EnvFilter::builder()
        .with_regex(false)
        .parse(value)
        .context(LogFilterSnafu)
}

pub fn initialize(config: &LoggingConfig) -> Result<Logging> {
    config.validate()?;
    let filter = parse_filter(&config.filter)?;
    let sink_errors = Arc::new(AtomicU64::new(0));
    let sink = CheckedWriter {
        inner: io::stdout(),
        errors: sink_errors.clone(),
    };
    let (writer, worker) = NonBlockingBuilder::default()
        .buffered_lines_limit(config.queue_events)
        .lossy(true)
        .finish(sink);
    let health = LogHealth {
        queue_drops: writer.error_counter(),
        oversized_events: Arc::new(AtomicU64::new(0)),
        sink_errors,
    };
    let bounded = BoundedWriter {
        inner: writer,
        limit: config.max_event_bytes,
        dropped: health.oversized_events.clone(),
    };
    let (layer, handle) = reload::Layer::new(filter);
    let registry = tracing_subscriber::registry().with(layer);
    if config.json {
        let subscriber =
            registry.with(tracing_subscriber::fmt::layer().json().with_writer(bounded));
        tracing::subscriber::set_global_default(subscriber).context(LoggingSnafu)?;
    } else {
        let subscriber = registry.with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(bounded),
        );
        tracing::subscriber::set_global_default(subscriber).context(LoggingSnafu)?;
    }
    Ok(Logging {
        _worker: worker,
        health,
        control: LogControl {
            handle,
            state: Arc::new(Mutex::new(FilterState {
                generation: 0,
                expiry: None,
            })),
            baseline: config.filter.clone(),
        },
    })
}

struct CheckedWriter<W> {
    inner: W,
    errors: Arc<AtomicU64>,
}

impl<W: Write> Write for CheckedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let result = self.inner.write(bytes);
        if result.is_err() {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn flush(&mut self) -> io::Result<()> {
        let result = self.inner.flush();
        if result.is_err() {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

#[derive(Clone)]
struct BoundedWriter {
    inner: NonBlocking,
    limit: usize,
    dropped: Arc<AtomicU64>,
}

impl<'a> MakeWriter<'a> for BoundedWriter {
    type Writer = EventWriter;
    fn make_writer(&'a self) -> Self::Writer {
        EventWriter {
            inner: self.inner.clone(),
            bytes: Vec::new(),
            limit: self.limit,
            discarded: false,
            dropped: self.dropped.clone(),
        }
    }
}

struct EventWriter {
    inner: NonBlocking,
    bytes: Vec<u8>,
    limit: usize,
    discarded: bool,
    dropped: Arc<AtomicU64>,
}

impl Write for EventWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.discarded {
            let fits = self
                .bytes
                .len()
                .checked_add(bytes.len())
                .is_some_and(|len| len <= self.limit);
            if !fits || self.bytes.try_reserve(bytes.len()).is_err() {
                self.discarded = true;
                self.bytes.clear();
            } else {
                self.bytes.extend_from_slice(bytes);
            }
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for EventWriter {
    fn drop(&mut self) {
        if self.discarded || self.inner.write_all(&self.bytes).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);
    impl Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn oversized_events_are_dropped_whole_without_corrupting_following_events() {
        let buffer = Buffer(Arc::new(Mutex::new(Vec::new())));
        let (writer, guard) = NonBlockingBuilder::default()
            .buffered_lines_limit(8)
            .finish(buffer.clone());
        let dropped = Arc::new(AtomicU64::new(0));
        let bounded = BoundedWriter {
            inner: writer,
            limit: 10,
            dropped: dropped.clone(),
        };
        {
            let mut event = bounded.make_writer();
            event.write_all(b"{\"large\":").unwrap();
            event.write_all(b"123456789}").unwrap();
        }
        bounded.make_writer().write_all(b"{}\n").unwrap();
        drop(guard);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        assert_eq!(*buffer.0.lock().unwrap(), b"{}\n");
    }

    #[test]
    fn sink_errors_are_independent_from_queue_drops() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::ErrorKind::StorageFull.into())
            }
            fn flush(&mut self) -> io::Result<()> {
                Err(io::ErrorKind::StorageFull.into())
            }
        }
        let count = Arc::new(AtomicU64::new(0));
        let mut writer = CheckedWriter {
            inner: Broken,
            errors: count.clone(),
        };
        assert!(writer.write_all(b"event").is_err());
        assert!(writer.flush().is_err());
        assert_eq!(count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn temporary_filter_expires_and_older_timer_cannot_restore_newer_filter() {
        let (layer, handle) = reload::Layer::new(parse_filter("warn").unwrap());
        let subscriber = tracing_subscriber::registry().with(layer);
        let _subscriber = subscriber;
        let control = LogControl {
            handle,
            state: Arc::new(Mutex::new(FilterState {
                generation: 0,
                expiry: None,
            })),
            baseline: "warn".to_owned(),
        };
        control
            .temporary_filter("info", Duration::from_millis(50))
            .unwrap();
        control
            .temporary_filter("debug", Duration::from_millis(200))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let current = control.handle.with_current(ToString::to_string).unwrap();
        assert_eq!(current, "debug");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if control.handle.with_current(ToString::to_string).unwrap() == "warn" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(control.temporary_filter("trace", Duration::ZERO).is_err());
        assert!(
            control
                .temporary_filter("trace", Duration::from_secs(301))
                .is_err()
        );
    }
}
