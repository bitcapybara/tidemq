use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tracing::Instrument;

use super::files::{Asset, StaticFiles};
use super::lifecycle::Metrics;
use crate::runtime::process::{Phase, Process};

const RECHECK_DEADLINE: Duration = Duration::from_secs(1);
const RECHECK_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone)]
pub(super) struct AssetHealth(Arc<Inner>);

struct Inner {
    files: StaticFiles,
    process: Process,
    metrics: Arc<Metrics>,
    pending: Vec<AtomicBool>,
    send: mpsc::Sender<Failure>,
}

pub(super) struct Failure {
    asset: Asset,
    detected: Instant,
    class: &'static str,
}

impl AssetHealth {
    pub(super) fn new(
        files: StaticFiles,
        process: Process,
        metrics: Arc<Metrics>,
    ) -> (Self, mpsc::Receiver<Failure>) {
        let count = files.required_count().max(1);
        let (send, receive) = mpsc::channel(count);
        (
            Self(Arc::new(Inner {
                files,
                process,
                metrics,
                send,
                pending: (0..count).map(|_| AtomicBool::new(false)).collect(),
            })),
            receive,
        )
    }

    pub(super) fn report(&self, name: &str, class: &'static str) {
        let Some(asset) = self.0.files.required(name) else {
            return;
        };
        if matches!(
            self.0.process.phase(),
            Phase::Failed | Phase::Draining | Phase::Stopped
        ) {
            return;
        }
        self.0.metrics.asset_errors.inc();
        let Some(pending) = self.0.pending.get(asset.id) else {
            return;
        };
        if pending.swap(true, Ordering::AcqRel) {
            return;
        }
        let failure = Failure {
            asset: asset.clone(),
            detected: Instant::now(),
            class,
        };
        if self.0.send.try_send(failure).is_err() {
            // One slot per trusted asset makes saturation an internal supervision failure.
            self.0.process.fail("console_assets");
        }
    }

    pub(super) fn observe(
        &self,
        body: Body,
        name: &str,
        remaining: Option<u64>,
        verify_content: bool,
    ) -> Body {
        let Some(asset) = self.0.files.required(name) else {
            return body;
        };
        let digest = (verify_content && remaining == Some(asset.length))
            .then(|| (blake3::Hasher::new(), asset.digest));
        Body::new(ObservedBody {
            inner: body,
            health: self.clone(),
            name: name.to_owned(),
            remaining,
            digest,
        })
    }

    pub(super) async fn run(
        &self,
        mut receive: mpsc::Receiver<Failure>,
        mut shutdown: watch::Receiver<Option<Instant>>,
    ) {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => return,
                failure = receive.recv() => {
                    let Some(failure) = failure else { return; };
                    let span = tracing::info_span!(target: "tidemq_trace", "console_asset_recheck", asset_id = failure.asset.id, class = failure.class);
                    tokio::select! {
                        biased;
                        _ = shutdown.changed() => return,
                        _ = self.recheck(failure).instrument(span) => {}
                    }
                }
            }
        }
    }

    async fn recheck(&self, failure: Failure) {
        if matches!(
            self.0.process.phase(),
            Phase::Failed | Phase::Draining | Phase::Stopped
        ) {
            return;
        }
        self.0.metrics.asset_rechecks.inc();
        let deadline = failure.detected + RECHECK_DEADLINE;
        let recovered = tokio::time::timeout_at(deadline, async {
            loop {
                tokio::time::sleep(RECHECK_INTERVAL).await;
                if self
                    .0
                    .files
                    .recheck(&failure.asset)
                    .await
                    .is_ok_and(|valid| valid)
                {
                    break;
                }
            }
        })
        .await
        .is_ok();
        if recovered {
            self.0.metrics.asset_recoveries.inc();
            if let Some(pending) = self.0.pending.get(failure.asset.id) {
                pending.store(false, Ordering::Release);
            }
        } else if !matches!(
            self.0.process.phase(),
            Phase::Failed | Phase::Draining | Phase::Stopped
        ) {
            self.0.metrics.asset_failures.inc();
            self.0.process.fail("console_assets");
        }
    }
}

struct ObservedBody {
    inner: Body,
    health: AssetHealth,
    name: String,
    remaining: Option<u64>,
    digest: Option<(blake3::Hasher, blake3::Hash)>,
}

impl ObservedBody {
    fn observe_data(&mut self, frame: &Frame<Bytes>) -> bool {
        let Some(bytes) = frame.data_ref() else {
            return true;
        };
        if let Some(remaining) = self.remaining.as_mut() {
            *remaining = remaining.saturating_sub(bytes.len() as u64);
        }
        if let Some((hasher, _)) = self.digest.as_mut() {
            hasher.update(bytes);
        }
        // Content-Length can finish the body without another poll for EOF.
        if self.remaining == Some(0) {
            return self
                .digest
                .take()
                .is_none_or(|(hasher, expected)| hasher.finalize() == expected);
        }
        true
    }
}

impl HttpBody for ObservedBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        match Pin::new(&mut self.inner).poll_frame(context) {
            Poll::Ready(Some(Err(error))) => {
                self.health.report(&self.name, "stream_read");
                Poll::Ready(Some(Err(Box::new(error))))
            }
            Poll::Ready(Some(Ok(frame))) => {
                if !self.observe_data(&frame) {
                    self.health.report(&self.name, "stream_integrity");
                    return Poll::Ready(Some(Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Console asset content differs from startup",
                    )))));
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(None) if self.remaining.is_some_and(|remaining| remaining > 0) => {
                self.remaining = Some(0);
                self.health.report(&self.name, "stream_truncated");
                Poll::Ready(Some(Err(Box::new(std::io::Error::from(
                    std::io::ErrorKind::UnexpectedEof,
                )))))
            }
            result => result.map(|frame| {
                frame.map(|frame| frame.map_err(|error| Box::new(error) as Self::Error))
            }),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.remaining.is_none_or(|remaining| remaining == 0) && self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
