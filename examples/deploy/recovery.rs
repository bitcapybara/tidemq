use std::{fmt::Write, time::Duration};

use anyhow::{Context, Result, ensure};
use serde::Serialize;
use tidemq_protocol::{
    common::{ErrorCode, Outcome},
    producer::canonical_produce_digest,
};
use tidemq_sdk::{Error, Id, PendingBatch, ProduceReceipt, Producer, public};
use tokio::time::Instant;

use super::output::request_deadline;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PendingIdentity {
    pub operation_id: String,
    pub producer_epoch: u64,
    pub first_sequence: u64,
    pub digest: String,
}

impl PendingIdentity {
    fn read(batch: &PendingBatch) -> Result<Self> {
        Ok(Self {
            operation_id: batch.operation_id().to_string(),
            producer_epoch: batch.producer_epoch(),
            first_sequence: batch.first_sequence(),
            digest: hex(batch.digest())?,
        })
    }
}

#[derive(Clone, Serialize)]
pub struct ErrorObservation {
    pub kind: &'static str,
    pub source_kind: Option<&'static str>,
    pub outcome_unknown: bool,
    pub retry_allowed: bool,
    pub remote_code: Option<String>,
    pub remote_outcome: Option<String>,
}

impl ErrorObservation {
    pub fn read(error: &Error) -> Self {
        let inner = match error {
            Error::ProduceUnknown { source } | Error::RegistrationUnknown { source, .. } => {
                source.as_ref()
            }
            _ => error,
        };
        let (code, outcome) = match inner {
            Error::Remote { code, outcome, .. } => {
                (Some(format!("{code:?}")), Some(format!("{outcome:?}")))
            }
            _ => (None, None),
        };
        Self {
            kind: error_kind(error),
            source_kind: (!std::ptr::eq(error, inner)).then(|| error_kind(inner)),
            outcome_unknown: error.outcome_unknown(),
            retry_allowed: retry_allowed(error),
            remote_code: code,
            remote_outcome: outcome,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct BatchEvidence {
    pub producer_id: String,
    pub producer_epoch: u64,
    pub first_sequence: u64,
    pub message_count: u32,
    pub expected_digest: String,
    pub original_pending: Option<PendingIdentity>,
    pub initial_error: Option<ErrorObservation>,
    pub last_retry_error: Option<ErrorObservation>,
    pub retry_attempts: u32,
    pub receipt_seen: bool,
    pub duplicate_receipt: Option<bool>,
}

impl BatchEvidence {
    pub fn new(
        topic: Id,
        producer: &Producer,
        first: u64,
        messages: &[public::Message],
    ) -> Result<Self> {
        let digest =
            canonical_produce_digest(topic, 0, producer.id(), producer.epoch(), first, messages)?;
        Ok(Self {
            producer_id: producer.id().to_string(),
            producer_epoch: producer.epoch(),
            first_sequence: first,
            message_count: u32::try_from(messages.len())?,
            expected_digest: hex(&digest)?,
            original_pending: None,
            initial_error: None,
            last_retry_error: None,
            retry_attempts: 0,
            receipt_seen: false,
            duplicate_receipt: None,
        })
    }

    fn check_pending(&self, batch: &PendingBatch) -> Result<()> {
        let identity = PendingIdentity::read(batch)?;
        ensure!(
            identity.producer_epoch == self.producer_epoch
                && identity.first_sequence == self.first_sequence
                && identity.digest == self.expected_digest,
            "Pending batch differs from the original epoch, sequence or canonical digest"
        );
        if let Some(original) = &self.original_pending {
            ensure!(
                &identity == original,
                "Retry changed the original operation identity"
            );
        }
        Ok(())
    }

    fn receipt(&mut self, receipt: ProduceReceipt) -> Result<ProduceReceipt> {
        ensure!(
            receipt.first_offset == self.first_sequence
                && receipt.message_count == self.message_count,
            "Receipt differs from the original batch range"
        );
        self.receipt_seen = true;
        self.duplicate_receipt = Some(receipt.duplicate);
        Ok(receipt)
    }
}

pub struct Batch {
    pending: Option<PendingBatch>,
    receipt: Option<ProduceReceipt>,
}

impl Batch {
    pub async fn begin(
        producer: &Producer,
        messages: Vec<public::Message>,
        evidence: &mut BatchEvidence,
        deadline: Instant,
    ) -> Result<Self> {
        match tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
            .scope(producer.produce(messages))
            .await
        {
            Ok(receipt) => Ok(Self {
                pending: None,
                receipt: Some(evidence.receipt(receipt)?),
            }),
            Err(error) => {
                evidence.initial_error = Some(ErrorObservation::read(&error));
                if !retry_allowed(&error) {
                    return Err(anyhow::Error::new(error))
                        .context("Produce returned a terminal error");
                }
                let pending = producer.pending_batches().await?;
                ensure!(
                    pending.len() == 1,
                    "Recoverable produce has no unique original PendingBatch; replacement production is forbidden"
                );
                let batch = pending
                    .first()
                    .context("Original PendingBatch is unavailable")?
                    .clone();
                evidence.check_pending(&batch)?;
                evidence.original_pending = Some(PendingIdentity::read(&batch)?);
                Ok(Self {
                    pending: Some(batch),
                    receipt: None,
                })
            }
        }
    }

    pub fn unresolved(&self) -> bool {
        self.pending.is_some() && self.receipt.is_none()
    }

    pub async fn finish(
        &mut self,
        producer: &Producer,
        evidence: &mut BatchEvidence,
        deadline: Instant,
    ) -> Result<ProduceReceipt> {
        if let Some(receipt) = self.receipt {
            return Ok(receipt);
        }
        let batch = self
            .pending
            .as_ref()
            .context("Original PendingBatch is missing")?;
        let until = deadline.min(
            Instant::now()
                .checked_add(Duration::from_secs(60))
                .context("Retry budget overflow")?,
        );
        for attempt in 0..8 {
            verify_retained(producer, evidence, batch).await?;
            evidence.retry_attempts = evidence
                .retry_attempts
                .checked_add(1)
                .context("Retry counter overflow")?;
            match tidemq_sdk::RequestContext::with_deadline(request_deadline(until)?)
                .scope(producer.retry(batch))
                .await
            {
                Ok(receipt) => {
                    evidence.check_pending(batch)?;
                    self.receipt = Some(evidence.receipt(receipt)?);
                    ensure!(
                        producer.pending_batches().await?.is_empty(),
                        "Successful retry left a pending batch"
                    );
                    return Ok(receipt);
                }
                Err(error) => {
                    evidence.last_retry_error = Some(ErrorObservation::read(&error));
                    if !retry_allowed(&error) {
                        return Err(anyhow::Error::new(error))
                            .context("Original batch retry returned a terminal error");
                    }
                    verify_retained(producer, evidence, batch).await?;
                    if attempt == 7 || Instant::now() >= until {
                        return Err(anyhow::Error::new(error))
                            .context("Original batch retry budget exhausted");
                    }
                    let next = Instant::now()
                        .checked_add(Duration::from_millis(200))
                        .context("Retry delay overflow")?;
                    tokio::time::sleep_until(next.min(until)).await;
                }
            }
        }
        anyhow::bail!("Original batch retry budget exhausted")
    }
}

async fn verify_retained(
    producer: &Producer,
    evidence: &BatchEvidence,
    original: &PendingBatch,
) -> Result<()> {
    ensure!(
        producer.id().to_string() == evidence.producer_id
            && producer.epoch() == evidence.producer_epoch,
        "Producer identity changed during recovery"
    );
    evidence.check_pending(original)?;
    let batches = producer.pending_batches().await?;
    ensure!(
        batches.len() == 1,
        "The original unresolved batch must remain the only pending batch"
    );
    evidence.check_pending(
        batches
            .first()
            .context("Original pending batch was discarded")?,
    )
}

pub fn retry_allowed(error: &Error) -> bool {
    error.outcome_unknown()
        || matches!(
            error,
            Error::Remote {
                code: ErrorCode::Unavailable | ErrorCode::Fenced,
                outcome: Outcome::Rejected,
                retryable: true,
                ..
            }
        )
}

fn error_kind(error: &Error) -> &'static str {
    match error {
        Error::Configuration { .. } => "configuration",
        Error::Protocol { .. } => "protocol",
        Error::Transport { .. } => "transport",
        Error::Rpc { .. } => "rpc",
        Error::Resolve { .. } => "resolve",
        Error::Deadline => "deadline",
        Error::Cancelled => "cancelled",
        Error::RequestUnknown { .. } => "request_unknown",
        Error::Admission { .. } => "admission",
        Error::Memory { .. } => "memory_configuration",
        Error::Remote { .. } => "remote",
        Error::ResponseMismatch { .. } => "response_mismatch",
        Error::ProducerBlocked => "producer_blocked",
        Error::SessionExpired => "session_expired",
        Error::RegistrationUnknown { .. } => "registration_unknown",
        Error::ProduceUnknown { .. } => "produce_unknown",
        Error::StaleDelivery => "stale_delivery",
        Error::KeyBlocked => "key_blocked",
        Error::Closed => "closed",
    }
}

fn hex(bytes: &[u8]) -> Result<String> {
    let mut text = String::new();
    text.try_reserve_exact(
        bytes
            .len()
            .checked_mul(2)
            .context("Digest length overflow")?,
    )?;
    for byte in bytes {
        write!(&mut text, "{byte:02x}")?;
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rejected(code: ErrorCode, retryable: bool) -> Error {
        Error::Remote {
            code,
            message: "test service rejection".into(),
            outcome: Outcome::Rejected,
            retryable,
            expected_sequence: None,
            expected_registration_epoch: None,
            retry_after_ms: None,
        }
    }

    #[test]
    fn retries_require_typed_unknown_or_explicit_transient_rejection() {
        assert!(retry_allowed(&rejected(ErrorCode::Unavailable, true)));
        assert!(retry_allowed(&rejected(ErrorCode::Fenced, true)));
        assert!(!retry_allowed(&rejected(ErrorCode::Unavailable, false)));
        assert!(!retry_allowed(&Error::SessionExpired));
        assert!(!retry_allowed(&Error::ResponseMismatch {
            reason: "test response binding differs".into()
        }));
        assert!(!retry_allowed(&Error::Deadline));
        let unknown = Error::ProduceUnknown {
            source: Box::new(Error::Closed),
        };
        assert!(retry_allowed(&unknown));
        let observation = ErrorObservation::read(&unknown);
        assert_eq!(observation.kind, "produce_unknown");
        assert_eq!(observation.source_kind, Some("closed"));
        assert_eq!(hex(&[0, 1, 15, 255]).unwrap(), "00010fff");
    }
}
