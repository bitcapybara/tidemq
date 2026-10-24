use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde::Serialize;
use tidemq_sdk::{Error, ErrorKind, Message, PendingSend, Producer, SendReceipt};
use tokio::time::Instant;

use super::output::request_deadline;

#[derive(Clone, Serialize)]
pub struct ErrorObservation {
    pub kind: String,
    pub outcome_unknown: bool,
}
impl ErrorObservation {
    pub fn read(error: &Error) -> Self {
        Self {
            kind: format!("{:?}", error.kind()),
            outcome_unknown: error.outcome_unknown(),
        }
    }
}

#[derive(Clone, Serialize)]
pub struct BatchEvidence {
    pub first_sequence: u64,
    pub message_count: usize,
    pub original_pending: usize,
    pub initial_error: Option<ErrorObservation>,
    pub last_retry_error: Option<ErrorObservation>,
    pub retry_attempts: u32,
    pub receipt_seen: bool,
    pub message_ids: Vec<String>,
}
impl BatchEvidence {
    pub fn new(first: u64, count: usize) -> Self {
        Self {
            first_sequence: first,
            message_count: count,
            original_pending: 0,
            initial_error: None,
            last_retry_error: None,
            retry_attempts: 0,
            receipt_seen: false,
            message_ids: Vec::new(),
        }
    }
}

pub struct Batch {
    pending: Vec<(usize, PendingSend)>,
    receipts: Vec<Option<SendReceipt>>,
}
impl Batch {
    pub async fn begin(
        producer: &Producer,
        messages: Vec<Message>,
        evidence: &mut BatchEvidence,
        deadline: Instant,
    ) -> Result<Self> {
        let count = messages.len();
        let mut batch = Self {
            pending: Vec::new(),
            receipts: vec![None; count],
        };
        match tidemq_sdk::RequestContext::with_deadline(request_deadline(deadline)?)
            .scope(producer.send_batch(messages))
            .await
        {
            Ok(result) => {
                ensure!(
                    result.outcomes.len() == count,
                    "Batch omitted an original send outcome"
                );
                for (index, result) in result.outcomes.into_iter().enumerate() {
                    match result {
                        Ok(receipt) => {
                            *batch
                                .receipts
                                .get_mut(index)
                                .context("Receipt outside batch")? = Some(receipt)
                        }
                        Err(Error::SendUnresolved { pending, source }) => {
                            evidence
                                .initial_error
                                .get_or_insert_with(|| ErrorObservation::read(&source));
                            batch.pending.push((index, pending));
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            Err(Error::BatchSendInterrupted { pending, source }) => {
                ensure!(
                    pending.len() == count,
                    "Interrupted batch was not entirely admitted"
                );
                evidence.initial_error = Some(ErrorObservation::read(&source));
                batch.pending = pending.into_iter().enumerate().collect();
            }
            Err(error) => return Err(error.into()),
        }
        evidence.original_pending = batch.pending.len();
        Ok(batch)
    }

    pub fn unresolved(&self) -> bool {
        !self.pending.is_empty()
    }

    pub async fn finish(
        &mut self,
        evidence: &mut BatchEvidence,
        deadline: Instant,
    ) -> Result<Vec<SendReceipt>> {
        let until = deadline.min(
            Instant::now()
                .checked_add(Duration::from_secs(60))
                .context("Retry budget overflow")?,
        );
        for (index, pending) in &self.pending {
            let mut complete = false;
            for attempt in 0..8 {
                evidence.retry_attempts = evidence
                    .retry_attempts
                    .checked_add(1)
                    .context("Retry count overflow")?;
                match tidemq_sdk::RequestContext::with_deadline(request_deadline(until)?)
                    .scope(pending.retry())
                    .await
                {
                    Ok(receipt) => {
                        *self
                            .receipts
                            .get_mut(*index)
                            .context("Recovered receipt outside batch")? = Some(receipt);
                        complete = true;
                        break;
                    }
                    Err(error) => {
                        evidence.last_retry_error = Some(ErrorObservation::read(&error));
                        if !retry_allowed(&error) || attempt == 7 || Instant::now() >= until {
                            return Err(anyhow::Error::new(error))
                                .context("Original send recovery failed");
                        }
                        tokio::time::sleep_until(
                            until.min(Instant::now() + Duration::from_millis(200)),
                        )
                        .await;
                    }
                }
            }
            ensure!(complete, "Original send remained unresolved");
        }
        let receipts = self
            .receipts
            .iter()
            .map(|receipt| receipt.context("Missing original receipt"))
            .collect::<Result<Vec<_>>>()?;
        evidence.receipt_seen = true;
        evidence.message_ids = receipts
            .iter()
            .map(|receipt| receipt.message_id().to_string())
            .collect();
        self.pending.clear();
        Ok(receipts)
    }
}

fn retry_allowed(error: &Error) -> bool {
    error.outcome_unknown()
        || matches!(
            error.kind(),
            ErrorKind::Unavailable | ErrorKind::ResourceExhausted
        )
}
