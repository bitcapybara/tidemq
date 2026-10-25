use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use prost::Message;
use tidemq_protocol::common::{ErrorCode, Frame, FrameKind, Outcome};
use tidemq_protocol::framing::decode_payload;
use tidemq_protocol::{ProtocolLimits, public};
use tokio::time::Instant;

use crate::broker::DataRejection;
use crate::lifecycle::{GuardedPage, VerifiedColdDelivery};
use crate::metadata::PartitionKey;
use crate::ownership::{ControlLease, Work};
use crate::producer::MessageData;
use crate::subscription;

use super::service::{Service, forbidden, frame_kind};
use super::wire;
use super::{Error, Result};

#[cfg(test)]
#[path = "consumer_tests.rs"]
pub(super) mod consumer_tests;

fn payload<T: Message + Default>(frame: &Frame, limits: &ProtocolLimits) -> Result<T> {
    decode_payload(frame.payload.clone(), limits)
        .map_err(|source| Error::operation("decode public business request", source))
}

pub(crate) fn partition(frame: &Frame, limits: &ProtocolLimits) -> Result<PartitionKey> {
    macro_rules! from {
        ($ty:ty) => {{
            let request: $ty = payload(frame, limits)?;
            PartitionKey {
                topic_id: wire::id(&request.topic_id)?,
                partition: request.partition_id,
            }
        }};
    }
    Ok(match frame_kind(frame)? {
        FrameKind::Publish => from!(public::Publish),
        FrameKind::RetirePublishes => from!(public::RetirePublishes),
        FrameKind::GroupRead => from!(public::GroupRead),
        FrameKind::GroupAck => from!(public::GroupAck),
        FrameKind::GroupRetireRead | FrameKind::GroupRetireAck => from!(public::GroupRetire),
        _ => return Err(forbidden()),
    })
}

impl Service {
    pub async fn data_lease(
        &self,
        group: u64,
        partition: PartitionKey,
        deadline: Instant,
    ) -> Result<Arc<ControlLease>> {
        self.data_lease_for(group, partition, deadline, Duration::ZERO)
            .await
    }

    async fn data_lease_for(
        &self,
        group: u64,
        partition: PartitionKey,
        deadline: Instant,
        minimum_remaining: Duration,
    ) -> Result<Arc<ControlLease>> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        self.require_leader(replica)?;
        self.authoritative_group(group, deadline).await?;
        let fence = replica
            .state
            .try_with_application(|state| {
                state
                    .ownership()
                    .and_then(|state| state.fence(partition))
                    .cloned()
            })
            .await
            .map_err(|source| Error::operation("read authoritative data fence", source))?
            .ok_or_else(Error::unavailable)?;
        let slot = {
            let mut leases = self
                .leases
                .lock()
                .map_err(|_| Error::invalid("Lease cache is unavailable"))?;
            if leases.len() >= 2048 && !leases.contains_key(&partition) {
                leases.retain(|_, slot| {
                    Arc::strong_count(slot) > 1
                        || slot.try_lock().ok().is_none_or(|lease| {
                            lease
                                .as_ref()
                                .is_some_and(|lease| lease.expires_at() > std::time::Instant::now())
                        })
                });
            }
            if leases.len() >= 2048 && !leases.contains_key(&partition) {
                return Err(Error::reject(
                    ErrorCode::QuotaExceeded,
                    Outcome::Rejected,
                    true,
                ));
            }
            leases
                .entry(partition)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
                .clone()
        };
        let mut cached = wire::within(deadline, slot.lock()).await?;
        if let Some(lease) = cached.as_ref().filter(|lease| {
            lease.check(&fence, Work::ControlMutation).is_ok()
                && lease
                    .expires_at()
                    .saturating_duration_since(std::time::Instant::now())
                    >= minimum_remaining
        }) {
            return Ok(lease.clone());
        }
        let lease = Arc::new(
            self.authority
                .active_lease(&fence, deadline)
                .await
                .map_err(|source| Error::operation("refresh data control lease", source))?,
        );
        *cached = Some(lease.clone());
        if lease
            .expires_at()
            .saturating_duration_since(std::time::Instant::now())
            < minimum_remaining
        {
            return Err(Error::unavailable());
        }
        Ok(lease)
    }
}

pub(super) fn cold_message(
    page: &GuardedPage,
    offset: u64,
) -> Result<Option<(MessageData, VerifiedColdDelivery)>> {
    let records = page
        .records()
        .map_err(|source| Error::operation("validate cold read pin", source))?;
    let Some(record) = records.iter().find(|record| record.offset == offset) else {
        return Ok(None);
    };
    let proof = page
        .verified_delivery(offset)
        .map_err(|source| Error::operation("bind cold delivery proof", source))?;
    Ok(Some((
        MessageData {
            context: Bytes::copy_from_slice(record.context()),
            key: Bytes::copy_from_slice(record.key()),
            body: Bytes::copy_from_slice(record.body()),
        },
        proof,
    )))
}

pub(crate) fn unix_millis() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| Error::operation("read wall clock", source))?;
    u64::try_from(duration.as_millis()).map_err(|_| Error::invalid("Wall-clock timestamp overflow"))
}

pub(crate) fn data_rejection(reason: DataRejection) -> Error {
    match reason {
        DataRejection::Quota(reason) => {
            use crate::broker::QuotaRejection as R;
            let (code, retryable) = match reason {
                R::Exhausted | R::Occupied => (ErrorCode::QuotaExceeded, true),
                R::Missing | R::StaleGrant => (ErrorCode::Unavailable, true),
                R::InvalidGrant => (ErrorCode::InvalidRequest, false),
                R::Arithmetic => (ErrorCode::InternalError, false),
            };
            Error::reject_with(code, Outcome::Rejected, retryable, reason.to_string())
        }
        DataRejection::Transfer(reason) => {
            use crate::broker::TransferDataRejection as R;
            let (code, retryable) = match reason {
                R::Frozen => (ErrorCode::Fenced, true),
                R::Capacity => (ErrorCode::QuotaExceeded, true),
                R::NotReady => (ErrorCode::Unavailable, true),
                R::Conflict => (ErrorCode::ContentConflict, false),
                R::InvalidPlan | R::Encoding => (ErrorCode::InvalidRequest, false),
            };
            Error::reject_with(code, Outcome::Rejected, retryable, reason.to_string())
        }
        reason @ DataRejection::ShardSealed => Error::reject_with(
            ErrorCode::Fenced,
            Outcome::Rejected,
            false,
            reason.to_string(),
        ),
        DataRejection::Producer(reason) => producer_error(reason),
        DataRejection::Subscription(reason) => subscription_error(reason),
        reason @ (DataRejection::Retention(_) | DataRejection::Lifecycle(_)) => Error::reject_with(
            ErrorCode::Unavailable,
            Outcome::Rejected,
            true,
            reason.to_string(),
        ),
        reason @ DataRejection::Fence(_) => Error::reject_with(
            ErrorCode::Fenced,
            Outcome::Rejected,
            true,
            reason.to_string(),
        ),
        reason @ (DataRejection::PartitionMissing | DataRejection::Uninitialized) => {
            Error::reject_with(
                ErrorCode::Unavailable,
                Outcome::Rejected,
                true,
                reason.to_string(),
            )
        }
        reason @ DataRejection::PartitionCapacity => Error::reject_with(
            ErrorCode::QuotaExceeded,
            Outcome::Rejected,
            true,
            reason.to_string(),
        ),
        reason => Error::invalid(reason.to_string()),
    }
}
fn producer_error(reason: crate::producer::Rejection) -> Error {
    use crate::producer::Rejection as R;
    let code = match &reason {
        R::SequenceGap { .. } => ErrorCode::SequenceGap,
        R::RegistrationConflict { .. } | R::AlreadyOpen { .. } => ErrorCode::RegistrationConflict,
        R::SessionExpired | R::UnknownSession => ErrorCode::SessionExpired,
        R::ResultExpired { .. } => ErrorCode::DeduplicationExpired,
        R::OffsetExpired { .. } => ErrorCode::OffsetExpired,
        R::SessionCapacity | R::DataCapacity => ErrorCode::QuotaExceeded,
        R::SequenceConflict | R::SessionConflict | R::DigestMismatch => ErrorCode::ContentConflict,
        _ => ErrorCode::InvalidRequest,
    };
    let mut error = Error::reject_with(
        code,
        Outcome::Rejected,
        code == ErrorCode::QuotaExceeded,
        reason.to_string(),
    );
    if let Error::Rejected { failure } = &mut error {
        match reason {
            R::SequenceGap { expected } => failure.expected_sequence = Some(expected),
            R::RegistrationConflict { expected } => {
                failure.expected_registration_epoch = Some(expected)
            }
            _ => {}
        }
    }
    error
}
pub(super) fn subscription_error(reason: subscription::Rejection) -> Error {
    use subscription::Rejection as R;
    let code = match &reason {
        R::RecoveryPending => ErrorCode::Unavailable,
        R::NotFound | R::Deleted => ErrorCode::NotFound,
        R::AssignmentBusy => ErrorCode::AssignmentBusy,
        R::StaleGeneration | R::SessionMismatch => ErrorCode::StaleGeneration,
        R::StaleAttempt | R::InvalidToken | R::NotDelivered => ErrorCode::StaleDelivery,
        R::OutOfRetention => ErrorCode::OffsetExpired,
        R::OutcomeExpired => ErrorCode::DeduplicationExpired,
        R::IdentityConflict => ErrorCode::ContentConflict,
        R::WindowFull
        | R::InFlightMessages
        | R::InFlightBytes
        | R::IdentityBudget
        | R::AttemptLimit
        | R::RetryNotDue
        | R::CutoverPending => ErrorCode::QuotaExceeded,
        _ => ErrorCode::InvalidRequest,
    };
    Error::reject_with(
        code,
        Outcome::Rejected,
        matches!(code, ErrorCode::QuotaExceeded | ErrorCode::Unavailable),
        reason.to_string(),
    )
}
