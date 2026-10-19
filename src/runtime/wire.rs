use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use serde::{Serialize, de::DeserializeOwned};
use tidemq_protocol::common::{ErrorCode, ErrorReply, Frame, FrameKind, Outcome, TrafficClass};
use tidemq_protocol::framing::{decode_payload, encode_payload, frame_limit, new_frame};
use tidemq_protocol::internal::{InternalCommand, InternalResult};
use tidemq_protocol::{Id, ProtocolLimits};
use tokio::time::{Instant, timeout_at};

use crate::replication::rpc::PeerPool;
use crate::transport::rpc::{RpcClient, RpcResponse};

use super::{Error, Failure, Result};

pub(crate) const AUTHORITY: u32 = 1;
pub(crate) const OWNERSHIP: u32 = 2;
pub(crate) const FORWARD: u32 = 3;
pub(crate) const LEADER: u32 = 4;
pub(crate) const CREATE_SUBSCRIPTION: u32 = 5;

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Request {
    #[prost(uint32, tag = "1")]
    pub kind: u32,
    #[prost(bytes = "bytes", tag = "2")]
    pub body: Bytes,
    #[prost(string, tag = "3")]
    pub principal: String,
    #[prost(string, tag = "4")]
    pub scope: String,
    #[prost(uint32, tag = "5")]
    pub hops: u32,
}

impl Request {
    pub fn control<T: Serialize>(kind: u32, value: &T) -> Result<Self> {
        Ok(Self {
            kind,
            body: json(value)?,
            principal: String::new(),
            scope: String::new(),
            hops: 0,
        })
    }
}

pub(crate) fn json<T: Serialize>(value: &T) -> Result<Bytes> {
    let bytes = serde_json::to_vec(value)
        .map_err(|source| Error::operation("encode internal semantic value", source))?;
    if bytes.len() > 12 * 1024 {
        return Err(Error::invalid(
            "Internal control value exceeds its finite bound",
        ));
    }
    Ok(bytes.into())
}

pub(crate) fn decode_json<T: DeserializeOwned>(value: &[u8]) -> Result<T> {
    if value.len() > 12 * 1024 {
        return Err(Error::invalid(
            "Internal control value exceeds its finite bound",
        ));
    }
    serde_json::from_slice(value)
        .map_err(|source| Error::operation("decode internal semantic value", source))
}

#[derive(Clone)]
pub(crate) struct PeerRpc {
    pub pool: Arc<PeerPool>,
    pub limits: ProtocolLimits,
}

pub(crate) struct Pending {
    response: RpcResponse,
    rpc: RpcClient,
    request_id: Bytes,
    operation_id: Bytes,
    class: TrafficClass,
    limits: ProtocolLimits,
}

impl PeerRpc {
    // These independent identity, class, ordering, and deadline fields bind one wire attempt.
    #[allow(clippy::too_many_arguments)]
    pub async fn submit(
        &self,
        target: u64,
        group: u64,
        class: TrafficClass,
        operation: Id,
        request: Request,
        deadline: Instant,
        affinity: u64,
    ) -> Result<Pending> {
        let request_id = Id::random()
            .map_err(|source| Error::operation("allocate internal request identity", source))?;
        let payload = InternalCommand {
            group_id: group,
            operation_id: Bytes::copy_from_slice(operation.as_bytes()),
            encoded_command: encode_payload(&request, frame_limit(class, &self.limits))
                .map_err(|source| Error::operation("encode internal request", source))?,
        };
        drop(request);
        let operation_id = payload.operation_id.clone();
        let mut frame = new_frame(
            FrameKind::InternalCommand,
            class,
            request_id,
            operation,
            &payload,
            remaining(deadline)?,
            &self.limits,
        )
        .map_err(|source| Error::operation("frame internal request", source))?;
        frame.traceparent = crate::observability::tracing::current_traceparent();
        drop(payload);
        let rpc = self
            .pool
            .connection(target, class, deadline)
            .await
            .map_err(|source| Error::operation("connect internal peer", source))?;
        let response = if class == TrafficClass::Control {
            rpc.submit(frame, deadline).await
        } else {
            rpc.submit_affinity(frame, deadline, affinity).await
        }
        .map_err(|source| Error::operation("submit internal semantic request", source))?;
        Ok(Pending {
            response,
            rpc,
            request_id: Bytes::copy_from_slice(request_id.as_bytes()),
            operation_id,
            class,
            limits: self.limits.clone(),
        })
    }

    pub async fn call(
        &self,
        target: u64,
        group: u64,
        request: Request,
        deadline: Instant,
    ) -> Result<Bytes> {
        let operation = Id::random()
            .map_err(|source| Error::operation("allocate control operation identity", source))?;
        self.submit(
            target,
            group,
            TrafficClass::Control,
            operation,
            request,
            deadline,
            group,
        )
        .await?
        .receive()
        .await
    }
}

impl Pending {
    pub async fn receive(self) -> Result<Bytes> {
        let received = self
            .response
            .receive()
            .await
            .map_err(|source| Error::operation("receive internal semantic result", source))?;
        let frame = received.frame();
        if frame.request_id != self.request_id
            || frame.operation_id != self.operation_id
            || frame.traffic_class != i32::from(self.class)
        {
            self.rpc.close(Instant::now()).await;
            return Err(Error::invalid(
                "Internal response identity differs from its request",
            ));
        }
        if frame.kind == i32::from(FrameKind::Error) {
            return Err(decode_error(frame, &self.limits)?);
        }
        if frame.kind != i32::from(FrameKind::InternalResult) {
            return Err(Error::invalid("Internal response kind is invalid"));
        }
        let result: InternalResult = decode_payload(frame.payload.clone(), &self.limits)
            .map_err(|source| Error::operation("decode internal response envelope", source))?;
        if result.operation_id != self.operation_id {
            return Err(Error::invalid(
                "Internal result operation differs from its request",
            ));
        }
        Ok(result.encoded_result)
    }
}

pub(crate) fn remaining(deadline: Instant) -> Result<u64> {
    let millis = deadline
        .saturating_duration_since(Instant::now())
        .as_millis();
    let millis = u64::try_from(millis)
        .map_err(|_| Error::invalid("Request deadline exceeds the supported range"))?;
    if millis == 0 || millis > 300_000 {
        return Err(Error::reject(
            ErrorCode::DeadlineExceeded,
            Outcome::Rejected,
            true,
        ));
    }
    Ok(millis)
}

pub(crate) async fn within<T>(
    deadline: Instant,
    future: impl std::future::Future<Output = T>,
) -> Result<T> {
    timeout_at(deadline, future)
        .await
        .map_err(|_| Error::unknown())
}

pub(crate) fn response<M: Message>(
    request: &Frame,
    kind: FrameKind,
    value: &M,
    limits: &ProtocolLimits,
) -> Result<Frame> {
    let class = TrafficClass::try_from(request.traffic_class)
        .map_err(|_| Error::invalid("Response traffic class is invalid"))?;
    let mut reply = new_frame(
        kind,
        class,
        id(&request.request_id)?,
        id(&request.operation_id)?,
        value,
        request.remaining_timeout_ms,
        limits,
    )
    .map_err(|source| Error::operation("frame response", source))?;
    reply.traceparent = request.traceparent.clone();
    Ok(reply)
}

pub(crate) fn internal_response(
    request: &Frame,
    value: Bytes,
    limits: &ProtocolLimits,
) -> Result<Frame> {
    response(
        request,
        FrameKind::InternalResult,
        &InternalResult {
            operation_id: request.operation_id.clone(),
            encoded_result: value,
        },
        limits,
    )
}

pub(crate) fn error_response(
    request: &Frame,
    error: &Error,
    limits: &ProtocolLimits,
) -> Result<Frame> {
    let failure = error.failure();
    response(
        request,
        FrameKind::Error,
        &ErrorReply {
            code: failure.code,
            outcome: failure.outcome,
            retryable: failure.retryable,
            message:
                "Operation did not complete; use its typed error and original business identity"
                    .into(),
            expected_sequence: failure.expected_sequence,
            expected_registration_epoch: failure.expected_registration_epoch,
            retry_after_ms: None,
        },
        limits,
    )
}

fn decode_error(frame: &Frame, limits: &ProtocolLimits) -> Result<Error> {
    let error: ErrorReply = decode_payload(frame.payload.clone(), limits)
        .map_err(|source| Error::operation("decode internal failure", source))?;
    ErrorCode::try_from(error.code).map_err(|_| Error::invalid("Unknown internal error code"))?;
    Outcome::try_from(error.outcome).map_err(|_| Error::invalid("Unknown internal outcome"))?;
    Ok(Error::Rejected {
        failure: Failure {
            code: error.code,
            outcome: error.outcome,
            retryable: error.retryable,
            expected_sequence: error.expected_sequence,
            expected_registration_epoch: error.expected_registration_epoch,
        },
    })
}

pub(crate) fn id(value: &[u8]) -> Result<Id> {
    Id::from_slice(value).map_err(|source| Error::operation("validate business identity", source))
}
