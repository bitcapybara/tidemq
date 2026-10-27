use crate::{Client, Error, Result};
use bytes::Bytes;
use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    console_operation::{Operation, WriteRequest},
    public,
};

impl Client {
    /// The caller allocates the identity before submission and retains it across clients.
    pub async fn console_write(&self, request: &WriteRequest) -> Result<Operation> {
        let json = serde_json::to_vec(request).map_err(|_| Error::Configuration {
            reason: "console operation cannot be encoded",
        })?;
        if json.len() > 1_500_000 {
            return Err(Error::Configuration {
                reason: "console operation exceeds its finite byte limit",
            });
        }
        let request_wire = public::ConsoleWrite {
            json: Bytes::from(json),
        };
        let reply: public::ConsoleOperationResult = self
            .operation(|deadline| {
                self.request(
                    FrameKind::ConsoleWrite,
                    FrameKind::ConsoleOperationResult,
                    TrafficClass::Bulk,
                    request.operation_id,
                    &request_wire,
                    deadline,
                )
            })
            .await?;
        parse(reply, request.operation_id)
    }

    pub async fn console_operation(&self, operation: Id) -> Result<Operation> {
        let request = public::GetConsoleOperation {
            operation_id: Bytes::copy_from_slice(operation.as_bytes()),
        };
        let reply: public::ConsoleOperationResult = self
            .operation(|deadline| {
                self.request(
                    FrameKind::GetConsoleOperation,
                    FrameKind::ConsoleOperationResult,
                    TrafficClass::Bulk,
                    operation,
                    &request,
                    deadline,
                )
            })
            .await?;
        let result = parse(reply, operation)?;
        if result.secret.is_some() {
            return Err(mismatch());
        }
        Ok(result)
    }
}

fn parse(reply: public::ConsoleOperationResult, operation: Id) -> Result<Operation> {
    if reply.json.len() > 16_384 {
        return Err(mismatch());
    }
    let result: Operation = serde_json::from_slice(&reply.json).map_err(|_| mismatch())?;
    if result.id != operation {
        return Err(mismatch());
    }
    Ok(result)
}
fn mismatch() -> Error {
    Error::ResponseMismatch {
        reason: "console receipt changed its identity or bounds".into(),
    }
}
