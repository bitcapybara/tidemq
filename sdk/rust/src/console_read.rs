use crate::{Client, Error, Result};
use bytes::Bytes;
use tidemq_protocol::{
    Id,
    common::{FrameKind, TrafficClass},
    console::{Query, Reply},
    public,
};

impl Client {
    pub async fn console_read(&self, query: Query) -> Result<Reply> {
        let json = serde_json::to_vec(&query).map_err(|_| Error::Configuration {
            reason: "console query encoding failed",
        })?;
        if json.len() > 12 * 1024 {
            return Err(Error::Configuration {
                reason: "console query exceeds its finite bound",
            });
        }
        let request = public::ConsoleRead {
            json: Bytes::from(json),
        };
        let operation = Id::new_v7()?;
        let result: public::ConsoleReadResult = self
            .operation(|deadline| {
                self.request(
                    FrameKind::ConsoleRead,
                    FrameKind::ConsoleReadResult,
                    TrafficClass::Bulk,
                    operation,
                    &request,
                    deadline,
                )
            })
            .await?;
        if result.json.len() > 256 * 1024 {
            return Err(mismatch());
        }
        let reply: Reply = serde_json::from_slice(&result.json).map_err(|_| mismatch())?;
        let matches = matches!(
            (&query, &reply),
            (Query::Topics { .. }, Reply::Topics(_))
                | (Query::Topic { .. }, Reply::Topic(_))
                | (Query::Shards { .. }, Reply::Shards(_))
                | (Query::Messages { .. }, Reply::Messages(_))
                | (Query::Subscriptions { .. }, Reply::Subscriptions(_))
                | (Query::Members { .. }, Reply::Members(_))
                | (Query::Progress { .. }, Reply::Progress(_))
                | (Query::Producers { .. }, Reply::Producers(_))
                | (Query::Cluster { .. }, Reply::Cluster(_))
        );
        if !matches {
            return Err(mismatch());
        }
        Ok(reply)
    }
}

fn mismatch() -> Error {
    Error::ResponseMismatch {
        reason: "console response exceeded its bound or changed its query kind".into(),
    }
}

#[cfg(test)]
mod tests {
    use tidemq_protocol::console::{Decimal, Query};

    #[test]
    fn console_json_preserves_u64_and_rejects_unknown_commands() {
        let value = Decimal(u64::MAX);
        let encoded = serde_json::to_string(&value).unwrap();
        assert_eq!(encoded, "\"18446744073709551615\"");
        assert_eq!(serde_json::from_str::<Decimal>(&encoded).unwrap(), value);
        assert!(serde_json::from_str::<Decimal>("9007199254740993").is_err());
        assert!(serde_json::from_str::<Decimal>("\"01\"").is_err());
        assert!(
            serde_json::from_str::<Query>(r#"{"kind":"execute","command":"arbitrary"}"#).is_err()
        );
        assert!(serde_json::from_str::<Query>(r#"{"kind":"topics","tenant":null,"namespace":null,"after":null,"limit":10,"command":"unexpected"}"#).is_err());
    }
}
