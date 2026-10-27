//! Stable, bounded Console writes and receipts. Secret message bytes never enter receipts.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    Id,
    console::{Decimal, Observation},
};

pub const RESULT_WINDOW_MS: u64 = 86_400_000;
pub const FIRST_SUBMISSION_MS: u64 = 300_000;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WriteRequest {
    pub operation_id: Id,
    pub command: WriteCommand,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum WriteCommand {
    CreateTopic {
        tenant: Id,
        namespace: Id,
        name: String,
    },
    DeleteTopic {
        topic: Id,
    },
    CreatePrincipal {
        name: String,
        tenant: Id,
        namespace: Option<Id>,
        permissions: Vec<String>,
    },
    SetPrincipalEnabled {
        principal: Id,
        enabled: bool,
    },
    IssueCredential {
        principal: Id,
        lifetime_ms: Decimal,
    },
    RevokeCredential {
        credential: Id,
    },
    ReplaceGrants {
        principal: Id,
        expected_revision: Decimal,
        grants: Vec<Grant>,
    },
    ReplaceScope {
        principal: Id,
        expected_revision: Decimal,
        scope: Scope,
        permissions: Vec<String>,
    },
    SetPermission {
        principal: Id,
        scope: Scope,
        permission: String,
        allow: bool,
    },
    Send {
        topic: Id,
        payload_base64: String,
        key_base64: Option<String>,
        properties_base64: BTreeMap<String, String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Scope {
    pub tenant: Id,
    pub namespace: Option<Id>,
    pub topic: Option<Id>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Grant {
    pub scope: Scope,
    pub permissions: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Succeeded,
    Rejected,
    Unknown,
    Expired,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    pub id: Id,
    pub actor: String,
    pub target: String,
    pub scope: String,
    pub status: Status,
    pub receipt: Option<String>,
    pub reason: Option<String>,
    pub resource_id: Option<Id>,
    pub secret_unavailable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
    #[serde(flatten)]
    pub observation: Observation,
}

impl Operation {
    pub fn forget_secret(&mut self) {
        if self.secret.take().is_some() {
            self.secret_unavailable = true;
        }
    }
}

/// The timestamp is an expiry fence, never an authorization credential.
pub fn created_at_ms(id: Id) -> Option<u64> {
    if id.as_bytes().get(6).copied()? >> 4 != 7 {
        return None;
    }
    let mut value = 0_u64;
    for byte in id.as_bytes().get(..6)? {
        value = value.checked_mul(256)?.checked_add(u64::from(*byte))?;
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn operation_expiry_requires_a_timestamp_and_receipts_cannot_retain_secrets() {
        assert!(created_at_ms(Id::new_v7().unwrap()).is_some());
        assert!(created_at_ms(Id::random().unwrap()).is_none());
        let request = WriteRequest {
            operation_id: Id::new_v7().unwrap(),
            command: WriteCommand::CreateTopic {
                tenant: Id::new_v7().unwrap(),
                namespace: Id::new_v7().unwrap(),
                name: "events".into(),
            },
        };
        let encoded = serde_json::to_vec(&request).unwrap();
        let decoded: WriteRequest = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.operation_id, request.operation_id);
    }
}
