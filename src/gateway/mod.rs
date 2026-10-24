//! Authenticated resource access and finite QUIC forwarding.

use std::sync::Arc;

use tidemq_protocol::common::{ErrorCode, FrameKind, Outcome};

use crate::runtime::{Error, Result};

#[derive(Clone)]
pub(crate) struct Access {
    pub principal: Arc<str>,
    pub scope: Arc<str>,
    pub session: Option<tidemq_protocol::Id>,
}

impl Access {
    pub fn authorize_topic(&self, name: &str) -> Result<()> {
        if self.scope.as_ref() == "*" || self.scope.strip_prefix("topic:") == Some(name) {
            Ok(())
        } else {
            Err(forbidden())
        }
    }
}

fn forbidden() -> Error {
    Error::reject(ErrorCode::Forbidden, Outcome::Rejected, false)
}

pub(crate) fn external_kind(kind: FrameKind) -> bool {
    matches!(
        kind,
        FrameKind::CreateLogicalTopic
            | FrameKind::GetTopicLayout
            | FrameKind::ChangeTopicLayout
            | FrameKind::ManageIdentity
            | FrameKind::ConsumerControl
            | FrameKind::SubscriptionAdmin
            | FrameKind::PublisherControl
            | FrameKind::Publish
            | FrameKind::RetirePublishes
            | FrameKind::GroupRead
            | FrameKind::GroupAck
            | FrameKind::GroupRetireRead
            | FrameKind::GroupRetireAck
            | FrameKind::GetTopic
            | FrameKind::Cancel
            | FrameKind::Diagnose
    )
}
