use crate::replication::Application;

use super::{Command, MetadataState, Response};

impl Application for MetadataState {
    const NAME: &'static str = "tidemq.metadata";
    const VERSION: u32 = 7;
    type Command = Command;
    type Response = Response;

    fn apply(&mut self, command: Self::Command) -> Self::Response {
        MetadataState::apply(self, command)
    }

    fn validate(&self) -> std::io::Result<()> {
        self.validate_snapshot()
            .map_err(|reason| std::io::Error::other(format!("Invalid metadata snapshot: {reason}")))
    }
}
