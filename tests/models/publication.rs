use super::{Fault, Outcome, commit};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum Metadata {
    #[default]
    Absent,
    Registered,
    CancelRequested,
    Published,
    Aborted,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum Data {
    #[default]
    Absent,
    Authorized,
    Completed,
    Aborted,
}

#[derive(Clone, Copy, Debug)]
pub enum Command {
    Register,
    Authorize,
    Publish,
    Complete,
    RequestCancel,
    Abort,
    FinishCancel,
    CaptureSnapshot,
    ReleaseSnapshot,
    Collect,
}

impl Command {
    pub const ALL: [Self; 10] = [
        Self::Register,
        Self::Authorize,
        Self::Publish,
        Self::Complete,
        Self::RequestCancel,
        Self::Abort,
        Self::FinishCancel,
        Self::CaptureSnapshot,
        Self::ReleaseSnapshot,
        Self::Collect,
    ];
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Publication {
    pub metadata: Metadata,
    pub data: Data,
    pub object_exists: bool,
    pub snapshot_pin: bool,
    pub registration_proof: bool,
}

impl Default for Publication {
    fn default() -> Self {
        Self {
            metadata: Metadata::Absent,
            data: Data::Absent,
            object_exists: true,
            snapshot_pin: false,
            registration_proof: false,
        }
    }
}

impl Publication {
    pub fn apply(&mut self, command: Command, fault: Fault) -> Outcome {
        commit(self, fault, |state| state.transition(command))
    }

    fn transition(&mut self, command: Command) -> bool {
        match command {
            Command::Register if self.metadata == Metadata::Absent && self.object_exists => {
                self.metadata = Metadata::Registered;
                self.registration_proof = true;
            }
            Command::Authorize
                if self.registration_proof
                    && matches!(self.data, Data::Absent | Data::Authorized) =>
            {
                // A delayed registration proof remains possible after cancellation starts.
                self.data = Data::Authorized;
            }
            Command::Publish
                if self.metadata == Metadata::Registered && self.data == Data::Authorized =>
            {
                self.metadata = Metadata::Published;
            }
            Command::Complete
                if self.metadata == Metadata::Published
                    && matches!(self.data, Data::Authorized | Data::Completed) =>
            {
                self.data = Data::Completed;
            }
            Command::RequestCancel if self.metadata == Metadata::Registered => {
                self.metadata = Metadata::CancelRequested;
            }
            Command::Abort
                if self.metadata == Metadata::CancelRequested
                    && matches!(self.data, Data::Absent | Data::Authorized | Data::Aborted) =>
            {
                // CancelRequested is a durable conditional barrier against publication.
                self.data = Data::Aborted;
            }
            Command::FinishCancel
                if self.metadata == Metadata::CancelRequested && self.data == Data::Aborted =>
            {
                self.metadata = Metadata::Aborted;
            }
            Command::CaptureSnapshot if self.protected_by_live_metadata() => {
                self.snapshot_pin = true;
            }
            Command::ReleaseSnapshot if self.snapshot_pin => self.snapshot_pin = false,
            Command::Collect
                if self.metadata == Metadata::Aborted
                    && self.data == Data::Aborted
                    && !self.snapshot_pin =>
            {
                self.object_exists = false;
            }
            _ => return false,
        }
        true
    }

    fn protected_by_live_metadata(&self) -> bool {
        matches!(
            self.metadata,
            Metadata::Registered | Metadata::CancelRequested | Metadata::Published
        )
    }

    pub fn assert_safe(&self) {
        if self.metadata == Metadata::Published || self.data == Data::Completed {
            assert!(self.object_exists, "a published object must exist");
            assert!(matches!(self.data, Data::Authorized | Data::Completed));
        }
        if self.metadata == Metadata::Aborted {
            assert_eq!(self.data, Data::Aborted);
        }
        if self.protected_by_live_metadata() || self.snapshot_pin {
            assert!(
                self.object_exists,
                "live or snapshot references protect data"
            );
        }
        if !self.object_exists {
            assert_eq!(self.metadata, Metadata::Aborted);
            assert_eq!(self.data, Data::Aborted);
            assert!(!self.snapshot_pin);
        }
    }

    pub fn crash_and_restore(&self) -> Self {
        // This model codec is not the service's durable snapshot format.
        let snapshot = serde_json::to_vec(self).unwrap();
        serde_json::from_slice(&snapshot).unwrap()
    }
}
