#![allow(dead_code)] // Shared fixtures are used by separate integration-test binaries.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use serde::{Deserialize, Serialize};
use tidemq::replication::{Application, DiskStore, StorageIdentity, StorageOptions, TypeConfig};
use tidemq_protocol::Id;

pub struct Directory(PathBuf);

impl Directory {
    pub async fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("tidemq-replication-{}", Id::random().unwrap()));
        tokio::fs::create_dir(&path).await.unwrap();
        Self(path)
    }
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct State {
    pub cluster: Option<Id>,
    pub group: u64,
    pub writer_epoch: u64,
    pub next_offset: u64,
    pub values: BTreeMap<u64, u64>,
    pub results: BTreeMap<u64, (u64, u64)>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Initialize {
        cluster: Id,
        group: u64,
    },
    Fence(u64),
    Append {
        epoch: u64,
        request: u64,
        value: u64,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum Response {
    #[default]
    Noop,
    Fenced,
    Appended {
        offset: u64,
    },
    Rejected,
}

impl Application for State {
    const NAME: &'static str = "replication-test-ledger";
    const VERSION: u32 = 1;
    type Command = Command;
    type Response = Response;

    fn apply(&mut self, command: Command) -> Response {
        match command {
            Command::Initialize { cluster, group } => {
                self.cluster = Some(cluster);
                self.group = group;
                Response::Noop
            }
            Command::Fence(epoch) if epoch >= self.writer_epoch => {
                self.writer_epoch = epoch;
                Response::Fenced
            }
            Command::Fence(_) => Response::Rejected,
            Command::Append {
                epoch,
                request,
                value,
            } => {
                if let Some((offset, original)) = self.results.get(&request) {
                    return if *original == value {
                        Response::Appended { offset: *offset }
                    } else {
                        Response::Rejected
                    };
                }
                if epoch != self.writer_epoch || self.values.len() >= 4096 {
                    return Response::Rejected;
                }
                let offset = self.next_offset;
                let Some(next) = offset.checked_add(1) else {
                    return Response::Rejected;
                };
                self.values.insert(offset, value);
                self.results.insert(request, (offset, value));
                self.next_offset = next;
                Response::Appended { offset }
            }
        }
    }

    fn validate(&self) -> std::io::Result<()> {
        if self.values.len() > 4096
            || self.results.len() > 4096
            || self.values.len() as u64 != self.next_offset
        {
            return Err(std::io::Error::other(
                "Test ledger violates its state budget or offset invariant",
            ));
        }
        Ok(())
    }
}

pub type Config = TypeConfig<State>;
pub type Raft = tidemq::replication::Raft<State>;

pub fn identity(cluster: Id, group: u64, node: u64) -> StorageIdentity {
    StorageIdentity::new::<State>(cluster, group, node)
}

pub fn log_id(term: u64, index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, 0), index)
}

pub fn entry(term: u64, index: u64, command: Command) -> Entry<Config> {
    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Normal(command),
    }
}

pub async fn store(directory: &Path, identity: StorageIdentity) -> DiskStore<State> {
    DiskStore::open(directory, identity, StorageOptions::default(), None)
        .await
        .unwrap()
}
