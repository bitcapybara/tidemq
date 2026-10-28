use std::sync::Arc;

use openraft::RaftSnapshotBuilder;
use tokio::time::{Instant, timeout_at};

use crate::replication::{DiskLog, DiskStateMachine, Raft};

use super::local::{Shared, position};
use super::{
    Application, Applied, Error, ExecutionPosition, LocalGroup, Result, SnapshotInfo,
    StorageIdentity,
};

#[derive(Clone)]
pub enum GroupHandle<A: Application> {
    Local(LocalGroup<A>),
    Raft {
        raft: Raft<A>,
        state: DiskStateMachine<A>,
        log: DiskLog<A>,
    },
}

#[derive(Clone)]
enum State<A: Application> {
    Local(Arc<Shared<A>>),
    Raft(DiskStateMachine<A>),
}

#[derive(Clone)]
pub struct GroupState<A: Application>(State<A>);

impl From<openraft::LogId<u64>> for ExecutionPosition {
    fn from(position: openraft::LogId<u64>) -> Self {
        Self::Raft {
            term: position.leader_id.term,
            leader_node: position.leader_id.node_id,
            index: position.index,
        }
    }
}

impl<A: Application> GroupHandle<A> {
    pub fn from_raft(raft: Raft<A>, state: DiskStateMachine<A>, log: DiskLog<A>) -> Self {
        Self::Raft { raft, state, log }
    }

    pub fn state(&self) -> GroupState<A> {
        match self {
            Self::Local(group) => group.state(),
            Self::Raft { state, .. } => GroupState(State::Raft(state.clone())),
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }
    pub fn raft(&self) -> Option<&Raft<A>> {
        match self {
            Self::Raft { raft, .. } => Some(raft),
            Self::Local(_) => None,
        }
    }
    pub fn raft_state(&self) -> Option<&DiskStateMachine<A>> {
        match self {
            Self::Raft { state, .. } => Some(state),
            Self::Local(_) => None,
        }
    }
    pub fn identity(&self) -> &StorageIdentity {
        match self {
            Self::Local(group) => group.identity(),
            Self::Raft { state, .. } => state.storage_identity(),
        }
    }
    pub fn local_node(&self) -> u64 {
        self.identity().node_id
    }

    pub async fn commit(
        &self,
        command: A::Command,
        deadline: Instant,
    ) -> Result<Applied<A::Response>> {
        match self {
            Self::Local(group) => group.submit(command, deadline).await,
            Self::Raft { raft, .. } => raft_commit(raft, command, deadline).await,
        }
    }

    pub async fn commit_completion(
        &self,
        command: A::Command,
        deadline: Instant,
    ) -> Result<Applied<A::Response>> {
        match self {
            Self::Local(group) => group.submit_completion(command, deadline).await,
            Self::Raft { raft, .. } => raft_commit(raft, command, deadline).await,
        }
    }

    pub async fn ensure_read(&self, deadline: Instant) -> Result<()> {
        timeout_at(deadline, async {
            if let Some(raft) = self.raft() {
                raft.ensure_linearizable()
                    .await
                    .map_err(|error| match error {
                        openraft::error::RaftError::APIError(
                            openraft::error::CheckIsLeaderError::ForwardToLeader(source),
                        ) => Error::ForwardToLeader { source },
                        openraft::error::RaftError::Fatal(error) => Error::fatal(error.to_string()),
                        error => Error::unknown(format!("Raft read barrier failed: {error}")),
                    })?;
            }
            self.state().try_with_application(|_| ()).await
        })
        .await
        .map_err(|_| Error::unknown("Group read barrier deadline elapsed"))?
    }

    pub async fn snapshot(&self, deadline: Instant) -> Result<Option<SnapshotInfo>> {
        match self {
            Self::Local(group) => group.snapshot(deadline).await,
            Self::Raft { state, .. } => {
                let mut state = state.clone();
                let snapshot = timeout_at(deadline, state.build_snapshot())
                    .await
                    .map_err(|_| Error::unknown("Raft snapshot deadline elapsed"))?
                    .map_err(|e| Error::fatal(format!("Raft snapshot failed: {e}")))?;
                Ok(snapshot.meta.last_log_id.map(|log_id| SnapshotInfo {
                    position: log_id.into(),
                    snapshot_id: snapshot.meta.snapshot_id,
                }))
            }
        }
    }

    pub async fn close(&self, deadline: Instant) -> Result<()> {
        match self {
            Self::Local(group) => group.drain(deadline).await,
            Self::Raft { raft, log, .. } => {
                timeout_at(deadline, raft.shutdown())
                    .await
                    .map_err(|_| Error::unknown("Raft shutdown deadline elapsed"))?
                    .map_err(|e| Error::fatal(format!("Raft shutdown failed: {e}")))?;
                timeout_at(deadline, log.close())
                    .await
                    .map_err(|_| Error::unknown("Raft writer close deadline elapsed"))??;
                Ok(())
            }
        }
    }
}

async fn raft_commit<A: Application>(
    raft: &Raft<A>,
    command: A::Command,
    deadline: Instant,
) -> Result<Applied<A::Response>> {
    if Instant::now() >= deadline {
        return Err(Error::not_submitted(
            "Deadline elapsed before Raft submission",
        ));
    }
    let response = timeout_at(deadline, raft.client_write(command))
        .await
        .map_err(|_| Error::unknown("Raft command deadline elapsed after submission"))?
        .map_err(raft_write_error)?;
    Ok(Applied {
        position: response.log_id.into(),
        response: response.data,
    })
}

pub(super) fn raft_write_error(
    error: openraft::error::RaftError<
        u64,
        openraft::error::ClientWriteError<u64, openraft::BasicNode>,
    >,
) -> Error {
    match error {
        openraft::error::RaftError::APIError(
            openraft::error::ClientWriteError::ForwardToLeader(source),
        ) => Error::ForwardToLeader { source },
        openraft::error::RaftError::Fatal(error) => Error::fatal(error.to_string()),
        error => Error::unknown(error.to_string()),
    }
}

impl<A: Application> GroupState<A> {
    pub(super) fn local(shared: Arc<Shared<A>>) -> Self {
        Self(State::Local(shared))
    }

    pub fn storage_identity(&self) -> &StorageIdentity {
        match &self.0 {
            State::Local(shared) => &shared.storage.identity,
            State::Raft(state) => state.storage_identity(),
        }
    }
    pub fn raft_state(&self) -> Option<&DiskStateMachine<A>> {
        match &self.0 {
            State::Local(_) => None,
            State::Raft(state) => Some(state),
        }
    }

    /// The synchronous projection must be bounded, free of IO and retained-history scans.
    pub async fn try_with_application<R>(&self, query: impl FnOnce(&A) -> R) -> Result<R> {
        self.try_with_applied(|application, _| query(application))
            .await
    }

    pub async fn try_with_applied<R>(
        &self,
        query: impl FnOnce(&A, Option<ExecutionPosition>) -> R,
    ) -> Result<R> {
        match &self.0 {
            State::Local(shared) => {
                let view = shared.view.read().await;
                shared.ensure_readable()?;
                if view.failed {
                    return Err(Error::fatal(
                        "Application stopped after a local apply failure",
                    ));
                }
                Ok(query(&view.application, position(view.sequence)))
            }
            State::Raft(state) => Ok(state
                .try_with_applied(|application, log| {
                    query(application, log.map(ExecutionPosition::from))
                })
                .await?),
        }
    }

    /// Diagnostic projection can expose failed state; use try_with_application for authority.
    pub async fn with_application<R>(&self, query: impl FnOnce(&A) -> R) -> R {
        self.with_applied(|application, _| query(application)).await
    }

    pub async fn with_applied<R>(
        &self,
        query: impl FnOnce(&A, Option<ExecutionPosition>) -> R,
    ) -> R {
        match &self.0 {
            State::Local(shared) => {
                let view = shared.view.read().await;
                query(&view.application, position(view.sequence))
            }
            State::Raft(state) => {
                state
                    .with_applied(|application, log| {
                        query(application, log.map(ExecutionPosition::from))
                    })
                    .await
            }
        }
    }
}
