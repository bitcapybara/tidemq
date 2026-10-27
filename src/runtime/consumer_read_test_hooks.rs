//! Bounded, persistent post-commit reply faults for original-read recovery tests.

use std::{
    collections::BTreeSet,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use tidemq_protocol::{Id, common::Frame, public::GroupRead};
use tokio::{
    sync::Notify,
    time::{Instant, timeout_at},
};

use crate::metadata::PartitionKey;

const MAX_TARGETS: usize = 8;
const MAX_ATTEMPTS: usize = 4096;
static INSTALLED: Mutex<Option<Arc<Fault>>> = Mutex::new(None);

struct Fault {
    cluster: Id,
    partitions: BTreeSet<PartitionKey>,
    attempts: Mutex<Vec<ReadAttempt>>,
    changed: Notify,
    overflowed: AtomicBool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadAttempt {
    pub partition: PartitionKey,
    pub subscription: Id,
    pub member: Id,
    pub member_epoch: u64,
    pub operation: Id,
    pub sequence: u64,
    pub max_messages: u32,
    pub max_bytes: u64,
}

pub struct InstalledReadReplyFault(Arc<Fault>);

impl InstalledReadReplyFault {
    pub fn install(cluster: Id, partitions: Vec<PartitionKey>) -> io::Result<Self> {
        if partitions.is_empty() || partitions.len() > MAX_TARGETS {
            return Err(io::Error::other(
                "Read reply fault requires between one and eight targets",
            ));
        }
        let count = partitions.len();
        let partitions: BTreeSet<_> = partitions.into_iter().collect();
        if partitions.len() != count {
            return Err(io::Error::other(
                "Read reply fault targets must be distinct",
            ));
        }
        let mut attempts = Vec::new();
        attempts
            .try_reserve_exact(MAX_ATTEMPTS)
            .map_err(io::Error::other)?;
        let mut slot = INSTALLED
            .lock()
            .map_err(|_| io::Error::other("Read reply fault is poisoned"))?;
        if slot.is_some() {
            return Err(io::Error::other(
                "A persistent read reply fault is already installed",
            ));
        }
        let fault = Arc::new(Fault {
            cluster,
            partitions,
            attempts: Mutex::new(attempts),
            changed: Notify::new(),
            overflowed: AtomicBool::new(false),
        });
        *slot = Some(fault.clone());
        Ok(Self(fault))
    }

    pub fn attempts(&self) -> Vec<ReadAttempt> {
        self.0
            .attempts
            .lock()
            .map(|attempts| attempts.clone())
            .unwrap_or_default()
    }

    pub fn overflowed(&self) -> bool {
        self.0.overflowed.load(Ordering::Acquire)
    }

    pub async fn wait_for_targets(&self, count: usize, deadline: Instant) -> bool {
        if count == 0 || count > self.0.partitions.len() {
            return false;
        }
        loop {
            let changed = self.0.changed.notified();
            let seen: BTreeSet<_> = self
                .attempts()
                .iter()
                .map(|attempt| attempt.partition)
                .collect();
            if seen.len() >= count {
                return true;
            }
            if timeout_at(deadline, changed).await.is_err() {
                return false;
            }
        }
    }

    pub fn release(&self) {
        if let Ok(mut slot) = INSTALLED.lock()
            && slot
                .as_ref()
                .is_some_and(|fault| Arc::ptr_eq(fault, &self.0))
        {
            slot.take();
        }
    }
}

impl Drop for InstalledReadReplyFault {
    fn drop(&mut self) {
        self.release();
    }
}

pub(super) fn suppress(
    cluster: Id,
    partition: PartitionKey,
    frame: &Frame,
    request: &GroupRead,
) -> bool {
    let fault = INSTALLED.lock().ok().and_then(|slot| slot.clone());
    let Some(fault) =
        fault.filter(|fault| fault.cluster == cluster && fault.partitions.contains(&partition))
    else {
        return false;
    };
    if let (Ok(subscription), Ok(member), Ok(operation), Ok(mut attempts)) = (
        Id::from_slice(&request.subscription_id),
        Id::from_slice(&request.member_id),
        Id::from_slice(&frame.operation_id),
        fault.attempts.lock(),
    ) {
        if attempts.len() < MAX_ATTEMPTS {
            attempts.push(ReadAttempt {
                partition,
                subscription,
                member,
                member_epoch: request.member_epoch,
                operation,
                sequence: request.sequence,
                max_messages: request.max_messages,
                max_bytes: request.max_bytes,
            });
        } else {
            fault.overflowed.store(true, Ordering::Release);
        }
    }
    fault.changed.notify_one();
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn persistent_fault_matches_scope_records_original_identity_and_releases() {
        let cluster = Id::new_v7().unwrap();
        let partition = PartitionKey {
            topic_id: Id::new_v7().unwrap(),
            partition: 0,
        };
        assert!(InstalledReadReplyFault::install(cluster, vec![]).is_err());
        assert!(InstalledReadReplyFault::install(cluster, vec![partition; 2]).is_err());
        let fault = InstalledReadReplyFault::install(cluster, vec![partition]).unwrap();
        let request = GroupRead {
            subscription_id: Bytes::copy_from_slice(Id::new_v7().unwrap().as_bytes()),
            member_id: Bytes::copy_from_slice(Id::new_v7().unwrap().as_bytes()),
            member_epoch: 3,
            sequence: 7,
            max_messages: 1,
            max_bytes: 262656,
            ..Default::default()
        };
        let frame = Frame {
            operation_id: Bytes::copy_from_slice(Id::new_v7().unwrap().as_bytes()),
            ..Default::default()
        };
        assert!(!suppress(
            Id::new_v7().unwrap(),
            partition,
            &frame,
            &request
        ));
        assert!(suppress(cluster, partition, &frame, &request));
        assert!(suppress(cluster, partition, &frame, &request));
        assert!(fault.wait_for_targets(1, Instant::now()).await);
        assert_eq!(fault.attempts().len(), 2);
        assert_eq!(fault.attempts().first(), fault.attempts().get(1));
        for _ in 0..MAX_ATTEMPTS {
            assert!(suppress(cluster, partition, &frame, &request));
        }
        assert!(fault.overflowed());
        assert_eq!(fault.attempts().len(), MAX_ATTEMPTS);
        fault.release();
        assert!(!suppress(cluster, partition, &frame, &request));
        drop(fault);
        let replacement = InstalledReadReplyFault::install(cluster, vec![partition]).unwrap();
        assert!(!replacement.wait_for_targets(1, Instant::now()).await);
    }
}
