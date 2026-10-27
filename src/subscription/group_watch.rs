use super::*;

pub(crate) struct WatchState {
    pub version: u64,
    pub fingerprint: [u8; 32],
    pub candidate_bytes: Option<u64>,
    pub unseen: bool,
    pub due_ms: Option<u64>,
    pub eligible: bool,
}

impl GroupCursor {
    #[cfg(feature = "test-hooks")]
    pub(crate) fn trace_read_state(
        &self,
        request: GroupRead,
        producer_next: u64,
    ) -> crate::runtime::read_trace_test_hooks::ReadTraceEvent {
        use crate::runtime::read_trace_test_hooks::{ReadTraceEvent, ReadTraceMember};
        let member = self.members.get(&(request.member, request.epoch));
        ReadTraceEvent {
            subscription: self.subscription,
            member: request.member,
            epoch: request.epoch,
            sequence: request.sequence,
            now_ms: request.now_ms,
            budget_messages: 0,
            budget_bytes: 0,
            budget_results: 0,
            cursor_next: self.next,
            producer_next,
            cursor_windows: self.slots.len(),
            ready: self.ready.len(),
            eligible_ready: self
                .ready
                .iter()
                .filter(|(due, offset)| {
                    *due <= request.now_ms
                        && self.slots.get(offset).is_some_and(|slot| {
                            member.is_some_and(|member| self.owns(member, slot.candidate))
                        })
                })
                .count(),
            candidates: 0,
            candidate_min_bytes: None,
            group_windows: 0,
            group_reads: 0,
            group_bytes: 0,
            member_state: member.map(|member| ReadTraceMember {
                active: member.active,
                expires_ms: member.grant.expires_ms,
                in_flight_messages: member.in_flight,
                in_flight_bytes: member.bytes,
                max_messages: member.grant.max_messages,
                max_bytes: member.grant.max_bytes,
            }),
            deliveries: None,
        }
    }

    /// Delivery-grant expiry does not revoke a live member's read-only observation.
    pub(crate) fn watch_state(
        &self,
        member: Id,
        epoch: u64,
        client: Id,
        credential: Id,
        committed_end: u64,
        now_ms: u64,
    ) -> Result<WatchState> {
        let member = self
            .members
            .get(&(member, epoch))
            .filter(|row| {
                row.active && row.grant.client == client && row.grant.credential == credential
            })
            .ok_or(Rejection::SessionMismatch)?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"tidemq/watch-cursor/1\0");
        for value in [self.version, self.frontier, self.next, committed_end] {
            hash.update(&value.to_be_bytes());
        }
        hash.update(&[u8::from(self.gate_open)]);
        let mut candidate_bytes = None;
        let mut due_ms = None;
        let mut blocked = BTreeSet::new();
        for slot in self.slots.values() {
            // Persisted local cursor changes count; empty read identities and saved ACKs do not.
            hash.update(&slot.candidate.offset.to_be_bytes());
            hash.update(&slot.attempt.to_be_bytes());
            let (tag, due) = match slot.state {
                SlotState::Ready(due) => (1, due),
                SlotState::InFlight(..) => (2, 0),
                SlotState::Acked => (3, 0),
                SlotState::Paused => (4, 0),
            };
            hash.update(&[tag]);
            hash.update(&due.to_be_bytes());
            if self.model == SubscriptionKind::Stream && slot.state == SlotState::Paused {
                blocked.insert(slot.candidate.hash);
            }
        }
        for (due, offset) in &self.ready {
            let slot = self.slots.get(offset).ok_or(Rejection::InvalidSnapshot)?;
            if !self.owns(member, slot.candidate) || blocked.contains(&slot.candidate.hash) {
                continue;
            }
            if slot.attempt >= slot.attempt_limit {
                if self.model == SubscriptionKind::Stream {
                    blocked.insert(slot.candidate.hash);
                }
                continue;
            }
            if *due > now_ms {
                due_ms = Some(due_ms.map_or(*due, |previous: u64| previous.min(*due)));
            } else if slot.candidate.bytes <= member.grant.max_bytes.saturating_sub(member.bytes) {
                candidate_bytes = Some(
                    candidate_bytes.map_or(slot.candidate.bytes, |previous: u64| {
                        previous.min(slot.candidate.bytes)
                    }),
                );
            }
        }
        let unseen =
            self.next < committed_end && self.slots.len() < self.limits.max_window_slots as usize;
        if unseen && member.bytes < member.grant.max_bytes {
            candidate_bytes.get_or_insert(1);
        }
        Ok(WatchState {
            version: self.version,
            fingerprint: *hash.finalize().as_bytes(),
            candidate_bytes,
            unseen,
            due_ms,
            eligible: self.gate_open
                && member.in_flight < member.grant.max_messages
                && member.bytes < member.grant.max_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (GroupCursor, Id) {
        let id = Id::new_v7().unwrap();
        let mut cursor = GroupCursor::new(
            GroupIdentity {
                topic: id,
                shard: id,
                subscription: id,
            },
            SubscriptionKind::Queue,
            0,
            [42; 32],
            Limits::default(),
        )
        .unwrap();
        cursor
            .install_members(
                2,
                &[MemberGrant {
                    member: id,
                    client: id,
                    credential: id,
                    epoch: 1,
                    expires_ms: 100,
                    max_messages: 8,
                    max_bytes: 4096,
                    lanes: vec![],
                }],
                0,
            )
            .unwrap();
        (cursor, id)
    }
    #[test]
    fn expired_delivery_grant_does_not_disable_read_only_member_observation() {
        let (mut cursor, id) = fixture();
        let before = cursor.clone();
        assert!(cursor.watch_state(id, 1, id, id, 1, 200).unwrap().eligible);
        assert_eq!(cursor, before);
        cursor.install_members(3, &[], 200).unwrap();
        assert!(cursor.watch_state(id, 1, id, id, 1, 200).is_err());
    }
    #[test]
    fn empty_reads_and_their_retirement_do_not_change_readiness() {
        let (mut cursor, id) = fixture();
        let before = cursor.watch_state(id, 1, id, id, 0, 0).unwrap().fingerprint;
        cursor
            .read(
                GroupRead {
                    member: id,
                    epoch: 1,
                    sequence: 1,
                    now_ms: 0,
                    max_messages: 1,
                    max_bytes: 4096,
                },
                &[],
            )
            .unwrap();
        cursor.retire_read(id, 1, 1).unwrap();
        assert_eq!(
            cursor.watch_state(id, 1, id, id, 0, 0).unwrap().fingerprint,
            before
        );
    }
    #[test]
    fn delayed_nack_is_a_timer_dependency_and_watch_timeout_never_redelivers() {
        let (mut cursor, id) = fixture();
        let read = cursor
            .read(
                GroupRead {
                    member: id,
                    epoch: 1,
                    sequence: 1,
                    now_ms: 0,
                    max_messages: 1,
                    max_bytes: 4096,
                },
                &[Candidate {
                    offset: 0,
                    hash: 0,
                    bytes: 1,
                }],
            )
            .unwrap();
        cursor
            .acknowledge(GroupAck {
                member: id,
                epoch: 1,
                sequence: 1,
                now_ms: 0,
                model: SubscriptionKind::Queue,
                proofs: read.deliveries,
                nack_delay_ms: Some(50),
            })
            .unwrap();
        let before = cursor.clone();
        let early = cursor.watch_state(id, 1, id, id, 1, 49).unwrap();
        assert_eq!(early.due_ms, Some(50));
        assert!(early.candidate_bytes.is_none());
        assert_eq!(
            cursor
                .watch_state(id, 1, id, id, 1, 50)
                .unwrap()
                .candidate_bytes,
            Some(1)
        );
        assert_eq!(cursor, before);
    }
}
