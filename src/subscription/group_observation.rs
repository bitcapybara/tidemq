use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupProgress {
    pub frontier: u64,
    pub window_end: u64,
    pub ready: u64,
    pub in_flight: u64,
    pub acked: u64,
    pub paused: u64,
    pub unconfirmed: Option<u64>,
    pub tail: Option<u64>,
    pub gate_open: bool,
}

impl GroupCursor {
    /// Observe persisted slots without reaping members, retrying deliveries, or advancing ACK state.
    pub fn observe_progress(&self, committed_end: u64) -> GroupProgress {
        let mut result = GroupProgress {
            frontier: self.frontier,
            window_end: self.next,
            ready: 0,
            in_flight: 0,
            acked: 0,
            paused: 0,
            unconfirmed: None,
            tail: committed_end.checked_sub(self.next),
            gate_open: self.gate_open,
        };
        for slot in self.slots.values() {
            match slot.state {
                SlotState::Ready(_) => result.ready = result.ready.saturating_add(1),
                SlotState::InFlight(..) => result.in_flight = result.in_flight.saturating_add(1),
                SlotState::Acked => result.acked = result.acked.saturating_add(1),
                SlotState::Paused => result.paused = result.paused.saturating_add(1),
            }
        }
        result.unconfirmed = committed_end
            .checked_sub(self.frontier)
            .and_then(|span| span.checked_sub(result.acked));
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observation_keeps_ack_holes_and_never_moves_the_consumer_cursor() {
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
        cursor.next = 4;
        for (offset, state) in [
            (0, SlotState::InFlight(id, 1)),
            (1, SlotState::Acked),
            (2, SlotState::Ready(10)),
            (3, SlotState::Paused),
        ] {
            cursor.slots.insert(
                offset,
                Slot {
                    candidate: Candidate {
                        offset,
                        hash: 0,
                        bytes: 1,
                    },
                    attempt: 1,
                    attempt_limit: 3,
                    state,
                },
            );
        }
        let before = cursor.clone();
        let summary = cursor.observe_progress(10);
        assert_eq!(summary.frontier, 0);
        assert_eq!(summary.window_end, 4);
        assert_eq!(
            (
                summary.ready,
                summary.in_flight,
                summary.acked,
                summary.paused
            ),
            (1, 1, 1, 1)
        );
        assert_eq!(summary.unconfirmed, Some(9));
        assert_eq!(summary.tail, Some(6));
        assert_eq!(cursor, before);
    }
}
