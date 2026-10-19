use std::collections::BTreeSet;

use tidemq::subscription::*;
use tidemq_protocol::Id;

struct Fixture {
    state: SubscriptionState,
    id: Id,
    session: Id,
    generations: Generations,
    context: DataContext,
}

impl Fixture {
    fn new(limits: Limits, cold_before: u64) -> Self {
        let id = Id::random().unwrap();
        let session = Id::random().unwrap();
        let context = DataContext {
            committed_end: 100_000,
            retained_floor: 0,
        };
        let mut state = SubscriptionState::new(Id::random().unwrap(), 7, limits).unwrap();
        state
            .apply(
                context,
                Command::Create {
                    subscription_id: id,
                    operation_id: Id::random().unwrap(),
                    start_offset: 0,
                    signing_key: SigningKey::try_from([71; 32]).unwrap(),
                },
            )
            .into_result()
            .unwrap();
        let Outcome::Assigned(assignment) = state
            .apply(
                context,
                Command::Claim {
                    subscription_id: id,
                    expected: Generations {
                        cursor: 1,
                        assignment: 0,
                    },
                    session_id: session,
                    replace_session_id: None,
                    cold_before,
                },
            )
            .into_result()
            .unwrap()
        else {
            panic!("Expected an assignment")
        };
        Self {
            state,
            id,
            session,
            generations: assignment.generations,
            context,
        }
    }

    fn apply(&mut self, command: Command) -> Result<Outcome> {
        let before = self.state.clone();
        let result = self.state.apply(self.context, command).into_result();
        self.state.validate().unwrap();
        if result.is_err() {
            assert_eq!(self.state, before, "Rejected commands are atomic");
        }
        result
    }

    fn delivery_command(
        &self,
        offset: u64,
        attempt: u32,
        now_ms: u64,
        lane: DeliveryLane,
    ) -> Command {
        Command::RecordDelivery {
            subscription_id: self.id,
            generations: self.generations,
            session_id: self.session,
            operation_id: Id::random().unwrap(),
            offset,
            expected_attempt: attempt,
            body_bytes: 10,
            now_ms,
            lane,
        }
    }

    fn deliver(
        &mut self,
        offset: u64,
        attempt: u32,
        now_ms: u64,
        lane: DeliveryLane,
    ) -> DeliveryReceipt {
        let command = self.delivery_command(offset, attempt, now_ms, lane);
        let Outcome::Delivery(receipt) = self.apply(command).unwrap() else {
            panic!("Expected a delivery receipt")
        };
        receipt
    }

    fn ack(&mut self, tokens: Vec<DeliveryToken>) -> Result<AckReceipt> {
        let result = self.apply(Command::Ack {
            subscription_id: self.id,
            generations: self.generations,
            tokens,
        })?;
        let Outcome::Acknowledged(receipt) = result else {
            panic!("Expected an ACK receipt")
        };
        Ok(receipt)
    }

    fn barrier(&mut self, received_before: u64) -> Result<Outcome> {
        self.apply(Command::ReceiveBarrier {
            subscription_id: self.id,
            generations: self.generations,
            session_id: self.session,
            delivery_generation: self.generations.delivery(),
            received_before,
        })
    }

    fn replace(&mut self, cold_before: u64) -> Assignment {
        let next_session = Id::random().unwrap();
        let Outcome::Assigned(assignment) = self
            .apply(Command::Claim {
                subscription_id: self.id,
                expected: self.generations,
                session_id: next_session,
                replace_session_id: Some(self.session),
                cold_before,
            })
            .unwrap()
        else {
            panic!("Expected a replacement assignment")
        };
        self.session = next_session;
        self.generations = assignment.generations;
        assignment
    }
}

#[test]
fn authenticated_tokens_survive_frontier_compaction_and_reject_every_tampered_byte() {
    let mut f = Fixture::new(Limits::default(), 0);
    let receipt = f.deliver(0, 0, 0, DeliveryLane::Realtime);
    assert_eq!(receipt.token.as_bytes().len(), DELIVERY_TOKEN_BYTES);
    assert_eq!(
        f.ack(vec![receipt.token.clone()])
            .unwrap()
            .acknowledged_before,
        1
    );
    assert_eq!(
        f.ack(vec![receipt.token.clone()])
            .unwrap()
            .already_acknowledged,
        1
    );
    for index in 0..DELIVERY_TOKEN_BYTES {
        let mut bytes = receipt.token.as_bytes().to_vec();
        *bytes.get_mut(index).unwrap() ^= 1;
        let token = DeliveryToken::try_from(bytes).unwrap();
        assert_eq!(f.ack(vec![token]), Err(Rejection::InvalidToken));
    }
    assert!(DeliveryToken::try_from(vec![1; DELIVERY_TOKEN_BYTES - 1]).is_err());
    assert_eq!(format!("{:?}", receipt.token), "DeliveryToken([REDACTED])");
    assert_eq!(
        format!("{:?}", SigningKey::try_from([71; 32]).unwrap()),
        "SigningKey([REDACTED])"
    );
    let encoded = postcard::to_stdvec(&f.state).unwrap();
    let mut recovered: SubscriptionState = postcard::from_bytes(&encoded).unwrap();
    recovered.validate().unwrap();
    let result = recovered
        .apply(
            f.context,
            Command::Ack {
                subscription_id: f.id,
                generations: f.generations,
                tokens: vec![receipt.token],
            },
        )
        .into_result()
        .unwrap();
    assert!(matches!(
        result,
        Outcome::Acknowledged(AckReceipt {
            already_acknowledged: 1,
            ..
        })
    ));
}

#[test]
fn recording_attempts_is_idempotent_and_ack_batches_count_duplicates_without_allocating_holes() {
    let mut f = Fixture::new(Limits::default(), 0);
    let command = f.delivery_command(0, 0, 0, DeliveryLane::Realtime);
    let Outcome::Delivery(original) = f.apply(command.clone()).unwrap() else {
        panic!("Expected delivery")
    };
    let Outcome::Delivery(repeated) = f.apply(command).unwrap() else {
        panic!("Expected delivery")
    };
    assert_eq!(original.token, repeated.token);
    assert_eq!(original.attempt, repeated.attempt);
    assert!(repeated.duplicate);
    let receipt = f.ack(vec![original.token.clone(), original.token]).unwrap();
    assert_eq!(
        receipt,
        AckReceipt {
            acknowledged_before: 1,
            accepted: 1,
            already_acknowledged: 1
        }
    );
    assert_eq!(f.state.diagnostics(f.id).unwrap().window_slots, 0);
}

#[test]
fn stale_attempt_cannot_acknowledge_a_replacement_and_lost_ack_reply_is_retryable() {
    let mut f = Fixture::new(Limits::default(), 0);
    let first = f.deliver(0, 0, 0, DeliveryLane::Realtime);
    let early = f.delivery_command(0, 1, first.retry_at_ms - 1, DeliveryLane::Realtime);
    assert_eq!(f.apply(early), Err(Rejection::RetryNotDue));
    let second = f.deliver(0, 1, first.retry_at_ms, DeliveryLane::Realtime);
    assert_eq!(second.attempt, 2);
    assert_eq!(
        f.ack(vec![first.token.clone()]),
        Err(Rejection::StaleAttempt)
    );
    let committed = f.ack(vec![second.token.clone()]).unwrap();
    assert_eq!(committed.acknowledged_before, 1);
    assert_eq!(f.ack(vec![second.token]).unwrap().already_acknowledged, 1);
    assert_eq!(f.ack(vec![first.token]).unwrap().already_acknowledged, 1);
}

#[test]
fn slow_frontier_bounds_acknowledged_holes_and_preserves_ack_admission_when_full() {
    let limits = Limits {
        max_window_slots: 4,
        max_in_flight_messages: 4,
        max_total_window_slots: 4,
        max_in_flight_bytes: 40,
        max_total_in_flight_bytes: 40,
        ..Limits::default()
    };
    let mut f = Fixture::new(limits, 0);
    let tokens: Vec<_> = (0..4)
        .map(|offset| f.deliver(offset, 0, 0, DeliveryLane::Realtime).token)
        .collect();
    let receipt = f.ack(tokens.iter().skip(1).cloned().collect()).unwrap();
    assert_eq!(receipt.acknowledged_before, 0);
    let stats = f.state.diagnostics(f.id).unwrap();
    assert_eq!(
        (
            stats.window_slots,
            stats.acknowledged_holes,
            stats.in_flight_bytes
        ),
        (4, 3, 10)
    );
    let next = f.delivery_command(4, 0, 0, DeliveryLane::Realtime);
    assert_eq!(f.apply(next), Err(Rejection::WindowFull));
    assert_eq!(
        f.ack(vec![tokens.first().unwrap().clone()])
            .unwrap()
            .acknowledged_before,
        4
    );
    assert_eq!(f.state.diagnostics(f.id).unwrap().window_slots, 0);
    f.deliver(4, 0, 0, DeliveryLane::Realtime);
}

#[test]
fn assignment_claim_requires_explicit_compare_and_replace_and_reissues_unacknowledged_attempts() {
    let mut f = Fixture::new(Limits::default(), 0);
    let old = f.deliver(0, 0, 0, DeliveryLane::Realtime);
    let old_generations = f.generations;
    assert_eq!(
        f.apply(Command::Claim {
            subscription_id: f.id,
            expected: f.generations,
            session_id: Id::random().unwrap(),
            replace_session_id: None,
            cold_before: 0,
        }),
        Err(Rejection::AssignmentBusy)
    );
    let assignment = f.replace(0);
    assert!(assignment.generations.assignment > old_generations.assignment);
    assert_eq!(assignment.issued_before, 0);
    assert_eq!(f.ack(vec![old.token]), Err(Rejection::StaleGeneration));
    let replacement = f.deliver(0, 1, 0, DeliveryLane::Realtime);
    assert_eq!(replacement.attempt, 2);
    assert_eq!(
        f.ack(vec![replacement.token]).unwrap().acknowledged_before,
        1
    );
}

#[test]
fn backwards_seek_changes_cursor_and_assignment_and_retries_return_original_seek_result() {
    let mut f = Fixture::new(Limits::default(), 0);
    let old = f.deliver(0, 0, 0, DeliveryLane::Realtime);
    f.ack(vec![old.token.clone()]).unwrap();
    let command = Command::Seek {
        subscription_id: f.id,
        operation_id: Id::random().unwrap(),
        expected_cursor_generation: f.generations.cursor,
        offset: 0,
    };
    let Outcome::Seeked(position) = f.apply(command.clone()).unwrap() else {
        panic!("Expected seek")
    };
    f.generations = position.generations;
    assert_eq!(f.ack(vec![old.token]), Err(Rejection::StaleGeneration));
    let Outcome::Assigned(assignment) = f
        .apply(Command::Claim {
            subscription_id: f.id,
            expected: position.generations,
            session_id: f.session,
            replace_session_id: None,
            cold_before: 0,
        })
        .unwrap()
    else {
        panic!("Expected assignment")
    };
    f.generations = assignment.generations;
    assert_eq!(f.apply(command).unwrap(), Outcome::Seeked(position));
    let new = f.deliver(0, 0, 0, DeliveryLane::Realtime);
    assert_eq!(new.attempt, 1);
    assert_eq!(f.ack(vec![new.token]).unwrap().acknowledged_before, 1);
}

#[test]
fn retention_floor_and_subscription_mutations_share_authoritative_ordering() {
    let mut f = Fixture::new(Limits::default(), 0);
    f.context.retained_floor = 1;
    let command = f.delivery_command(0, 0, 0, DeliveryLane::Realtime);
    assert_eq!(f.apply(command), Err(Rejection::RetentionConflict));
    f.context.retained_floor = 0;
    let token = f.deliver(0, 0, 0, DeliveryLane::Realtime).token;
    f.ack(vec![token]).unwrap();
    assert_eq!(f.state.protected_before(), Some(1));
    f.context.retained_floor = 1;
    assert_eq!(
        f.apply(Command::Seek {
            subscription_id: f.id,
            operation_id: Id::random().unwrap(),
            expected_cursor_generation: f.generations.cursor,
            offset: 0
        }),
        Err(Rejection::OutOfRetention)
    );
    f.apply(Command::Delete {
        subscription_id: f.id,
        expected_cursor_generation: f.generations.cursor,
    })
    .unwrap();
    assert_eq!(f.state.protected_before(), None);
    assert_eq!(
        f.apply(Command::Create {
            subscription_id: f.id,
            operation_id: Id::random().unwrap(),
            start_offset: 1,
            signing_key: SigningKey::try_from([99; 32]).unwrap()
        }),
        Err(Rejection::Deleted)
    );
}

#[test]
fn cold_to_hot_switch_requires_continuous_sdk_receive_barrier_independent_of_business_ack() {
    let mut f = Fixture::new(Limits::default(), 2);
    let zero = f.deliver(0, 0, 0, DeliveryLane::Bulk);
    let one = f.deliver(1, 0, 0, DeliveryLane::Bulk);
    f.ack(vec![zero.token, one.token]).unwrap();
    assert_eq!(
        f.state.diagnostics(f.id).unwrap().phase,
        Phase::AwaitReceiveBarrier
    );
    let hot = f.delivery_command(2, 0, 0, DeliveryLane::Realtime);
    assert_eq!(f.apply(hot), Err(Rejection::CutoverPending));
    assert_eq!(f.barrier(3), Err(Rejection::InvalidBarrier));
    f.barrier(2).unwrap();
    f.deliver(2, 0, 0, DeliveryLane::Realtime);
    assert_eq!(f.state.diagnostics(f.id).unwrap().phase, Phase::Realtime);
    assert_eq!(f.barrier(1), Err(Rejection::InvalidBarrier));
}

#[test]
fn cold_retries_remain_on_bulk_after_realtime_delivery_starts() {
    let mut f = Fixture::new(Limits::default(), 2);
    let zero = f.deliver(0, 0, 0, DeliveryLane::Bulk);
    let one = f.deliver(1, 0, 0, DeliveryLane::Bulk);
    f.barrier(2).unwrap();
    let hot = f.deliver(2, 0, 0, DeliveryLane::Realtime);
    let wrong = f.delivery_command(0, 1, zero.retry_at_ms, DeliveryLane::Realtime);
    assert_eq!(f.apply(wrong), Err(Rejection::WrongLane));
    let retry = f.deliver(0, 1, zero.retry_at_ms, DeliveryLane::Bulk);
    assert_eq!(retry.attempt, 2);
    assert_eq!(f.ack(vec![zero.token]), Err(Rejection::StaleAttempt));
    assert_eq!(
        f.ack(vec![hot.token, one.token])
            .unwrap()
            .acknowledged_before,
        0
    );
    assert_eq!(f.ack(vec![retry.token]).unwrap().acknowledged_before, 3);
}

#[test]
fn takeover_barrier_cannot_use_old_issued_prefix_and_acknowledged_skip_ranges_are_explicit() {
    let mut f = Fixture::new(Limits::default(), 3);
    let zero = f.deliver(0, 0, 0, DeliveryLane::Bulk);
    let one = f.deliver(1, 0, 0, DeliveryLane::Bulk);
    f.deliver(2, 0, 0, DeliveryLane::Bulk);
    f.ack(vec![one.token]).unwrap();
    f.replace(3);
    assert_eq!(f.barrier(3), Err(Rejection::InvalidBarrier));
    f.deliver(2, 1, 0, DeliveryLane::Bulk);
    assert_eq!(f.state.assignment(f.id).unwrap().issued_before, 0);
    f.deliver(0, 1, 0, DeliveryLane::Bulk);
    assert_eq!(f.state.assignment(f.id).unwrap().issued_before, 3);
    assert_eq!(
        f.state
            .acknowledged_ranges(f.id, f.generations, 0, 3, 8)
            .unwrap(),
        vec![AcknowledgedRange { start: 1, end: 2 }]
    );
    f.barrier(3).unwrap();
    assert_eq!(f.ack(vec![zero.token]), Err(Rejection::StaleGeneration));
    assert_eq!(f.state.protected_before(), Some(0));
}

#[test]
fn exhausted_attempts_remain_protected_without_starving_other_retry_work() {
    let mut f = Fixture::new(
        Limits {
            max_delivery_attempts: 2,
            ..Limits::default()
        },
        0,
    );
    let first = f.deliver(0, 0, 0, DeliveryLane::Realtime);
    let last = f.deliver(0, 1, first.retry_at_ms, DeliveryLane::Realtime);
    let pending = f.deliver(1, 0, 0, DeliveryLane::Realtime);
    let due = f.state.due_retries(f.id, 100_000, 10).unwrap();
    assert_eq!(
        due.iter().map(|item| item.offset).collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(
        f.state.diagnostics(f.id).unwrap().blocked,
        Some(BlockReason::AttemptsExhausted)
    );
    let retry = f.delivery_command(0, 2, last.retry_at_ms, DeliveryLane::Realtime);
    assert_eq!(f.apply(retry), Err(Rejection::AttemptLimit));
    assert_eq!(f.state.protected_before(), Some(0));
    f.ack(vec![last.token, pending.token]).unwrap();
    assert_eq!(f.state.protected_before(), Some(2));
}

#[test]
fn owner_fence_invalidates_assignments_atomically_and_snapshot_validation_rejects_tampering() {
    let mut f = Fixture::new(Limits::default(), 0);
    f.deliver(0, 0, 0, DeliveryLane::Realtime);
    f.state.invalidate_assignments(3).unwrap();
    f.state.validate().unwrap();
    assert_eq!(f.state.assignment(f.id), Err(Rejection::SessionMismatch));
    let digest = f.state.recovery_digest().unwrap();
    f.state.invalidate_assignments(3).unwrap();
    assert_eq!(f.state.recovery_digest().unwrap(), digest);
    assert_eq!(
        f.state.invalidate_assignments(2),
        Err(Rejection::StaleGeneration)
    );
    let mut snapshot = serde_json::to_value(&f.state).unwrap();
    snapshot
        .as_object_mut()
        .unwrap()
        .insert("total_slots".into(), serde_json::json!(0));
    let tampered: SubscriptionState = serde_json::from_value(snapshot).unwrap();
    assert_eq!(tampered.validate(), Err(Rejection::InvalidSnapshot));
}

#[test]
fn deterministic_ack_permutations_preserve_monotonic_frontier_and_bounded_snapshots() {
    let mut f = Fixture::new(
        Limits {
            max_window_slots: 8,
            max_in_flight_messages: 8,
            ..Limits::default()
        },
        0,
    );
    let mut seed = 0x3fa2_b611_u64;
    for base in (0..800).step_by(8) {
        let mut tokens: Vec<_> = (base..base + 8)
            .map(|offset| f.deliver(offset, 0, 0, DeliveryLane::Realtime))
            .collect();
        let mut acknowledged = BTreeSet::new();
        let mut frontier = base;
        while !tokens.is_empty() {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let index = (seed as usize) % tokens.len();
            let receipt = tokens.remove(index);
            acknowledged.insert(receipt.offset);
            while acknowledged.contains(&frontier) {
                frontier += 1;
            }
            assert_eq!(
                f.ack(vec![receipt.token]).unwrap().acknowledged_before,
                frontier
            );
            let image = postcard::to_stdvec(&f.state).unwrap();
            assert!(image.len() < 4_096, "Compacted ACK history remains bounded");
            f.state = postcard::from_bytes(&image).unwrap();
            f.state.validate().unwrap();
        }
        assert_eq!(f.state.diagnostics(f.id).unwrap().window_slots, 0);
    }
}

#[test]
fn a_bad_token_rejects_the_entire_batch_without_confirming_the_valid_subset() {
    let mut f = Fixture::new(Limits::default(), 0);
    let zero = f.deliver(0, 0, 0, DeliveryLane::Realtime);
    let one = f.deliver(1, 0, 0, DeliveryLane::Realtime);
    let mut bytes = one.token.as_bytes().to_vec();
    *bytes.last_mut().unwrap() ^= 1;
    assert_eq!(
        f.ack(vec![
            zero.token.clone(),
            DeliveryToken::try_from(bytes).unwrap()
        ]),
        Err(Rejection::InvalidToken)
    );
    assert_eq!(f.state.protected_before(), Some(0));
    assert_eq!(f.state.diagnostics(f.id).unwrap().in_flight_messages, 2);
    assert_eq!(
        f.ack(vec![zero.token, one.token])
            .unwrap()
            .acknowledged_before,
        2
    );
}

#[test]
fn aggregate_byte_admission_is_bounded_across_subscriptions_and_ack_releases_it() {
    let mut f = Fixture::new(
        Limits {
            max_in_flight_bytes: 20,
            max_total_in_flight_bytes: 20,
            ..Limits::default()
        },
        0,
    );
    let original_id = f.id;
    let original_session = f.session;
    let first = f.deliver(0, 0, 0, DeliveryLane::Realtime);
    let second_id = Id::random().unwrap();
    f.apply(Command::Create {
        subscription_id: second_id,
        operation_id: Id::random().unwrap(),
        start_offset: 0,
        signing_key: SigningKey::try_from([87; 32]).unwrap(),
    })
    .unwrap();
    f.id = second_id;
    f.session = Id::random().unwrap();
    f.apply(Command::Claim {
        subscription_id: f.id,
        expected: Generations {
            cursor: 1,
            assignment: 0,
        },
        session_id: f.session,
        replace_session_id: None,
        cold_before: 0,
    })
    .unwrap();
    f.deliver(0, 0, 0, DeliveryLane::Realtime);
    let blocked = f.delivery_command(1, 0, 0, DeliveryLane::Realtime);
    assert_eq!(f.apply(blocked), Err(Rejection::InFlightBytes));
    let second_session = f.session;
    f.id = original_id;
    f.session = original_session;
    f.ack(vec![first.token]).unwrap();
    f.id = second_id;
    f.session = second_session;
    f.deliver(1, 0, 0, DeliveryLane::Realtime);
    assert_eq!(f.state.diagnostics(f.id).unwrap().total_in_flight_bytes, 20);
}

#[test]
fn retired_identities_consume_the_finite_fence_budget_and_claim_retries_keep_the_assignment() {
    let mut f = Fixture::new(
        Limits {
            max_subscription_identities: 1,
            ..Limits::default()
        },
        0,
    );
    let original = f.state.assignment(f.id).unwrap();
    let repeated = f
        .apply(Command::Claim {
            subscription_id: f.id,
            expected: Generations {
                cursor: 1,
                assignment: 0,
            },
            session_id: f.session,
            replace_session_id: None,
            cold_before: 0,
        })
        .unwrap();
    assert_eq!(repeated, Outcome::Assigned(original));
    f.apply(Command::Delete {
        subscription_id: f.id,
        expected_cursor_generation: 1,
    })
    .unwrap();
    assert_eq!(
        f.apply(Command::Create {
            subscription_id: Id::random().unwrap(),
            operation_id: Id::random().unwrap(),
            start_offset: 0,
            signing_key: SigningKey::try_from([87; 32]).unwrap()
        }),
        Err(Rejection::IdentityBudget)
    );
}
