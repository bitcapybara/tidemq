use tidemq::subscription::{
    AckDisposition, Candidate, GroupAck, GroupCursor, GroupIdentity, GroupRead, Limits,
    MemberGrant, Rejection,
};
use tidemq_protocol::{
    Id,
    topic::{HashRange, SubscriptionKind},
};

fn id() -> Id {
    Id::new_v7().unwrap()
}
fn cursor(model: SubscriptionKind) -> GroupCursor {
    GroupCursor::new(
        GroupIdentity {
            topic: id(),
            shard: id(),
            subscription: id(),
        },
        model,
        0,
        [42; 32],
        Limits::default(),
    )
    .unwrap()
}
fn grant(member: Id, lanes: Vec<HashRange>) -> MemberGrant {
    MemberGrant {
        member,
        client: member,
        credential: member,
        epoch: 1,
        expires_ms: 15000,
        lanes,
        max_messages: 64,
        max_bytes: 1024,
    }
}
fn read(member: Id, sequence: u64, now_ms: u64) -> GroupRead {
    GroupRead {
        member,
        epoch: 1,
        sequence,
        now_ms,
        max_messages: 16,
        max_bytes: 1024,
    }
}

#[test]
fn stream_acknowledges_only_the_receiving_members_sparse_prefix() {
    let mut cursor = cursor(SubscriptionKind::Stream);
    let (left, right) = HashRange::FULL.split(100).unwrap();
    let first = id();
    let second = id();
    cursor
        .install_members(
            1,
            &[grant(first, vec![left]), grant(second, vec![right])],
            0,
        )
        .unwrap();
    let candidates: Vec<_> = [0, 100, 0, 100]
        .into_iter()
        .enumerate()
        .map(|(offset, hash)| Candidate {
            offset: offset as u64,
            hash,
            bytes: 1,
        })
        .collect();
    let first_read = cursor.read(read(first, 1, 1), &candidates).unwrap();
    let second_read = cursor.read(read(second, 1, 1), &[]).unwrap();
    assert_eq!(
        first_read
            .deliveries
            .iter()
            .map(|proof| proof.offset)
            .collect::<Vec<_>>(),
        [0, 2]
    );
    assert_eq!(
        second_read
            .deliveries
            .iter()
            .map(|proof| proof.offset)
            .collect::<Vec<_>>(),
        [1, 3]
    );
    let ack = GroupAck {
        member: first,
        epoch: 1,
        sequence: 1,
        now_ms: 2,
        model: SubscriptionKind::Stream,
        proofs: first_read.deliveries,
        nack_delay_ms: None,
    };
    assert_eq!(
        cursor.acknowledge(ack.clone()).unwrap(),
        AckDisposition::Acknowledged
    );
    assert_eq!(cursor.acknowledged_before(), 1);
    assert_eq!(
        cursor.acknowledge(ack).unwrap(),
        AckDisposition::Acknowledged
    );
    assert_eq!(
        cursor.protected_before(),
        0,
        "An unretired Read result must still protect its replay body"
    );
    cursor.retire_read(first, 1, 1).unwrap();
    cursor.retire_read(second, 1, 1).unwrap();
    cursor
        .acknowledge(GroupAck {
            member: second,
            epoch: 1,
            sequence: 1,
            now_ms: 3,
            model: SubscriptionKind::Stream,
            proofs: second_read.deliveries,
            nack_delay_ms: None,
        })
        .unwrap();
    assert_eq!(cursor.acknowledged_before(), 4);
    cursor.validate().unwrap();
}

#[test]
fn queue_nack_replays_the_original_schedule_after_a_new_attempt_is_delivered() {
    let mut cursor = cursor(SubscriptionKind::Queue);
    let first = id();
    let second = id();
    cursor
        .install_members(1, &[grant(first, vec![]), grant(second, vec![])], 0)
        .unwrap();
    let original = cursor
        .read(
            read(first, 1, 1),
            &[Candidate {
                offset: 0,
                hash: 0,
                bytes: 1,
            }],
        )
        .unwrap();
    cursor.retire_read(first, 1, 1).unwrap();
    let nack = GroupAck {
        member: first,
        epoch: 1,
        sequence: 1,
        now_ms: 100,
        model: SubscriptionKind::Queue,
        proofs: original.deliveries.clone(),
        nack_delay_ms: Some(50),
    };
    let result = cursor.acknowledge(nack.clone()).unwrap();
    assert_eq!(result, AckDisposition::RedeliveryScheduled { due_ms: 150 });
    assert!(
        cursor
            .read(read(second, 1, 149), &[])
            .unwrap()
            .deliveries
            .is_empty()
    );
    cursor.retire_read(second, 1, 1).unwrap();
    let redelivery = cursor.read(read(second, 2, 150), &[]).unwrap();
    assert_eq!(redelivery.deliveries.first().unwrap().attempt, 2);
    let mut repeated = nack;
    repeated.now_ms = 1000;
    assert_eq!(cursor.acknowledge(repeated).unwrap(), result);
    assert_eq!(
        cursor.acknowledge(GroupAck {
            member: first,
            epoch: 1,
            sequence: 2,
            now_ms: 1000,
            model: SubscriptionKind::Queue,
            proofs: original.deliveries,
            nack_delay_ms: None
        }),
        Err(Rejection::StaleAttempt)
    );
    cursor.validate().unwrap();
}

#[test]
fn lost_read_reply_replays_exactly_once_and_retirement_never_issues_another_attempt() {
    let mut cursor = cursor(SubscriptionKind::Queue);
    let member = id();
    cursor
        .install_members(1, &[grant(member, vec![])], 0)
        .unwrap();
    let request = read(member, 1, 1);
    let original = cursor
        .read(
            request,
            &[Candidate {
                offset: 0,
                hash: 0,
                bytes: 1,
            }],
        )
        .unwrap();
    assert_eq!(cursor.read(read(member, 1, 100), &[]).unwrap(), original);
    let mut changed = request;
    changed.max_messages = 1;
    assert_eq!(cursor.read(changed, &[]), Err(Rejection::IdentityConflict));
    cursor.retire_read(member, 1, 1).unwrap();
    assert_eq!(cursor.read(request, &[]), Err(Rejection::OutcomeExpired));
    let encoded = postcard::to_stdvec(&cursor).unwrap();
    let restored: GroupCursor = postcard::from_bytes(&encoded).unwrap();
    restored.validate().unwrap();
}

#[test]
fn range_handoff_fences_old_attempts_before_new_owner_receives_in_source_order() {
    let mut cursor = cursor(SubscriptionKind::Stream);
    let first = id();
    let second = id();
    cursor
        .install_members(1, &[grant(first, vec![HashRange::FULL])], 0)
        .unwrap();
    let original = cursor
        .read(
            read(first, 1, 1),
            &[Candidate {
                offset: 0,
                hash: 7,
                bytes: 1,
            }],
        )
        .unwrap();
    cursor
        .install_members(2, &[grant(second, vec![HashRange::FULL])], 2)
        .unwrap();
    let redelivered = cursor
        .read(
            read(second, 1, 3),
            &[Candidate {
                offset: 1,
                hash: 7,
                bytes: 1,
            }],
        )
        .unwrap();
    assert_eq!(
        redelivered
            .deliveries
            .iter()
            .map(|proof| (proof.offset, proof.attempt))
            .collect::<Vec<_>>(),
        [(0, 2), (1, 1)]
    );
    assert_eq!(
        cursor.acknowledge(GroupAck {
            member: first,
            epoch: 1,
            sequence: 1,
            now_ms: 3,
            model: SubscriptionKind::Stream,
            proofs: original.deliveries,
            nack_delay_ms: None
        }),
        Err(Rejection::SessionMismatch)
    );
    cursor.validate().unwrap();
}

#[test]
fn bad_token_rejects_the_entire_ack_batch_and_ack_holes_fill_the_bounded_window() {
    let limits = Limits {
        max_window_slots: 2,
        max_in_flight_messages: 2,
        ..Limits::default()
    };
    let mut cursor = GroupCursor::new(
        GroupIdentity {
            topic: id(),
            shard: id(),
            subscription: id(),
        },
        SubscriptionKind::Stream,
        0,
        [9; 32],
        limits,
    )
    .unwrap();
    let member = id();
    let mut bounded_grant = grant(member, vec![HashRange::FULL]);
    bounded_grant.max_messages = 2;
    cursor.install_members(1, &[bounded_grant], 0).unwrap();
    let original = cursor
        .read(
            GroupRead {
                max_messages: 2,
                ..read(member, 1, 1)
            },
            &[
                Candidate {
                    offset: 0,
                    hash: 0,
                    bytes: 1,
                },
                Candidate {
                    offset: 1,
                    hash: 0,
                    bytes: 1,
                },
            ],
        )
        .unwrap();
    cursor.retire_read(member, 1, 1).unwrap();
    let mut proofs = original.deliveries.clone();
    *proofs.get_mut(1).unwrap().mac.first_mut().unwrap() ^= 1;
    assert_eq!(
        cursor.acknowledge(GroupAck {
            member,
            epoch: 1,
            sequence: 1,
            now_ms: 2,
            model: SubscriptionKind::Stream,
            proofs,
            nack_delay_ms: None
        }),
        Err(Rejection::StaleAttempt)
    );
    assert_eq!(cursor.acknowledged_before(), 0);
    assert!(
        cursor
            .read(
                GroupRead {
                    max_messages: 2,
                    ..read(member, 2, 3)
                },
                &[Candidate {
                    offset: 2,
                    hash: 0,
                    bytes: 1
                }]
            )
            .unwrap()
            .deliveries
            .is_empty()
    );
    assert_eq!(cursor.next_offset(), 2);
    cursor
        .acknowledge(GroupAck {
            member,
            epoch: 1,
            sequence: 2,
            now_ms: 4,
            model: SubscriptionKind::Stream,
            proofs: original.deliveries,
            nack_delay_ms: None,
        })
        .unwrap();
    assert_eq!(cursor.acknowledged_before(), 2);
    cursor.validate().unwrap();
}
