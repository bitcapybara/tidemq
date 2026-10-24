use super::*;

#[test]
fn shared_read_budget_limits_deliveries_and_replay_keeps_the_original_byte_charge() {
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
            1,
            &[MemberGrant {
                member: id,
                client: id,
                credential: id,
                epoch: 1,
                expires_ms: 15000,
                lanes: Vec::new(),
                max_messages: 64,
                max_bytes: 8192,
            }],
            0,
        )
        .unwrap();
    let request = GroupRead {
        member: id,
        epoch: 1,
        sequence: 1,
        now_ms: 1,
        max_messages: 64,
        max_bytes: 8192,
    };
    let candidates: Vec<_> = (0..8)
        .map(|offset| Candidate {
            offset,
            hash: 0,
            bytes: if offset == 0 { 64 } else { 32 },
        })
        .collect();
    assert_eq!(cursor.first_delivery_bytes(&request, &candidates), Some(64));
    let result = cursor
        .read_admitted(
            request,
            &candidates,
            ReadBudget {
                bytes: 1024,
                messages: 2,
                results: 64,
            },
        )
        .unwrap();
    assert_eq!(result.deliveries.len(), 2);
    assert_eq!(result.delivery_bytes, 96);
    assert_eq!(
        cursor
            .read_admitted(
                request,
                &[],
                ReadBudget {
                    bytes: 0,
                    messages: 0,
                    results: 0,
                }
            )
            .unwrap(),
        result
    );
    cursor.validate().unwrap();
}

#[test]
fn retiring_a_read_releases_proof_capacity_without_acknowledging_messages() {
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
            1,
            &[MemberGrant {
                member: id,
                client: id,
                credential: id,
                epoch: 1,
                expires_ms: 15000,
                lanes: Vec::new(),
                max_messages: 64,
                max_bytes: 8192,
            }],
            0,
        )
        .unwrap();
    let request = GroupRead {
        member: id,
        epoch: 1,
        sequence: 1,
        now_ms: 1,
        max_messages: 64,
        max_bytes: 8192,
    };
    let candidates: Vec<_> = (0..64)
        .map(|offset| Candidate {
            offset,
            hash: 0,
            bytes: 32,
        })
        .collect();
    let original = cursor.read(request, &candidates).unwrap();
    assert_eq!(original.deliveries.len(), 64);
    let retained = |cursor: &GroupCursor| {
        cursor
            .members
            .get(&(id, 1))
            .unwrap()
            .read
            .as_ref()
            .unwrap()
            .deliveries
            .capacity()
    };
    assert!(retained(&cursor) >= 64);
    cursor.retire_read(id, 1, 1).unwrap();
    assert_eq!(retained(&cursor), 0);
    assert_eq!(cursor.frontier, 0);
    assert_eq!(cursor.in_flight_bytes(), 64 * 32);
    assert_eq!(cursor.read(request, &[]), Err(Rejection::OutcomeExpired));
    cursor.retire_read(id, 1, 1).unwrap();
    for (sequence, proof) in original.deliveries.into_iter().enumerate() {
        assert_eq!(
            cursor
                .acknowledge(GroupAck {
                    member: id,
                    epoch: 1,
                    sequence: sequence as u64 + 1,
                    now_ms: 2,
                    model: SubscriptionKind::Queue,
                    proofs: vec![proof],
                    nack_delay_ms: None,
                })
                .unwrap(),
            AckDisposition::Acknowledged
        );
    }
    assert_eq!(cursor.frontier, 64);
    assert_eq!(cursor.in_flight_bytes(), 0);
    let restored: GroupCursor =
        postcard::from_bytes(&postcard::to_stdvec(&cursor).unwrap()).unwrap();
    restored.validate().unwrap();
}
