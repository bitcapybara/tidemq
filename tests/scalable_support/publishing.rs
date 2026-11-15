use super::*;
use bytes::Bytes;
use tidemq_protocol::topic::{MessageMetadata, SendIdentity, message_digest, routing_hash};

fn permit(topic: Id) -> PublisherPermit {
    PublisherPermit {
        topic,
        producer: Id::new_v7().unwrap(),
        registration: 1,
        client: Id::new_v7().unwrap(),
        credential: Id::new_v7().unwrap(),
        expires_ms: 1_000,
        live: true,
    }
}

fn message(permit: &PublisherPermit, sequence: u64, key: Option<&[u8]>) -> PublishRequest {
    let identity = SendIdentity {
        topic: permit.topic,
        producer: permit.producer,
        registration: permit.registration,
        sequence,
    };
    let properties = [("version".into(), Bytes::from_static(b"1"))].into();
    let digest = message_digest(identity, key, b"payload", &properties).unwrap();
    let context = MessageMetadata {
        key_present: key.is_some(),
        routing_hash: routing_hash(key, identity.producer, sequence).unwrap(),
        properties,
    };
    PublishRequest {
        identity,
        digest,
        retired_before: 1,
        message: MessageData {
            key: Bytes::copy_from_slice(key.unwrap_or_default()),
            body: Bytes::from_static(b"payload"),
            context: postcard::to_stdvec(&context).unwrap().into(),
        },
    }
}

fn publish(
    state: &mut ProducerState,
    permit: &PublisherPermit,
    request: PublishRequest,
    now: u64,
) -> Result<PublishReceipt> {
    state.publish_admitted(permit, request, now, u64::MAX, usize::MAX, |_, _, _| Ok(()))
}

#[test]
fn publish_batch_commits_each_receipt_and_replays_duplicates() {
    let topic = Id::new_v7().unwrap();
    let permit = permit(topic);
    let mut state = ProducerState::new(topic, 0, ProducerLimits::default()).unwrap();
    let requests = vec![message(&permit, 1, None), message(&permit, 2, Some(b"key"))];
    let receipts = state
        .publish_batch_admitted(
            &permit,
            requests.clone(),
            1,
            u64::MAX,
            usize::MAX,
            |_, _, _| Ok(()),
        )
        .unwrap();
    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].offset, 0);
    assert_eq!(receipts[1].offset, 1);
    assert!(receipts.iter().all(|receipt| !receipt.duplicate));
    let duplicates = state
        .publish_batch_admitted(&permit, requests, 2, u64::MAX, usize::MAX, |_, _, _| Ok(()))
        .unwrap();
    assert_eq!(duplicates.len(), 2);
    assert_eq!(duplicates[0].offset, 0);
    assert_eq!(duplicates[1].offset, 1);
    assert!(duplicates.iter().all(|receipt| receipt.duplicate));
    assert_eq!(state.committed_end(), 2);

    let duplicate_sequence = message(&permit, 3, None);
    let aliases = state
        .publish_batch_admitted(
            &permit,
            vec![duplicate_sequence.clone(), duplicate_sequence.clone()],
            3,
            u64::MAX,
            usize::MAX,
            |_, _, _| Ok(()),
        )
        .unwrap();
    assert_eq!(aliases.len(), 2);
    assert_eq!(aliases[0].offset, 2);
    assert_eq!(aliases[1].offset, 2);
    assert!(!aliases[0].duplicate);
    assert!(aliases[1].duplicate);

    let mut expired = permit.clone();
    expired.expires_ms = 1;
    let replay = state
        .publish_batch_admitted(
            &expired,
            vec![duplicate_sequence],
            2,
            u64::MAX,
            usize::MAX,
            |_, _, _| Ok(()),
        )
        .unwrap();
    assert!(replay[0].duplicate);
    assert_eq!(state.committed_end(), 3);
    state.validate().unwrap();
}

#[test]
fn first_publish_batch_respects_result_capacity() {
    let topic = Id::new_v7().unwrap();
    let permit = permit(topic);
    let limits = ProducerLimits::default();
    let mut state = ProducerState::new(topic, 0, limits.clone()).unwrap();
    let requests = (1..=limits.max_results_per_session as u64 + 1)
        .map(|sequence| message(&permit, sequence, None))
        .collect();

    assert_eq!(
        state.publish_batch_admitted(&permit, requests, 1, u64::MAX, usize::MAX, |_, _, _| Ok(())),
        Err(Rejection::SessionCapacity)
    );
    state.validate().unwrap();
}

#[test]
fn sparse_global_sequences_are_not_gaps_and_original_results_survive_permit_expiry() {
    let topic = Id::new_v7().unwrap();
    let permit = permit(topic);
    let mut state = ProducerState::new(topic, 0, ProducerLimits::default()).unwrap();
    let early = message(&permit, 1, None);
    let distant = message(&permit, 1_000_000, Some(b""));
    assert_eq!(
        publish(&mut state, &permit, distant.clone(), 1)
            .unwrap()
            .offset,
        0
    );
    assert_eq!(
        publish(&mut state, &permit, early.clone(), 2)
            .unwrap()
            .offset,
        1
    );
    assert_eq!(state.committed_end(), 2);
    let duplicate = publish(&mut state, &permit, distant, 2_000).unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.offset, 0);
    state.retire_publishes(&permit, 2).unwrap();
    assert_eq!(
        publish(&mut state, &permit, early, 2_000),
        Err(Rejection::ResultExpired { before: 2 })
    );
    assert_eq!(state.committed_end(), 2);
    state.validate().unwrap();
    let recovered: ProducerState =
        postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
    assert_eq!(
        state.recovery_digest().unwrap(),
        recovered.recovery_digest().unwrap()
    );
}

#[test]
fn result_capacity_never_silently_evicts_unclaimed_results_or_accepts_changed_content() {
    let topic = Id::new_v7().unwrap();
    let permit = permit(topic);
    let mut state = ProducerState::new(
        topic,
        0,
        ProducerLimits {
            max_results_per_session: 2,
            ..ProducerLimits::default()
        },
    )
    .unwrap();
    let first = message(&permit, 1, Some(b"key"));
    publish(&mut state, &permit, first.clone(), 1).unwrap();
    publish(&mut state, &permit, message(&permit, 1_000_000, None), 2).unwrap();
    assert_eq!(
        publish(&mut state, &permit, message(&permit, 3, None), 3),
        Err(Rejection::SessionCapacity)
    );
    assert!(publish(&mut state, &permit, first, 4).unwrap().duplicate);
    let changed = message(&permit, 1, Some(b"different"));
    assert_eq!(
        publish(&mut state, &permit, changed, 4),
        Err(Rejection::SequenceConflict)
    );
    let mut next = message(&permit, 3, None);
    next.retired_before = 2;
    publish(&mut state, &permit, next, 5).unwrap();
    assert_eq!(state.committed_end(), 3);
    state.validate().unwrap();
}

#[test]
fn authority_confirmed_cleanup_preserves_the_last_global_recovery_window_and_fences_late_commands()
{
    let topic = Id::new_v7().unwrap();
    let original = permit(topic);
    let mut state = ProducerState::new(topic, 0, ProducerLimits::default()).unwrap();
    let request = message(&original, 1, None);
    publish(&mut state, &original, request.clone(), 1).unwrap();
    assert_eq!(state.publisher_cleanup_candidates(700_000).count(), 1);
    assert_eq!(
        state.cleanup_publisher(&original, 700_000),
        Err(Rejection::SessionConflict)
    );
    let mut renewed = original.clone();
    renewed.expires_ms = 1_000_000;
    renewed.live = false;
    state.cleanup_publisher(&renewed, 700_000).unwrap();
    assert!(
        publish(&mut state, &original, request.clone(), 700_000)
            .unwrap()
            .duplicate
    );
    assert_eq!(state.publisher_cleanup_candidates(700_000).count(), 0);
    state.cleanup_publisher(&renewed, 1_620_001).unwrap();
    assert_eq!(state.publish_state_bytes(), 0);
    assert_eq!(
        publish(&mut state, &original, request, 1),
        Err(Rejection::SessionExpired)
    );
    assert_eq!(state.committed_end(), 1);
    state.validate().unwrap();
}

#[test]
fn closing_an_unseen_publisher_fences_late_sends_and_reaping_keeps_the_clock_fence() {
    let topic = Id::new_v7().unwrap();
    let mut permit = permit(topic);
    let mut state = ProducerState::new(topic, 0, ProducerLimits::default()).unwrap();
    permit.live = false;
    state.retire_publishes(&permit, 1).unwrap();
    assert!(state.publisher_closed(message(&permit, 1, None).identity));
    permit.live = true;
    assert_eq!(
        publish(&mut state, &permit, message(&permit, 1, None), 2),
        Err(Rejection::SessionConflict)
    );
    assert_eq!(
        publish(&mut state, &permit, message(&permit, 1, None), 700_000),
        Err(Rejection::SessionExpired)
    );
    assert!(!state.publisher_closed(message(&permit, 1, None).identity));
    assert_eq!(
        publish(&mut state, &permit, message(&permit, 1, None), 1),
        Err(Rejection::SessionExpired)
    );
    assert_eq!(state.committed_end(), 0);
    state.validate().unwrap();
}
