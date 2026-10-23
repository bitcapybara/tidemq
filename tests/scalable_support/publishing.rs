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
