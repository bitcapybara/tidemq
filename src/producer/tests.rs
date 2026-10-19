use bytes::Bytes;
use tidemq_protocol::Id;

use super::*;

fn state(limits: ProducerLimits) -> ProducerState {
    ProducerState::new(Id::random().unwrap(), 3, limits).unwrap()
}

fn register(state: &mut ProducerState, now: u64) -> SessionInfo {
    let epoch = state.next_registration_epoch();
    state
        .register(
            now,
            RegisterProducer {
                producer_id: Id::random().unwrap(),
                expected_registration_epoch: epoch,
            },
        )
        .unwrap()
}

fn batch(
    state: &ProducerState,
    session: &SessionInfo,
    sequence: u64,
    bodies: &[&str],
) -> AppendBatch {
    let messages: Vec<_> = bodies
        .iter()
        .map(|body| MessageData {
            key: Bytes::from_static(b"sensitive-business-key"),
            body: Bytes::copy_from_slice(body.as_bytes()),
        })
        .collect();
    let content_digest = tidemq_protocol::producer::canonical_produce_digest_parts(
        state.topic_id(),
        state.partition_id(),
        session.producer_id,
        session.producer_epoch,
        sequence,
        messages
            .iter()
            .map(|message| (message.key.as_ref(), message.body.as_ref())),
    )
    .unwrap();
    AppendBatch {
        topic_id: state.topic_id(),
        partition_id: state.partition_id(),
        producer_id: session.producer_id,
        producer_epoch: session.producer_epoch,
        first_sequence: sequence,
        messages,
        content_digest,
    }
}

#[test]
fn original_batch_results_sequence_conflicts_and_gaps_are_deterministic() {
    let mut state = state(ProducerLimits::default());
    let session = register(&mut state, 100);
    let request = batch(&state, &session, 0, &["private-payload", "second"]);
    let result = state.append(101, request.clone()).unwrap();
    assert_eq!(result.first_offset, 0);
    assert_eq!(result.message_count, 2);
    assert!(!result.duplicate);
    let duplicate = state.append(102, request.clone()).unwrap();
    assert_eq!(duplicate.first_offset, result.first_offset);
    assert_eq!(duplicate.message_count, result.message_count);
    assert!(duplicate.duplicate);
    assert_eq!(state.committed_end(), 2);
    let conflict = batch(&state, &session, 0, &["different", "second"]);
    assert_eq!(
        state.append(103, conflict),
        Err(Rejection::SequenceConflict)
    );
    let overlap = batch(&state, &session, 1, &["second"]);
    assert_eq!(state.append(104, overlap), Err(Rejection::SequenceConflict));
    let gap = batch(&state, &session, 4, &["future"]);
    assert_eq!(
        state.append(105, gap),
        Err(Rejection::SequenceGap { expected: 2 })
    );
    assert_eq!(state.committed_end(), 2);
    let next = batch(&state, &session, 2, &["third"]);
    assert_eq!(state.append(106, next).unwrap().first_offset, 2);
    state.validate().unwrap();
}

#[test]
fn registration_cas_retirement_and_fixed_expiry_need_no_unbounded_tombstones() {
    let limits = ProducerLimits {
        max_sessions: 2,
        session_ttl_ms: 10,
        result_ttl_ms: 5,
        ..ProducerLimits::default()
    };
    let mut state = state(limits);
    let first = register(&mut state, 100);
    let retry = RegisterProducer {
        producer_id: first.producer_id,
        expected_registration_epoch: first.producer_epoch,
    };
    assert_eq!(state.register(101, retry.clone()).unwrap(), first);
    let collision = RegisterProducer {
        producer_id: Id::random().unwrap(),
        expected_registration_epoch: first.producer_epoch,
    };
    assert_eq!(
        state.register(101, collision),
        Err(Rejection::RegistrationConflict { expected: 2 })
    );
    let second = register(&mut state, 101);
    let blocked = RegisterProducer {
        producer_id: Id::random().unwrap(),
        expected_registration_epoch: state.next_registration_epoch(),
    };
    assert_eq!(
        state.register(102, blocked),
        Err(Rejection::SessionCapacity)
    );
    state
        .retire(103, first.producer_id, first.producer_epoch)
        .unwrap();
    assert_eq!(
        state.register(104, retry.clone()),
        Err(Rejection::SessionExpired)
    );
    state.advance_time(111).unwrap();
    assert_eq!(
        state.session(second.producer_id, second.producer_epoch),
        Err(Rejection::SessionExpired)
    );
    assert_eq!(state.diagnostic().active_sessions, 0);
    assert_eq!(state.next_registration_epoch(), 3);
    assert_eq!(state.register(112, retry), Err(Rejection::SessionExpired));
    for time in 112..212 {
        let session = register(&mut state, time);
        state
            .retire(time, session.producer_id, session.producer_epoch)
            .unwrap();
    }
    assert_eq!(state.diagnostic().active_sessions, 0);
    assert_eq!(state.next_registration_epoch(), 103);
    state.validate().unwrap();
}

#[test]
fn result_window_eviction_and_time_expiry_never_append_an_old_sequence() {
    let limits = ProducerLimits {
        max_results_per_session: 1,
        result_ttl_ms: 10,
        ..ProducerLimits::default()
    };
    let mut state = state(limits);
    let session = register(&mut state, 100);
    let first = batch(&state, &session, 0, &["first"]);
    let second = batch(&state, &session, 1, &["second"]);
    state.append(101, first.clone()).unwrap();
    state.append(102, second.clone()).unwrap();
    assert_eq!(
        state.append(103, first),
        Err(Rejection::ResultExpired { before: 1 })
    );
    assert_eq!(
        state.append(112, second),
        Err(Rejection::ResultExpired { before: 2 })
    );
    assert_eq!(state.committed_end(), 2);
    let third = batch(&state, &session, 2, &["third"]);
    state.append(113, third).unwrap();
    assert_eq!(
        state
            .session(session.producer_id, session.producer_epoch)
            .unwrap()
            .result_floor_sequence,
        2
    );
    state.validate().unwrap();
}

#[test]
fn content_limits_capacity_and_hot_reads_preserve_committed_ranges() {
    let limits = ProducerLimits {
        max_hot_messages: 2,
        max_batch_messages: 2,
        ..ProducerLimits::default()
    };
    let mut state = state(limits);
    let session = register(&mut state, 10);
    let mut invalid = batch(&state, &session, 0, &["first"]);
    invalid.content_digest = [0; 32];
    assert_eq!(state.append(11, invalid), Err(Rejection::DigestMismatch));
    let request = batch(&state, &session, 0, &["first", "second"]);
    state.append(12, request).unwrap();
    let overflow = batch(&state, &session, 2, &["third"]);
    assert_eq!(state.append(13, overflow), Err(Rejection::DataCapacity));
    assert_eq!(state.committed_end(), 2);
    let page = state.hot_read(1, 1, 100).unwrap();
    assert_eq!(page.first_offset, 1);
    assert_eq!(page.next_offset, 2);
    assert_eq!(
        page.messages.first().unwrap().body,
        Bytes::from_static(b"second")
    );
    assert_eq!(state.hot_read(0, 1, 1), Err(Rejection::ReadLimit));
    assert_eq!(
        state.hot_read(3, 1, 100),
        Err(Rejection::OffsetNotCommitted { end: 2 })
    );
    assert!(state.hot_read(2, 1, 100).unwrap().messages.is_empty());
    state.validate().unwrap();
}

#[test]
fn binary_snapshot_roundtrip_preserves_authority_and_safe_debug() {
    let mut state = state(ProducerLimits::default());
    let session = register(&mut state, 100);
    let request = batch(&state, &session, 0, &["private-payload"]);
    state.append(101, request.clone()).unwrap();
    let debug = format!("{state:?} {request:?}");
    assert!(!debug.contains("private-payload"));
    assert!(!debug.contains("sensitive-business-key"));
    let encoded = postcard::to_stdvec(&state).unwrap();
    let mut restored: ProducerState = postcard::from_bytes(&encoded).unwrap();
    restored.validate().unwrap();
    assert_eq!(restored, state);
    assert_eq!(restored.recovery_digest(), state.recovery_digest());
    assert!(restored.append(102, request).unwrap().duplicate);
    restored.advance_time(101).unwrap();
    assert_eq!(restored.logical_time_ms(), 102);
}

#[test]
fn compact_payload_releases_an_oversized_untrusted_backing_allocation() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Owner {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let backing = Bytes::from_owner(Owner {
        bytes: vec![b'x'; 1024 * 1024],
        dropped: dropped.clone(),
    });
    let mut state = state(ProducerLimits::default());
    let session = register(&mut state, 1);
    let mut request = batch(&state, &session, 0, &["small-body"]);
    request.messages.first_mut().unwrap().key = backing.slice(..1);
    request.messages.first_mut().unwrap().body = backing.slice(..1);
    drop(backing);
    assert!(!dropped.load(Ordering::Acquire));
    request.compact_payload().unwrap();
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(
        request.messages.first().unwrap().key,
        Bytes::from_static(b"x")
    );
    assert_eq!(
        request.messages.first().unwrap().body,
        Bytes::from_static(b"x")
    );
}

#[test]
fn failed_reservation_leaves_the_authoritative_snapshot_unchanged() {
    let mut state = state(ProducerLimits::default());
    let session = register(&mut state, 100);
    let before = postcard::to_stdvec(&state).unwrap();
    assert_eq!(
        state.reserve_append(session.producer_epoch, usize::MAX),
        Err(Rejection::Allocation)
    );
    assert_eq!(postcard::to_stdvec(&state).unwrap(), before);
    state.validate().unwrap();
}
