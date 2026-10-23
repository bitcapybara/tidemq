use super::*;
use bytes::Bytes;

fn populated(count: usize) -> ProducerState {
    let mut state =
        ProducerState::new(Id::new_v7().unwrap(), 0, ProducerLimits::default()).unwrap();
    let session = state
        .register(
            0,
            RegisterProducer {
                producer_id: Id::new_v7().unwrap(),
                expected_registration_epoch: 1,
            },
        )
        .unwrap();
    let messages = vec![
        MessageData {
            key: Bytes::from_static(b"key"),
            body: Bytes::from_static(b"body"),
            context: Bytes::new(),
        };
        count
    ];
    let content_digest = tidemq_protocol::producer::canonical_produce_digest_parts(
        state.topic_id(),
        state.partition_id(),
        session.producer_id,
        session.producer_epoch,
        0,
        messages
            .iter()
            .map(|message| (message.key.as_ref(), message.body.as_ref())),
    )
    .unwrap();
    state
        .append(
            1,
            AppendBatch {
                topic_id: state.topic_id(),
                partition_id: state.partition_id(),
                producer_id: session.producer_id,
                producer_epoch: session.producer_epoch,
                first_sequence: 0,
                messages,
                content_digest,
            },
        )
        .unwrap();
    state
}

#[test]
fn full_reclamation_releases_peak_capacity_without_shrinking_live_or_small_queues() {
    let mut small = populated(2);
    let warm = small.messages.capacity();
    small.reclaim_hot(2).unwrap();
    assert_eq!(small.messages.capacity(), warm);

    let mut large = populated(1024);
    let peak = large.messages.capacity();
    assert!(peak * size_of::<MessageData>() > 64 * 1024);
    large.reclaim_hot(1023).unwrap();
    assert_eq!(large.messages.capacity(), peak);
    assert_eq!(
        large.hot_read(1023, 1, 1024).unwrap().messages[0].body,
        b"body".as_slice()
    );
    large.reclaim_hot(1024).unwrap();
    assert_eq!(large.messages.capacity(), 0);
    assert_eq!(large.hot_bytes(), 0);
    assert_eq!(large.committed_end(), 1024);
    assert_eq!(large.local_reclaim_end(), 1024);
    large.validate().unwrap();
}
