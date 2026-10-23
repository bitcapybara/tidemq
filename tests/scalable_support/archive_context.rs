use super::*;
use tidemq_protocol::topic::MessageMetadata;

#[test]
fn immutable_records_retain_absent_keys_empty_keys_hashes_and_properties() {
    let metadata = |key_present, routing_hash| MessageMetadata {
        key_present,
        routing_hash,
        properties: [("content-type".into(), Bytes::from_static(b"private-value"))].into(),
    };
    let contexts = [metadata(false, 17), metadata(true, 29)];
    let records: Vec<_> = contexts
        .iter()
        .enumerate()
        .map(|(offset, context)| Record {
            offset: offset as u64,
            key: Bytes::new(),
            body: Bytes::from_static(b"payload"),
            context: postcard::to_stdvec(context).unwrap().into(),
        })
        .collect();
    let topic = Id::new_v7().unwrap();
    let budget = MemoryBudget::new(tidemq_transport::budget::MemoryLimits {
        total_bytes: 4 * 1024 * 1024,
        control_reserved_bytes: 512 * 1024,
    })
    .unwrap();
    let pack = Pack::build(
        PackIdentity {
            cluster_id: Id::new_v7().unwrap(),
            group_id: 7,
            object_id: Id::new_v7().unwrap(),
        },
        &[CommittedPartition {
            group_id: 7,
            topic_id: topic,
            partition_id: 0,
            offsets: OffsetRange::new(0, 2).unwrap(),
            committed_end: 2,
            records: &records,
        }],
        PackLimits::default(),
        &budget,
    )
    .unwrap();
    let index = pack.descriptor().partitions.first().unwrap();
    let bytes = view(
        &pack.bytes(),
        index.byte_start as usize,
        index.byte_end as usize,
    )
    .unwrap();
    let restored = decode_records(index, bytes, PackLimits::default(), &budget).unwrap();
    for (record, expected) in restored.iter().zip(contexts) {
        assert!(record.key().is_empty());
        assert_eq!(record.body(), b"payload");
        let context: MessageMetadata = postcard::from_bytes(record.context()).unwrap();
        assert_eq!(context, expected);
        assert!(!format!("{context:?}").contains("private-value"));
    }
}
