use bytes::Bytes;
use tidemq::{
    archive::{CommittedPartition, Pack, PackIdentity, PackLimits, Record, validate_pack},
    budget::MemoryBudget,
};
use tidemq_protocol::{Id, state::OffsetRange};
use tidemq_transport::budget::MemoryLimits;

fn budget() -> MemoryBudget {
    MemoryBudget::new(MemoryLimits {
        total_bytes: 16 * 1024 * 1024,
        control_reserved_bytes: 1024 * 1024,
    })
    .unwrap()
}

fn record(offset: u64) -> Record {
    Record {
        offset,
        key: Bytes::from_static(b"private-key"),
        body: Bytes::from_static(b"private-body"),
    }
}

#[test]
fn pack_round_trips_multiple_committed_partitions_and_redacts_payloads() {
    let topic = Id::new_v7().unwrap();
    let identity = PackIdentity {
        cluster_id: Id::new_v7().unwrap(),
        group_id: 9,
        object_id: Id::new_v7().unwrap(),
    };
    let records = [record(7), record(8)];
    let second = [record(30)];
    let partitions = [
        CommittedPartition {
            group_id: 9,
            topic_id: topic,
            partition_id: 0,
            offsets: OffsetRange::new(7, 9).unwrap(),
            committed_end: 12,
            records: &records,
        },
        CommittedPartition {
            group_id: 9,
            topic_id: topic,
            partition_id: 1,
            offsets: OffsetRange::new(30, 31).unwrap(),
            committed_end: 31,
            records: &second,
        },
    ];
    let pack = Pack::build(identity, &partitions, PackLimits::default(), &budget()).unwrap();
    pack.validate(PackLimits::default()).unwrap();
    assert_eq!(pack.descriptor().partitions.len(), 2);
    assert!(!format!("{pack:?} {records:?}").contains("private-body"));
    let mut damaged = pack.bytes().to_vec();
    damaged[80 + 68] ^= 1;
    assert!(
        validate_pack(
            pack.descriptor(),
            &Bytes::from(damaged.clone()),
            PackLimits::default()
        )
        .is_err()
    );
    let mut descriptor = pack.descriptor().clone();
    descriptor.checksum = *blake3::hash(&damaged).as_bytes();
    assert!(validate_pack(&descriptor, &Bytes::from(damaged), PackLimits::default()).is_err());
}

#[test]
fn rejects_uncommitted_cross_group_duplicate_gapped_and_oversized_records() {
    let identity = PackIdentity {
        cluster_id: Id::new_v7().unwrap(),
        group_id: 9,
        object_id: Id::new_v7().unwrap(),
    };
    let topic = Id::new_v7().unwrap();
    let records = [record(1), record(3)];
    let mut part = CommittedPartition {
        group_id: 9,
        topic_id: topic,
        partition_id: 0,
        offsets: OffsetRange::new(1, 3).unwrap(),
        committed_end: 3,
        records: &records,
    };
    assert!(Pack::build(identity, &[part], PackLimits::default(), &budget()).is_err());
    let records = [record(1), record(2)];
    part = CommittedPartition {
        group_id: 9,
        topic_id: topic,
        partition_id: 0,
        offsets: OffsetRange::new(1, 3).unwrap(),
        committed_end: 2,
        records: &records,
    };
    assert!(Pack::build(identity, &[part], PackLimits::default(), &budget()).is_err());
    part = CommittedPartition {
        group_id: 8,
        topic_id: topic,
        partition_id: 0,
        offsets: OffsetRange::new(1, 3).unwrap(),
        committed_end: 3,
        records: &records,
    };
    assert!(Pack::build(identity, &[part], PackLimits::default(), &budget()).is_err());
    part = CommittedPartition {
        group_id: 9,
        topic_id: topic,
        partition_id: 0,
        offsets: OffsetRange::new(1, 3).unwrap(),
        committed_end: 3,
        records: &records,
    };
    assert!(
        Pack::build(
            identity,
            &[part],
            PackLimits {
                max_message_bytes: 1,
                ..PackLimits::default()
            },
            &budget()
        )
        .is_err()
    );
}

#[test]
fn encoded_bytes_keep_memory_admission_until_every_clone_is_released() {
    let budget = budget();
    let before = budget.available_data_bytes();
    let identity = PackIdentity {
        cluster_id: Id::new_v7().unwrap(),
        group_id: 9,
        object_id: Id::new_v7().unwrap(),
    };
    let records = [record(1)];
    let pack = Pack::build(
        identity,
        &[CommittedPartition {
            group_id: 9,
            topic_id: Id::new_v7().unwrap(),
            partition_id: 0,
            offsets: OffsetRange::new(1, 2).unwrap(),
            committed_end: 2,
            records: &records,
        }],
        PackLimits::default(),
        &budget,
    )
    .unwrap();
    let bytes = pack.bytes();
    drop(pack);
    assert!(budget.available_data_bytes() < before);
    drop(bytes);
    assert_eq!(budget.available_data_bytes(), before);
}
