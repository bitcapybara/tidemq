use std::collections::BTreeMap;

use bytes::Bytes;
use tidemq_protocol::{Id, state::OffsetRange};

use crate::{
    archive::{CommittedPartition, Pack, PackIdentity, PackLimits, Record},
    budget::MemoryBudget,
    metadata::PartitionKey,
    ownership::Grant,
    producer::{AppendBatch, MessageData, ProducerLimits, ProducerState, RegisterProducer},
};

use super::catalog::key;
use super::{
    Catalog, CatalogCommand, DataCommand, DataState, Rejection,
    types::{IntentPhase, PinRecord, Plan},
};

fn plan(object_id: Id, replaces: Vec<Id>, generation: u64) -> Plan {
    plan_range(object_id, replaces, generation, 0)
}

fn plan_range(object_id: Id, replaces: Vec<Id>, generation: u64, offset: u64) -> Plan {
    let cluster = "11111111-1111-4111-8111-111111111111".parse().unwrap();
    let topic = "22222222-2222-4222-8222-222222222222".parse().unwrap();
    let partition = PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let record = Record {
        offset,
        key: Bytes::from_static(b"k"),
        body: Bytes::from_static(b"body"),
    };
    let pack = Pack::build(
        PackIdentity {
            cluster_id: cluster,
            group_id: 9,
            object_id,
        },
        &[CommittedPartition {
            group_id: 9,
            topic_id: topic,
            partition_id: 0,
            offsets: OffsetRange::new(offset, offset + 1).unwrap(),
            committed_end: offset + 1,
            records: &[record],
        }],
        PackLimits::default(),
        &MemoryBudget::new(&crate::config::MemoryConfig::default()).unwrap(),
    )
    .unwrap();
    Plan {
        descriptor: pack.descriptor().clone(),
        grants: vec![Grant {
            cluster_id: cluster,
            partition,
            group_id: 9,
            operation_id: Id::new_v7().unwrap(),
            epoch: 1,
            owner_node: 1,
            expected_route_generation: 0,
            metadata_revision: 1,
        }],
        expected_generations: BTreeMap::from([(partition, generation)]),
        replaces,
    }
}

#[test]
fn canceled_unseen_intent_cannot_be_authorized_after_snapshot_roundtrip() {
    let plan = plan(Id::new_v7().unwrap(), vec![], 0);
    let id = plan.descriptor.identity.object_id;
    let mut catalog = Catalog::default();
    let mut data = DataState::default();
    catalog
        .apply(CatalogCommand::Register(plan.clone()))
        .unwrap();
    catalog
        .apply(CatalogCommand::RequestCancel { object_id: id })
        .unwrap();
    data.apply(DataCommand::Abort(plan.clone())).unwrap();
    catalog
        .apply(CatalogCommand::FinishCancel {
            object_id: id,
            data_generation: 2,
        })
        .unwrap();
    catalog
        .apply(CatalogCommand::BeginDelete { object_id: id })
        .unwrap();
    catalog
        .apply(CatalogCommand::Deleted { object_id: id })
        .unwrap();
    let mut restored: DataState =
        postcard::from_bytes(&postcard::to_stdvec(&data).unwrap()).unwrap();
    assert_eq!(
        restored.apply(DataCommand::Authorize(plan.clone())),
        Err(Rejection::Aborted)
    );
    let mut catalog: Catalog =
        postcard::from_bytes(&postcard::to_stdvec(&catalog).unwrap()).unwrap();
    assert_eq!(
        catalog.apply(CatalogCommand::Publish { object_id: id }),
        Err(Rejection::NotPublished)
    );
    assert_eq!(
        catalog.apply(CatalogCommand::Pin(PinRecord {
            id: Id::new_v7().unwrap(),
            group_id: 9,
            objects: vec![id],
            ttl_millis: 50
        })),
        Err(Rejection::References)
    );
    catalog.validate().unwrap();
    restored.validate().unwrap();
}

#[test]
fn pin_discovery_is_group_scoped_bounded_and_survives_snapshot_roundtrip() {
    let object = plan(Id::new_v7().unwrap(), vec![], 0);
    let object_id = object.descriptor.identity.object_id;
    let mut catalog = Catalog::default();
    catalog.apply(CatalogCommand::Register(object)).unwrap();
    catalog
        .apply(CatalogCommand::Uploaded { object_id })
        .unwrap();
    catalog
        .apply(CatalogCommand::Publish { object_id })
        .unwrap();
    let mut pins = Vec::new();
    for _ in 0..3 {
        let pin = PinRecord {
            id: Id::new_v7().unwrap(),
            group_id: 9,
            objects: vec![object_id],
            ttl_millis: 10,
        };
        catalog.apply(CatalogCommand::Pin(pin.clone())).unwrap();
        pins.push(pin);
    }
    pins.sort_by_key(|pin| pin.id);
    let first = catalog.pins_page(9, None, 2).unwrap();
    assert_eq!(
        first.pins,
        pins.iter().take(2).map(|pin| pin.id).collect::<Vec<_>>()
    );
    let second = catalog.pins_page(9, first.next_after, 2).unwrap();
    assert_eq!(
        second.pins,
        pins.iter().skip(2).map(|pin| pin.id).collect::<Vec<_>>()
    );
    assert_eq!(second.next_after, None);
    assert!(catalog.pins_page(10, None, 2).unwrap().pins.is_empty());
    assert_eq!(catalog.capacity_usage_for(10).read_pins, 0);
    assert_eq!(catalog.capacity_usage_for(9).read_pins, 3);
    assert_eq!(catalog.pins_page(9, None, 65), Err(Rejection::Capacity));
    let mut wrong = pins.first().unwrap().clone();
    wrong.id = Id::new_v7().unwrap();
    wrong.group_id = 10;
    assert_eq!(
        catalog.apply(CatalogCommand::Pin(wrong)),
        Err(Rejection::References)
    );
    let mut restored: Catalog =
        postcard::from_bytes(&postcard::to_stdvec(&catalog).unwrap()).unwrap();
    restored.validate().unwrap();
    for pin in pins {
        restored.apply(CatalogCommand::ReleasePin { pin }).unwrap();
    }
    restored.validate().unwrap();
    assert!(restored.pins_page(9, None, 64).unwrap().pins.is_empty());
    assert_eq!(restored.capacity_usage_for(9).read_pins, 0);
}

#[test]
fn compaction_preserves_old_reads_and_recovery_until_both_barriers_release() {
    let original = plan(Id::new_v7().unwrap(), vec![], 0);
    let old_id = original.descriptor.identity.object_id;
    let partition = *original.expected_generations.keys().next().unwrap();
    let mut catalog = Catalog::default();
    let mut data = DataState::default();
    catalog
        .apply(CatalogCommand::Register(original.clone()))
        .unwrap();
    data.apply(DataCommand::Authorize(original.clone()))
        .unwrap();
    catalog
        .apply(CatalogCommand::Uploaded { object_id: old_id })
        .unwrap();
    catalog
        .apply(CatalogCommand::Publish { object_id: old_id })
        .unwrap();
    data.apply(DataCommand::Published {
        plan: original.clone(),
        generations: BTreeMap::from([(partition, 1)]),
    })
    .unwrap();
    let pin = PinRecord {
        id: Id::new_v7().unwrap(),
        group_id: 9,
        objects: vec![old_id],
        ttl_millis: 1_000,
    };
    catalog.apply(CatalogCommand::Pin(pin.clone())).unwrap();
    let replacement = plan(Id::new_v7().unwrap(), vec![old_id], 1);
    let new_id = replacement.descriptor.identity.object_id;
    catalog
        .apply(CatalogCommand::Register(replacement.clone()))
        .unwrap();
    data.apply(DataCommand::Authorize(replacement.clone()))
        .unwrap();
    catalog
        .apply(CatalogCommand::Uploaded { object_id: new_id })
        .unwrap();
    catalog
        .apply(CatalogCommand::Publish { object_id: new_id })
        .unwrap();
    data.apply(DataCommand::Published {
        plan: replacement.clone(),
        generations: BTreeMap::from([(partition, 2)]),
    })
    .unwrap();
    assert_eq!(
        catalog.apply(CatalogCommand::BeginDelete { object_id: old_id }),
        Err(Rejection::Pinned)
    );
    catalog.apply(CatalogCommand::ReleasePin { pin }).unwrap();
    assert_eq!(
        catalog.apply(CatalogCommand::BeginDelete { object_id: old_id }),
        Err(Rejection::RecoveryDependency)
    );
    data.apply(DataCommand::Checkpoint { object_id: new_id })
        .unwrap();
    catalog
        .apply(CatalogCommand::Checkpoint {
            object_id: new_id,
            generation: 1,
            voters: vec![1],
            snapshots: vec![(1, "actual-snapshot-fixture".into(), 9)],
        })
        .unwrap();
    catalog
        .apply(CatalogCommand::BeginDelete { object_id: old_id })
        .unwrap();
    assert_eq!(
        catalog.apply(CatalogCommand::Pin(PinRecord {
            id: Id::new_v7().unwrap(),
            group_id: 9,
            objects: vec![old_id],
            ttl_millis: 50
        })),
        Err(Rejection::References)
    );
    assert_eq!(data.intent(old_id).unwrap().phase, IntentPhase::Retired);
    catalog.validate().unwrap();
    data.validate().unwrap();
}

#[test]
fn local_reclaim_preserves_logical_floor_original_results_and_append_admission_atomicity() {
    let mut producer =
        ProducerState::new(Id::new_v7().unwrap(), 0, ProducerLimits::default()).unwrap();
    let session = producer
        .register(
            1,
            RegisterProducer {
                producer_id: Id::new_v7().unwrap(),
                expected_registration_epoch: 1,
            },
        )
        .unwrap();
    let messages = vec![MessageData {
        key: Bytes::from_static(b"key"),
        body: Bytes::from_static(b"body"),
    }];
    let content_digest = tidemq_protocol::producer::canonical_produce_digest_parts(
        producer.topic_id(),
        0,
        session.producer_id,
        session.producer_epoch,
        0,
        messages.iter().map(|m| (m.key.as_ref(), m.body.as_ref())),
    )
    .unwrap();
    let request = AppendBatch {
        topic_id: producer.topic_id(),
        partition_id: 0,
        producer_id: session.producer_id,
        producer_epoch: session.producer_epoch,
        first_sequence: 0,
        messages,
        content_digest,
    };
    let before = postcard::to_stdvec(&producer).unwrap();
    assert_eq!(
        producer.append_admitted(1, request.clone(), u64::MAX, usize::MAX, |_, _| Err(
            crate::producer::Rejection::DataCapacity
        )),
        Err(crate::producer::Rejection::DataCapacity)
    );
    assert_eq!(postcard::to_stdvec(&producer).unwrap(), before);
    producer.append(1, request.clone()).unwrap();
    producer.reclaim_hot(1).unwrap();
    assert_eq!(producer.hot_bytes(), 0);
    assert_eq!(producer.local_reclaim_end(), 1);
    assert_eq!(producer.retained_floor(), 0);
    assert!(producer.append(1, request).unwrap().duplicate);
    producer.validate().unwrap();
    let restored: ProducerState =
        postcard::from_bytes(&postcard::to_stdvec(&producer).unwrap()).unwrap();
    restored.validate().unwrap();
    assert_eq!(restored.local_reclaim_end(), 1);
    assert_eq!(restored.retained_floor(), 0);
}

#[test]
fn retained_floor_retires_refs_without_skipping_the_next_data_publication_sequence() {
    let first = plan(Id::new_v7().unwrap(), vec![], 0);
    let id = first.descriptor.identity.object_id;
    let partition = *first.expected_generations.keys().next().unwrap();
    let mut catalog = Catalog::default();
    let mut data = DataState::default();
    catalog
        .apply(CatalogCommand::Register(first.clone()))
        .unwrap();
    data.apply(DataCommand::Authorize(first.clone())).unwrap();
    catalog
        .apply(CatalogCommand::Uploaded { object_id: id })
        .unwrap();
    catalog
        .apply(CatalogCommand::Publish { object_id: id })
        .unwrap();
    data.apply(DataCommand::Published {
        plan: first.clone(),
        generations: BTreeMap::from([(partition, 1)]),
    })
    .unwrap();
    let proof = super::types::FloorEvidence {
        cluster_id: first.descriptor.identity.cluster_id,
        group_id: 9,
        partition,
        before: 1,
        manifest_generation: 1,
        published_end: 1,
        position: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 9),
        voters: std::collections::BTreeSet::from([1]),
    };
    catalog
        .apply(CatalogCommand::RetireBefore(proof.clone()))
        .unwrap();
    assert!(catalog.manifest(partition).unwrap().segments.is_empty());
    assert_eq!(catalog.manifest(partition).unwrap().generation, 2);
    assert_eq!(
        catalog.manifest(partition).unwrap().publication_generation,
        1
    );
    assert_eq!(
        catalog.apply(CatalogCommand::BeginDelete { object_id: id }),
        Err(Rejection::RecoveryDependency)
    );
    assert_eq!(
        catalog.capacity_usage().reclaim_lag_upper_bytes,
        first.descriptor.total_bytes
    );
    let second = plan_range(Id::new_v7().unwrap(), vec![], 2, 1);
    let second_id = second.descriptor.identity.object_id;
    catalog
        .apply(CatalogCommand::Register(second.clone()))
        .unwrap();
    data.apply(DataCommand::Authorize(second.clone())).unwrap();
    catalog
        .apply(CatalogCommand::Uploaded {
            object_id: second_id,
        })
        .unwrap();
    catalog
        .apply(CatalogCommand::Publish {
            object_id: second_id,
        })
        .unwrap();
    data.apply(DataCommand::Published {
        plan: second.clone(),
        generations: BTreeMap::from([(partition, 2)]),
    })
    .unwrap();
    assert_eq!(catalog.manifest(partition).unwrap().generation, 3);
    assert_eq!(data.manifest(partition).unwrap().generation, 2);
    catalog
        .apply(CatalogCommand::FloorCheckpoint {
            proof,
            snapshots: vec![(1, "safe-floor-snapshot".into(), 9)],
        })
        .unwrap();
    catalog
        .apply(CatalogCommand::BeginDelete { object_id: id })
        .unwrap();
    catalog
        .apply(CatalogCommand::Deleted { object_id: id })
        .unwrap();
    assert_eq!(
        catalog.capacity_usage().registered_physical_upper_bytes,
        second.descriptor.total_bytes
    );
    let page = catalog.objects_page(9, None, 1).unwrap();
    assert_eq!(page.objects.len(), 1);
    assert!(page.next_after.is_some());
    let final_page = catalog.objects_page(9, page.next_after, 1).unwrap();
    assert_eq!(final_page.objects.len(), 1);
    assert!(final_page.next_after.is_none());
    assert_ne!(
        page.objects.first().unwrap().0,
        final_page.objects.first().unwrap().0
    );
    catalog.validate().unwrap();
    data.validate().unwrap();
}

#[test]
fn archive_reader_can_finish_publishing_logically_expired_hot_data() {
    let mut producer =
        ProducerState::new(Id::new_v7().unwrap(), 0, ProducerLimits::default()).unwrap();
    let session = producer
        .register(
            1,
            RegisterProducer {
                producer_id: Id::new_v7().unwrap(),
                expected_registration_epoch: 1,
            },
        )
        .unwrap();
    let messages = vec![MessageData {
        key: Bytes::from_static(b"k"),
        body: Bytes::from_static(b"body"),
    }];
    let content_digest = tidemq_protocol::producer::canonical_produce_digest_parts(
        producer.topic_id(),
        0,
        session.producer_id,
        1,
        0,
        messages
            .iter()
            .map(|message| (message.key.as_ref(), message.body.as_ref())),
    )
    .unwrap();
    producer
        .append(
            1,
            AppendBatch {
                topic_id: producer.topic_id(),
                partition_id: 0,
                producer_id: session.producer_id,
                producer_epoch: 1,
                first_sequence: 0,
                messages,
                content_digest,
            },
        )
        .unwrap();
    producer.set_retained_floor(1).unwrap();
    assert!(producer.hot_read(0, 1, 1024).is_err());
    assert_eq!(producer.archive_read(0, 1, 1024).unwrap().messages.len(), 1);
    assert_eq!(producer.local_reclaim_end(), 0);
}

fn shared_plan(id: Id, ranges: &[(u32, u64, u64)], replaces: Vec<Id>) -> Plan {
    let base = plan(id, vec![], 0);
    let topic = base.descriptor.partitions.first().unwrap().topic_id;
    let cluster = base.descriptor.identity.cluster_id;
    let records = ranges
        .iter()
        .map(|(_, start, end)| {
            (*start..*end)
                .map(|offset| Record {
                    offset,
                    key: Bytes::from_static(b"k"),
                    body: Bytes::from_static(b"body"),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let parts = ranges
        .iter()
        .zip(&records)
        .map(|((partition, start, end), records)| CommittedPartition {
            group_id: 9,
            topic_id: topic,
            partition_id: *partition,
            offsets: OffsetRange::new(*start, *end).unwrap(),
            committed_end: *end,
            records,
        })
        .collect::<Vec<_>>();
    let pack = Pack::build(
        PackIdentity {
            cluster_id: cluster,
            group_id: 9,
            object_id: id,
        },
        &parts,
        PackLimits::default(),
        &MemoryBudget::new(&crate::config::MemoryConfig::default()).unwrap(),
    )
    .unwrap();
    let mut grants = Vec::new();
    let mut expected_generations = BTreeMap::new();
    for (partition, _, _) in ranges {
        let partition = PartitionKey {
            topic_id: topic,
            partition: *partition,
        };
        let mut grant = base.grants.first().unwrap().clone();
        grant.partition = partition;
        grants.push(grant);
        expected_generations.insert(partition, u64::from(!replaces.is_empty()));
    }
    Plan {
        descriptor: pack.descriptor().clone(),
        grants,
        expected_generations,
        replaces,
    }
}

#[test]
fn shared_compaction_requires_all_live_ranges_and_keeps_registered_sources_durable() {
    let source = shared_plan(Id::new_v7().unwrap(), &[(0, 0, 3), (1, 0, 3)], vec![]);
    let old_id = source.descriptor.identity.object_id;
    let first = key(source.descriptor.partitions.first().unwrap());
    let second = key(source.descriptor.partitions.get(1).unwrap());
    let mut catalog = Catalog::default();
    let mut data = DataState::default();
    catalog
        .apply(CatalogCommand::Register(source.clone()))
        .unwrap();
    data.apply(DataCommand::Authorize(source.clone())).unwrap();
    catalog
        .apply(CatalogCommand::Uploaded { object_id: old_id })
        .unwrap();
    catalog
        .apply(CatalogCommand::Publish { object_id: old_id })
        .unwrap();
    data.apply(DataCommand::Published {
        plan: source.clone(),
        generations: BTreeMap::from([(first, 1), (second, 1)]),
    })
    .unwrap();
    let bad = shared_plan(Id::new_v7().unwrap(), &[(0, 1, 3)], vec![old_id]);
    assert_eq!(
        catalog.apply(CatalogCommand::Register(bad.clone())),
        Err(Rejection::InvalidRange)
    );
    assert_eq!(
        data.apply(DataCommand::Authorize(bad)),
        Err(Rejection::InvalidRange)
    );
    let proof = super::types::FloorEvidence {
        cluster_id: source.descriptor.identity.cluster_id,
        group_id: 9,
        partition: first,
        before: 1,
        manifest_generation: 1,
        published_end: 3,
        position: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 9),
        voters: std::collections::BTreeSet::from([1]),
    };
    catalog
        .apply(CatalogCommand::RetireBefore(proof.clone()))
        .unwrap();
    assert!(catalog.compaction_ranges(old_id).unwrap().is_none());
    catalog
        .apply(CatalogCommand::FloorCheckpoint {
            proof,
            snapshots: vec![(1, "floor-checkpoint".into(), 9)],
        })
        .unwrap();
    data.apply(DataCommand::FloorCheckpoint {
        partition: first,
        before: 1,
    })
    .unwrap();
    let selected = catalog.compaction_ranges(old_id).unwrap().unwrap();
    assert_eq!(selected.len(), 2);
    assert_eq!(
        selected.first().unwrap().offsets,
        OffsetRange::new(1, 3).unwrap()
    );
    assert_eq!(
        selected.get(1).unwrap().offsets,
        OffsetRange::new(0, 3).unwrap()
    );
    let missing_live = shared_plan(Id::new_v7().unwrap(), &[(0, 1, 3)], vec![old_id]);
    assert_eq!(
        catalog.apply(CatalogCommand::Register(missing_live.clone())),
        Err(Rejection::InvalidRange)
    );
    assert_eq!(
        data.apply(DataCommand::Authorize(missing_live)),
        Err(Rejection::InvalidRange)
    );
    let replacement = shared_plan(Id::new_v7().unwrap(), &[(0, 1, 3), (1, 0, 3)], vec![old_id]);
    let new_id = replacement.descriptor.identity.object_id;
    catalog
        .apply(CatalogCommand::Register(replacement.clone()))
        .unwrap();
    assert_eq!(
        catalog.object(old_id).unwrap().recovery_replacement,
        Some(new_id)
    );
    assert!(catalog.compaction_ranges(old_id).unwrap().is_none());
    let other = shared_plan(Id::new_v7().unwrap(), &[(0, 1, 3), (1, 0, 3)], vec![old_id]);
    assert_eq!(
        catalog.apply(CatalogCommand::Register(other)),
        Err(Rejection::References)
    );
    let mut restored: Catalog =
        postcard::from_bytes(&postcard::to_stdvec(&catalog).unwrap()).unwrap();
    restored.validate().unwrap();
    assert_eq!(
        restored.object(old_id).unwrap().recovery_replacement,
        Some(new_id)
    );
    restored
        .apply(CatalogCommand::RequestCancel { object_id: new_id })
        .unwrap();
    data.apply(DataCommand::Abort(replacement)).unwrap();
    restored
        .apply(CatalogCommand::FinishCancel {
            object_id: new_id,
            data_generation: 2,
        })
        .unwrap();
    assert_eq!(restored.object(old_id).unwrap().recovery_replacement, None);
    restored.validate().unwrap();
    data.validate().unwrap();
}
