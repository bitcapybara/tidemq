use super::{
    Catalog, CatalogCommand, CheckpointScope, DataCommand, DataState, Rejection,
    types::{DataOutcome, Plan},
};
use crate::{
    archive::{CommittedPartition, Pack, PackIdentity, PackLimits, Record},
    budget::MemoryBudget,
    metadata::PartitionKey,
    ownership::Grant,
};
use bytes::Bytes;
use tidemq_protocol::{Id, state::OffsetRange};

fn shared_plan(topic: Id, object: Id, generation: u64, replaces: Vec<Id>) -> Plan {
    let cluster = "11111111-1111-4111-8111-111111111111".parse().unwrap();
    let partitions: Vec<_> = (0..2)
        .map(|partition| PartitionKey {
            topic_id: topic,
            partition,
        })
        .collect();
    let record = Record {
        offset: 0,
        context: Bytes::new(),
        key: Bytes::new(),
        body: Bytes::from_static(b"history"),
    };
    let ranges: Vec<_> = partitions
        .iter()
        .map(|partition| CommittedPartition {
            group_id: 9,
            topic_id: topic,
            partition_id: partition.partition,
            offsets: OffsetRange::new(0, 1).unwrap(),
            committed_end: 1,
            records: std::slice::from_ref(&record),
        })
        .collect();
    let pack = Pack::build(
        PackIdentity {
            cluster_id: cluster,
            group_id: 9,
            object_id: object,
        },
        &ranges,
        PackLimits::default(),
        &MemoryBudget::new(&crate::config::MemoryConfig::default()).unwrap(),
    )
    .unwrap();
    Plan {
        descriptor: pack.descriptor().clone(),
        grants: partitions
            .iter()
            .map(|partition| Grant {
                cluster_id: cluster,
                partition: *partition,
                group_id: 9,
                epoch: 1,
                owner_node: 1,
                expected_route_generation: 0,
                metadata_revision: 1,
                operation_id: crate::metadata::grant_operation_id(cluster, *partition, 1, 1, 0)
                    .unwrap(),
            })
            .collect(),
        expected_generations: partitions
            .into_iter()
            .map(|partition| (partition, generation))
            .collect(),
        replaces,
    }
}

fn publish(catalog: &mut Catalog, data: &mut DataState, plan: &Plan) {
    let object_id = plan.descriptor.identity.object_id;
    catalog
        .apply(CatalogCommand::Register(plan.clone()))
        .unwrap();
    data.apply(DataCommand::Authorize(plan.clone())).unwrap();
    catalog
        .apply(CatalogCommand::Uploaded { object_id })
        .unwrap();
    catalog
        .apply(CatalogCommand::Publish { object_id })
        .unwrap();
    data.apply(DataCommand::Published {
        plan: plan.clone(),
        generations: plan
            .expected_generations
            .iter()
            .map(|(partition, generation)| (*partition, generation + 1))
            .collect(),
    })
    .unwrap();
    catalog.validate().unwrap();
    data.validate().unwrap();
}

#[test]
fn importing_an_additional_scope_requires_a_new_local_checkpoint() {
    let plan = shared_plan(Id::new_v7().unwrap(), Id::new_v7().unwrap(), 0, vec![]);
    let object_id = plan.descriptor.identity.object_id;
    let partitions: Vec<_> = plan.expected_generations.keys().copied().collect();
    let mut source = DataState::default();
    publish(&mut Catalog::default(), &mut source, &plan);
    source
        .apply(DataCommand::Checkpoint {
            object_id,
            partitions: partitions.clone(),
        })
        .unwrap();
    let target = DataState::default()
        .import_authority_slice(source.authority_slice(partitions[1], 1 << 20).unwrap())
        .unwrap();
    target.validate().unwrap();
    let mut target = target;
    assert_eq!(
        target
            .apply(DataCommand::Checkpoint {
                object_id,
                partitions: vec![partitions[1]]
            })
            .unwrap(),
        DataOutcome::Checkpoint { generation: 1 }
    );
    let mut target = target
        .import_authority_slice(source.authority_slice(partitions[0], 1 << 20).unwrap())
        .unwrap();
    target.validate().unwrap();
    assert_eq!(
        target.apply(DataCommand::Reclaim {
            partition: partitions[0],
            end: 1,
            checkpoint_generation: 1
        }),
        Err(Rejection::CheckpointRequired)
    );
    assert_eq!(
        target
            .apply(DataCommand::Checkpoint {
                object_id,
                partitions: partitions.clone()
            })
            .unwrap(),
        DataOutcome::Checkpoint { generation: 2 }
    );
    target
        .apply(DataCommand::Reclaim {
            partition: partitions[0],
            end: 1,
            checkpoint_generation: 2,
        })
        .unwrap();
}

#[test]
fn a_shared_replacement_releases_recovery_only_after_both_authorities_checkpoint() {
    let topic = Id::new_v7().unwrap();
    let original = shared_plan(topic, Id::new_v7().unwrap(), 0, vec![]);
    let old = original.descriptor.identity.object_id;
    let replacement = shared_plan(topic, Id::new_v7().unwrap(), 1, vec![old]);
    let new = replacement.descriptor.identity.object_id;
    let mut catalog = Catalog::default();
    let mut data = DataState::default();
    publish(&mut catalog, &mut data, &original);
    publish(&mut catalog, &mut data, &replacement);
    let partitions: Vec<_> = replacement.expected_generations.keys().copied().collect();
    catalog
        .apply(CatalogCommand::Checkpoint {
            object_id: new,
            group_id: 9,
            scopes: vec![CheckpointScope {
                partition: partitions[0],
                binding_generation: 1,
            }],
            generation: 19,
            voters: vec![1],
            snapshots: vec![(1, "source-scope-snapshot".into(), 100)],
        })
        .unwrap();
    assert_eq!(
        catalog.apply(CatalogCommand::BeginDelete { object_id: old }),
        Err(Rejection::RecoveryDependency)
    );
    catalog
        .apply(CatalogCommand::Checkpoint {
            object_id: new,
            group_id: 10,
            scopes: vec![CheckpointScope {
                partition: partitions[1],
                binding_generation: 2,
            }],
            generation: 1,
            voters: vec![2],
            snapshots: vec![(2, "target-scope-snapshot".into(), 7)],
        })
        .unwrap();
    catalog
        .apply(CatalogCommand::BeginDelete { object_id: old })
        .unwrap();
    let recovered: Catalog = postcard::from_bytes(&postcard::to_stdvec(&catalog).unwrap()).unwrap();
    recovered.validate().unwrap();
}
