use super::authority_data::{application, plan, publish};
use crate::{
    archive::{CommittedPartition, Pack, PackIdentity, PackLimits, Record},
    broker::{DataCommand, DataResponse},
    budget::MemoryBudget,
    lifecycle::{DataCommand as LifecycleCommand, DataOperation, DataOutcome, Plan},
    replication::Application,
};
use std::collections::BTreeMap;
use tidemq_protocol::{Id, state::OffsetRange};

#[test]
fn reclamation_reports_its_own_committed_bytes_when_another_message_arrives_between_samples() {
    let transfer = plan();
    let mut app = application(&transfer);
    assert!(matches!(
        publish(&mut app, transfer.partition, transfer.shard, 1),
        DataResponse::Published(_)
    ));
    let page = app
        .producer(transfer.partition)
        .unwrap()
        .hot_read(0, 1, 1024)
        .unwrap();
    let message = page.messages.first().unwrap();
    let record = Record {
        offset: 0,
        key: message.key.clone(),
        body: message.body.clone(),
        context: message.context.clone(),
    };
    let pack = Pack::build(
        PackIdentity {
            cluster_id: transfer.source.cluster_id,
            group_id: transfer.source.group_id,
            object_id: Id::new_v7().unwrap(),
        },
        &[CommittedPartition {
            group_id: transfer.source.group_id,
            topic_id: transfer.partition.topic_id,
            partition_id: transfer.partition.partition,
            offsets: OffsetRange::new(0, 1).unwrap(),
            committed_end: 1,
            records: &[record],
        }],
        PackLimits::default(),
        &MemoryBudget::new(&crate::config::MemoryConfig::default()).unwrap(),
    )
    .unwrap();
    let archive = Plan {
        descriptor: pack.descriptor().clone(),
        grants: vec![transfer.source.clone()],
        expected_generations: BTreeMap::from([(transfer.partition, 0)]),
        replaces: Vec::new(),
    };
    let command = |command| {
        DataCommand::Lifecycle(DataOperation {
            command,
            grants: vec![transfer.source.clone()],
        })
    };
    assert!(matches!(
        app.apply(command(LifecycleCommand::Authorize(archive.clone()))),
        DataResponse::Lifecycle(_)
    ));
    assert!(matches!(
        app.apply(command(LifecycleCommand::Published {
            plan: archive.clone(),
            generations: BTreeMap::from([(transfer.partition, 1)])
        })),
        DataResponse::Lifecycle(_)
    ));
    let DataResponse::Lifecycle(checkpoint) = app.apply(command(LifecycleCommand::Checkpoint {
        object_id: archive.descriptor.identity.object_id,
        partitions: vec![transfer.partition],
    })) else {
        panic!("Expected a durable checkpoint");
    };
    let DataOutcome::Checkpoint { generation } = checkpoint.outcome else {
        panic!("Expected a checkpoint generation");
    };
    let sampled_before = app.hot_bytes();
    assert!(matches!(
        publish(&mut app, transfer.partition, transfer.shard, 2),
        DataResponse::Published(_)
    ));
    let concurrent_bytes = app.hot_bytes().checked_sub(sampled_before).unwrap();
    let reclaim = command(LifecycleCommand::Reclaim {
        partition: transfer.partition,
        end: 1,
        checkpoint_generation: generation,
    });
    let DataResponse::Lifecycle(reclaimed) = app.apply(reclaim.clone()) else {
        panic!("Expected a committed reclamation");
    };
    assert_eq!(
        reclaimed.outcome,
        DataOutcome::Reclaimed {
            bytes: sampled_before
        }
    );
    assert_eq!(app.hot_bytes(), concurrent_bytes);
    assert!(sampled_before.saturating_sub(app.hot_bytes()) < sampled_before);
    let DataResponse::Lifecycle(replayed) = app.apply(reclaim) else {
        panic!("Expected an idempotent reclamation");
    };
    assert_eq!(replayed.outcome, DataOutcome::Reclaimed { bytes: 0 });
    app.validate().unwrap();
}
