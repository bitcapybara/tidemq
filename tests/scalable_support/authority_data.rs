use std::collections::BTreeMap;

use crate::{
    broker::{
        AuthorityDataCommand, DataApplication, DataCommand, DataGroupConfig, DataRejection,
        DataResponse, TransferCapacity, TransferDataRejection, TransferOutcome, WriterFence,
    },
    metadata::{AuthorityTransfer, Command, PartitionKey, TransferStage},
    ownership::{CommitPosition, FenceCommand, Grant},
    producer::{MessageData, ProducerLimits, PublishRequest, PublisherPermit},
    replication::Application,
};
use bytes::Bytes;
use tidemq_protocol::{
    Id,
    topic::{HashRange, MessageMetadata, SendIdentity, message_digest, routing_hash},
};

fn plan() -> AuthorityTransfer {
    let cluster = Id::new_v7().unwrap();
    let partition = PartitionKey {
        topic_id: Id::new_v7().unwrap(),
        partition: 0,
    };
    let grant = |group, epoch, owner| Grant {
        cluster_id: cluster,
        partition,
        group_id: group,
        owner_node: owner,
        epoch,
        metadata_revision: epoch,
        expected_route_generation: epoch - 1,
        operation_id: Command::grant_ownership(1, cluster, partition, owner, epoch - 1, epoch - 1)
            .unwrap()
            .operation_id(),
    };
    AuthorityTransfer {
        id: Id::new_v7().unwrap(),
        shard: Id::new_v7().unwrap(),
        partition,
        layout_revision: 1,
        binding_generation: 1,
        source: grant(2, 1, 1),
        target: grant(3, 2, 2),
        source_voters: [1].into(),
        target_voters: [2].into(),
        checkpoint_voters: [2].into(),
        stage: TransferStage::Prepare,
        frozen_cut: None,
        manifest: None,
        imported: None,
        checkpoints: BTreeMap::new(),
        retired_cut: None,
    }
}

fn application(plan: &AuthorityTransfer) -> DataApplication {
    let mut app = DataApplication::default();
    assert_eq!(
        app.apply(DataCommand::Initialize(DataGroupConfig {
            cluster_id: plan.source.cluster_id,
            group_id: 2,
            max_partitions: 4,
            max_hot_bytes: 1 << 20,
            max_hot_messages: 1024,
            producer: ProducerLimits::default(),
            subscription: Default::default(),
            retention: Default::default(),
        })),
        DataResponse::Initialized
    );
    install(&mut app, plan.source.clone());
    app
}

fn install(app: &mut DataApplication, grant: Grant) {
    let partition = grant.partition;
    let writer = WriterFence {
        node_id: grant.owner_node,
        owner_epoch: grant.epoch,
    };
    assert!(matches!(
        app.apply(DataCommand::InstallFence(FenceCommand { grant })),
        DataResponse::Fenced(_)
    ));
    assert!(matches!(
        app.apply(DataCommand::Recover { partition, writer }),
        DataResponse::Recovered(_)
    ));
}

fn publish(
    app: &mut DataApplication,
    partition: PartitionKey,
    shard: Id,
    sequence: u64,
) -> DataResponse {
    app.apply(publish_command(partition, shard, sequence))
}

fn publish_command(partition: PartitionKey, shard: Id, sequence: u64) -> DataCommand {
    let producer = partition.topic_id;
    let identity = SendIdentity {
        topic: partition.topic_id,
        producer,
        registration: 1,
        sequence,
    };
    let metadata = MessageMetadata {
        key_present: false,
        routing_hash: routing_hash(None, producer, sequence).unwrap(),
        properties: BTreeMap::new(),
    };
    DataCommand::Publish {
        partition,
        writer: WriterFence {
            node_id: 1,
            owner_epoch: 1,
        },
        birth: (
            shard,
            HashRange {
                start: 0,
                end: None,
            },
        ),
        permit: PublisherPermit {
            topic: partition.topic_id,
            producer,
            registration: 1,
            client: producer,
            credential: producer,
            expires_ms: 100_000,
            live: true,
        },
        request: PublishRequest {
            identity,
            digest: message_digest(identity, None, b"payload", &metadata.properties).unwrap(),
            retired_before: 1,
            message: MessageData {
                key: Bytes::new(),
                body: Bytes::from_static(b"payload"),
                context: postcard::to_stdvec(&metadata).unwrap().into(),
            },
        },
        now_ms: 1,
    }
}

fn capacity() -> TransferCapacity {
    TransferCapacity {
        max_bytes: 65_536,
        max_hot_bytes: 1024,
        max_hot_messages: 16,
    }
}

#[test]
fn a_frozen_shard_keeps_a_stable_export_while_other_shards_continue_writing() {
    let plan = plan();
    let mut source = application(&plan);
    let first = publish(&mut source, plan.partition, plan.shard, 1);
    assert!(
        matches!(first, DataResponse::Published(_)),
        "Initial publication failed: {first:?}"
    );
    let other = PartitionKey {
        topic_id: Id::new_v7().unwrap(),
        partition: 0,
    };
    let other_shard = Id::new_v7().unwrap();
    let mut grant = plan.source.clone();
    grant.partition = other;
    grant.operation_id = Command::grant_ownership(1, grant.cluster_id, other, 1, 0, 0)
        .unwrap()
        .operation_id();
    install(&mut source, grant);
    assert!(matches!(
        publish(&mut source, other, other_shard, 1),
        DataResponse::Published(_)
    ));
    let freeze = DataCommand::AuthorityTransfer(AuthorityDataCommand::Freeze {
        plan: Box::new(plan.clone()),
        capacity: capacity(),
    });
    let frozen = source.apply(freeze.clone());
    assert!(matches!(
        &frozen,
        DataResponse::AuthorityTransfer(TransferOutcome::Frozen {
            committed_end: 1,
            ..
        })
    ));
    assert_eq!(source.apply(freeze), frozen);
    let mut premature = Vec::new();
    assert_eq!(
        source.write_transfer_payload(plan.id, &mut premature),
        Err(TransferDataRejection::NotReady)
    );
    assert!(premature.is_empty());
    assert_eq!(
        publish(&mut source, plan.partition, plan.shard, 2),
        DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::Frozen))
    );
    let cut = CommitPosition {
        term: 1,
        leader_node: 1,
        index: 1000,
    };
    let outcome = source.apply(DataCommand::AuthorityTransfer(
        AuthorityDataCommand::RecordCut { plan: plan.id, cut },
    ));
    assert_eq!(
        outcome,
        DataResponse::AuthorityTransfer(TransferOutcome::Cut(cut))
    );
    assert_eq!(
        source.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::RecordCut {
                plan: plan.id,
                cut: CommitPosition { index: 1001, ..cut },
            }
        )),
        DataResponse::AuthorityTransfer(TransferOutcome::Cut(cut))
    );
    let mut expected = Vec::new();
    source
        .write_transfer_payload(plan.id, &mut expected)
        .unwrap();
    assert!(matches!(
        publish(&mut source, other, other_shard, 2),
        DataResponse::Published(_)
    ));
    let mut source: DataApplication =
        postcard::from_bytes(&postcard::to_stdvec(&source).unwrap()).unwrap();
    source.validate().unwrap();
    let mut recovered = Vec::new();
    source
        .write_transfer_payload(plan.id, &mut recovered)
        .unwrap();
    assert_eq!(recovered, expected);
    assert_eq!(
        publish(&mut source, plan.partition, plan.shard, 2),
        DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::Frozen))
    );
    assert_eq!(source.producer(other).unwrap().committed_end(), 2);
    let candidates = source.archive_candidates(None, 64).unwrap();
    assert_eq!(candidates.partitions.len(), 1);
    assert_eq!(candidates.partitions[0].partition, other);
}

#[test]
fn an_empty_shard_can_transfer_without_reserving_hot_message_capacity() {
    let plan = plan();
    let mut app = application(&plan);
    let outcome = app.apply(DataCommand::AuthorityTransfer(
        AuthorityDataCommand::Freeze {
            plan: Box::new(plan),
            capacity: TransferCapacity {
                max_hot_bytes: 0,
                max_hot_messages: 0,
                ..capacity()
            },
        },
    ));
    assert!(
        matches!(
            outcome,
            DataResponse::AuthorityTransfer(TransferOutcome::Frozen {
                committed_end: 0,
                ..
            })
        ),
        "Empty shard freeze was rejected: {outcome:?}"
    );
    app.validate().unwrap();
}

#[test]
fn insufficient_transfer_capacity_leaves_the_source_writable() {
    let plan = plan();
    let mut app = application(&plan);
    assert!(matches!(
        publish(&mut app, plan.partition, plan.shard, 1),
        DataResponse::Published(_)
    ));
    let original = postcard::to_stdvec(&app).unwrap();
    for insufficient in [
        TransferCapacity {
            max_hot_messages: 0,
            ..capacity()
        },
        TransferCapacity {
            max_hot_bytes: 0,
            ..capacity()
        },
        TransferCapacity {
            max_bytes: 64,
            ..capacity()
        },
    ] {
        assert_eq!(
            app.apply(DataCommand::AuthorityTransfer(
                AuthorityDataCommand::Freeze {
                    plan: Box::new(plan.clone()),
                    capacity: insufficient,
                }
            )),
            DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::Capacity))
        );
        assert_eq!(postcard::to_stdvec(&app).unwrap(), original);
    }
    assert!(matches!(
        publish(&mut app, plan.partition, plan.shard, 2),
        DataResponse::Published(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_three_replica_restart_preserves_the_frozen_source_and_original_cut() {
    let mut cluster = super::support::Cluster::new(&[1, 2, 3]).await;
    let mut plan = plan();
    plan.source = cluster.grant(plan.partition, 1);
    plan.source_voters = [1, 2, 3].into();
    plan.target.cluster_id = cluster.config.cluster_id;
    plan.target.group_id = cluster.config.group_id + 1;
    plan.target.operation_id = Command::grant_ownership(
        1,
        cluster.config.cluster_id,
        plan.partition,
        plan.target.owner_node,
        1,
        1,
    )
    .unwrap()
    .operation_id();
    plan.target_voters = [1, 2, 3].into();
    plan.checkpoint_voters = plan.target_voters.clone();
    cluster.fence_and_recover(&plan.source).await;
    assert!(matches!(
        cluster
            .write(publish_command(plan.partition, plan.shard, 1))
            .await,
        DataResponse::Published(_)
    ));
    let freeze = DataCommand::AuthorityTransfer(AuthorityDataCommand::Freeze {
        plan: Box::new(plan.clone()),
        capacity: capacity(),
    });
    let response = cluster
        .node(1)
        .raft
        .client_write(freeze.clone())
        .await
        .unwrap();
    assert!(
        matches!(response.data, DataResponse::AuthorityTransfer(_)),
        "Source freeze was rejected: {:?}",
        response.data
    );
    let cut = CommitPosition {
        term: response.log_id.leader_id.term,
        leader_node: response.log_id.leader_id.node_id,
        index: response.log_id.index,
    };
    let record =
        DataCommand::AuthorityTransfer(AuthorityDataCommand::RecordCut { plan: plan.id, cut });
    assert_eq!(
        cluster.write(record.clone()).await,
        DataResponse::AuthorityTransfer(TransferOutcome::Cut(cut))
    );
    cluster.restart(1).await;
    assert_eq!(cluster.write(freeze).await, response.data);
    assert_eq!(
        cluster.write(record).await,
        DataResponse::AuthorityTransfer(TransferOutcome::Cut(cut))
    );
    assert_eq!(
        cluster
            .write(publish_command(plan.partition, plan.shard, 2))
            .await,
        DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::Frozen))
    );
    cluster.shutdown().await;
}
