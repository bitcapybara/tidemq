use std::collections::BTreeMap;

use crate::{
    broker::{
        AuthorityDataCommand, DataApplication, DataCommand, DataGroupConfig, DataRejection,
        DataResponse, TransferCapacity, TransferDataRejection, TransferOutcome, TransferPage,
        WriterFence,
    },
    metadata::{
        AuthorityTransfer, Command, ImportEvidence, PartitionKey, TransferCheckpoint,
        TransferManifest, TransferStage, TransferStep,
    },
    ownership::{
        CommitPosition, FenceCommand, FenceRecord, Grant, RecoveredOwnership, RecoveryCheckpoint,
        RecoveryEvidence,
    },
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
    initialized(
        plan.source.cluster_id,
        plan.source.group_id,
        Some(plan.source.clone()),
    )
}

fn initialized(cluster: Id, group: u64, grant: Option<Grant>) -> DataApplication {
    let mut app = DataApplication::default();
    assert_eq!(
        app.apply(DataCommand::Initialize(DataGroupConfig {
            cluster_id: cluster,
            group_id: group,
            max_partitions: 4,
            max_hot_bytes: 1 << 20,
            max_hot_messages: 1024,
            producer: ProducerLimits::default(),
            subscription: Default::default(),
            retention: Default::default(),
        })),
        DataResponse::Initialized
    );
    if let Some(grant) = grant {
        install(&mut app, grant);
    }
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
    publish_body(partition, shard, sequence, Bytes::from_static(b"payload"))
}

fn publish_body(partition: PartitionKey, shard: Id, sequence: u64, body: Bytes) -> DataCommand {
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
            digest: message_digest(identity, None, &body, &metadata.properties).unwrap(),
            retired_before: 1,
            message: MessageData {
                key: Bytes::new(),
                body,
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
        max_window_slots: 16,
        max_read_results: 16,
        max_ack_slots: 16,
        max_members: 16,
        max_in_flight_bytes: 1024,
    }
}

#[test]
fn target_reservations_expand_without_double_counting_or_changing_original_reserve_results() {
    let mut plan = plan();
    let mut target = initialized(plan.source.cluster_id, plan.target.group_id, None);
    let initial = TransferCapacity {
        max_hot_bytes: 0,
        max_hot_messages: 0,
        ..capacity()
    };
    let reserve = DataCommand::AuthorityTransfer(AuthorityDataCommand::Reserve {
        plan: Box::new(plan.clone()),
        capacity: initial,
    });
    let reserved = DataResponse::AuthorityTransfer(TransferOutcome::Reserved { plan: plan.id });
    assert_eq!(target.apply(reserve.clone()), reserved);
    let available = target.expanded_transfer_capacity(plan.id).unwrap();
    assert!(available.max_hot_bytes > initial.max_hot_bytes);
    assert!(available.max_members > initial.max_members);
    let expanded = TransferCapacity {
        max_hot_bytes: available.max_hot_bytes / 2,
        ..available
    };
    let expand = DataCommand::AuthorityTransfer(AuthorityDataCommand::ExpandReservation {
        plan: plan.id,
        capacity: expanded,
    });
    assert_eq!(target.apply(expand.clone()), reserved);
    target.validate().unwrap();
    assert_eq!(target.transfer_capacity(plan.id).unwrap(), expanded);
    assert_eq!(target.apply(reserve), reserved);
    assert_eq!(
        target.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::ExpandReservation {
                plan: plan.id,
                capacity: initial
            }
        )),
        reserved
    );
    assert_eq!(target.transfer_capacity(plan.id).unwrap(), expanded);
    let excessive = TransferCapacity {
        max_hot_bytes: available.max_hot_bytes + 1,
        ..available
    };
    assert_eq!(
        target.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::ExpandReservation {
                plan: plan.id,
                capacity: excessive
            }
        )),
        DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::Capacity))
    );
    assert_eq!(target.transfer_capacity(plan.id).unwrap(), expanded);
    let mut target: DataApplication =
        postcard::from_bytes(&postcard::to_stdvec(&target).unwrap()).unwrap();
    target.validate().unwrap();
    assert_eq!(target.apply(expand.clone()), reserved);
    let cut = CommitPosition {
        term: 1,
        leader_node: 1,
        index: 100,
    };
    plan.advance(TransferStep::Frozen(cut)).unwrap();
    plan.advance(TransferStep::Exported(TransferManifest {
        bytes: 100,
        digest: [9; 32],
        source_cut: cut,
        committed_end: 0,
    }))
    .unwrap();
    assert_eq!(
        target.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::BeginImport {
                plan: Box::new(plan.clone())
            }
        )),
        DataResponse::AuthorityTransfer(TransferOutcome::ImportProgress {
            plan: plan.id,
            received: 0
        })
    );
    assert_eq!(
        target.expanded_transfer_capacity(plan.id).unwrap(),
        expanded
    );
    assert_eq!(
        target.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::ExpandReservation {
                plan: plan.id,
                capacity: available
            }
        )),
        DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::Conflict))
    );
    assert_eq!(target.apply(expand), reserved);
    target.validate().unwrap();
}

#[test]
fn retiring_another_target_scope_makes_capacity_available_to_the_original_waiting_transfer() {
    let plan = plan();
    let mut other = self::plan();
    other.source.group_id = plan.target.group_id;
    other.target.group_id = 4;
    for grant in [&mut other.source, &mut other.target] {
        grant.cluster_id = plan.source.cluster_id;
        grant.operation_id = crate::metadata::grant_operation_id(
            grant.cluster_id,
            grant.partition,
            grant.epoch,
            grant.owner_node,
            grant.expected_route_generation,
        )
        .unwrap();
    }
    let mut target = initialized(
        plan.source.cluster_id,
        plan.target.group_id,
        Some(other.source.clone()),
    );
    assert!(matches!(
        target.apply(publish_body(
            other.partition,
            other.shard,
            1,
            Bytes::from(vec![1; 240 * 1024])
        )),
        DataResponse::Published(_)
    ));
    let initial = target.transfer_capacity(plan.id).unwrap();
    assert_eq!(
        target.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::Reserve {
                plan: Box::new(plan.clone()),
                capacity: initial
            }
        )),
        DataResponse::AuthorityTransfer(TransferOutcome::Reserved { plan: plan.id })
    );
    let mut source = application(&plan);
    for sequence in 1..=4 {
        assert!(matches!(
            source.apply(publish_body(
                plan.partition,
                plan.shard,
                sequence,
                Bytes::from(vec![2; 240 * 1024])
            )),
            DataResponse::Published(_)
        ));
    }
    assert_eq!(
        source.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::Freeze {
                plan: Box::new(plan.clone()),
                capacity: initial
            }
        )),
        DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::Capacity))
    );
    let other_capacity = TransferCapacity {
        max_bytes: 1024 * 1024,
        max_hot_bytes: 512 * 1024,
        ..capacity()
    };
    assert!(matches!(
        target.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::Freeze {
                plan: Box::new(other.clone()),
                capacity: other_capacity
            }
        )),
        DataResponse::AuthorityTransfer(TransferOutcome::Frozen { .. })
    ));
    let cut = CommitPosition {
        term: 1,
        leader_node: 1,
        index: 100,
    };
    target.apply(DataCommand::AuthorityTransfer(
        AuthorityDataCommand::RecordCut {
            plan: other.id,
            cut,
        },
    ));
    let mut bytes = Vec::new();
    target.write_transfer_payload(other.id, &mut bytes).unwrap();
    let other = checkpointed_report(other, &bytes, cut);
    assert!(matches!(
        target.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::ReclaimSource {
                plan: Box::new(other)
            }
        )),
        DataResponse::AuthorityTransfer(TransferOutcome::SourceReclaimed { .. })
    ));
    assert_eq!(target.transfer_capacity(plan.id).unwrap(), initial);
    let expanded = target.expanded_transfer_capacity(plan.id).unwrap();
    assert!(expanded.max_hot_bytes > initial.max_hot_bytes);
    assert_eq!(
        target.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::ExpandReservation {
                plan: plan.id,
                capacity: expanded
            }
        )),
        DataResponse::AuthorityTransfer(TransferOutcome::Reserved { plan: plan.id })
    );
    target.validate().unwrap();
    assert!(matches!(
        source.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::Freeze {
                plan: Box::new(plan),
                capacity: expanded
            }
        )),
        DataResponse::AuthorityTransfer(TransferOutcome::Frozen { .. })
    ));
    source.validate().unwrap();
}

fn checkpointed_report(
    mut plan: AuthorityTransfer,
    bytes: &[u8],
    cut: CommitPosition,
) -> AuthorityTransfer {
    let manifest = TransferManifest {
        bytes: bytes.len() as u64,
        digest: *blake3::hash(bytes).as_bytes(),
        source_cut: cut,
        committed_end: 1,
    };
    plan.advance(TransferStep::Frozen(cut)).unwrap();
    plan.advance(TransferStep::Exported(manifest.clone()))
        .unwrap();
    let committed = CommitPosition {
        term: 1,
        leader_node: 2,
        index: 1,
    };
    plan.advance(TransferStep::Imported(ImportEvidence {
        manifest: manifest.clone(),
        recovery: RecoveryEvidence {
            recovered: RecoveredOwnership {
                fence: FenceRecord {
                    grant: plan.target.clone(),
                    fence_generation: 1,
                },
                checkpoint: RecoveryCheckpoint {
                    fence_generation: 1,
                    producer_state_revision: 1,
                    ack_state_revision: 1,
                    committed_end: 1,
                    state_digest: [9; 32],
                },
            },
            committed,
        },
    }))
    .unwrap();
    plan.advance(TransferStep::Published).unwrap();
    plan.advance(TransferStep::Checkpointed(vec![TransferCheckpoint {
        voter: 2,
        position: CommitPosition {
            index: 2,
            ..committed
        },
        manifest_digest: manifest.digest,
    }]))
    .unwrap();
    plan
}

#[test]
fn source_reclamation_keeps_a_permanent_fence_after_its_payload_is_removed() {
    let original = plan();
    let mut source = application(&original);
    publish(&mut source, original.partition, original.shard, 1);
    let freeze = DataCommand::AuthorityTransfer(AuthorityDataCommand::Freeze {
        plan: Box::new(original.clone()),
        capacity: capacity(),
    });
    let frozen = source.apply(freeze.clone());
    let cut = CommitPosition {
        term: 1,
        leader_node: 1,
        index: 100,
    };
    source.apply(DataCommand::AuthorityTransfer(
        AuthorityDataCommand::RecordCut {
            plan: original.id,
            cut,
        },
    ));
    let mut bytes = Vec::new();
    source
        .write_transfer_payload(original.id, &mut bytes)
        .unwrap();
    let report = checkpointed_report(original.clone(), &bytes, cut);
    assert_eq!(
        source.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::ReclaimSource {
                plan: Box::new(original.clone())
            }
        )),
        DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::NotReady))
    );
    assert_eq!(
        source.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::RecordRetiredCut {
                plan: original.id,
                cut
            }
        )),
        DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::NotReady))
    );
    let mut corrupt = report.clone();
    corrupt.manifest.as_mut().unwrap().digest[0] ^= 1;
    let digest = corrupt.manifest.as_ref().unwrap().digest;
    corrupt.imported.as_mut().unwrap().manifest.digest = digest;
    for checkpoint in corrupt.checkpoints.values_mut() {
        checkpoint.manifest_digest = digest;
    }
    corrupt.validate().unwrap();
    assert_eq!(
        source.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::ReclaimSource {
                plan: Box::new(corrupt)
            }
        )),
        DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::Conflict))
    );
    let reclaim = DataCommand::AuthorityTransfer(AuthorityDataCommand::ReclaimSource {
        plan: Box::new(report),
    });
    let reclaimed = DataResponse::AuthorityTransfer(TransferOutcome::SourceReclaimed {
        plan: original.id,
        partition: original.partition,
    });
    assert_eq!(source.apply(reclaim.clone()), reclaimed);
    assert!(source.producer(original.partition).is_none());
    assert_eq!((source.hot_bytes(), source.hot_messages()), (0, 0));
    source = postcard::from_bytes(&postcard::to_stdvec(&source).unwrap()).unwrap();
    source.validate().unwrap();
    let retired = CommitPosition { index: 101, ..cut };
    assert_eq!(
        source.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::RecordRetiredCut {
                plan: original.id,
                cut: retired
            }
        )),
        DataResponse::AuthorityTransfer(TransferOutcome::Cut(retired))
    );
    assert_eq!(source.apply(reclaim), reclaimed);
    assert_eq!(source.apply(freeze), frozen);
    assert_eq!(
        source.apply(DataCommand::InstallFence(FenceCommand {
            grant: original.source.clone()
        })),
        DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::Frozen))
    );
    let mut later = original.source;
    later.epoch = 3;
    later.expected_route_generation = 2;
    later.metadata_revision = 3;
    later.operation_id = Command::grant_ownership(1, later.cluster_id, later.partition, 1, 2, 2)
        .unwrap()
        .operation_id();
    assert_eq!(
        source.apply(DataCommand::InstallFence(FenceCommand { grant: later })),
        DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::Frozen))
    );
    assert_eq!(
        source.write_transfer_payload(original.id, &mut Vec::new()),
        Err(TransferDataRejection::NotReady)
    );
    source.validate().unwrap();
}

#[test]
fn a_retired_source_accepts_a_later_import_without_replaying_old_retirement() {
    let forward = plan();
    let mut backward = forward.clone();
    backward.id = Id::new_v7().unwrap();
    backward.layout_revision = 2;
    backward.binding_generation = 2;
    backward.source = forward.target.clone();
    backward.target = Grant {
        cluster_id: forward.source.cluster_id,
        partition: forward.partition,
        group_id: forward.source.group_id,
        epoch: 3,
        owner_node: 1,
        expected_route_generation: 2,
        metadata_revision: 3,
        operation_id: crate::metadata::grant_operation_id(
            forward.source.cluster_id,
            forward.partition,
            3,
            1,
            2,
        )
        .unwrap(),
    };
    backward.source_voters = forward.target_voters.clone();
    backward.target_voters = forward.source_voters.clone();
    backward.checkpoint_voters = backward.target_voters.clone();
    backward.validate().unwrap();
    let mut groups = [
        application(&forward),
        initialized(forward.source.cluster_id, forward.target.group_id, None),
    ];
    let other = PartitionKey {
        topic_id: Id::new_v7().unwrap(),
        partition: 0,
    };
    let other_shard = Id::new_v7().unwrap();
    let mut other_grant = forward.source.clone();
    other_grant.partition = other;
    other_grant.operation_id =
        crate::metadata::grant_operation_id(other_grant.cluster_id, other, 1, 1, 0).unwrap();
    install(&mut groups[0], other_grant);
    publish(&mut groups[0], other, other_shard, 1);
    publish(&mut groups[0], forward.partition, forward.shard, 1);
    let mut original_reclaim = None;
    for (round, plan) in [forward.clone(), backward.clone()].into_iter().enumerate() {
        let from = round;
        let to = 1 - round;
        assert!(matches!(
            groups[to].apply(DataCommand::AuthorityTransfer(
                AuthorityDataCommand::Reserve {
                    plan: Box::new(plan.clone()),
                    capacity: capacity(),
                }
            )),
            DataResponse::AuthorityTransfer(TransferOutcome::Reserved { .. })
        ));
        assert!(matches!(
            groups[from].apply(DataCommand::AuthorityTransfer(
                AuthorityDataCommand::Freeze {
                    plan: Box::new(plan.clone()),
                    capacity: capacity(),
                }
            )),
            DataResponse::AuthorityTransfer(TransferOutcome::Frozen { .. })
        ));
        let cut = CommitPosition {
            term: 1,
            leader_node: *plan.source_voters.iter().next().unwrap(),
            index: if round == 0 { 100 } else { 500 },
        };
        groups[from].apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::RecordCut { plan: plan.id, cut },
        ));
        let mut bytes = Vec::new();
        groups[from]
            .write_transfer_payload(plan.id, &mut bytes)
            .unwrap();
        let mut exported = plan.clone();
        exported.advance(TransferStep::Frozen(cut)).unwrap();
        exported
            .advance(TransferStep::Exported(TransferManifest {
                bytes: bytes.len() as u64,
                digest: *blake3::hash(&bytes).as_bytes(),
                source_cut: cut,
                committed_end: 1,
            }))
            .unwrap();
        groups[to].apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::BeginImport {
                plan: Box::new(exported),
            },
        ));
        for (index, chunk) in bytes
            .chunks(crate::metadata::TRANSFER_PAGE_BYTES)
            .enumerate()
        {
            assert!(matches!(
                groups[to].apply(DataCommand::AuthorityTransfer(
                    AuthorityDataCommand::ImportPage(TransferPage {
                        plan: plan.id,
                        offset: (index * crate::metadata::TRANSFER_PAGE_BYTES) as u64,
                        bytes: Bytes::copy_from_slice(chunk),
                        digest: *blake3::hash(chunk).as_bytes(),
                    })
                )),
                DataResponse::AuthorityTransfer(TransferOutcome::ImportProgress { .. })
            ));
        }
        let imported = groups[to].apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::FinishImport { plan: plan.id },
        ));
        assert!(
            matches!(
                imported,
                DataResponse::AuthorityTransfer(TransferOutcome::Imported(_))
            ),
            "Import failed: {imported:?}"
        );
        if round == 0 {
            let command = DataCommand::AuthorityTransfer(AuthorityDataCommand::ReclaimSource {
                plan: Box::new(checkpointed_report(plan.clone(), &bytes, cut)),
            });
            assert!(matches!(
                groups[from].apply(command.clone()),
                DataResponse::AuthorityTransfer(TransferOutcome::SourceReclaimed { .. })
            ));
            groups[from].apply(DataCommand::AuthorityTransfer(
                AuthorityDataCommand::RecordRetiredCut {
                    plan: plan.id,
                    cut: CommitPosition { index: 101, ..cut },
                },
            ));
            original_reclaim = Some(command);
            assert!(groups[from].producer(forward.partition).is_none());
            assert!(matches!(
                publish(&mut groups[from], other, other_shard, 2),
                DataResponse::Published(_)
            ));
        }
        for group in &mut groups {
            *group = postcard::from_bytes(&postcard::to_stdvec(group).unwrap()).unwrap();
            group.validate().unwrap();
        }
    }
    let mut next = publish_command(forward.partition, forward.shard, 2);
    if let DataCommand::Publish { writer, .. } = &mut next {
        writer.owner_epoch = 3;
    }
    assert!(matches!(groups[0].apply(next), DataResponse::Published(_)));
    assert!(matches!(
        groups[0].apply(original_reclaim.unwrap()),
        DataResponse::AuthorityTransfer(TransferOutcome::SourceReclaimed { .. })
    ));
    groups[0].apply(DataCommand::AuthorityTransfer(
        AuthorityDataCommand::Freeze {
            plan: Box::new(forward.clone()),
            capacity: capacity(),
        },
    ));
    assert_eq!(
        groups[0]
            .producer(forward.partition)
            .unwrap()
            .committed_end(),
        2
    );
    assert_eq!(groups[0].producer(other).unwrap().committed_end(), 2);
    groups[0].validate().unwrap();
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
    let owned = source.transfer_export(plan.id).unwrap();
    let mut owned_bytes = Vec::new();
    owned.write_to(&mut owned_bytes).unwrap();
    assert_eq!(owned_bytes, expected);
    assert_eq!(
        source.transfer_export_size(plan.id).unwrap(),
        expected.len() as u64
    );
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
    let mut detached_bytes = Vec::new();
    owned.write_to(&mut detached_bytes).unwrap();
    assert_eq!(detached_bytes, expected);
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

#[test]
fn target_import_replays_the_original_publication_after_paged_recovery() {
    let mut plan = plan();
    let mut source = application(&plan);
    let original_command = publish_body(
        plan.partition,
        plan.shard,
        1,
        Bytes::from(vec![b'p'; 70 * 1024]),
    );
    let original = source.apply(original_command.clone());
    assert!(matches!(original, DataResponse::Published(_)));
    let mut target = initialized(plan.source.cluster_id, plan.target.group_id, None);
    let capacity = TransferCapacity {
        max_hot_bytes: 128 * 1024,
        max_bytes: 256 * 1024,
        ..capacity()
    };
    let reserve = DataCommand::AuthorityTransfer(AuthorityDataCommand::Reserve {
        plan: Box::new(plan.clone()),
        capacity,
    });
    let reserved = DataResponse::AuthorityTransfer(TransferOutcome::Reserved { plan: plan.id });
    assert_eq!(target.apply(reserve.clone()), reserved);
    assert_eq!(target.apply(reserve), reserved);
    assert_eq!(target.transfer_capacity(plan.id).unwrap(), capacity);
    source.apply(DataCommand::AuthorityTransfer(
        AuthorityDataCommand::Freeze {
            plan: Box::new(plan.clone()),
            capacity,
        },
    ));
    let cut = CommitPosition {
        term: 1,
        leader_node: 1,
        index: 100,
    };
    source.apply(DataCommand::AuthorityTransfer(
        AuthorityDataCommand::RecordCut { plan: plan.id, cut },
    ));
    let mut bytes = Vec::new();
    source.write_transfer_payload(plan.id, &mut bytes).unwrap();
    plan.advance(TransferStep::Frozen(cut)).unwrap();
    plan.advance(TransferStep::Exported(TransferManifest {
        bytes: bytes.len() as u64,
        digest: *blake3::hash(&bytes).as_bytes(),
        source_cut: cut,
        committed_end: 1,
    }))
    .unwrap();
    target.apply(DataCommand::AuthorityTransfer(
        AuthorityDataCommand::BeginImport {
            plan: Box::new(plan.clone()),
        },
    ));
    assert_eq!(
        target.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::FinishImport { plan: plan.id }
        )),
        DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::NotReady))
    );
    let mut offset = 0;
    for chunk in bytes.chunks(crate::metadata::TRANSFER_PAGE_BYTES) {
        let page = DataCommand::AuthorityTransfer(AuthorityDataCommand::ImportPage(TransferPage {
            plan: plan.id,
            offset,
            bytes: Bytes::copy_from_slice(chunk),
            digest: *blake3::hash(chunk).as_bytes(),
        }));
        offset += chunk.len() as u64;
        let expected = DataResponse::AuthorityTransfer(TransferOutcome::ImportProgress {
            plan: plan.id,
            received: offset,
        });
        let mut bad_page = page.clone();
        if let DataCommand::AuthorityTransfer(AuthorityDataCommand::ImportPage(page)) =
            &mut bad_page
        {
            let mut damaged = page.bytes.to_vec();
            damaged[0] ^= 1;
            page.bytes = damaged.into();
        }
        assert_eq!(
            target.apply(bad_page),
            DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::InvalidPlan))
        );
        assert_eq!(target.apply(page.clone()), expected);
        assert_eq!(target.apply(page), expected);
        target = postcard::from_bytes(&postcard::to_stdvec(&target).unwrap()).unwrap();
        target.validate().unwrap();
    }
    let finish =
        DataCommand::AuthorityTransfer(AuthorityDataCommand::FinishImport { plan: plan.id });
    let imported = target.apply(finish.clone());
    assert!(
        matches!(&imported, DataResponse::AuthorityTransfer(TransferOutcome::Imported(recovered))
        if recovered.fence.grant == plan.target && recovered.checkpoint.committed_end == 1),
        "Target import was rejected: {imported:?}"
    );
    target.validate().unwrap();
    assert_eq!(target.apply(finish), imported);
    let mut changed = bytes
        .chunks(crate::metadata::TRANSFER_PAGE_BYTES)
        .next()
        .unwrap()
        .to_vec();
    changed[0] ^= 1;
    let digest = *blake3::hash(&changed).as_bytes();
    assert_eq!(
        target.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::ImportPage(TransferPage {
                plan: plan.id,
                offset: 0,
                bytes: changed.into(),
                digest,
            })
        )),
        DataResponse::Rejected(DataRejection::Transfer(TransferDataRejection::Conflict))
    );
    let mut retry = original_command;
    if let DataCommand::Publish { writer, .. } = &mut retry {
        *writer = WriterFence {
            node_id: plan.target.owner_node,
            owner_epoch: plan.target.epoch,
        };
    }
    let replay = target.apply(retry);
    assert!(
        matches!((original, replay), (DataResponse::Published(first), DataResponse::Published(repeated))
        if first.offset == repeated.offset && repeated.duplicate)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_three_replica_groups_preserve_original_reads_and_nack_results_after_import() {
    use crate::subscription::{
        GroupAck, GroupCommand, GroupIdentity, GroupOutcome, GroupRead, MemberGrant, SigningKey,
    };
    use tidemq_protocol::topic::SubscriptionKind;
    let source = super::support::Cluster::new(&[1, 2, 3]).await;
    let mut target =
        super::support::Cluster::new_group(source.config.cluster_id, 42, &[1, 2, 3]).await;
    let mut plan = plan();
    plan.source = source.grant(plan.partition, 1);
    plan.source_voters = [1, 2, 3].into();
    plan.target = Grant {
        cluster_id: source.config.cluster_id,
        partition: plan.partition,
        group_id: 42,
        owner_node: 1,
        epoch: 2,
        expected_route_generation: 1,
        metadata_revision: 2,
        operation_id: Command::grant_ownership(
            1,
            source.config.cluster_id,
            plan.partition,
            1,
            1,
            1,
        )
        .unwrap()
        .operation_id(),
    };
    plan.target_voters = [1, 2, 3].into();
    plan.checkpoint_voters = plan.target_voters.clone();
    source.fence_and_recover(&plan.source).await;
    for sequence in 1..=2 {
        assert!(matches!(
            source
                .write(publish_body(
                    plan.partition,
                    plan.shard,
                    sequence,
                    Bytes::from(vec![b'p'; 70 * 1024])
                ))
                .await,
            DataResponse::Published(_)
        ));
    }
    let subscription = Id::new_v7().unwrap();
    let member = Id::new_v7().unwrap();
    let partition = plan.partition;
    let data = |writer: &Grant, command| DataCommand::ConsumerGroup {
        partition,
        writer: WriterFence {
            node_id: writer.owner_node,
            owner_epoch: writer.epoch,
        },
        command,
    };
    assert_eq!(
        source
            .write(data(
                &plan.source,
                GroupCommand::Create {
                    identity: GroupIdentity {
                        topic: plan.partition.topic_id,
                        shard: plan.shard,
                        subscription
                    },
                    model: SubscriptionKind::Queue,
                    start: 0,
                    signing_key: SigningKey::try_from([41; 32]).unwrap(),
                    blocked: false,
                }
            ))
            .await,
        DataResponse::ConsumerGroup(GroupOutcome::Created)
    );
    source
        .write(data(
            &plan.source,
            GroupCommand::InstallMembers {
                subscription,
                version: 1,
                grants: vec![MemberGrant {
                    member,
                    client: member,
                    credential: member,
                    epoch: 1,
                    expires_ms: 100_000,
                    lanes: vec![],
                    max_messages: 8,
                    max_bytes: 256 * 1024,
                }],
                now_ms: 1000,
            },
        ))
        .await;
    let read = GroupRead {
        member,
        epoch: 1,
        sequence: 1,
        now_ms: 1000,
        max_messages: 2,
        max_bytes: 256 * 1024,
    };
    let original_read = source
        .write(data(
            &plan.source,
            GroupCommand::Read {
                subscription,
                request: read,
                cold: vec![],
            },
        ))
        .await;
    let DataResponse::ConsumerGroup(GroupOutcome::Read(deliveries)) = &original_read else {
        panic!("Expected an original read result: {original_read:?}");
    };
    assert_eq!(deliveries.deliveries.len(), 2);
    let ack = GroupAck {
        member,
        epoch: 1,
        sequence: 1,
        now_ms: 1000,
        model: SubscriptionKind::Queue,
        proofs: vec![deliveries.deliveries[0].clone()],
        nack_delay_ms: None,
    };
    let original_ack = source
        .write(data(
            &plan.source,
            GroupCommand::Acknowledge {
                subscription,
                request: ack.clone(),
            },
        ))
        .await;
    let nack = GroupAck {
        sequence: 2,
        proofs: vec![deliveries.deliveries[1].clone()],
        nack_delay_ms: Some(100),
        ..ack.clone()
    };
    let original_nack = source
        .write(data(
            &plan.source,
            GroupCommand::Acknowledge {
                subscription,
                request: nack.clone(),
            },
        ))
        .await;
    assert!(matches!(
        &original_nack,
        DataResponse::ConsumerGroup(GroupOutcome::Acknowledged(_))
    ));
    let capacity = TransferCapacity {
        max_bytes: 512 * 1024,
        max_hot_bytes: 256 * 1024,
        max_in_flight_bytes: 256 * 1024,
        ..capacity()
    };
    assert!(matches!(
        target
            .write(DataCommand::AuthorityTransfer(
                AuthorityDataCommand::Reserve {
                    plan: Box::new(plan.clone()),
                    capacity
                }
            ))
            .await,
        DataResponse::AuthorityTransfer(TransferOutcome::Reserved { .. })
    ));
    let frozen = source
        .node(1)
        .raft
        .client_write(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::Freeze {
                plan: Box::new(plan.clone()),
                capacity,
            },
        ))
        .await
        .unwrap();
    assert!(
        matches!(
            frozen.data,
            DataResponse::AuthorityTransfer(TransferOutcome::Frozen { .. })
        ),
        "Source freeze failed: {:?}",
        frozen.data
    );
    let cut = CommitPosition {
        term: frozen.log_id.leader_id.term,
        leader_node: frozen.log_id.leader_id.node_id,
        index: frozen.log_id.index,
    };
    source
        .write(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::RecordCut { plan: plan.id, cut },
        ))
        .await;
    let bytes = source
        .node(1)
        .state
        .try_with_application(|app| {
            let mut bytes = Vec::new();
            app.write_transfer_payload(plan.id, &mut bytes).unwrap();
            bytes
        })
        .await
        .unwrap();
    plan.advance(TransferStep::Frozen(cut)).unwrap();
    plan.advance(TransferStep::Exported(TransferManifest {
        bytes: bytes.len() as u64,
        digest: *blake3::hash(&bytes).as_bytes(),
        source_cut: cut,
        committed_end: 2,
    }))
    .unwrap();
    target
        .write(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::BeginImport {
                plan: Box::new(plan.clone()),
            },
        ))
        .await;
    for (index, chunk) in bytes
        .chunks(crate::metadata::TRANSFER_PAGE_BYTES)
        .enumerate()
    {
        let page = DataCommand::AuthorityTransfer(AuthorityDataCommand::ImportPage(TransferPage {
            plan: plan.id,
            offset: (index * crate::metadata::TRANSFER_PAGE_BYTES) as u64,
            bytes: Bytes::copy_from_slice(chunk),
            digest: *blake3::hash(chunk).as_bytes(),
        }));
        assert!(matches!(
            target.write(page).await,
            DataResponse::AuthorityTransfer(TransferOutcome::ImportProgress { .. })
        ));
        if index == 0 {
            target.restart(1).await;
        }
    }
    let imported = target
        .write(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::FinishImport { plan: plan.id },
        ))
        .await;
    assert!(
        matches!(
            imported,
            DataResponse::AuthorityTransfer(TransferOutcome::Imported(_))
        ),
        "Target import failed: {imported:?}"
    );
    target.restart(1).await;
    assert_eq!(
        target
            .write(data(
                &plan.target,
                GroupCommand::Read {
                    subscription,
                    request: read,
                    cold: vec![]
                }
            ))
            .await,
        original_read
    );
    assert_eq!(
        target
            .write(data(
                &plan.target,
                GroupCommand::RetireRead {
                    subscription,
                    member,
                    epoch: 1,
                    sequence: 1
                }
            ))
            .await,
        DataResponse::ConsumerGroup(GroupOutcome::ReadRetired)
    );
    let second = target
        .write(data(
            &plan.target,
            GroupCommand::Read {
                subscription,
                request: GroupRead {
                    sequence: 2,
                    now_ms: 1101,
                    ..read
                },
                cold: vec![],
            },
        ))
        .await;
    let DataResponse::ConsumerGroup(GroupOutcome::Read(second)) = second else {
        panic!("Expected redelivery after import: {second:?}");
    };
    assert_eq!(second.deliveries.len(), 1);
    assert_eq!(second.deliveries[0].offset, 1);
    assert_eq!(second.deliveries[0].attempt, 2);
    assert_eq!(
        target
            .write(data(
                &plan.target,
                GroupCommand::Acknowledge {
                    subscription,
                    request: ack.clone()
                }
            ))
            .await,
        original_ack
    );
    assert_eq!(
        target
            .write(data(
                &plan.target,
                GroupCommand::Acknowledge {
                    subscription,
                    request: nack.clone()
                }
            ))
            .await,
        original_nack
    );
    assert_eq!(
        target
            .write(data(
                &plan.target,
                GroupCommand::Acknowledge {
                    subscription,
                    request: GroupAck {
                        sequence: 3,
                        nack_delay_ms: None,
                        ..nack
                    }
                }
            ))
            .await,
        DataResponse::Rejected(DataRejection::Subscription(
            crate::subscription::Rejection::StaleAttempt
        ))
    );
    target
        .node(1)
        .state
        .try_with_application(|app| app.validate())
        .await
        .unwrap()
        .unwrap();
    target.shutdown().await;
    source.shutdown().await;
}
