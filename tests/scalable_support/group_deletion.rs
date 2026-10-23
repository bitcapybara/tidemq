use super::authority_data::{application, initialized, plan, publish};
use crate::{
    broker::{DataApplication, DataCommand, DataRejection, DataResponse, WriterFence},
    metadata::{AuthorityTransfer, MEMBER_RECOVERY_MS},
    replication::Application,
    subscription::{
        AckDisposition, GroupAck, GroupCommand, GroupIdentity, GroupOutcome, GroupRead,
        MemberGrant, ReadResult, Rejection,
    },
};
use tidemq_protocol::{Id, topic::SubscriptionKind};

fn apply(
    app: &mut DataApplication,
    plan: &AuthorityTransfer,
    command: GroupCommand,
) -> DataResponse {
    let result = app.apply(DataCommand::ConsumerGroup {
        partition: plan.partition,
        writer: WriterFence {
            node_id: plan.source.owner_node,
            owner_epoch: plan.source.epoch,
        },
        command,
    });
    app.validate().unwrap();
    result
}

#[test]
fn deleted_subscription_tombstones_follow_the_shard_into_a_different_raft_authority() {
    use crate::{
        broker::{AuthorityDataCommand, TransferOutcome, TransferPage},
        metadata::{TRANSFER_PAGE_BYTES, TransferManifest, TransferStep},
        ownership::CommitPosition,
    };
    let mut plan = plan();
    let mut source = application(&plan);
    assert!(matches!(
        publish(&mut source, plan.partition, plan.shard, 1),
        DataResponse::Published(_)
    ));
    let original_ack_bytes = source.capacity_usage().unwrap().ack_state_logical_bytes;
    let subscription = Id::new_v7().unwrap();
    create(&mut source, &plan, subscription);
    let deletion = Id::new_v7().unwrap();
    assert_eq!(
        apply(
            &mut source,
            &plan,
            GroupCommand::Delete {
                subscription,
                deletion,
                now_ms: 10
            }
        ),
        DataResponse::ConsumerGroup(GroupOutcome::Deleted { deletion })
    );
    let mut target = initialized(plan.source.cluster_id, plan.target.group_id, None);
    let capacity = target.transfer_capacity(plan.id).unwrap();
    assert!(matches!(
        target.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::Reserve {
                plan: Box::new(plan.clone()),
                capacity
            }
        )),
        DataResponse::AuthorityTransfer(TransferOutcome::Reserved { .. })
    ));
    assert!(matches!(
        source.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::Freeze {
                plan: Box::new(plan.clone()),
                capacity
            }
        )),
        DataResponse::AuthorityTransfer(TransferOutcome::Frozen { .. })
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
    let mut offset = 0;
    for bytes in bytes.chunks(TRANSFER_PAGE_BYTES) {
        assert!(matches!(
            target.apply(DataCommand::AuthorityTransfer(
                AuthorityDataCommand::ImportPage(TransferPage {
                    plan: plan.id,
                    offset,
                    bytes: bytes.to_vec().into(),
                    digest: *blake3::hash(bytes).as_bytes()
                })
            )),
            DataResponse::AuthorityTransfer(TransferOutcome::ImportProgress { .. })
        ));
        offset += bytes.len() as u64;
    }
    assert!(matches!(
        target.apply(DataCommand::AuthorityTransfer(
            AuthorityDataCommand::FinishImport { plan: plan.id }
        )),
        DataResponse::AuthorityTransfer(TransferOutcome::Imported(_))
    ));
    let writer = WriterFence {
        node_id: plan.target.owner_node,
        owner_epoch: plan.target.epoch,
    };
    assert_eq!(
        target.apply(DataCommand::ConsumerGroup {
            partition: plan.partition,
            writer,
            command: GroupCommand::Create {
                identity: GroupIdentity {
                    topic: plan.partition.topic_id,
                    shard: plan.shard,
                    subscription
                },
                model: SubscriptionKind::Queue,
                start: 0,
                signing_key: [8; 32].try_into().unwrap(),
                blocked: false
            }
        }),
        DataResponse::Rejected(DataRejection::Subscription(Rejection::Deleted))
    );
    assert_eq!(
        target.apply(DataCommand::ConsumerGroup {
            partition: plan.partition,
            writer,
            command: GroupCommand::Delete {
                subscription,
                deletion,
                now_ms: 20
            }
        }),
        DataResponse::ConsumerGroup(GroupOutcome::Deleted { deletion })
    );
    assert_eq!(
        target.capacity_usage().unwrap().ack_state_logical_bytes,
        original_ack_bytes + 64
    );
    target.validate().unwrap();
}

fn create(app: &mut DataApplication, plan: &AuthorityTransfer, subscription: Id) -> DataResponse {
    apply(
        app,
        plan,
        GroupCommand::Create {
            identity: GroupIdentity {
                topic: plan.partition.topic_id,
                shard: plan.shard,
                subscription,
            },
            model: SubscriptionKind::Queue,
            start: 0,
            signing_key: [7; 32].try_into().unwrap(),
            blocked: false,
        },
    )
}

fn grant() -> MemberGrant {
    MemberGrant {
        member: Id::new_v7().unwrap(),
        client: Id::new_v7().unwrap(),
        credential: Id::new_v7().unwrap(),
        epoch: 1,
        expires_ms: 1000,
        lanes: Vec::new(),
        max_messages: 8,
        max_bytes: 4096,
    }
}

fn read(
    app: &mut DataApplication,
    plan: &AuthorityTransfer,
    subscription: Id,
    grant: &MemberGrant,
) -> ReadResult {
    assert_eq!(
        apply(
            app,
            plan,
            GroupCommand::InstallMembers {
                subscription,
                version: 1,
                grants: vec![grant.clone()],
                now_ms: 1
            }
        ),
        DataResponse::ConsumerGroup(GroupOutcome::MembersInstalled)
    );
    match apply(
        app,
        plan,
        GroupCommand::Read {
            subscription,
            request: GroupRead {
                member: grant.member,
                epoch: grant.epoch,
                sequence: 1,
                now_ms: 2,
                max_messages: 1,
                max_bytes: 4096,
            },
            cold: Vec::new(),
        },
    ) {
        DataResponse::ConsumerGroup(GroupOutcome::Read(result)) => result,
        response => panic!("Read was rejected: {response:?}"),
    }
}

#[test]
fn deleting_a_fenced_subscription_keeps_unretired_reads_and_acknowledgements_protected() {
    let plan = plan();
    let mut app = application(&plan);
    assert!(matches!(
        publish(&mut app, plan.partition, plan.shard, 1),
        DataResponse::Published(_)
    ));
    let subscription = Id::new_v7().unwrap();
    assert_eq!(
        create(&mut app, &plan, subscription),
        DataResponse::ConsumerGroup(GroupOutcome::Created)
    );
    let member = grant();
    let result = read(&mut app, &plan, subscription, &member);
    assert_eq!(result.deliveries.len(), 1);
    let deletion = Id::new_v7().unwrap();
    let delete = GroupCommand::Delete {
        subscription,
        deletion,
        now_ms: 10,
    };
    assert_eq!(
        apply(&mut app, &plan, delete.clone()),
        DataResponse::Rejected(DataRejection::Subscription(Rejection::AssignmentBusy))
    );
    assert_eq!(
        apply(
            &mut app,
            &plan,
            GroupCommand::Acknowledge {
                subscription,
                request: GroupAck {
                    member: member.member,
                    epoch: 1,
                    sequence: 1,
                    now_ms: 3,
                    model: SubscriptionKind::Queue,
                    proofs: result.deliveries,
                    nack_delay_ms: None
                }
            }
        ),
        DataResponse::ConsumerGroup(GroupOutcome::Acknowledged(AckDisposition::Acknowledged))
    );
    assert_eq!(
        apply(
            &mut app,
            &plan,
            GroupCommand::InstallMembers {
                subscription,
                version: 2,
                grants: Vec::new(),
                now_ms: 4
            }
        ),
        DataResponse::ConsumerGroup(GroupOutcome::MembersInstalled)
    );
    assert_eq!(
        apply(&mut app, &plan, delete.clone()),
        DataResponse::Rejected(DataRejection::Subscription(Rejection::RecoveryPending))
    );
    assert_eq!(
        app.consumer_group_cursor(plan.partition, subscription)
            .unwrap()
            .protected_before(),
        0
    );
    apply(
        &mut app,
        &plan,
        GroupCommand::RetireRead {
            subscription,
            member: member.member,
            epoch: 1,
            sequence: 1,
        },
    );
    assert_eq!(
        apply(&mut app, &plan, delete.clone()),
        DataResponse::Rejected(DataRejection::Subscription(Rejection::RecoveryPending))
    );
    apply(
        &mut app,
        &plan,
        GroupCommand::RetireAck {
            subscription,
            member: member.member,
            epoch: 1,
            sequence: 1,
        },
    );
    let deleted = DataResponse::ConsumerGroup(GroupOutcome::Deleted { deletion });
    assert_eq!(apply(&mut app, &plan, delete.clone()), deleted);
    let mut app: DataApplication =
        postcard::from_bytes(&postcard::to_stdvec(&app).unwrap()).unwrap();
    app.validate().unwrap();
    assert_eq!(apply(&mut app, &plan, delete.clone()), deleted);
    assert!(
        app.consumer_group_cursor(plan.partition, subscription)
            .is_none()
    );
    assert_eq!(
        create(&mut app, &plan, subscription),
        DataResponse::Rejected(DataRejection::Subscription(Rejection::Deleted))
    );
    assert_eq!(
        apply(
            &mut app,
            &plan,
            GroupCommand::Delete {
                subscription,
                deletion: Id::new_v7().unwrap(),
                now_ms: 10
            }
        ),
        DataResponse::Rejected(DataRejection::Subscription(Rejection::IdentityConflict))
    );
    let replacement = Id::new_v7().unwrap();
    assert_eq!(
        create(&mut app, &plan, replacement),
        DataResponse::ConsumerGroup(GroupOutcome::Created)
    );
    assert_eq!(apply(&mut app, &plan, delete), deleted);
    assert!(
        app.consumer_group_cursor(plan.partition, replacement)
            .is_some()
    );
    assert_eq!(app.producer(plan.partition).unwrap().committed_end(), 1);
}

#[test]
fn deletion_waits_for_a_durable_member_fence_even_after_cached_grants_expire() {
    let plan = plan();
    let mut app = application(&plan);
    assert!(matches!(
        publish(&mut app, plan.partition, plan.shard, 1),
        DataResponse::Published(_)
    ));
    let subscription = Id::new_v7().unwrap();
    create(&mut app, &plan, subscription);
    let member = grant();
    read(&mut app, &plan, subscription, &member);
    let deletion = Id::new_v7().unwrap();
    let expired = member.expires_ms + MEMBER_RECOVERY_MS + 1;
    assert_eq!(
        apply(
            &mut app,
            &plan,
            GroupCommand::Delete {
                subscription,
                deletion,
                now_ms: expired
            }
        ),
        DataResponse::Rejected(DataRejection::Subscription(Rejection::AssignmentBusy))
    );
    apply(
        &mut app,
        &plan,
        GroupCommand::InstallMembers {
            subscription,
            version: 2,
            grants: Vec::new(),
            now_ms: 3,
        },
    );
    let mut app: DataApplication =
        postcard::from_bytes(&postcard::to_stdvec(&app).unwrap()).unwrap();
    assert_eq!(
        apply(
            &mut app,
            &plan,
            GroupCommand::Delete {
                subscription,
                deletion,
                now_ms: 4
            }
        ),
        DataResponse::Rejected(DataRejection::Subscription(Rejection::RecoveryPending))
    );
    assert_eq!(
        apply(
            &mut app,
            &plan,
            GroupCommand::Delete {
                subscription,
                deletion,
                now_ms: expired
            }
        ),
        DataResponse::ConsumerGroup(GroupOutcome::Deleted { deletion })
    );
    assert!(
        app.consumer_group_cursor(plan.partition, subscription)
            .is_none()
    );
}
