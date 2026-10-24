use super::authority_data::{application, plan, publish};
use crate::{
    broker::{DataCommand, DataRejection, DataResponse, QuotaRejection, WriterFence},
    replication::Application,
    subscription::{GroupAck, GroupCommand, GroupIdentity, GroupOutcome, GroupRead, MemberGrant},
};
use tidemq_protocol::{Id, topic::SubscriptionKind};

#[test]
fn tenant_inflight_credit_is_shared_across_subscriptions_and_frozen_quotas_allow_ack_and_replay() {
    for model in [SubscriptionKind::Queue, SubscriptionKind::Stream] {
        let plan = plan();
        let mut app = application(&plan);
        let writer = WriterFence {
            node_id: plan.source.owner_node,
            owner_epoch: plan.source.epoch,
        };
        assert!(matches!(
            publish(&mut app, plan.partition, plan.shard, 1),
            DataResponse::Published(_)
        ));
        let mut quota = app.tenant_quota(plan.partition).unwrap().clone();
        quota.version += 1;
        quota.capacity.inflight_messages = 1;
        let install = |grant| DataCommand::InstallQuota {
            partition: plan.partition,
            writer,
            grant,
            now_ms: 1,
        };
        assert_eq!(
            app.apply(install(quota.clone())),
            DataResponse::QuotaInstalled(quota.clone())
        );
        let member = Id::new_v7().unwrap();
        let subscriptions = [Id::new_v7().unwrap(), Id::new_v7().unwrap()];
        let command = |command| DataCommand::ConsumerGroup {
            partition: plan.partition,
            writer,
            command,
        };
        for subscription in subscriptions {
            assert_eq!(
                app.apply(command(GroupCommand::Create {
                    identity: GroupIdentity {
                        topic: plan.partition.topic_id,
                        shard: plan.shard,
                        subscription
                    },
                    model,
                    start: 0,
                    signing_key: [42; 32].try_into().unwrap(),
                    blocked: false,
                })),
                DataResponse::ConsumerGroup(GroupOutcome::Created)
            );
            assert_eq!(
                app.apply(command(GroupCommand::InstallMembers {
                    subscription,
                    version: 1,
                    now_ms: 1,
                    grants: vec![MemberGrant {
                        member,
                        client: member,
                        credential: member,
                        epoch: 1,
                        expires_ms: 15000,
                        lanes: if model == SubscriptionKind::Stream {
                            vec![tidemq_protocol::topic::HashRange {
                                start: 0,
                                end: None,
                            }]
                        } else {
                            Vec::new()
                        },
                        max_messages: 8,
                        max_bytes: 8192
                    }],
                })),
                DataResponse::ConsumerGroup(GroupOutcome::MembersInstalled)
            );
        }
        let request = GroupRead {
            member,
            epoch: 1,
            sequence: 1,
            now_ms: 2,
            max_messages: 8,
            max_bytes: 8192,
        };
        let read = |subscription| {
            command(GroupCommand::Read {
                subscription,
                request,
                cold: Vec::new(),
            })
        };
        let DataResponse::ConsumerGroup(GroupOutcome::Read(first)) =
            app.apply(read(subscriptions[0]))
        else {
            panic!("Expected a committed delivery");
        };
        assert_eq!(first.deliveries.len(), 1);
        let DataResponse::ConsumerGroup(GroupOutcome::Read(other)) =
            app.apply(read(subscriptions[1]))
        else {
            panic!("Expected a bounded empty read");
        };
        assert!(other.deliveries.is_empty());
        assert_eq!(
            app.tenant_quota_usage(plan.partition)
                .unwrap()
                .inflight_messages,
            1
        );
        quota.version += 1;
        quota.frozen = true;
        assert_eq!(
            app.apply(install(quota.clone())),
            DataResponse::QuotaInstalled(quota)
        );
        assert_eq!(
            app.apply(read(subscriptions[0])),
            DataResponse::ConsumerGroup(GroupOutcome::Read(first.clone()))
        );
        assert_eq!(
            app.apply(command(GroupCommand::Read {
                subscription: subscriptions[1],
                request: GroupRead {
                    sequence: 2,
                    ..request
                },
                cold: Vec::new(),
            })),
            DataResponse::Rejected(DataRejection::Quota(QuotaRejection::Exhausted))
        );
        assert!(matches!(
            app.apply(command(GroupCommand::Acknowledge {
                subscription: subscriptions[0],
                request: GroupAck {
                    member,
                    epoch: 1,
                    sequence: 1,
                    now_ms: 3,
                    model,
                    proofs: first.deliveries,
                    nack_delay_ms: None
                },
            })),
            DataResponse::ConsumerGroup(GroupOutcome::Acknowledged(_))
        ));
        assert_eq!(
            app.tenant_quota_usage(plan.partition)
                .unwrap()
                .inflight_messages,
            0
        );
        let snapshot = postcard::to_stdvec(&app).unwrap();
        let restored: crate::broker::DataApplication = postcard::from_bytes(&snapshot).unwrap();
        restored.validate().unwrap();
        assert_eq!(
            restored
                .tenant_quota_usage(plan.partition)
                .unwrap()
                .backlog_bytes,
            app.tenant_quota_usage(plan.partition)
                .unwrap()
                .backlog_bytes
        );
    }
}

#[test]
fn protecting_historical_messages_checks_tenant_backlog_before_creating_the_cursor() {
    let plan = plan();
    let mut app = application(&plan);
    assert!(matches!(
        publish(&mut app, plan.partition, plan.shard, 1),
        DataResponse::Published(_)
    ));
    let writer = WriterFence {
        node_id: plan.source.owner_node,
        owner_epoch: plan.source.epoch,
    };
    let mut quota = app.tenant_quota(plan.partition).unwrap().clone();
    quota.version += 1;
    quota.capacity.backlog_bytes = 0;
    assert_eq!(
        app.apply(DataCommand::InstallQuota {
            partition: plan.partition,
            writer,
            grant: quota.clone(),
            now_ms: 1
        }),
        DataResponse::QuotaInstalled(quota)
    );
    let subscription = Id::new_v7().unwrap();
    assert_eq!(
        app.apply(DataCommand::ConsumerGroup {
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
                signing_key: [42; 32].try_into().unwrap(),
                blocked: false,
            },
        }),
        DataResponse::Rejected(DataRejection::Quota(QuotaRejection::Exhausted))
    );
    assert!(
        app.consumer_group_cursor(plan.partition, subscription)
            .is_none()
    );
    app.validate().unwrap();
}
