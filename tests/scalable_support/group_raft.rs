use super::support::*;
use crate::broker::{DataCommand, DataResponse, ProducerCommand};
use crate::metadata::PartitionKey;
use crate::producer::RegisterProducer;
use crate::replication::Application;
use crate::subscription::{
    AckDisposition, GroupAck, GroupCommand, GroupIdentity, GroupOutcome, GroupRead, MemberGrant,
    SigningKey,
};
use tidemq_protocol::{Id, topic::SubscriptionKind};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_consumers_replay_nack_and_attempts_after_a_three_replica_restart() {
    let mut cluster = Cluster::new(&[1, 2, 3]).await;
    let partition = PartitionKey {
        topic_id: Id::new_v7().unwrap(),
        partition: 0,
    };
    let grant = cluster.grant(partition, 1);
    cluster.fence_and_recover(&grant).await;
    let writer = writer(&grant);
    let DataResponse::Registered(session) = cluster
        .write(DataCommand::Producer {
            partition,
            writer,
            now_ms: 1000,
            command: ProducerCommand::Register(RegisterProducer {
                producer_id: Id::new_v7().unwrap(),
                expected_registration_epoch: 1,
            }),
        })
        .await
    else {
        panic!("Expected a registered producer");
    };
    for (sequence, body) in [(0, b"first".as_slice()), (1, b"second".as_slice())] {
        assert!(matches!(
            cluster
                .write(DataCommand::Producer {
                    partition,
                    writer,
                    now_ms: 1000,
                    command: ProducerCommand::Append(batch(partition, &session, sequence, body))
                })
                .await,
            DataResponse::Produced(_)
        ));
    }
    let subscription = Id::new_v7().unwrap();
    let command = |command| DataCommand::ConsumerGroup {
        partition,
        writer,
        command,
    };
    assert_eq!(
        cluster
            .write(command(GroupCommand::Create {
                blocked: false,
                identity: GroupIdentity {
                    topic: partition.topic_id,
                    shard: Id::new_v7().unwrap(),
                    subscription
                },
                model: SubscriptionKind::Queue,
                start: 0,
                signing_key: SigningKey::try_from([42; 32]).unwrap()
            }))
            .await,
        DataResponse::ConsumerGroup(GroupOutcome::Created)
    );
    let first = Id::new_v7().unwrap();
    let second = Id::new_v7().unwrap();
    let grants = [first, second]
        .into_iter()
        .map(|member| MemberGrant {
            member,
            client: member,
            credential: member,
            epoch: 1,
            expires_ms: 15000,
            lanes: Vec::new(),
            max_messages: 8,
            max_bytes: 4096,
        })
        .collect();
    cluster
        .write(command(GroupCommand::InstallMembers {
            subscription,
            version: 1,
            grants,
            now_ms: 1000,
        }))
        .await;
    let read = GroupRead {
        member: first,
        epoch: 1,
        sequence: 1,
        now_ms: 1000,
        max_messages: 1,
        max_bytes: 2048,
    };
    let DataResponse::ConsumerGroup(GroupOutcome::Read(original)) = cluster
        .write(command(GroupCommand::Read {
            subscription,
            request: read,
        }))
        .await
    else {
        panic!("Expected an original durable Read result");
    };
    let repeated = cluster
        .write(command(GroupCommand::Read {
            subscription,
            request: read,
        }))
        .await;
    assert_eq!(
        repeated,
        DataResponse::ConsumerGroup(GroupOutcome::Read(original.clone()))
    );
    cluster
        .write(command(GroupCommand::RetireRead {
            subscription,
            member: first,
            epoch: 1,
            sequence: 1,
        }))
        .await;
    let nack = GroupAck {
        member: first,
        epoch: 1,
        sequence: 1,
        now_ms: 1000,
        model: SubscriptionKind::Queue,
        proofs: original.deliveries,
        nack_delay_ms: Some(10),
    };
    let expected = DataResponse::ConsumerGroup(GroupOutcome::Acknowledged(
        AckDisposition::RedeliveryScheduled { due_ms: 1010 },
    ));
    assert_eq!(
        cluster
            .write(command(GroupCommand::Acknowledge {
                subscription,
                request: nack.clone()
            }))
            .await,
        expected
    );
    cluster.restart(1).await;
    assert_eq!(
        cluster
            .write(command(GroupCommand::Acknowledge {
                subscription,
                request: nack
            }))
            .await,
        expected
    );
    let DataResponse::ConsumerGroup(GroupOutcome::Read(redelivered)) = cluster
        .write(command(GroupCommand::Read {
            subscription,
            request: GroupRead {
                member: second,
                epoch: 1,
                sequence: 1,
                now_ms: 1011,
                max_messages: 8,
                max_bytes: 2048,
            },
        }))
        .await
    else {
        panic!("Expected a durable redelivery after restart");
    };
    assert_eq!(
        redelivered
            .deliveries
            .iter()
            .find(|proof| proof.offset == 0)
            .unwrap()
            .attempt,
        2
    );
    assert_eq!(
        redelivered
            .deliveries
            .iter()
            .find(|proof| proof.offset == 1)
            .unwrap()
            .attempt,
        1
    );
    cluster
        .node(1)
        .state
        .with_application(|state| state.validate().unwrap())
        .await;
    cluster.shutdown().await;
}
