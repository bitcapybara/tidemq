use tidemq_protocol::Id;

use crate::metadata::PartitionKey;
use crate::ownership::{FenceCommand, FenceRejection};
use crate::producer::RegisterProducer;

use super::super::{BrokerLimits, DataCommand, DataRejection, DataResponse, ProducerCommand};
use super::support::{Cluster, batch, context, lease, writer};

#[tokio::test]
async fn a_new_fence_requires_recovery_preserves_original_results_and_freezes_its_checkpoint() {
    let cluster = Cluster::new(&[1]).await;
    let partition = PartitionKey {
        topic_id: Id::new_v7().unwrap(),
        partition: 7,
    };
    let first = cluster.grant(partition, 1);
    cluster.fence_and_recover(&first).await;
    let broker = cluster.broker(BrokerLimits::default());
    let authorization = lease(&first);
    let session = broker
        .register(
            &authorization,
            context(),
            partition,
            RegisterProducer {
                producer_id: Id::new_v7().unwrap(),
                expected_registration_epoch: 1,
            },
        )
        .await
        .unwrap();
    let original = batch(partition, &session, 0, b"private-business-payload");
    broker
        .produce(&authorization, context(), original.clone())
        .await
        .unwrap();
    let second = cluster.grant(partition, 2);
    let fence = DataCommand::InstallFence(FenceCommand {
        grant: second.clone(),
    });
    assert!(matches!(
        cluster.write(fence.clone()).await,
        DataResponse::Fenced(_)
    ));
    let old_append = DataCommand::Producer {
        partition,
        writer: writer(&first),
        now_ms: session.expires_at_ms - 1,
        command: ProducerCommand::Append(original.clone()),
    };
    assert_eq!(
        cluster.write(old_append).await,
        DataResponse::Rejected(DataRejection::Fence(FenceRejection::StaleEpoch))
    );
    let recover = DataCommand::Recover {
        partition,
        writer: writer(&second),
    };
    let premature = DataCommand::Producer {
        partition,
        writer: writer(&second),
        now_ms: 0,
        command: ProducerCommand::Append(original.clone()),
    };
    assert_eq!(
        cluster.write(premature).await,
        DataResponse::Rejected(DataRejection::Fence(FenceRejection::RecoveryRequired))
    );
    let checkpoint = cluster.write(recover.clone()).await;
    assert!(matches!(checkpoint, DataResponse::Recovered(_)));
    let replay = broker
        .produce(&lease(&second), context(), original)
        .await
        .unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.first_offset, 0);
    assert_eq!(cluster.write(recover).await, checkpoint);
    assert!(matches!(
        cluster.write(fence).await,
        DataResponse::Fenced(_)
    ));
    cluster
        .node(1)
        .state
        .with_application(|application| {
            assert_eq!(application.producer(partition).unwrap().committed_end(), 1);
            let debug = format!("{application:?}");
            assert!(!debug.contains("private-business-payload"));
            assert!(!debug.contains("private-key"));
            crate::replication::Application::validate(application).unwrap();
        })
        .await;
    drop(broker);
    cluster.shutdown().await;
}
