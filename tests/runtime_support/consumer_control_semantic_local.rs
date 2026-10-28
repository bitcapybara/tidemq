use super::*;
use crate::runtime::wire::semantic_local_tests::Fixture;

#[tokio::test]
async fn local_consumer_progress_uses_the_durable_cursor_without_remote_encoding() {
    let fixture = Fixture::new().await;
    let consumer = fixture
        .client
        .new_stream_consumer("typed/app/events", "workers")
        .build()
        .await
        .unwrap();
    let (row, shard) = fixture
        .service
        .metadata
        .as_ref()
        .unwrap()
        .state
        .try_with_application(|state| {
            (
                state
                    .consumer_subscriptions()
                    .find(|row| row.topic == fixture.partition.topic_id)
                    .unwrap()
                    .clone(),
                state.topology().shard(fixture.shard).unwrap().clone(),
            )
        })
        .await
        .unwrap();
    let result = wire::without_remote_encoding(async {
        let deadline = Instant::now() + std::time::Duration::from_secs(3);
        let plan = row.members.plan();
        let version = plan.version * 2 + u64::from(plan.fenced);
        let grants = row
            .members
            .grants(super::super::data::unix_millis()?)
            .unwrap();
        fixture
            .service
            .install_consumer_domain(&row, shard.route.id, version, grants, deadline)
            .await?;
        fixture
            .service
            .consumer_control(
                shard.route.group,
                Control::Ensure {
                    partition: shard.partition,
                    shard: shard.route.id,
                    subscription: row.id,
                    model: row.members.model(),
                    initial: row.members.initial_position(),
                    blocked: false,
                },
                deadline,
            )
            .await?;
        fixture
            .service
            .consumer_control(
                shard.route.group,
                Control::OpenGate {
                    partition: shard.partition,
                    subscription: row.id,
                },
                deadline,
            )
            .await?;
        let deletion = fixture
            .service
            .consumer_control(
                shard.route.group,
                Control::Delete {
                    partition: shard.partition,
                    domain: shard.route.id,
                    subscription: row.id,
                    deletion: Id::new_v7().unwrap(),
                },
                deadline,
            )
            .await;
        assert!(
            deletion.is_err(),
            "A deletion needs the current durable plan"
        );
        assert!(
            fixture
                .service
                .consumer_domain_progress(&shard, row.id, Instant::now())
                .await
                .is_err(),
            "Expired semantic work must not be admitted"
        );
        fixture
            .service
            .consumer_domain_progress(&shard, row.id, deadline)
            .await
    })
    .await;
    if let Ok(progress) = &result {
        assert_eq!(progress.frontier, 0);
        assert!(progress.gate_open);
    }
    consumer.close().await.unwrap();
    fixture.close().await;
    result.unwrap();
}
