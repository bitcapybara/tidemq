use super::*;
use crate::runtime::wire::semantic_local_tests::Fixture;

#[tokio::test]
async fn local_shard_load_reads_real_published_state_without_remote_encoding() {
    let fixture = Fixture::new().await;
    let producer = fixture
        .client
        .new_producer("typed/app/events")
        .build()
        .await
        .unwrap();
    producer.send("load-observation").await.unwrap();
    producer.close().await.unwrap();
    let result = wire::without_remote_encoding(fixture.service.query_shard_loads(
        2,
        &[fixture.partition],
        Instant::now() + std::time::Duration::from_secs(3),
    ))
    .await;
    let reply = result.as_ref().ok();
    if let Some(reply) = reply {
        assert_eq!(reply.node, fixture.service.config.node_id);
        assert_eq!(reply.group, 2);
        assert_eq!(reply.instance, fixture.service.instance);
        assert_eq!(
            reply.reports.first().unwrap().as_ref().unwrap().partition,
            fixture.partition
        );
        assert!(reply.sampled_unix_ms > 0);
    }
    fixture.close().await;
    result.unwrap();
}
