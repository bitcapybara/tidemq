use super::*;
use crate::runtime::wire::semantic_local_tests::Fixture;

#[tokio::test]
async fn local_seal_commits_and_replays_the_original_plan_without_remote_encoding() {
    let fixture = Fixture::new().await;
    let producer = fixture
        .client
        .new_producer("typed/app/events")
        .build()
        .await
        .unwrap();
    producer.send("sealed-state").await.unwrap();
    producer.close().await.unwrap();
    let plan = Id::new_v7().unwrap();
    let result = wire::without_remote_encoding(async {
        let deadline = Instant::now() + std::time::Duration::from_secs(3);
        let end = fixture
            .service
            .seal_data_domain(plan, fixture.partition, 2, deadline)
            .await?;
        assert_eq!(end, 1);
        let replayed = fixture
            .service
            .seal_data_domain(plan, fixture.partition, 2, deadline)
            .await?;
        assert_eq!(replayed, end);
        Ok::<_, Error>(())
    })
    .await;
    fixture.close().await;
    result.unwrap();
}
