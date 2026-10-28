use super::*;
use crate::runtime::wire::semantic_local_tests::Fixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_seal_deadline_keeps_a_handed_off_write_unknown_and_recovers_its_plan() {
    use std::time::Duration;
    let fixture = Fixture::new().await;
    let producer = fixture
        .client
        .new_producer("typed/app/events")
        .build()
        .await
        .unwrap();
    producer.send("handed-off-seal").await.unwrap();
    producer.close().await.unwrap();
    let service = fixture.service.clone();
    let partition = fixture.partition;
    let plan = Id::new_v7().unwrap();
    let directory = service.config.data_dir.join("group-2");
    let pause = crate::execution::LocalExecutionHook::new(&directory, "before_apply", false);
    let handler = service.clone();
    let until = Instant::now() + Duration::from_millis(250);
    let mut pending =
        tokio::spawn(async move { handler.seal_data_domain(plan, partition, 2, until).await });
    tokio::time::timeout(Duration::from_secs(2), pause.entered())
        .await
        .unwrap();
    let observation =
        tokio::time::timeout_at(until + Duration::from_millis(200), &mut pending).await;
    let respected_deadline = observation.is_ok();
    pause.release();
    let result = match observation {
        Ok(result) => result.unwrap(),
        Err(_) => pending.await.unwrap(),
    };
    let outcome = result.err().unwrap().failure().outcome;
    let replica = service.data.get(&2).unwrap();
    replica
        .group
        .ensure_read(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    let original = replica
        .state
        .try_with_application(|state| state.shard_seal(partition))
        .await
        .unwrap();
    let replayed = service
        .seal_data_domain(plan, partition, 2, Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    drop(pause);
    fixture.close().await;
    assert!(
        respected_deadline,
        "A handed-off seal waiter outlived its original deadline"
    );
    assert_eq!(
        outcome,
        i32::from(tidemq_protocol::common::Outcome::Unknown)
    );
    assert_eq!(
        original,
        Some((plan, 1)),
        "The original executor-owned seal must finish after its waiter times out"
    );
    assert_eq!(replayed, 1);
}

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
