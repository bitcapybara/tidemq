use super::*;
use crate::runtime::wire::semantic_local_tests::Fixture;

#[tokio::test]
async fn local_retention_prepares_applies_and_completes_without_adapter_roundtrips() {
    let fixture = Fixture::retention().await;
    let producer = fixture
        .client
        .new_producer("typed/app/events")
        .build()
        .await
        .unwrap();
    producer.send("retained-message").await.unwrap();
    producer.close().await.unwrap();
    let eligible = Instant::now() + std::time::Duration::from_secs(3);
    let (now, status) = loop {
        let now = crate::runtime::data::unix_millis().unwrap();
        let status = fixture
            .service
            .data
            .get(&2)
            .unwrap()
            .state
            .try_with_application(|state| state.retention_status(fixture.partition, now).unwrap())
            .await
            .unwrap();
        if status.maximum_floor == 1 {
            break (now, status);
        }
        assert!(
            Instant::now() < eligible,
            "Published data never became retention eligible"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    assert_eq!(status.maximum_floor, 1);
    let deadline = Instant::now() + std::time::Duration::from_secs(3);
    let route = fixture
        .service
        .ensure_route(fixture.partition, deadline)
        .await
        .unwrap();
    let result = wire::without_remote_encoding(async {
        assert!(
            fixture
                .service
                .retention_control(Control::Inspect(fixture.partition), deadline)
                .await?
                .is_none()
        );
        fixture
            .service
            .reserve_and_advance_retention(
                2,
                fixture.partition,
                status,
                now,
                crate::broker::WriterFence {
                    node_id: route.owner_node,
                    owner_epoch: route.epoch,
                },
                deadline,
            )
            .await?;
        let plan = fixture
            .service
            .retention_control(Control::Inspect(fixture.partition), deadline)
            .await?
            .unwrap();
        assert!(plan.completed_revision.unwrap() > plan.command.expected_revision);
        let floor = fixture
            .service
            .data
            .get(&2)
            .unwrap()
            .state
            .try_with_application(|state| {
                state
                    .retention_status(fixture.partition, now)
                    .unwrap()
                    .floor
            })
            .await
            .unwrap();
        assert_eq!(floor, 1);
        Ok::<_, Error>(())
    })
    .await;
    fixture.close().await;
    result.unwrap();
}
