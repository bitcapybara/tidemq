use super::*;
use crate::runtime::wire::semantic_local_tests::Fixture;

fn poll_without_waking<F: std::future::Future>(
    work: std::pin::Pin<&mut F>,
) -> std::task::Poll<F::Output> {
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    std::future::Future::poll(work, &mut context)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_retention_projection_wait_obeys_the_original_deadline_after_the_barrier() {
    use std::time::Duration;
    let fixture = Fixture::new().await;
    let service = fixture.service.clone();
    let metadata = service.metadata.as_ref().unwrap();
    let diagnostic = metadata
        .state
        .try_with_application(|state| state.diagnose_ownership(fixture.partition))
        .await
        .unwrap()
        .unwrap();
    let command = crate::metadata::Command::grant_ownership(
        service.meta_revision().await.unwrap(),
        service.cluster,
        fixture.partition,
        service.config.node_id,
        diagnostic.epoch,
        diagnostic.route_generation,
    )
    .unwrap();
    let guard = service.metadata_mutation.lock().await;
    let directory = service.config.data_dir.join("group-1");
    let first = crate::execution::LocalExecutionHook::new(&directory, "before_apply", false);
    let group = metadata.group.clone();
    let write = command.clone();
    let first_writer = tokio::spawn(async move {
        group
            .commit(write, Instant::now() + Duration::from_secs(5))
            .await
    });
    first.entered().await;

    let until = Instant::now() + Duration::from_millis(250);
    let mut pending =
        Box::pin(service.retention_control(Control::Inspect(fixture.partition), until));
    assert!(poll_without_waking(pending.as_mut()).is_pending());
    let second = crate::execution::LocalExecutionHook::new(&directory, "before_apply", false);
    let group = metadata.group.clone();
    let second_writer = tokio::spawn(async move {
        group
            .commit(command, Instant::now() + Duration::from_secs(5))
            .await
    });
    first.release();
    first_writer.await.unwrap().unwrap();
    let view = metadata.state.clone();
    tokio::time::timeout_at(until, async {
        loop {
            tokio::task::yield_now().await;
            let mut probe = Box::pin(view.try_with_application(|_| ()));
            if poll_without_waking(probe.as_mut()).is_pending() {
                break;
            }
        }
    })
    .await
    .expect("The second metadata writer did not queue behind the retention barrier");
    assert!(poll_without_waking(pending.as_mut()).is_pending());
    second.entered().await;
    let observation =
        tokio::time::timeout_at(until + Duration::from_millis(200), pending.as_mut()).await;
    let respected_deadline = observation.is_ok();
    second.release();
    second_writer.await.unwrap().unwrap();
    let late_success = match observation {
        Ok(result) => {
            assert!(result.is_err());
            drop(pending);
            false
        }
        Err(_) => pending.await.is_ok(),
    };
    eprintln!(
        "Typed retention deadline ledger: respected_deadline={respected_deadline} late_success={late_success}"
    );
    drop((first, second, guard));
    fixture.close().await;
    assert!(
        respected_deadline,
        "A local retention projection outlived its original deadline after a successful barrier"
    );
}

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
