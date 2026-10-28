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

fn poll_without_waking<F: std::future::Future>(
    work: std::pin::Pin<&mut F>,
) -> std::task::Poll<F::Output> {
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    std::future::Future::poll(work, &mut context)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_load_projection_wait_obeys_the_original_deadline_after_the_barrier() {
    let fixture = Fixture::new().await;
    let service = fixture.service.clone();
    let partition = fixture.partition;
    let grant = service
        .metadata
        .as_ref()
        .unwrap()
        .state
        .try_with_application(|state| state.grant(fixture.partition).cloned())
        .await
        .unwrap()
        .unwrap();
    let replica = service.data.get(&2).unwrap();
    let directory = service.config.data_dir.join("group-2");
    let first = crate::execution::LocalExecutionHook::new(&directory, "before_apply", false);
    let command = || crate::broker::DataCommand::Recover {
        partition,
        writer: crate::broker::WriterFence {
            node_id: grant.owner_node,
            owner_epoch: grant.epoch,
        },
    };
    let group = replica.group.clone();
    let write = command();
    let first_writer = tokio::spawn(async move {
        group
            .commit(write, Instant::now() + std::time::Duration::from_secs(5))
            .await
    });
    first.entered().await;

    let partitions = [partition];
    let until = Instant::now() + std::time::Duration::from_millis(250);
    let mut pending = Box::pin(service.query_shard_loads(2, &partitions, until));
    assert!(poll_without_waking(pending.as_mut()).is_pending());
    let second = crate::execution::LocalExecutionHook::new(&directory, "before_apply", false);
    let group = replica.group.clone();
    let write = command();
    let second_writer = tokio::spawn(async move {
        group
            .commit(write, Instant::now() + std::time::Duration::from_secs(5))
            .await
    });
    first.release();
    first_writer.await.unwrap().unwrap();

    let view = replica.state.clone();
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
    .expect("The second data writer did not queue behind the barrier reader");
    assert!(poll_without_waking(pending.as_mut()).is_pending());
    second.entered().await;

    let observation = tokio::time::timeout_at(
        until + std::time::Duration::from_millis(200),
        pending.as_mut(),
    )
    .await;
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
        "Typed load deadline ledger: respected_deadline={respected_deadline} late_success={late_success}"
    );
    drop((first, second));
    fixture.close().await;
    assert!(
        respected_deadline,
        "A typed local shard-load projection outlived its original deadline after a successful barrier"
    );
}
