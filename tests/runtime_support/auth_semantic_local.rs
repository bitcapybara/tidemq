use super::*;
use crate::runtime::wire::semantic_local_tests::{Fixture, TOKEN};

#[tokio::test]
async fn local_authentication_and_scope_authorization_do_not_require_remote_encoding() {
    let fixture = Fixture::new().await;
    fixture.service.auth_cache.0.lock().unwrap().clear();
    let result = wire::without_remote_encoding(async {
        let deadline = Instant::now() + Duration::from_secs(3);
        let (authenticated, _) = fixture
            .service
            .auth_query(
                AuthQuery::Authenticate(TokenDigest::of(TOKEN).unwrap()),
                deadline,
            )
            .await?;
        assert_eq!(authenticated.credential, Some(fixture.credential));
        let (authorized, expires) = fixture
            .service
            .auth_query(
                AuthQuery::Authorize {
                    credential: fixture.credential,
                    permission: Permission::Inspect,
                    resource: AuthResource::Topic(fixture.partition.topic_id),
                },
                deadline,
            )
            .await?;
        assert_eq!(authorized.credential, Some(fixture.credential));
        assert!(expires > Instant::now());
        let (birth, _) = fixture
            .service
            .auth_query(AuthQuery::BirthShard(fixture.shard), deadline)
            .await?;
        assert_eq!(birth.birth.unwrap().route.id, fixture.shard);
        Ok::<_, Error>(())
    })
    .await;
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
async fn local_auth_projection_wait_obeys_the_original_deadline_after_the_barrier() {
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

    service.auth_cache.0.lock().unwrap().clear();
    let until = Instant::now() + Duration::from_millis(250);
    let mut pending = Box::pin(service.auth_query(
        AuthQuery::Authenticate(TokenDigest::of(TOKEN).unwrap()),
        until,
    ));
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
    .expect("The second metadata writer did not queue behind the barrier reader");
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
        "Typed auth deadline ledger: respected_deadline={respected_deadline} late_success={late_success}"
    );
    drop((first, second, guard));
    fixture.close().await;
    assert!(
        respected_deadline,
        "A typed local authentication projection outlived its original deadline after a successful barrier"
    );
}
