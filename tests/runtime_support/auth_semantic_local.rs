use super::*;
use crate::runtime::wire::semantic_local_tests::{Fixture, TOKEN};

#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn metadata_authorization_does_not_reenter_its_only_core_handler() {
    use crate::config::Role;
    use crate::runtime::data::consumer_tests::fixture::Fixture as RuntimeFixture;
    use crate::runtime::watch_test_hooks::CoreHandlerLimit;

    let mut runtime = RuntimeFixture::new(
        &[
            (1, Role::Metadata),
            (2, Role::Storage),
            (3, Role::Broker),
            (4, Role::Gateway),
        ],
        &[1],
        &[2],
    )
    .await;
    let cluster: Id = runtime.configs.get(&1).unwrap().cluster_id.parse().unwrap();
    let limit = CoreHandlerLimit::install(cluster, 1, 1).unwrap();
    runtime.enable_startup_timeline();
    runtime
        .record_startup_event("core_handler_limit_installed", Some(1), "limit=1")
        .await;
    runtime.start_node(1).await;
    assert!(limit.applied());
    runtime
        .record_startup_event("core_handler_limit_applied", Some(1), "applied=true")
        .await;
    runtime.wait_ready().await;
    for node in [2, 3, 4] {
        runtime.start_node(node).await;
    }
    runtime.wait_ready().await;
    let timeline = tokio::fs::read_to_string(runtime.directory.join("readiness-timeline.jsonl"))
        .await
        .unwrap();
    let events = timeline
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    let mut previous_elapsed = 0;
    for event in &events {
        assert!(
            event
                .get("event")
                .and_then(serde_json::Value::as_str)
                .is_some()
        );
        assert!(
            event
                .get("elapsed_ms")
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|elapsed| {
                    let monotonic = elapsed >= previous_elapsed;
                    previous_elapsed = elapsed;
                    monotonic
                })
        );
        assert!(
            event
                .get("detail")
                .and_then(serde_json::Value::as_str)
                .is_some()
        );
        let name = event
            .get("event")
            .and_then(serde_json::Value::as_str)
            .unwrap();
        match name {
            "running_node_started" | "readyz_state_changed" => {
                let node = event
                    .get("node_id")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap();
                assert!((1..=4).contains(&node));
            }
            "core_handler_limit_installed" | "core_handler_limit_applied" => {
                assert_eq!(
                    event.get("node_id").and_then(serde_json::Value::as_u64),
                    Some(1)
                );
            }
            "fixture_start"
            | "readiness_wait_start"
            | "fixture_ready"
            | "readiness_deadline_elapsed"
            | "readiness_diagnostics_written" => {
                assert!(event.get("node_id").is_some_and(serde_json::Value::is_null));
            }
            other => panic!("Unexpected runtime startup event: {other}"),
        }
    }
    let event_index = |name: &str, node: Option<u64>| {
        events
            .iter()
            .position(|event| {
                event.get("event").and_then(serde_json::Value::as_str) == Some(name)
                    && event.get("node_id").and_then(serde_json::Value::as_u64) == node
            })
            .unwrap()
    };
    let installed = event_index("core_handler_limit_installed", Some(1));
    let metadata_started = event_index("running_node_started", Some(1));
    let applied = event_index("core_handler_limit_applied", Some(1));
    let ready_events = events
        .iter()
        .enumerate()
        .filter(|(_, event)| {
            event.get("event").and_then(serde_json::Value::as_str) == Some("readyz_state_changed")
                && event.get("detail").and_then(serde_json::Value::as_str) == Some("readyz_200")
        })
        .collect::<Vec<_>>();
    let first_ready = events
        .iter()
        .position(|event| {
            event.get("event").and_then(serde_json::Value::as_str) == Some("fixture_ready")
        })
        .unwrap();
    let second_ready = events
        .iter()
        .enumerate()
        .find(|(index, event)| {
            *index > first_ready
                && event.get("event").and_then(serde_json::Value::as_str) == Some("fixture_ready")
        })
        .map(|(index, _)| index)
        .unwrap();
    assert!(installed < metadata_started);
    assert!(metadata_started < applied);
    assert!(applied < first_ready);
    for node in [2, 3, 4] {
        let started = event_index("running_node_started", Some(node));
        assert!(first_ready < started);
        assert!(started < second_ready);
        assert!(ready_events.iter().any(|(index, event)| {
            *index > started
                && *index < second_ready
                && event.get("node_id").and_then(serde_json::Value::as_u64) == Some(node)
        }));
    }
    assert!(ready_events.iter().any(|(index, event)| {
        *index < first_ready && event.get("node_id").and_then(serde_json::Value::as_u64) == Some(1)
    }));
    for node in 1..=4 {
        assert!(ready_events.iter().any(|(index, event)| {
            *index < second_ready
                && event.get("node_id").and_then(serde_json::Value::as_u64) == Some(node)
        }));
    }
    let client = runtime.client(4).await;
    let tenant = client.admin().create_tenant("auth").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = client
        .admin()
        .create_topic("auth/app/events")
        .await
        .unwrap();
    let service = runtime.running.get(&1).unwrap().service.clone();
    service.auth_cache.0.lock().unwrap().clear();
    let until = Instant::now() + Duration::from_secs(2);
    service.leaders.write().await.insert(1, (1, until));
    let pause = crate::runtime::test_hooks::InstalledPause::metadata_route(cluster, 1).unwrap();
    let query = {
        let client = client.clone();
        tokio::spawn(async move {
            tidemq_sdk::RequestContext::with_deadline(until)
                .scope(client.admin().topic_info("auth/app/events"))
                .await
        })
    };
    pause.wait(until).await.unwrap();
    service.auth_cache.0.lock().unwrap().clear();
    service.leaders.write().await.clear();
    // This current-thread runtime cannot repopulate the hint before auth polls leader discovery.
    let cold = service
        .auth_query(
            AuthQuery::Authenticate(TokenDigest::of(TOKEN).unwrap()),
            until,
        )
        .await;
    pause.resume();
    let result = query.await.unwrap();
    drop(pause);
    client.close().await.unwrap();
    runtime.close().await;
    drop(limit);
    assert!(cold.unwrap().0.credential.is_some());
    assert_eq!(result.unwrap().id(), topic.id());
}

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
