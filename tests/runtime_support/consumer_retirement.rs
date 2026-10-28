use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_storage_leader_hint_cannot_turn_original_read_retirement_into_forbidden() {
    stale_hint_retirement(CommitPoint::Read).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_committed_read_retirement_recovers_through_a_stale_storage_leader_hint() {
    stale_hint_retirement(CommitPoint::RetireRead).await;
}

async fn stale_hint_retirement(point: CommitPoint) {
    let _serial = COMMIT_SERIAL.lock().await;
    let mut fixture = Fixture::new(
        &[
            (11, Role::Metadata),
            (12, Role::Metadata),
            (13, Role::Metadata),
            (21, Role::Storage),
            (22, Role::Storage),
            (23, Role::Storage),
            (31, Role::Broker),
            (41, Role::Gateway),
        ],
        &[11, 12, 13],
        &[21, 22, 23],
    )
    .await;
    fixture.start_all().await;
    let client = fixture.client(41).await;
    let tenant = client.admin().create_tenant("test").await.unwrap();
    client
        .admin()
        .create_namespace(tenant, "app")
        .await
        .unwrap();
    let topic = client.admin().create_topic(TOPIC).await.unwrap();
    let partition = PartitionKey {
        topic_id: topic.id().into(),
        partition: 0,
    };
    let pause = InstalledCommitPause::at(partition, point);
    let queue = client
        .new_queue_consumer(TOPIC, "original")
        .build()
        .await
        .unwrap();
    let broker = fixture.running.get(&31).unwrap().service.clone();
    let route = broker.route(partition, deadline()).await.unwrap();
    let leader = broker.leader(route.group_id, deadline()).await.unwrap();
    let follower = [21, 22, 23]
        .into_iter()
        .find(|node| *node != leader)
        .unwrap();
    let follower_service = fixture.running.get(&follower).unwrap().service.clone();
    assert_eq!(
        follower_service
            .data
            .get(&route.group_id)
            .unwrap()
            .authority_node(),
        Some(leader)
    );

    // The selected mutation is durable, but its original SDK waiter has not received the reply.
    tokio::time::timeout_at(deadline(), pause.0.ready.notified())
        .await
        .unwrap();
    let original = queue.diagnostics().unwrap();
    let sequence = original.sample_read_sequence.unwrap();
    let operation = original.sample_read_operation.unwrap();
    assert_eq!(
        original.sample_read_applied,
        point == CommitPoint::RetireRead
    );
    assert_eq!(original.member_epoch, 1);
    let retry_route = (point == CommitPoint::RetireRead).then(|| {
        crate::runtime::test_hooks::InstalledPause::consumer_retirement_route(partition).unwrap()
    });
    if point == CommitPoint::RetireRead {
        assert_eq!(
            *pause.0.outcome.lock().unwrap(),
            Some(crate::subscription::GroupOutcome::ReadRetired),
            "The timeout must follow the real durable retirement"
        );
        let until = deadline();
        loop {
            let pending = queue.diagnostics().unwrap();
            assert_eq!(pending.sample_read_operation, Some(operation));
            assert_eq!(pending.sample_read_sequence, Some(sequence));
            if pending.ordinary_unknown > 0 && pending.sample_read_failures > 0 {
                assert!(pending.sample_read_applied && pending.sample_read_retiring);
                eprintln!(
                    "Durable retirement handoff ledger: operation={operation} sequence={sequence} durable=ReadRetired waiter=Unknown diagnostics={pending:?}"
                );
                break;
            }
            assert!(
                Instant::now() < until,
                "The original retirement waiter did not record Unknown"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        pause.0.resume.notify_one();
        retry_route
            .as_ref()
            .unwrap()
            .wait(deadline())
            .await
            .unwrap();
        let pending = queue.diagnostics().unwrap();
        assert_eq!(pending.sample_read_operation, Some(operation));
        assert_eq!(pending.sample_read_sequence, Some(sequence));
        assert!(pending.ordinary_unknown > 0);
    }
    // The original retry has reached routing, so the normal 500ms hint cannot expire before use.
    broker.leaders.write().await.insert(
        route.group_id,
        (follower, Instant::now() + Duration::from_millis(500)),
    );
    if let Some(retry_route) = retry_route {
        retry_route.resume();
    }
    pause.0.resume.notify_one();
    let until = deadline();
    let mut failure = None;
    let recovered = loop {
        if let Err(error) = queue.try_receive() {
            failure = Some(error);
            break false;
        }
        let current = queue.diagnostics().unwrap();
        if current
            .sample_read_sequence
            .is_some_and(|next| next > sequence)
        {
            assert_eq!(current.member_epoch, original.member_epoch);
            break true;
        }
        if Instant::now() >= until {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    eprintln!(
        "Original retirement ledger: operation={operation} sequence={sequence} leader={leader} stale_hint={follower} recovered={recovered} failure={failure:?} diagnostics={:?}",
        queue.diagnostics()
    );
    let mut delivered = false;
    if recovered {
        assert_real_retirement_denials(
            &fixture,
            &broker,
            &follower_service,
            &route,
            operation,
            sequence,
        )
        .await;
        let producer = client.new_producer(TOPIC).build().await.unwrap();
        let receipt = producer.send("after-original-retirement").await.unwrap();
        let message = tidemq_sdk::RequestContext::with_deadline(until)
            .scope(queue.receive())
            .await
            .unwrap();
        assert_eq!(message.message_id(), receipt.message_id());
        queue.ack(&message).await.unwrap();
        delivered = true;
        producer.close().await.unwrap();
    }
    drop(pause);
    let queue_close = queue.close().await;
    let client_close = client.close().await;
    fixture.close().await;
    if recovered {
        queue_close.unwrap();
        client_close.unwrap();
    }
    assert!(
        recovered && delivered,
        "A stale Storage leader hint terminated the original Read owner: {failure:?}"
    );
}

async fn assert_real_retirement_denials(
    fixture: &Fixture,
    broker: &Service,
    follower: &Service,
    route: &crate::metadata::Route,
    operation: Id,
    sequence: u64,
) {
    let metadata = broker.leader(1, deadline()).await.unwrap();
    let (subscription, member) = fixture
        .running
        .get(&metadata)
        .unwrap()
        .service
        .metadata
        .as_ref()
        .unwrap()
        .state
        .try_with_application(|state| {
            let row = state
                .consumer_subscriptions()
                .find(|row| row.topic == route.partition.topic_id && row.name == "original")
                .unwrap();
            (
                row.id,
                row.members.observed_members().next().unwrap().clone(),
            )
        })
        .await
        .unwrap();
    let frame = tidemq_protocol::framing::new_frame(
        FrameKind::GroupRetireRead,
        TrafficClass::Control,
        Id::new_v7().unwrap(),
        operation,
        &public::GroupRetire {
            topic_id: Bytes::copy_from_slice(route.partition.topic_id.as_bytes()),
            partition_id: route.partition.partition,
            subscription_id: Bytes::copy_from_slice(subscription.as_bytes()),
            member_id: Bytes::copy_from_slice(member.member.as_bytes()),
            member_epoch: member.epoch,
            sequence,
        },
        3_000,
        &broker.config.protocol,
    )
    .unwrap();
    let access = crate::gateway::Access {
        principal: Arc::from(format!("credential:{}", member.credential)),
        scope: Arc::from("*"),
        session: Some(member.client),
        tenant: None,
    };
    let leader = broker.leader(route.group_id, deadline()).await.unwrap();
    let error = follower
        .forward(
            leader,
            route.group_id,
            access.clone(),
            &frame,
            deadline(),
            None,
            2,
        )
        .await
        .unwrap_err();
    assert_forbidden(error);
    let mut wrong_owner = access.clone();
    wrong_owner.session = Some(Id::new_v7().unwrap());
    let error = broker
        .public(wrong_owner, &frame, deadline(), None, 1)
        .await
        .unwrap_err();
    assert_forbidden(error);
    let mut wrong_credential = access;
    wrong_credential.principal = Arc::from(format!("credential:{}", Id::new_v7().unwrap()));
    let error = broker
        .public(wrong_credential, &frame, deadline(), None, 1)
        .await
        .unwrap_err();
    assert_forbidden(error);
    eprintln!(
        "Retirement denial ledger: Storage delegation, wrong owner and unknown credential remain Forbidden/Rejected/nonretryable"
    );
}

fn assert_forbidden(error: Error) {
    let failure = error.failure();
    assert_eq!(failure.code, i32::from(ErrorCode::Forbidden));
    assert_eq!(failure.outcome, i32::from(Outcome::Rejected));
    assert!(!failure.retryable);
}
