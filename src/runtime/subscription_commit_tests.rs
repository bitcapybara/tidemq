use super::*;

use crate::server::WireService;
use crate::transport::Authorization;
use tidemq_protocol::common::{ErrorReply, TrafficClass};
use tidemq_protocol::framing::new_frame;
use tracing::instrument::WithSubscriber;

static COMMIT_HOOK: Mutex<Option<Arc<CommitHook>>> = Mutex::new(None);

struct CommitHook {
    partition: PartitionKey,
    expires_at: Mutex<Option<std::time::Instant>>,
    ready: Notify,
    resume: Notify,
}

struct InstalledCommitHook(Arc<CommitHook>);

impl InstalledCommitHook {
    fn new(partition: PartitionKey) -> Self {
        let hook = Arc::new(CommitHook {
            partition,
            expires_at: Mutex::new(None),
            ready: Notify::new(),
            resume: Notify::new(),
        });
        assert!(COMMIT_HOOK.lock().unwrap().replace(hook.clone()).is_none());
        Self(hook)
    }
}

impl Drop for InstalledCommitHook {
    fn drop(&mut self) {
        let mut slot = COMMIT_HOOK.lock().unwrap();
        if slot.as_ref().is_some_and(|hook| Arc::ptr_eq(hook, &self.0)) {
            slot.take();
        }
        self.0.resume.notify_one();
    }
}

pub(in crate::runtime::data) async fn after_commit(
    partition: PartitionKey,
    expires_at: std::time::Instant,
) {
    let hook = {
        let mut slot = COMMIT_HOOK.lock().unwrap();
        if slot
            .as_ref()
            .is_some_and(|hook| hook.partition == partition)
        {
            slot.take()
        } else {
            None
        }
    };
    if let Some(hook) = hook {
        *hook.expires_at.lock().unwrap() = Some(expires_at);
        hook.ready.notify_one();
        hook.resume.notified().await;
    }
}

#[test]
fn post_commit_fence_contexts_preserve_unknown_semantics_and_the_failed_stage() {
    let limits = ProtocolLimits::default();
    let request = new_frame(
        FrameKind::ClaimAssignment,
        TrafficClass::Control,
        Id::new_v7().unwrap(),
        Id::new_v7().unwrap(),
        &public::ClaimAssignment::default(),
        10_000,
        &limits,
    )
    .unwrap();
    let expected = wire::error_response(&request, &Error::unknown(), &limits).unwrap();
    for source in [
        std::io::Error::other("State-machine read failed"),
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Committed subscription writer fence is absent",
        ),
    ] {
        let error = Error::operation("read committed subscription writer fence", source);
        wire::assert_unknown_response_contract(
            &wire::error_response(&request, &error, &limits).unwrap(),
            &expected,
            &limits,
            "read committed subscription writer fence",
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_claim_expiry_retains_source_and_unknown_wire_semantics() {
    let _serial = SERIAL.lock().await;
    let mut fixture = Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
    fixture.start_all().await;
    let client = fixture.client(1).await;
    let (topic, subscription) = create(&client).await;
    let service = fixture.running.get(&1).unwrap().service.clone();
    let partition = PartitionKey {
        topic_id: topic,
        partition: 0,
    };
    let until = deadline();
    let session = Id::new_v7().unwrap();
    let request = new_frame(
        FrameKind::ClaimAssignment,
        TrafficClass::Control,
        Id::new_v7().unwrap(),
        Id::new_v7().unwrap(),
        &public::ClaimAssignment {
            topic_id: bytes_id(topic),
            partition_id: 0,
            subscription_id: bytes_id(subscription),
            expected_cursor_generation: 1,
            expected_assignment_generation: 0,
            session_id: bytes_id(session),
            ..public::ClaimAssignment::default()
        },
        10_000,
        &service.config.protocol,
    )
    .unwrap();
    let expected =
        wire::error_response(&request, &Error::unknown(), &service.config.protocol).unwrap();
    let buffer = crate::observability::logging::tests::Buffer(Arc::default());
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(move || writer.clone())
        .finish();
    let hook = InstalledCommitHook::new(partition);
    let submitted = request.clone();
    let handler = service.clone();
    let operation = tokio::spawn(
        async move {
            handler
                .handle(
                    Authorization::External {
                        principal: "runtime-test".into(),
                        scope: "*".into(),
                        allowed_kinds: vec![FrameKind::ClaimAssignment],
                    },
                    &submitted,
                    until,
                    None,
                )
                .await
        }
        .with_subscriber(subscriber),
    );
    tokio::time::timeout_at(until, hook.0.ready.notified())
        .await
        .expect("The real Claim must commit before the post-commit lease check");
    let assignment = service
        .data
        .get(&2)
        .unwrap()
        .state
        .try_with_application(|state| {
            state
                .subscriptions(partition)
                .unwrap()
                .assignment(subscription)
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(assignment.session_id, session);
    assert_eq!(assignment.generations.assignment, 1);
    // Expire the original lease only after the real Raft write has completed.
    let expires_at = hook.0.expires_at.lock().unwrap().unwrap();
    tokio::time::sleep_until(Instant::from_std(expires_at)).await;
    hook.0.resume.notify_one();
    let response = tokio::time::timeout_at(until, operation)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap();
    client.close().await;
    drop(client);
    fixture.close().await;
    wire::assert_unknown_response_contract(
        &response,
        &expected,
        &service.config.protocol,
        "verify committed subscription writer",
    );
    let failure: ErrorReply = decode_payload(response.payload, &service.config.protocol).unwrap();
    assert_eq!(failure.code, i32::from(ErrorCode::Unavailable));
    assert_eq!(failure.outcome, i32::from(Outcome::Unknown));
    assert!(failure.retryable);
    let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    let event = logs
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|event| {
            event["fields"]["event_code"] == "runtime_request_unknown"
                && event["fields"]["request_id"]
                    == wire::id(&request.request_id).unwrap().to_string()
        })
        .expect("The real request handler must log the Unknown result");
    let chain = event["fields"]["error"].as_str().unwrap();
    assert!(
        chain.contains("Control authorization has expired"),
        "The real post-commit lease error must remain in the warning: {event}"
    );
    assert_eq!(
        event["fields"]["operation"],
        "verify committed subscription writer"
    );
    eprintln!("Verified committed Claim diagnostic: {event}");
}
