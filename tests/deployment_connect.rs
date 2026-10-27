#[path = "deployment/connection.rs"]
mod connection;

use std::{collections::VecDeque, future::ready, sync::Arc, time::Duration};

use tidemq_protocol::common::{ErrorCode, Outcome};
use tidemq_sdk::{Error, ErrorKind};
use tokio::time::Instant;

fn remote(code: ErrorCode, retryable: bool) -> Error {
    Error::Remote {
        code,
        message: "Scripted connection setup refusal".into(),
        outcome: Outcome::Rejected,
        retryable,
        expected_sequence: None,
        expected_registration_epoch: None,
        retry_after_ms: None,
        original_outcome_known: false,
    }
}

#[tokio::test]
async fn transient_setup_refusals_retry_only_with_the_original_connect_deadline() {
    let original = Instant::now() + Duration::from_secs(2);
    let mut replies = VecDeque::from([
        Err(Error::Transport {
            source: tidemq::transport::Error::Admission {
                resource: "remote authenticated connection capacity",
            },
        }),
        Err(remote(ErrorCode::Unavailable, true)),
        Ok("connected"),
    ]);
    let mut attempts = 0;
    let connected = connection::connect_until(original, |deadline| {
        assert_eq!(deadline, original);
        assert!(Instant::now() < original);
        attempts += 1;
        ready(replies.pop_front().expect("Unexpected connection attempt"))
    })
    .await
    .unwrap();
    assert_eq!(connected, "connected");
    assert_eq!(attempts, 3);
    assert!(replies.is_empty());
}

#[tokio::test]
async fn permanent_authentication_configuration_and_server_refusals_are_not_retried() {
    for error in [
        remote(ErrorCode::Unauthenticated, true),
        remote(ErrorCode::Forbidden, true),
        remote(ErrorCode::QuotaExceeded, false),
        remote(ErrorCode::Unavailable, false),
        Error::Configuration {
            reason: "Invalid connection configuration",
        },
        Error::Transport {
            source: tidemq::transport::Error::Authentication {
                reason: "Credential rejected",
            },
        },
        Error::Background {
            source: Arc::new(remote(ErrorCode::Unavailable, false)),
        },
        Error::Query {
            source: Box::new(remote(ErrorCode::QuotaExceeded, false)),
        },
        Error::Deadline,
        Error::Cancelled,
    ] {
        let expected = error.kind();
        let mut refusal = Some(error);
        let mut attempts = 0;
        let result: tidemq_sdk::Result<()> =
            connection::connect_until(Instant::now() + Duration::from_secs(1), |_| {
                attempts += 1;
                ready(Err(refusal
                    .take()
                    .expect("A permanent refusal was retried")))
            })
            .await;
        assert_eq!(result.unwrap_err().kind(), expected);
        assert_eq!(attempts, 1);
    }
}

#[tokio::test]
async fn an_expired_connect_deadline_never_invokes_the_builder() {
    let mut attempts = 0;
    let result: tidemq_sdk::Result<()> = connection::connect_until(Instant::now(), |_| {
        attempts += 1;
        ready(Ok(()))
    })
    .await;
    assert_eq!(result.unwrap_err().kind(), ErrorKind::DeadlineExceeded);
    assert_eq!(attempts, 0);
}

#[tokio::test]
async fn a_stalled_builder_is_cancelled_at_the_original_connect_deadline() {
    let deadline = Instant::now() + Duration::from_millis(20);
    let mut attempts = 0;
    let result = connection::connect_until(deadline, |observed| {
        assert_eq!(observed, deadline);
        attempts += 1;
        std::future::pending::<tidemq_sdk::Result<()>>()
    })
    .await;
    assert_eq!(result.unwrap_err().kind(), ErrorKind::DeadlineExceeded);
    assert_eq!(attempts, 1);
}

#[tokio::test]
async fn persistent_backpressure_expires_without_resetting_the_connect_budget() {
    let deadline = Instant::now() + Duration::from_millis(350);
    let mut attempts = 0;
    let result: tidemq_sdk::Result<()> = tokio::time::timeout_at(
        deadline + Duration::from_secs(1),
        connection::connect_until(deadline, |observed| {
            assert_eq!(observed, deadline);
            assert!(Instant::now() < deadline);
            attempts += 1;
            ready(Err(remote(ErrorCode::QuotaExceeded, true)))
        }),
    )
    .await
    .expect("Connection retries extended the original finite budget");
    assert_eq!(result.unwrap_err().kind(), ErrorKind::ResourceExhausted);
    assert!(Instant::now() >= deadline);
    assert!(attempts <= 2);
}
