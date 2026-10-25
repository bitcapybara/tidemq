use super::*;
use tidemq_protocol::common::{ErrorCode, Outcome};

fn refusal(code: ErrorCode, outcome: Outcome, retryable: bool) -> Error {
    Error::Remote {
        code,
        outcome,
        retryable,
        message: "Cleanup refusal".into(),
        expected_sequence: None,
        expected_registration_epoch: None,
        retry_after_ms: None,
        original_outcome_known: false,
    }
}

#[tokio::test]
async fn transient_cleanup_uses_the_remaining_close_budget_after_a_request_finishes() {
    let mut calls = 0;
    let result = retry_cleanup(Instant::now() + Duration::from_secs(1), || {
        calls += 1;
        let current = calls;
        async move {
            if current < 8 {
                Err(refusal(ErrorCode::Unavailable, Outcome::Unknown, true))
            } else {
                Ok(42)
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(result, 42);
    assert_eq!(calls, 8);
}

#[tokio::test]
async fn a_permanent_refusal_stops_cleanup_without_erasing_an_earlier_unknown() {
    let mut calls = 0;
    let result: Result<()> = retry_cleanup(Instant::now() + Duration::from_secs(1), || {
        calls += 1;
        let current = calls;
        async move {
            Err(if current == 1 {
                refusal(ErrorCode::Unavailable, Outcome::Unknown, true)
            } else {
                refusal(ErrorCode::Forbidden, Outcome::Rejected, false)
            })
        }
    })
    .await;
    let error = result.unwrap_err();
    assert_eq!(calls, 2);
    assert!(error.outcome_unknown());
    assert_eq!(error.kind(), crate::ErrorKind::PermissionDenied);
}

#[tokio::test]
async fn cleanup_cannot_refresh_its_deadline_or_start_after_it_expires() {
    let mut calls = 0;
    let until = Instant::now() + Duration::from_millis(25);
    let result: Result<()> = tokio::time::timeout(
        Duration::from_secs(1),
        retry_cleanup(until, || {
            calls += 1;
            async { Err(refusal(ErrorCode::Unavailable, Outcome::Unknown, true)) }
        }),
    )
    .await
    .unwrap();
    let error = result.unwrap_err();
    if calls == 0 {
        assert!(matches!(error, Error::Deadline));
    } else {
        assert!(error.outcome_unknown());
    }
    assert!(calls <= 3);
    let previous = calls;
    let result: Result<()> = retry_cleanup(until, || {
        calls += 1;
        async { Ok(()) }
    })
    .await;
    assert!(matches!(result, Err(Error::Deadline)));
    assert_eq!(calls, previous);
}
