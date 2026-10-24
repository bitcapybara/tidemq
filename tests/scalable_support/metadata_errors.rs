use super::*;

#[test]
fn exhausted_identity_capacity_is_backpressure_and_does_not_revoke_the_consumer() {
    let failure = metadata_rejection(metadata::Rejection::Authorization(
        metadata::AuthRejection::Capacity,
    ))
    .failure();
    assert_eq!(failure.code, i32::from(ErrorCode::QuotaExceeded));
    assert!(failure.retryable);
    let forbidden = metadata_rejection(metadata::Rejection::Authorization(
        metadata::AuthRejection::Forbidden,
    ))
    .failure();
    assert_eq!(forbidden.code, i32::from(ErrorCode::Forbidden));
    assert!(!forbidden.retryable);
}
