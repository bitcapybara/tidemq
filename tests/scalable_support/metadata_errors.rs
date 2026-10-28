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

#[test]
fn identity_validation_failures_keep_their_specific_public_error_kind() {
    for (reason, code) in [
        (metadata::AuthRejection::NotFound, ErrorCode::NotFound),
        (
            metadata::AuthRejection::Conflict,
            ErrorCode::ContentConflict,
        ),
        (metadata::AuthRejection::Invalid, ErrorCode::InvalidRequest),
    ] {
        let failure = metadata_rejection(metadata::Rejection::Authorization(reason)).failure();
        assert_eq!(failure.code, i32::from(code));
        assert!(!failure.retryable);
    }
}

#[test]
fn local_cluster_operations_return_an_explicit_permanent_rejection() {
    let failure = metadata_rejection(metadata::Rejection::Unsupported).failure();
    assert_eq!(failure.code, i32::from(ErrorCode::InvalidRequest));
    assert!(!failure.retryable);
    assert_eq!(failure.outcome, i32::from(Outcome::Rejected));
    assert!(failure.message.contains("unsupported"));
}
