use std::{error::Error as StdError, future::Future};

use tidemq::{runtime, scaling::MembershipPlan};
use tokio::time::Instant;

pub(super) async fn replace_original<F, Attempt>(
    original: &MembershipPlan,
    deadline: Instant,
    mut attempt: F,
) -> runtime::Result<MembershipPlan>
where
    F: FnMut(Instant) -> Attempt,
    Attempt: Future<Output = runtime::Result<MembershipPlan>>,
{
    loop {
        if Instant::now() >= deadline {
            return Err(runtime::Error::unknown());
        }
        let result = tokio::time::timeout_at(deadline, attempt(deadline))
            .await
            .map_err(|_| runtime::Error::unknown())?;
        match result {
            Ok(plan) => {
                assert_eq!(
                    &plan, original,
                    "A recovered replacement must retain its original canonical plan"
                );
                return Ok(plan);
            }
            Err(error) if retryable_unknown(&error) && Instant::now() < deadline => {
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn retryable_unknown(error: &(dyn StdError + 'static)) -> bool {
    let mut source = Some(error);
    for _ in 0..16 {
        let Some(error) = source else { break };
        if let Some(runtime::Error::Rejected { failure }) = error.downcast_ref() {
            return failure.retryable
                && failure.outcome == i32::from(tidemq_protocol::common::Outcome::Unknown);
        }
        if matches!(
            error.downcast_ref::<tidemq::scaling::Error>(),
            Some(tidemq::scaling::Error::Unknown)
        ) {
            return true;
        }
        source = if let Some(io) = error.downcast_ref::<std::io::Error>() {
            io.get_ref().map(|inner| inner as &(dyn StdError + 'static))
        } else {
            error.source()
        };
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::ready, time::Duration};
    use tidemq::metadata::{DataGroup, Rejection};
    use tidemq_protocol::{Id, common::ErrorCode, common::Outcome};

    fn plan() -> MembershipPlan {
        MembershipPlan::new(
            Id::random().unwrap(),
            &DataGroup {
                group_id: 2,
                generation: 1,
                voters: [21, 22, 23].into(),
            },
            [21, 22, 24].into(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn original_plan_resumes_unknown_with_the_same_absolute_deadline() {
        let original = plan();
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut attempts = 0;
        let result = replace_original(&original, deadline, |observed_deadline| {
            assert_eq!(observed_deadline, deadline);
            attempts += 1;
            ready(if attempts == 1 {
                Err(runtime::Error::operation(
                    "execute replica replacement",
                    tidemq::scaling::Error::Remote {
                        source: Box::new(runtime::Error::unknown()),
                    },
                ))
            } else {
                Ok(original.clone())
            })
        })
        .await
        .unwrap();
        assert_eq!(result, original);
        assert_eq!(attempts, 2);
    }

    #[tokio::test]
    async fn permanent_rejections_and_unclassified_errors_stop_after_one_attempt() {
        let original = plan();
        for error in [
            runtime::Error::reject(ErrorCode::Forbidden, Outcome::Rejected, false),
            runtime::Error::reject(ErrorCode::Unavailable, Outcome::Unknown, false),
            runtime::Error::reject(ErrorCode::Unavailable, Outcome::Rejected, true),
            runtime::Error::operation(
                "metadata rejected the plan",
                tidemq::scaling::Error::Rejected {
                    reason: Rejection::Superseded,
                },
            ),
            runtime::Error::invalid("The replacement input is invalid"),
        ] {
            let mut error = Some(error);
            let mut attempts = 0;
            let result =
                replace_original(&original, Instant::now() + Duration::from_secs(30), |_| {
                    attempts += 1;
                    ready(Err(error
                        .take()
                        .expect("A permanent failure must not be retried")))
                })
                .await;
            assert!(result.is_err());
            assert_eq!(attempts, 1);
        }
    }

    #[tokio::test]
    async fn expired_deadline_never_starts_another_attempt() {
        let original = plan();
        let mut attempts = 0;
        let result = replace_original(&original, Instant::now(), |_| {
            attempts += 1;
            ready(Ok(original.clone()))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(attempts, 0);
    }
}
