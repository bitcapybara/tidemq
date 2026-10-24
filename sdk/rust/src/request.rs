//! Total request budgets and cooperative cancellation without background timer tasks.

use std::future::Future;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{Error, Result};

tokio::task_local! {
    static REQUEST_CONTEXT: Arc<ScopedContext>;
    static OPERATION: Arc<Operation>;
}

/// An absolute total deadline and a caller-owned cancellation parent.
/// Reusing this context across calls never refreshes its remaining budget.
#[derive(Clone, Debug)]
pub struct RequestContext {
    deadline: Instant,
    cancellation: CancellationToken,
}

impl RequestContext {
    pub fn new(deadline: Instant, cancellation: CancellationToken) -> Self {
        Self {
            deadline,
            cancellation,
        }
    }

    pub fn with_deadline(deadline: Instant) -> Self {
        Self::new(deadline, CancellationToken::new())
    }

    pub fn with_timeout(timeout: Duration) -> Result<Self> {
        Ok(Self::with_deadline(crate::client::deadline_after(timeout)?))
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Cancelling this handle cancels its scopes, never their ancestors.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn child(&self) -> Self {
        Self::new(self.deadline, self.cancellation.child_token())
    }

    /// Scopes SDK calls in the polled future. Spawned tasks must scope a clone.
    /// Nested scopes keep the earliest deadline and every parent's cancellation.
    pub async fn scope<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        let parent = REQUEST_CONTEXT.try_with(Arc::clone).ok();
        let depth = parent
            .as_ref()
            .map_or(Some(1), |parent| parent.depth.checked_add(1))
            .ok_or(Error::Configuration {
                reason: "request context nesting exceeds its bound",
            })?;
        if depth > 16 {
            return Err(Error::Configuration {
                reason: "request context nesting exceeds its bound",
            });
        }
        let deadline = parent
            .as_ref()
            .map_or(self.deadline, |parent| self.deadline.min(parent.deadline));
        let scope = Arc::new(ScopedContext {
            deadline,
            cancellation: self.cancellation.child_token(),
            parent,
            depth,
            abandoned: OnceLock::new(),
        });
        let mut future = Box::pin(REQUEST_CONTEXT.scope(scope.clone(), future));
        let (result, interrupted) = tokio::select! {
            biased;
            _ = scope.cancelled() => (Err(Error::Cancelled), true),
            _ = tokio::time::sleep_until(deadline) => (Err(Error::Deadline), true),
            result = &mut future => (result, false),
        };
        drop(future);
        if interrupted && let Some(operation) = scope.abandoned.get() {
            return result.map_err(|error| operation.interrupted(error));
        }
        result
    }
}

struct ScopedContext {
    deadline: Instant,
    cancellation: CancellationToken,
    parent: Option<Arc<Self>>,
    depth: usize,
    abandoned: OnceLock<Arc<Operation>>,
}

impl ScopedContext {
    async fn parent_cancelled(&self) {
        match &self.parent {
            Some(parent) => Box::pin(parent.cancelled()).await,
            None => std::future::pending().await,
        }
    }

    async fn cancelled(&self) {
        tokio::select! {
            _ = self.cancellation.cancelled() => {},
            _ = self.parent_cancelled() => {},
        }
    }
}

#[derive(Clone)]
pub(crate) enum Recovery {
    Send(crate::PendingSend),
    Batch(Vec<crate::PendingSend>),
    Ack(crate::PendingAck),
    SubscriptionDeletion(crate::PendingSubscriptionDeletion),
    Administration(crate::PendingAdmin),
}

struct Operation {
    deadline: Instant,
    sent: AtomicBool,
    recovery: OnceLock<Recovery>,
    scope: std::sync::Weak<ScopedContext>,
}

struct RunGuard {
    operation: Arc<Operation>,
    scope: Arc<ScopedContext>,
    complete: bool,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        if self.complete || !self.operation.sent.load(Ordering::Acquire) {
            return;
        }
        let mut scope = Some(self.scope.clone());
        while let Some(current) = scope {
            let _ = current.abandoned.set(self.operation.clone());
            scope = current.parent.clone();
        }
    }
}

impl Operation {
    fn interrupted(&self, error: Error) -> Error {
        let error = if self.sent.load(Ordering::Acquire) {
            Error::RequestUnknown {
                source: Box::new(error),
            }
        } else {
            error
        };
        match self.recovery.get() {
            Some(Recovery::Send(pending)) => Error::SendUnresolved {
                pending: pending.clone(),
                source: Arc::new(error),
            },
            Some(Recovery::Batch(pending)) => Error::BatchSendInterrupted {
                pending: pending.clone(),
                source: Arc::new(error),
            },
            Some(Recovery::Ack(pending)) => Error::Acknowledgement {
                pending: pending.clone(),
                source: Arc::new(error),
            },
            Some(Recovery::SubscriptionDeletion(pending)) => Error::SubscriptionDeletion {
                pending: pending.clone(),
                source: Arc::new(error),
            },
            Some(Recovery::Administration(pending)) => Error::Administration {
                pending: pending.clone(),
                source: Arc::new(error),
            },
            None => error,
        }
    }
}

/// Nested SDK calls use their original operation; retries cannot refresh the budget.
pub(crate) async fn run<T, F: Future<Output = Result<T>>>(
    timeout: Duration,
    closed: &CancellationToken,
    operation: impl FnOnce(Instant) -> F,
) -> Result<T> {
    if let Ok(parent) = OPERATION.try_with(Arc::clone) {
        let scope = REQUEST_CONTEXT
            .try_with(Arc::clone)
            .ok()
            .or_else(|| parent.scope.upgrade());
        let deadline = scope
            .as_ref()
            .map_or(parent.deadline, |scope| parent.deadline.min(scope.deadline));
        let mut guard = scope.map(|scope| RunGuard {
            operation: parent,
            scope,
            complete: false,
        });
        let result = operation(deadline).await;
        if let Some(guard) = &mut guard {
            guard.complete = true;
        }
        return result;
    }
    let default_deadline = crate::client::deadline_after(timeout)?;
    let scope = match REQUEST_CONTEXT.try_with(Arc::clone) {
        Ok(scope) => scope,
        Err(_) => Arc::new(ScopedContext {
            deadline: default_deadline,
            cancellation: CancellationToken::new(),
            parent: None,
            depth: 1,
            abandoned: OnceLock::new(),
        }),
    };
    let deadline = scope.deadline.min(default_deadline);
    let state = Arc::new(Operation {
        deadline,
        sent: AtomicBool::new(false),
        recovery: OnceLock::new(),
        scope: Arc::downgrade(&scope),
    });
    if Instant::now() >= deadline {
        return Err(Error::Deadline);
    }
    let cancellation = scope.cancellation.child_token();
    let _guard = cancellation.clone().drop_guard();
    let mut guard = RunGuard {
        operation: state.clone(),
        scope: scope.clone(),
        complete: false,
    };
    let result = OPERATION
        .scope(Arc::clone(&state), async {
            tokio::select! {
                biased;
                _ = closed.cancelled() => Err(state.interrupted(Error::Closed)),
                _ = scope.parent_cancelled() => Err(state.interrupted(Error::Cancelled)),
                _ = cancellation.cancelled() => Err(state.interrupted(Error::Cancelled)),
                result = tokio::time::timeout_at(deadline, operation(deadline)) => {
                    match result {
                        Ok(Err(error @ (Error::Deadline | Error::Cancelled | Error::Closed))) => {
                            Err(state.interrupted(error))
                        }
                        Ok(result) => result,
                        Err(_) => Err(state.interrupted(Error::Deadline)),
                    }
                },
            }
        })
        .await;
    guard.complete = true;
    result
}

pub(crate) fn mark_sent() {
    let _ = OPERATION.try_with(|operation| operation.sent.store(true, Ordering::Release));
}

// The outer context can drop the business future; retain its exact admitted work first.
pub(crate) fn retain(recovery: Recovery) {
    let _ = OPERATION.try_with(|operation| operation.recovery.set(recovery));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scopes_bound_result_futures_that_do_not_make_a_network_request() {
        let context = RequestContext::with_timeout(Duration::from_millis(5)).unwrap();
        assert!(matches!(
            context.scope(std::future::pending::<Result<()>>()).await,
            Err(Error::Deadline)
        ));
        let context = RequestContext::with_timeout(Duration::from_secs(1)).unwrap();
        context.cancellation_token().cancel();
        let polled = AtomicBool::new(false);
        let result = context
            .scope(async {
                polled.store(true, Ordering::Release);
                Ok(())
            })
            .await;
        assert!(matches!(result, Err(Error::Cancelled)));
        assert!(!polled.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn a_scope_keeps_unknown_outcomes_when_it_drops_an_admitted_operation() {
        let context = RequestContext::with_timeout(Duration::from_millis(10)).unwrap();
        let result = context
            .scope(run(
                Duration::from_secs(1),
                &CancellationToken::new(),
                |_| async {
                    mark_sent();
                    std::future::pending::<Result<()>>().await
                },
            ))
            .await;
        assert!(result.unwrap_err().outcome_unknown());
    }

    #[tokio::test]
    async fn expired_or_cancelled_requests_never_poll_the_operation() {
        let polled = AtomicBool::new(false);
        let context = RequestContext::with_deadline(Instant::now());
        let execute = || async {
            run(
                Duration::from_secs(1),
                &CancellationToken::new(),
                |_| async {
                    polled.store(true, Ordering::Release);
                    Ok(())
                },
            )
            .await
        };
        assert!(matches!(
            context.scope(execute()).await,
            Err(Error::Deadline)
        ));
        let context = RequestContext::with_timeout(Duration::from_secs(1)).unwrap();
        context.cancellation_token().cancel();
        assert!(matches!(
            context.scope(execute()).await,
            Err(Error::Cancelled)
        ));
        assert!(!polled.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn nested_scopes_keep_earliest_budget_across_sequential_calls() {
        let parent = RequestContext::with_timeout(Duration::from_secs(1)).unwrap();
        let later = RequestContext::with_timeout(Duration::from_secs(10)).unwrap();
        let expected = parent.deadline();
        parent
            .scope(later.scope(async {
                for _ in 0..2 {
                    run(
                        Duration::from_secs(20),
                        &CancellationToken::new(),
                        |deadline| async move {
                            assert_eq!(deadline, expected);
                            tokio::task::yield_now().await;
                            Ok(())
                        },
                    )
                    .await?;
                }
                Ok(())
            }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn long_scope_cannot_extend_client_timeout_or_refresh_nested_budget() {
        let context = RequestContext::with_timeout(Duration::from_secs(30)).unwrap();
        let latest = Instant::now() + Duration::from_secs(1);
        context
            .scope(run(
                Duration::from_millis(100),
                &CancellationToken::new(),
                |outer| async move {
                    assert!(outer < latest);
                    tokio::task::yield_now().await;
                    run(
                        Duration::from_secs(20),
                        &CancellationToken::new(),
                        |inner| async move {
                            assert_eq!(inner, outer);
                            Ok(())
                        },
                    )
                    .await
                },
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn child_cancellation_isolated_from_parent_and_siblings() {
        let parent = RequestContext::with_timeout(Duration::from_secs(1)).unwrap();
        let first = parent.child();
        let second = parent.child();
        first.cancellation_token().cancel();
        assert!(!parent.cancellation_token().is_cancelled());
        assert!(!second.cancellation_token().is_cancelled());
        assert!(matches!(
            first
                .scope(run(
                    Duration::from_secs(1),
                    &CancellationToken::new(),
                    |_| async { Ok(()) }
                ))
                .await,
            Err(Error::Cancelled)
        ));
        second
            .scope(run(
                Duration::from_secs(1),
                &CancellationToken::new(),
                |_| async { Ok(()) },
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancelling_outer_scope_interrupts_nested_pending_operation() {
        let parent = RequestContext::with_timeout(Duration::from_secs(3)).unwrap();
        let inner = RequestContext::with_timeout(Duration::from_secs(10)).unwrap();
        let cancellation = parent.cancellation_token();
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            parent
                .scope(inner.scope(run(
                    Duration::from_secs(20),
                    &CancellationToken::new(),
                    |_| async {
                        started.send(()).unwrap();
                        std::future::pending::<Result<()>>().await
                    },
                )))
                .await
        });
        ready.await.unwrap();
        cancellation.cancel();
        assert!(matches!(task.await.unwrap(), Err(Error::Cancelled)));
    }

    #[tokio::test]
    async fn dropping_operation_releases_permit_without_cancelling_context() {
        let context = RequestContext::with_timeout(Duration::from_secs(3)).unwrap();
        let cancellation = context.cancellation_token();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let reserved = Arc::clone(&semaphore);
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            context
                .scope(run(
                    Duration::from_secs(20),
                    &CancellationToken::new(),
                    |_| async {
                        let _permit = reserved.acquire_owned().await.unwrap();
                        started.send(()).unwrap();
                        std::future::pending::<Result<()>>().await
                    },
                ))
                .await
        });
        ready.await.unwrap();
        assert_eq!(semaphore.available_permits(), 0);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(semaphore.available_permits(), 1);
        assert!(!cancellation.is_cancelled());
    }
}
