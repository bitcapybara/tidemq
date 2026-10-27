use super::{Decimal, Error, Id, Instant, Result, Service, view, wire};
use std::time::Duration;

const PAGE_SAMPLE_BUDGET: Duration = Duration::from_millis(750);
const SHARDS_PER_SUBSCRIPTION: u32 = 32;

impl Service {
    pub(super) async fn console_enrich_subscriptions(
        &self,
        credential: Id,
        page: &mut view::Page<view::Subscription>,
        deadline: Instant,
    ) -> Result<()> {
        let until = deadline.min(Instant::now() + PAGE_SAMPLE_BUDGET);
        for row in &mut page.items {
            row.unconfirmed = None;
            row.progress_observation =
                view::SubscriptionProgressObservation::unknown("observationBudgetExhausted");
            if Instant::now() < until {
                self.console_sample_subscription(credential, row, until)
                    .await?;
            }
            page.observation.partial |= row.progress_observation.partial;
        }
        // The timestamp and revision still describe the metadata directory, not a data snapshot.
        page.observation.source = "metadataDirectoryWithPerShardGroupCursorSamples".into();
        Ok(())
    }

    async fn console_sample_subscription(
        &self,
        credential: Id,
        row: &mut view::Subscription,
        until: Instant,
    ) -> Result<()> {
        let progress = wire::within(
            until,
            self.console_progress(
                credential,
                row.topic_id,
                row.id,
                None,
                SHARDS_PER_SUBSCRIPTION,
                until,
            ),
        )
        .await;
        let page = match progress {
            Ok(Ok(page)) => page,
            Ok(Err(_)) => {
                row.progress_observation =
                    view::SubscriptionProgressObservation::unknown("dataObservationUnavailable");
                return Ok(());
            }
            Err(_) => {
                row.progress_observation.sampled_shards = None;
                return Ok(());
            }
        };
        let (unconfirmed, observation) = summarize_progress(&page);
        row.progress_observation = observation;
        if let Some(unconfirmed) = unconfirmed {
            match wire::within(
                until,
                self.console_subscription_shards_match(credential, row, &page),
            )
            .await
            {
                Ok(Ok(true)) => row.unconfirmed = Some(unconfirmed),
                Ok(Ok(false)) => mark_incomplete(row, "shardSetChanged"),
                Ok(Err(error)) => return Err(error),
                Err(_) => mark_incomplete(row, "observationBudgetExhausted"),
            }
        }
        Ok(())
    }

    async fn console_subscription_shards_match(
        &self,
        credential: Id,
        row: &view::Subscription,
        progress: &view::Page<view::Progress>,
    ) -> Result<bool> {
        self.metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state
                    .console_subscription(
                        credential,
                        super::super::data::unix_millis()?,
                        row.topic_id,
                        row.id,
                    )
                    .map_err(super::super::metadata::metadata_rejection)?;
                let shards = state
                    .topology()
                    .domains(row.topic_id)
                    .take(SHARDS_PER_SUBSCRIPTION as usize + 1)
                    .map(|birth| birth.route.id);
                Ok(shards.eq(progress.items.iter().map(|item| item.shard)))
            })
            .await
            .map_err(|source| {
                Error::operation("recheck subscription progress shard range", source)
            })?
    }
}

fn mark_incomplete(row: &mut view::Subscription, reason: &str) {
    row.unconfirmed = None;
    row.progress_observation.partial = true;
    row.progress_observation.reason = Some(reason.into());
}

fn summarize_progress(
    page: &view::Page<view::Progress>,
) -> (Option<Decimal>, view::SubscriptionProgressObservation) {
    let mut observation = view::SubscriptionProgressObservation::combine(
        page.items
            .iter()
            .map(|item| view::SubscriptionProgressObservation {
                source: "perShardLinearizableGroupCursorSamples".into(),
                observed_from_ms: item.observed_at_ms,
                observed_until_ms: item.observed_at_ms,
                sampled_shards: Some(Decimal(u64::from(item.observed_at_ms.is_some()))),
                partial: item.unconfirmed.is_none() || item.observed_at_ms.is_none(),
                reason: item.unconfirmed.is_none().then(|| {
                    item.reason
                        .clone()
                        .unwrap_or_else(|| "dataObservationUnavailable".into())
                }),
            }),
        page.next_cursor.is_none() && !page.observation.partial,
    );
    let reason = if page.items.is_empty() {
        Some("noShardObservations")
    } else if page.next_cursor.is_some() {
        Some("historicalShardPageIncomplete")
    } else {
        None
    };
    if let Some(reason) = reason {
        observation.partial = true;
        observation.reason = Some(reason.into());
    }
    let total = page
        .items
        .iter()
        .try_fold(0_u64, |total, item| total.checked_add(item.unconfirmed?.0));
    if total.is_none() && !observation.partial {
        observation.partial = true;
        observation.reason = Some("counterOverflow".into());
    }
    (
        (!observation.partial)
            .then_some(total)
            .flatten()
            .map(Decimal),
        observation,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(
        unconfirmed: Option<u64>,
        observed_at: Option<u64>,
        reason: Option<&str>,
    ) -> view::Progress {
        view::Progress {
            shard: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
            sealed: false,
            observed_at_ms: observed_at.map(Decimal),
            source: "dataLinearizableGroupCursor".into(),
            revision: Some(Decimal(1)),
            retained_start: Some(Decimal(0)),
            committed_end: Some(Decimal(10)),
            frontier: Some(Decimal(2)),
            window_end: Some(Decimal(8)),
            ready: Some(Decimal(2)),
            in_flight: Some(Decimal(3)),
            acked: Some(Decimal(1)),
            paused: Some(Decimal(0)),
            unconfirmed: unconfirmed.map(Decimal),
            tail: Some(Decimal(2)),
            reason: reason.map(str::to_owned),
            component_id: None,
        }
    }

    fn page(items: Vec<view::Progress>) -> view::Page<view::Progress> {
        view::Page {
            items,
            next_cursor: None,
            observation: view::Observation {
                observed_at_ms: Decimal(90),
                source: "metadataAndDataLinearizable".into(),
                revision: Some(Decimal(9)),
                partial: false,
            },
        }
    }

    #[test]
    fn console_subscription_summary_keeps_actual_sample_ranges_and_checked_totals() {
        let page = page(vec![
            sample(Some(3), Some(120), None),
            sample(Some(4), Some(100), None),
        ]);
        let (count, observation) = summarize_progress(&page);
        assert_eq!(count, Some(Decimal(7)));
        assert_eq!(observation.observed_from_ms, Some(Decimal(100)));
        assert_eq!(observation.observed_until_ms, Some(Decimal(120)));
        assert_eq!(observation.sampled_shards, Some(Decimal(2)));
        assert!(!observation.partial);
        assert!(observation.reason.is_none());
        assert_eq!(page.observation.observed_at_ms, Decimal(90));
        assert_eq!(page.observation.revision, Some(Decimal(9)));
    }

    #[test]
    fn console_subscription_summary_never_totals_a_partial_historical_shard_page() {
        let mut page = page(vec![sample(Some(3), Some(120), None)]);
        page.next_cursor = Some("historical-shard".into());
        let (count, observation) = summarize_progress(&page);
        assert!(count.is_none());
        assert!(observation.partial);
        assert_eq!(observation.sampled_shards, Some(Decimal(1)));
        assert_eq!(
            observation.reason.as_deref(),
            Some("historicalShardPageIncomplete")
        );
    }

    #[test]
    fn console_subscription_summary_preserves_unknown_cursor_and_unavailable_samples() {
        for (observed_at, reason, sampled) in [
            (Some(120), "cursorNotMaterialized", 2),
            (None, "dataObservationUnavailable", 1),
        ] {
            let mut page = page(vec![
                sample(Some(3), Some(100), None),
                sample(None, observed_at, Some(reason)),
            ]);
            page.observation.partial = true;
            let (count, observation) = summarize_progress(&page);
            assert!(count.is_none());
            assert!(observation.partial);
            assert_eq!(observation.reason.as_deref(), Some(reason));
            assert_eq!(observation.sampled_shards, Some(Decimal(sampled)));
            assert_eq!(observation.observed_from_ms, Some(Decimal(100)));
            assert_eq!(
                observation.observed_until_ms,
                Some(Decimal(observed_at.unwrap_or(100)))
            );
        }
    }

    #[test]
    fn console_subscription_summary_reports_overflow_without_wrapping_or_fake_zero() {
        let page = page(vec![
            sample(Some(u64::MAX), Some(100), None),
            sample(Some(1), Some(120), None),
        ]);
        let (count, observation) = summarize_progress(&page);
        assert!(count.is_none());
        assert!(observation.partial);
        assert_eq!(observation.reason.as_deref(), Some("counterOverflow"));
        assert_eq!(observation.sampled_shards, Some(Decimal(2)));

        let mut first = observation.clone();
        first.sampled_shards = Some(Decimal(u64::MAX));
        let mut second = observation;
        second.sampled_shards = Some(Decimal(1));
        let combined = view::SubscriptionProgressObservation::combine([first, second], true);
        assert!(combined.sampled_shards.is_none());
        assert!(combined.partial);
        assert_eq!(combined.reason.as_deref(), Some("counterOverflow"));
    }

    #[test]
    fn console_subscription_summary_aggregate_keeps_budget_unknown_and_actual_ranges() {
        let (_, known) = summarize_progress(&page(vec![sample(Some(3), Some(100), None)]));
        let mut exhausted =
            view::SubscriptionProgressObservation::unknown("observationBudgetExhausted");
        exhausted.sampled_shards = None;
        let combined = view::SubscriptionProgressObservation::combine([known, exhausted], false);
        assert!(combined.partial);
        assert!(combined.sampled_shards.is_none());
        assert_eq!(combined.observed_from_ms, Some(Decimal(100)));
        assert_eq!(combined.observed_until_ms, Some(Decimal(100)));
        assert_eq!(
            combined.reason.as_deref(),
            Some("observationBudgetExhausted")
        );
    }
}
