use super::*;
use tidemq_protocol::topic::HashRange;

impl DataApplication {
    pub(crate) fn publisher_cleanup_page(
        &self,
        after: Option<(PartitionKey, Id, u64)>,
        now_ms: u64,
    ) -> Vec<(PartitionKey, producer::PublisherPermit)> {
        self.partitions
            .iter()
            .filter(|(partition, _)| !self.authority.is_frozen(**partition))
            .flat_map(|(partition, state)| {
                state
                    .producer
                    .publisher_cleanup_candidates(now_ms)
                    .map(|permit| (*partition, permit))
            })
            .filter(|(partition, permit)| {
                after.is_none_or(|after| (*partition, permit.producer, permit.registration) > after)
            })
            .take(4)
            .collect()
    }
    pub(super) fn retire_publishes(
        &mut self,
        partition: PartitionKey,
        writer: WriterFence,
        permit: producer::PublisherPermit,
        before: u64,
    ) -> Result<DataResponse, DataRejection> {
        self.check_writer(partition, writer)?;
        let state = self
            .partitions
            .get_mut(&partition)
            .ok_or(DataRejection::PartitionMissing)?;
        state
            .producer
            .retire_publishes(&permit, before)
            .map_err(DataRejection::Producer)?;
        Ok(DataResponse::Retired)
    }

    pub(super) fn publish_message(
        &mut self,
        partition: PartitionKey,
        writer: WriterFence,
        birth: (Id, HashRange),
        permit: producer::PublisherPermit,
        request: producer::PublishRequest,
        now_ms: u64,
    ) -> Result<DataResponse, DataRejection> {
        self.check_writer(partition, writer)?;
        let hash = request.verify().map_err(DataRejection::Producer)?;
        birth
            .1
            .validate()
            .map_err(|_| DataRejection::InvalidConfiguration)?;
        if !birth.1.contains(hash) {
            return Err(DataRejection::Producer(producer::Rejection::WrongPartition));
        }
        let config = self.config.as_ref().ok_or(DataRejection::Uninitialized)?;
        let available_bytes = config
            .max_hot_bytes
            .saturating_sub(self.hot_state_bytes()?)
            .saturating_sub(
                self.authority
                    .reservation()
                    .map_or(0, |capacity| capacity.max_hot_bytes),
            )
            .saturating_sub(640);
        let available_messages = config
            .max_hot_messages
            .saturating_sub(self.hot_messages)
            .saturating_sub(
                self.authority
                    .reservation()
                    .map_or(0, |capacity| capacity.max_hot_messages),
            );
        let state = self
            .partitions
            .get_mut(&partition)
            .ok_or(DataRejection::PartitionMissing)?;
        if state.birth.is_some_and(|original| original != birth) {
            return Err(DataRejection::ConfigurationConflict);
        }
        state
            .producer
            .publisher_binding(&permit, request.identity)
            .map_err(DataRejection::Producer)?;
        if let Some(result) = state
            .producer
            .publish_result(request.identity, request.digest)
            .map_err(DataRejection::Producer)?
        {
            return Ok(DataResponse::Published(result));
        }
        if let Some((plan, committed_end)) = state.seal {
            return Ok(DataResponse::ShardSealed {
                plan,
                committed_end,
            });
        }
        if !permit.live {
            state
                .producer
                .retire_publishes(&permit, request.retired_before)
                .map_err(DataRejection::Producer)?;
        }
        if state.producer.publisher_closed(request.identity) {
            return Ok(DataResponse::PublishClosed);
        }
        let protected = state.protected_before();
        let backlog = state
            .retention
            .logical_backlog_upper_bytes(protected)
            .map_err(DataRejection::Retention)?;
        let quota = state
            .quota
            .as_mut()
            .ok_or(DataRejection::Quota(QuotaRejection::Missing))?;
        let bytes = request
            .message
            .logical_bytes()
            .ok_or(DataRejection::Quota(QuotaRejection::Arithmetic))?;
        let rate = quota
            .publish(backlog, protected.is_some(), bytes, now_ms)
            .map_err(DataRejection::Quota)?;
        let before = state.producer.hot_bytes();
        let result = state
            .producer
            .publish_admitted(
                &permit,
                request,
                now_ms,
                available_bytes,
                available_messages,
                |previous, end, bytes| {
                    state
                        .retention
                        .record_append(previous, end, now_ms, bytes)
                        .map_err(|reason| match reason {
                            crate::retention::Rejection::Allocation => {
                                producer::Rejection::Allocation
                            }
                            crate::retention::Rejection::TimestampBudget => {
                                producer::Rejection::DataCapacity
                            }
                            crate::retention::Rejection::CounterOverflow => {
                                producer::Rejection::CounterExhausted
                            }
                            _ => producer::Rejection::InvalidSnapshot,
                        })
                },
            )
            .map_err(DataRejection::Producer)?;
        state.birth = Some(birth);
        if !result.duplicate {
            quota.commit_publish(rate);
            self.hot_bytes = self
                .hot_bytes
                .saturating_add(state.producer.hot_bytes().saturating_sub(before));
            self.hot_messages = self.hot_messages.saturating_add(1);
        }
        Ok(DataResponse::Published(result))
    }
}
