use super::*;

impl Core {
    pub(super) fn update_info(
        &self,
        info: public::ConsumerControlResult,
        started: Instant,
    ) -> Result<()> {
        self.validate_assignment(&info)?;
        let expires = started
            .checked_add(Duration::from_millis(info.remaining_lease_ms))
            .ok_or(Error::Deadline)?;
        let mut state = self.state()?;
        let mut previous = self.info.lock().map_err(|_| Error::Closed)?;
        if info.assignment_version < previous.assignment_version {
            return Ok(());
        }
        if previous
            .lane_fences
            .iter()
            .zip(&info.lane_fences)
            .any(|(old, new)| new < old)
        {
            return Err(Error::ResponseMismatch {
                reason: "consumer assignment revocation fences regressed".into(),
            });
        }
        if info.assignment_version != previous.assignment_version
            || info.member_epoch != previous.member_epoch
            || info.assignment_fenced != previous.assignment_fenced
        {
            state.revoke_deliveries(self.model, &info)?;
        }
        *previous = info;
        *self.lease_until.lock().map_err(|_| Error::Closed)? = expires;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn validate_assignment(&self, info: &public::ConsumerControlResult) -> Result<()> {
        let version = info
            .assignment_version
            .checked_mul(2)
            .ok_or(Error::SessionExpired)?;
        if info.remaining_lease_ms > 15_000
            || info.member_epoch != self.epoch.load(Ordering::Acquire)
            || info.subscription_id.as_ref() != self.subscription.as_bytes()
            || info.assignment_version == 0
            || info.plan_active && !info.assignment_fenced
            || info.lane_fences.len()
                != if self.model == SubscriptionKind::Stream {
                    tidemq_protocol::topic::STREAM_FENCE_COUNT
                } else {
                    0
                }
            || info.lane_fences.iter().any(|fence| *fence > version)
            || !valid_lanes(self.model, &info.lanes)
        {
            return Err(Error::ResponseMismatch {
                reason: "consumer assignment or lease exceeds its supported bounds".into(),
            });
        }
        Ok(())
    }
}

fn valid_lanes(model: SubscriptionKind, lanes: &[public::ConsumerLane]) -> bool {
    if model == SubscriptionKind::Queue {
        return lanes.is_empty();
    }
    let atom = 1 << tidemq_protocol::topic::STREAM_FENCE_SHIFT;
    lanes.len() <= tidemq_protocol::topic::STREAM_FENCE_COUNT
        && lanes.iter().all(|lane| {
            lane.hash_start.is_multiple_of(atom)
                && lane
                    .hash_end
                    .is_none_or(|end| end > lane.hash_start && end.is_multiple_of(atom))
        })
        && lanes.windows(2).all(|pair| {
            pair.first().zip(pair.get(1)).is_some_and(|(left, right)| {
                left.hash_end.is_some_and(|end| end <= right.hash_start)
            })
        })
}

fn revocation(
    model: SubscriptionKind,
    info: &public::ConsumerControlResult,
    data: &ReceivedData,
) -> Result<Option<u64>> {
    if data.epoch != info.member_epoch {
        return info
            .assignment_version
            .checked_mul(2)
            .map(Some)
            .ok_or(Error::SessionExpired);
    }
    if model == SubscriptionKind::Queue {
        return Ok(None);
    }
    let fence = info
        .lane_fences
        .get((data.hash >> tidemq_protocol::topic::STREAM_FENCE_SHIFT) as usize)
        .copied()
        .ok_or(Error::Closed)?;
    Ok((data.assignment_version < fence).then_some(fence))
}

impl State {
    pub(super) fn enqueue(
        &mut self,
        model: SubscriptionKind,
        info: &public::ConsumerControlResult,
        message: ReceivedMessage,
    ) -> Result<()> {
        match revocation(model, info, &message.data)? {
            Some(fence) => self.revoked.push((fence, message.data)),
            None => self.queue.push_back(message),
        }
        Ok(())
    }

    fn revoke_deliveries(
        &mut self,
        model: SubscriptionKind,
        info: &public::ConsumerControlResult,
    ) -> Result<()> {
        for message in std::mem::take(&mut self.queue) {
            self.enqueue(model, info, message)?;
        }
        let mut revoked = Vec::new();
        for (sequence, message) in &self.ledger {
            if let Some(fence) = revocation(model, info, &message.data)? {
                revoked.push((*sequence, fence));
            }
        }
        for (sequence, fence) in revoked {
            if let Some(message) = self.ledger.remove(&sequence) {
                self.revoked.push((fence, message.data));
            }
            self.claims.remove(&sequence);
        }
        self.release_revoked(info)
    }

    pub(super) fn release_revoked(&mut self, info: &public::ConsumerControlResult) -> Result<()> {
        // Local removal releases credit only after every data domain observed the revocation.
        if !info.assignment_fenced {
            return Ok(());
        }
        let covered = info
            .assignment_version
            .checked_mul(2)
            .ok_or(Error::SessionExpired)?;
        for (fence, data) in std::mem::take(&mut self.revoked) {
            if fence <= covered {
                data.release_credit()?;
            } else {
                self.revoked.push((fence, data));
            }
        }
        Ok(())
    }
}
