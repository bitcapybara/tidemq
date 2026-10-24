use super::*;
use crate::subscription::MemberGrant;

impl MembersBook {
    pub(super) fn resize_lanes(&mut self, now: u64) -> Result<(), Rejection> {
        if self.model == SubscriptionKind::Queue {
            return Ok(());
        }
        let target = self
            .assigned(now)
            .count()
            .clamp(MIN_STREAM_LANES, STREAM_FENCE_COUNT);
        while self.lanes.len() < target {
            self.split_lane()?;
        }
        while self.lanes.len() > target {
            self.merge_lane()?;
        }
        Ok(())
    }

    fn split_lane(&mut self) -> Result<(), Rejection> {
        let (index, lane) = self
            .lanes
            .iter()
            .copied()
            .enumerate()
            .max_by_key(|(_, lane)| width(*lane))
            .ok_or(Rejection::InvalidEvidence)?;
        let boundary = u64::try_from(u128::from(lane.start) + width(lane) / 2)
            .map_err(|_| Rejection::InvalidEvidence)?;
        let (left, right) = lane
            .split(boundary)
            .map_err(|_| Rejection::InvalidEvidence)?;
        self.lanes = self
            .lanes
            .iter()
            .copied()
            .enumerate()
            .flat_map(|(at, lane)| {
                if at == index {
                    [Some(left), Some(right)]
                } else {
                    [Some(lane), None]
                }
            })
            .flatten()
            .collect();
        Ok(())
    }

    fn merge_lane(&mut self) -> Result<(), Rejection> {
        let (index, pair) = self
            .lanes
            .windows(2)
            .enumerate()
            .filter(|(_, pair)| {
                pair.first().zip(pair.get(1)).is_some_and(|(left, right)| {
                    let lane_width = width(*left);
                    lane_width > 0
                        && lane_width == width(*right)
                        && u128::from(left.start).is_multiple_of(lane_width * 2)
                })
            })
            .min_by_key(|(_, pair)| pair.iter().map(|range| width(*range)).sum::<u128>())
            .ok_or(Rejection::InvalidEvidence)?;
        let left = pair.first().copied().ok_or(Rejection::InvalidEvidence)?;
        let right = pair.get(1).copied().ok_or(Rejection::InvalidEvidence)?;
        let merged = left.merge(right).map_err(|_| Rejection::InvalidEvidence)?;
        self.lanes = self
            .lanes
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(at, lane)| {
                if at == index {
                    Some(merged)
                } else if at == index + 1 {
                    None
                } else {
                    Some(lane)
                }
            })
            .collect();
        Ok(())
    }

    pub(super) fn lane_assignments(
        &self,
        now: u64,
        issued: &[MemberGrant],
    ) -> Result<BTreeMap<Id, Vec<HashRange>>, Rejection> {
        let members: BTreeMap<_, _> = self
            .assigned(now)
            .map(|member| (member.member, member.epoch))
            .collect();
        let mut result: BTreeMap<_, Vec<HashRange>> =
            members.keys().map(|member| (*member, Vec::new())).collect();
        if members.is_empty() || self.model == SubscriptionKind::Queue {
            return Ok(result);
        }
        let quotas = lane_quotas(&members, issued, self.lanes.len());
        let mut pending = Vec::new();
        for lane in &self.lanes {
            let retained = issued.iter().find(|grant| {
                members.get(&grant.member) == Some(&grant.epoch)
                    && result.get(&grant.member).is_some_and(|lanes| {
                        quotas
                            .get(&grant.member)
                            .is_some_and(|limit| lanes.len() < *limit)
                    })
                    && grant
                        .lanes
                        .iter()
                        .any(|old| old.contains(lane.start) || lane.contains(old.start))
            });
            if let Some(grant) = retained {
                result
                    .get_mut(&grant.member)
                    .ok_or(Rejection::InvalidEvidence)?
                    .push(*lane);
            } else {
                pending.push(*lane);
            }
        }
        for lane in pending {
            let (_, lanes) = result
                .iter_mut()
                .filter(|(member, lanes)| {
                    quotas.get(member).is_some_and(|limit| lanes.len() < *limit)
                })
                .min_by_key(|(_, lanes)| lanes.len())
                .ok_or(Rejection::InvalidEvidence)?;
            lanes.push(lane);
        }
        for lanes in result.values_mut() {
            lanes.sort_by_key(|lane| lane.start);
        }
        Ok(result)
    }

    pub(super) fn validate_lanes(&self) -> Result<(), Rejection> {
        if self.model == SubscriptionKind::Queue {
            return if self.lanes.is_empty() {
                Ok(())
            } else {
                Err(Rejection::InvalidEvidence)
            };
        }
        if self.lanes.len() < MIN_STREAM_LANES
            || self.lanes.len() > STREAM_FENCE_COUNT
            || self.lanes.first().is_none_or(|lane| lane.start != 0)
            || self.lanes.last().is_none_or(|lane| lane.end.is_some())
            || self.lanes.iter().any(|lane| {
                let size = width(*lane);
                lane.validate().is_err()
                    || !size.is_power_of_two()
                    || !u128::from(lane.start).is_multiple_of(size)
                    || !lane.start.is_multiple_of(1 << STREAM_FENCE_SHIFT)
                    || lane
                        .end
                        .is_some_and(|end| !end.is_multiple_of(1 << STREAM_FENCE_SHIFT))
            })
            || self.lanes.windows(2).any(|pair| {
                pair.first()
                    .zip(pair.get(1))
                    .is_none_or(|(left, right)| left.end != Some(right.start))
            })
        {
            return Err(Rejection::InvalidEvidence);
        }
        let mut assigned = BTreeSet::new();
        for grant in &self.plan_grants {
            if grant.lanes.iter().any(|lane| !self.lanes.contains(lane)) {
                return Err(Rejection::InvalidEvidence);
            }
            for lane in &grant.lanes {
                if !assigned.insert(lane.start) {
                    return Err(Rejection::InvalidEvidence);
                }
            }
        }
        if !self.plan_grants.is_empty() && assigned.len() != self.lanes.len() {
            return Err(Rejection::InvalidEvidence);
        }
        Ok(())
    }
}

fn width(range: HashRange) -> u128 {
    range
        .end
        .map_or(1_u128 << 64, u128::from)
        .saturating_sub(u128::from(range.start))
}

fn lane_quotas(
    members: &BTreeMap<Id, u64>,
    issued: &[MemberGrant],
    count: usize,
) -> BTreeMap<Id, usize> {
    let mut priority: Vec<_> = members
        .iter()
        .map(|(member, epoch)| {
            let previous = issued
                .iter()
                .find(|grant| grant.member == *member && grant.epoch == *epoch)
                .map_or(0, |grant| grant.lanes.len());
            (*member, previous)
        })
        .collect();
    priority.sort_by_key(|(member, previous)| (std::cmp::Reverse(*previous), *member));
    priority
        .into_iter()
        .enumerate()
        .map(|(index, (member, _))| {
            (
                member,
                count / members.len() + usize::from(index < count % members.len()),
            )
        })
        .collect()
}
