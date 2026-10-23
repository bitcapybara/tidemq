use bytes::Bytes;
use tidemq_protocol::{Id, public};
use tokio::time::Instant;

use super::{Error, Result, service::Service};
use crate::{
    lifecycle::GuardedPage,
    metadata::Route,
    producer::{MessageData, Rejection},
    subscription::{DeliveryProof, GroupCommand, GroupOutcome, GroupRead, ReadResult},
};

impl Service {
    pub(super) async fn read_consumer_page(
        &self,
        route: &Route,
        subscription: Id,
        request: GroupRead,
        deadline: Instant,
    ) -> Result<(ReadResult, Option<GuardedPage>)> {
        let replica = self
            .data
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?;
        let cold_range = replica
            .state
            .try_with_application(|state| {
                let producer = state
                    .producer(route.partition)
                    .ok_or_else(super::metadata::not_found)?;
                let cursor = state
                    .consumer_group_cursor(route.partition, subscription)
                    .ok_or_else(super::metadata::not_found)?;
                let capacity = (cursor.limits().max_window_slots as usize)
                    .saturating_sub(cursor.window_slots())
                    .min(request.max_messages as usize);
                Ok::<_, Error>(
                    (cursor.original_read(&request).is_none()
                        && cursor.next_offset() < producer.local_reclaim_end()
                        && capacity > 0)
                        .then_some((cursor.next_offset(), capacity)),
                )
            })
            .await
            .map_err(|source| Error::operation("prepare consumer cold range", source))??;
        let mut page = None;
        let mut cold = Vec::new();
        let mut committed_deadline = deadline;
        if let Some((first, count)) = cold_range {
            let guarded = self
                .objects
                .get(&route.group_id)
                .ok_or_else(Error::unavailable)?
                .read(
                    route.partition,
                    first,
                    count,
                    request.max_bytes,
                    deadline.into_std(),
                )
                .await
                .map_err(|source| Error::operation("read consumer archive candidates", source))?;
            let mut bytes = 0_u64;
            for record in guarded
                .records()
                .map_err(|source| Error::operation("check consumer archive pin", source))?
            {
                let proof = guarded.verified_delivery(record.offset).map_err(|source| {
                    Error::operation("bind consumer archive candidate", source)
                })?;
                let Some(total) = bytes
                    .checked_add(proof.delivery_bytes)
                    .filter(|total| *total <= request.max_bytes)
                else {
                    break;
                };
                bytes = total;
                cold.push(proof);
            }
            committed_deadline = deadline.min(
                guarded
                    .valid_until()
                    .map_err(|_| Error::unavailable())?
                    .into(),
            );
            page = Some(guarded);
        }
        let result = self
            .group_commit(
                route,
                GroupCommand::Read {
                    subscription,
                    request,
                    cold,
                },
                committed_deadline,
            )
            .await;
        match result {
            Ok(GroupOutcome::Read(read)) => {
                if let Some(page) = &page {
                    page.check().map_err(|_| Error::unknown())?;
                }
                Ok((read, page))
            }
            result => {
                if let Some(page) = page {
                    let _ = page.release(deadline.into_std()).await;
                }
                match result {
                    Err(error) => Err(error),
                    _ => Err(Error::invalid("Read returned an invalid result")),
                }
            }
        }
    }

    pub(super) async fn consumer_delivery(
        &self,
        route: &Route,
        subscription: Id,
        proof: DeliveryProof,
        page: &mut Option<GuardedPage>,
        deadline: Instant,
    ) -> Result<public::GroupDelivery> {
        let message = self
            .consumer_message(route, proof.offset, page, deadline)
            .await?;
        let metadata = message
            .metadata(route.partition.topic_id)
            .map_err(|source| Error::operation("read stored consumer message context", source))?;
        let now = super::data::unix_millis()?;
        let current_attempt = self
            .data
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state
                    .consumer_group_cursor(route.partition, subscription)
                    .is_some_and(|cursor| cursor.delivery_is_current(&proof, now))
            })
            .await
            .map_err(|source| Error::operation("check original consumer attempt", source))?;
        Ok(public::GroupDelivery {
            key_present: metadata.key_present,
            properties: metadata.properties,
            current_attempt,
            key_hash: metadata.routing_hash,
            proof: Some(public::GroupProof {
                offset: proof.offset,
                attempt: proof.attempt,
                mac: Bytes::copy_from_slice(&proof.mac),
            }),
            key: message.key,
            body: message.body,
        })
    }

    async fn consumer_message(
        &self,
        route: &Route,
        offset: u64,
        page: &mut Option<GuardedPage>,
        deadline: Instant,
    ) -> Result<MessageData> {
        let hot = self
            .data
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| {
                state.producer(route.partition).map(|producer| {
                    producer.hot_read(offset, 1, u64::from(self.config.protocol.max_frame_bytes))
                })
            })
            .await
            .map_err(|source| Error::operation("read consumer hot body", source))?
            .ok_or_else(super::metadata::not_found)?;
        match hot {
            Ok(read) => {
                return read
                    .messages
                    .into_iter()
                    .next()
                    .ok_or_else(Error::unavailable);
            }
            Err(Rejection::OffsetExpired { .. }) => {}
            Err(source) => return Err(Error::operation("read original consumer hot body", source)),
        }
        if let Some(cached) = page
            .as_ref()
            .map(|page| super::data::cold_message(page, offset))
            .transpose()?
            .flatten()
        {
            return Ok(cached.0);
        }
        if let Some(previous) = page.take() {
            previous
                .release(deadline.into_std())
                .await
                .map_err(|source| Error::operation("release consumer archive pin", source))?;
        }
        let guarded = self
            .objects
            .get(&route.group_id)
            .ok_or_else(Error::unavailable)?
            .read(
                route.partition,
                offset,
                1,
                u64::from(self.config.protocol.max_frame_bytes),
                deadline.into_std(),
            )
            .await
            .map_err(|source| Error::operation("read original consumer archive body", source))?;
        let message = super::data::cold_message(&guarded, offset)?
            .ok_or_else(Error::unavailable)?
            .0;
        *page = Some(guarded);
        Ok(message)
    }
}
