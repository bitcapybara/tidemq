use super::*;
use crate::metadata::{MAX_AUTHORITY_TRANSFERS, TRANSFER_PAGE_BYTES};

#[derive(Deserialize)]
struct Payload {
    format: u32,
    plan: AuthorityTransfer,
    cut: CommitPosition,
    state: PartitionState,
    lifecycle: crate::lifecycle::AuthoritySlice,
}

impl TargetImport {
    fn progress(&self) -> TransferOutcome {
        TransferOutcome::ImportProgress {
            plan: self.plan.id,
            received: self.received,
        }
    }

    fn decode(&self) -> Result<Payload, TransferDataRejection> {
        let manifest = self
            .plan
            .manifest
            .as_ref()
            .ok_or(TransferDataRejection::NotReady)?;
        if self.received != manifest.bytes {
            return Err(TransferDataRejection::NotReady);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(
                usize::try_from(manifest.bytes).map_err(|_| TransferDataRejection::Capacity)?,
            )
            .map_err(|_| TransferDataRejection::Capacity)?;
        for page in self.pages.values() {
            bytes.extend_from_slice(
                page.bytes
                    .as_ref()
                    .ok_or(TransferDataRejection::InvalidPlan)?,
            );
        }
        if blake3::hash(&bytes).as_bytes() != &manifest.digest {
            return Err(TransferDataRejection::Conflict);
        }
        let payload: Payload =
            postcard::from_bytes(&bytes).map_err(|_| TransferDataRejection::Encoding)?;
        if payload.format != TRANSFER_FORMAT
            || payload.plan.stage != TransferStage::Prepare
            || !same_plan(&payload.plan, &self.plan)
            || payload.cut != manifest.source_cut
            || payload.state.producer.committed_end() != manifest.committed_end
            || payload.lifecycle.partition != self.plan.partition
        {
            return Err(TransferDataRejection::InvalidPlan);
        }
        payload
            .plan
            .validate()
            .map_err(|_| TransferDataRejection::InvalidPlan)?;
        payload
            .lifecycle
            .validate()
            .map_err(|_| TransferDataRejection::InvalidPlan)?;
        Ok(payload)
    }

    fn validate_pages(&self) -> Result<(), TransferDataRejection> {
        let mut received = 0_u64;
        for (offset, page) in &self.pages {
            if *offset != received || page.bytes.is_some() != self.recovered.is_none() {
                return Err(TransferDataRejection::InvalidPlan);
            }
            received = page_end(*offset, page.length, &self.plan)?;
            if page.bytes.as_ref().is_some_and(|bytes| {
                bytes.len() != page.length || blake3::hash(bytes).as_bytes() != &page.digest
            }) {
                return Err(TransferDataRejection::InvalidPlan);
            }
        }
        if received != self.received {
            return Err(TransferDataRejection::InvalidPlan);
        }
        Ok(())
    }
}

impl DataApplication {
    pub fn transfer_capacity(&self, plan: Id) -> Result<TransferCapacity, TransferDataRejection> {
        if let Some(target) = self.authority.targets.get(&plan) {
            return Ok(target.capacity);
        }
        if self.authority.reservation().is_some()
            || self.authority.targets.len() >= MAX_AUTHORITY_TRANSFERS
        {
            return Err(TransferDataRejection::Capacity);
        }
        self.available_transfer_capacity()
    }

    pub(crate) fn expanded_transfer_capacity(
        &self,
        plan: Id,
    ) -> Result<TransferCapacity, TransferDataRejection> {
        let target = self
            .authority
            .targets
            .get(&plan)
            .ok_or(TransferDataRejection::NotReady)?;
        if target.plan.manifest.is_some() || target.recovered.is_some() {
            return Ok(target.capacity);
        }
        let available = self.available_transfer_capacity()?;
        if !available.covers(target.capacity) {
            return Err(TransferDataRejection::Capacity);
        }
        Ok(available)
    }

    fn available_transfer_capacity(&self) -> Result<TransferCapacity, TransferDataRejection> {
        let config = self
            .config
            .as_ref()
            .ok_or(TransferDataRejection::InvalidPlan)?;
        if self.partitions.len() >= config.max_partitions {
            return Err(TransferDataRejection::Capacity);
        }
        let capacity = TransferCapacity {
            max_bytes: MAX_TRANSFER_BYTES,
            max_hot_bytes: config
                .max_hot_bytes
                .checked_sub(
                    self.hot_state_bytes()
                        .map_err(|_| TransferDataRejection::Capacity)?,
                )
                .ok_or(TransferDataRejection::Capacity)?
                .min(MAX_TRANSFER_BYTES),
            max_hot_messages: config
                .max_hot_messages
                .checked_sub(self.hot_messages)
                .ok_or(TransferDataRejection::Capacity)?,
            max_window_slots: 0,
            max_read_results: 0,
            max_ack_slots: 0,
            max_members: 0,
            max_in_flight_bytes: 0,
        };
        self.available_transfer_windows(capacity)
            .map_err(|_| TransferDataRejection::Capacity)
    }

    pub(super) fn expand_reservation(
        &mut self,
        plan: Id,
        capacity: TransferCapacity,
    ) -> Result<TransferOutcome, TransferDataRejection> {
        capacity.validate()?;
        let target = self
            .authority
            .targets
            .get(&plan)
            .ok_or(TransferDataRejection::NotReady)?;
        if target.capacity.covers(capacity) {
            return Ok(TransferOutcome::Reserved { plan });
        }
        if !capacity.covers(target.capacity)
            || target.plan.manifest.is_some()
            || target.recovered.is_some()
        {
            return Err(TransferDataRejection::Conflict);
        }
        self.validate_target_reservation(&target.plan, capacity)?;
        self.validate_target_group_capacity(None, capacity)
            .map_err(|_| TransferDataRejection::Capacity)?;
        self.authority
            .targets
            .get_mut(&plan)
            .ok_or(TransferDataRejection::NotReady)?
            .capacity = capacity;
        Ok(TransferOutcome::Reserved { plan })
    }

    pub(super) fn reserve_target(
        &mut self,
        plan: AuthorityTransfer,
        capacity: TransferCapacity,
    ) -> Result<TransferOutcome, TransferDataRejection> {
        plan.validate()
            .map_err(|_| TransferDataRejection::InvalidPlan)?;
        capacity.validate()?;
        if let Some(original) = self.authority.targets.get(&plan.id) {
            if !same_plan(&original.plan, &plan) || original.initial_capacity != capacity {
                return Err(TransferDataRejection::Conflict);
            }
            return Ok(TransferOutcome::Reserved { plan: plan.id });
        }
        if plan.stage != TransferStage::Prepare {
            return Err(TransferDataRejection::InvalidPlan);
        }
        if self.authority.reservation().is_some()
            || self.authority.targets.len() >= MAX_AUTHORITY_TRANSFERS
        {
            return Err(TransferDataRejection::Capacity);
        }
        self.validate_target_reservation(&plan, capacity)?;
        self.validate_target_group_capacity(None, capacity)
            .map_err(|_| TransferDataRejection::Capacity)?;
        let id = plan.id;
        self.authority.targets.insert(
            id,
            TargetImport {
                plan,
                initial_capacity: capacity,
                capacity,
                pages: BTreeMap::new(),
                received: 0,
                recovered: None,
            },
        );
        Ok(TransferOutcome::Reserved { plan: id })
    }

    pub(super) fn begin_import(
        &mut self,
        plan: AuthorityTransfer,
    ) -> Result<TransferOutcome, TransferDataRejection> {
        plan.validate()
            .map_err(|_| TransferDataRejection::InvalidPlan)?;
        if plan.stage != TransferStage::Exported {
            return Err(TransferDataRejection::InvalidPlan);
        }
        let target = self
            .authority
            .targets
            .get_mut(&plan.id)
            .ok_or(TransferDataRejection::NotReady)?;
        if !same_plan(&target.plan, &plan) {
            return Err(TransferDataRejection::Conflict);
        }
        if target.plan.manifest.is_some() {
            if target.plan.manifest != plan.manifest {
                return Err(TransferDataRejection::Conflict);
            }
            return Ok(target.progress());
        }
        if plan
            .manifest
            .as_ref()
            .is_none_or(|manifest| manifest.bytes > target.capacity.max_bytes)
        {
            return Err(TransferDataRejection::Capacity);
        }
        target.plan = plan;
        Ok(target.progress())
    }

    pub(super) fn import_page(
        &mut self,
        page: TransferPage,
    ) -> Result<TransferOutcome, TransferDataRejection> {
        let target = self
            .authority
            .targets
            .get_mut(&page.plan)
            .ok_or(TransferDataRejection::NotReady)?;
        let end = validate_page(&page, &target.plan)?;
        if let Some(original) = target.pages.get(&page.offset) {
            if original.digest != page.digest || original.length != page.bytes.len() {
                return Err(TransferDataRejection::Conflict);
            }
            return Ok(TransferOutcome::ImportProgress {
                plan: page.plan,
                received: end,
            });
        }
        if target.recovered.is_some() {
            return Err(TransferDataRejection::Conflict);
        }
        if page.offset != target.received {
            return Err(TransferDataRejection::InvalidPlan);
        }
        target.received = end;
        target.pages.insert(
            page.offset,
            ImportedPage {
                length: page.bytes.len(),
                bytes: Some(page.bytes),
                digest: page.digest,
            },
        );
        Ok(target.progress())
    }

    pub(super) fn finish_import(
        &mut self,
        plan: Id,
    ) -> Result<TransferOutcome, TransferDataRejection> {
        let target = self
            .authority
            .targets
            .get(&plan)
            .ok_or(TransferDataRejection::NotReady)?;
        if let Some(recovered) = &target.recovered {
            return Ok(TransferOutcome::Imported(recovered.clone()));
        }
        let payload = target.decode()?;
        let mut candidate = self.import_candidate(target, payload)?;
        let partition = target.plan.partition;
        let grant = target.plan.target.clone();
        let DataResponse::Recovered(recovered) = candidate
            .recover(
                partition,
                WriterFence {
                    node_id: grant.owner_node,
                    owner_epoch: grant.epoch,
                },
            )
            .map_err(|_| TransferDataRejection::InvalidPlan)?
        else {
            return Err(TransferDataRejection::InvalidPlan);
        };
        let state = candidate
            .partitions
            .remove(&partition)
            .ok_or(TransferDataRejection::InvalidPlan)?;
        let mut fences = self
            .ownership
            .clone()
            .ok_or(TransferDataRejection::InvalidPlan)?;
        fences
            .import_partition(grant, recovered.fence.fence_generation)
            .map_err(|_| TransferDataRejection::InvalidPlan)?;
        fences
            .complete_recovery(partition, recovered.checkpoint.clone())
            .map_err(|_| TransferDataRejection::InvalidPlan)?;
        let bytes = self
            .hot_bytes
            .checked_add(state.producer.hot_bytes())
            .ok_or(TransferDataRejection::Capacity)?;
        let messages = self
            .hot_messages
            .checked_add(state.producer.hot_message_count())
            .ok_or(TransferDataRejection::Capacity)?;
        let target = self
            .authority
            .targets
            .get_mut(&plan)
            .ok_or(TransferDataRejection::NotReady)?;
        for page in target.pages.values_mut() {
            page.bytes = None;
        }
        target.recovered = Some(recovered.clone());
        self.ownership = Some(fences);
        self.lifecycle = candidate.lifecycle;
        self.partitions.insert(partition, state);
        self.hot_bytes = bytes;
        self.hot_messages = messages;
        Ok(TransferOutcome::Imported(recovered))
    }

    fn import_candidate(
        &self,
        target: &TargetImport,
        payload: Payload,
    ) -> Result<DataApplication, TransferDataRejection> {
        self.validate_target_reservation(&target.plan, target.capacity)?;
        let config = self
            .config
            .as_ref()
            .ok_or(TransferDataRejection::InvalidPlan)?;
        let state = payload.state;
        if state
            .birth
            .is_some_and(|(shard, _)| shard != target.plan.shard)
            || state.birth.is_none() && state.producer.committed_end() != 0
            || state
                .groups
                .values()
                .any(|cursor| cursor.identity().shard != target.plan.shard)
            || state
                .producer
                .hot_bytes()
                .checked_add(state.producer.publish_state_bytes())
                .is_none_or(|bytes| bytes > target.capacity.max_hot_bytes)
            || state.producer.hot_message_count() > target.capacity.max_hot_messages
            || !super::super::groups::transfer_usage(&state)
                .map_err(|_| TransferDataRejection::InvalidPlan)?
                .fits(target.capacity)
        {
            return Err(TransferDataRejection::Capacity);
        }
        self.validate_target_group_capacity(Some(&state), target.capacity)
            .map_err(|_| TransferDataRejection::Capacity)?;
        let mut fences = FenceState::new(config.cluster_id, config.group_id, config.max_partitions)
            .map_err(|_| TransferDataRejection::InvalidPlan)?;
        fences
            .import_partition(
                target.plan.target.clone(),
                state.subscriptions.owner_fence_generation(),
            )
            .map_err(|_| TransferDataRejection::InvalidPlan)?;
        let candidate = DataApplication {
            config: Some(config.clone()),
            ownership: Some(fences),
            hot_bytes: state.producer.hot_bytes(),
            hot_messages: state.producer.hot_message_count(),
            partitions: [(target.plan.partition, state)].into(),
            lifecycle: self
                .lifecycle
                .import_authority_slice(payload.lifecycle)
                .map_err(|_| TransferDataRejection::InvalidPlan)?,
            authority: Default::default(),
        };
        candidate
            .validate_complete()
            .map_err(|_| TransferDataRejection::InvalidPlan)?;
        Ok(candidate)
    }

    fn validate_target_reservation(
        &self,
        plan: &AuthorityTransfer,
        capacity: TransferCapacity,
    ) -> Result<(), TransferDataRejection> {
        let config = self
            .config
            .as_ref()
            .ok_or(TransferDataRejection::InvalidPlan)?;
        if plan.target.cluster_id != config.cluster_id
            || plan.target.group_id != config.group_id
            || !self.authority.accepts_import(plan)
            || self.partitions.contains_key(&plan.partition)
            || self
                .ownership
                .as_ref()
                .is_none_or(|fences| fences.fence(plan.partition).is_some())
        {
            return Err(TransferDataRejection::InvalidPlan);
        }
        let producer_bytes = self
            .hot_state_bytes()
            .map_err(|_| TransferDataRejection::Capacity)?;
        if self.partitions.len() >= config.max_partitions
            || producer_bytes
                .checked_add(capacity.max_hot_bytes)
                .is_none_or(|bytes| bytes > config.max_hot_bytes)
            || self
                .hot_messages
                .checked_add(capacity.max_hot_messages)
                .is_none_or(|count| count > config.max_hot_messages)
        {
            return Err(TransferDataRejection::Capacity);
        }
        Ok(())
    }

    pub(super) fn validate_targets(&self) -> Result<(), TransferDataRejection> {
        if self.authority.targets.len() > MAX_AUTHORITY_TRANSFERS
            || self
                .authority
                .targets
                .values()
                .filter(|target| target.recovered.is_none())
                .count()
                > 1
        {
            return Err(TransferDataRejection::Capacity);
        }
        for (id, target) in &self.authority.targets {
            target
                .plan
                .validate()
                .map_err(|_| TransferDataRejection::InvalidPlan)?;
            target.capacity.validate()?;
            target.initial_capacity.validate()?;
            if *id != target.plan.id
                || !target.capacity.covers(target.initial_capacity)
                || !matches!(
                    target.plan.stage,
                    TransferStage::Prepare | TransferStage::Exported
                )
                || self.config.as_ref().is_none_or(|config| {
                    config.cluster_id != target.plan.target.cluster_id
                        || config.group_id != target.plan.target.group_id
                })
            {
                return Err(TransferDataRejection::InvalidPlan);
            }
            if let Some(recovered) = &target.recovered {
                target.validate_pages()?;
                if recovered.fence.grant != target.plan.target
                    || !recovered.checkpoint.validate(&recovered.fence)
                    || target.plan.manifest.as_ref().is_none_or(|manifest| {
                        manifest.bytes != target.received
                            || manifest.committed_end != recovered.checkpoint.committed_end
                    })
                {
                    return Err(TransferDataRejection::InvalidPlan);
                }
            } else {
                self.validate_target_reservation(&target.plan, target.capacity)?;
                target.validate_pages()?;
                if target
                    .plan
                    .manifest
                    .as_ref()
                    .is_some_and(|manifest| manifest.bytes > target.capacity.max_bytes)
                {
                    return Err(TransferDataRejection::Capacity);
                }
            }
        }
        Ok(())
    }
}

fn validate_page(
    page: &TransferPage,
    plan: &AuthorityTransfer,
) -> Result<u64, TransferDataRejection> {
    if blake3::hash(&page.bytes).as_bytes() != &page.digest {
        return Err(TransferDataRejection::InvalidPlan);
    }
    page_end(page.offset, page.bytes.len(), plan)
}

fn page_end(
    offset: u64,
    length: usize,
    plan: &AuthorityTransfer,
) -> Result<u64, TransferDataRejection> {
    let manifest = plan
        .manifest
        .as_ref()
        .ok_or(TransferDataRejection::NotReady)?;
    let bytes = u64::try_from(length).map_err(|_| TransferDataRejection::Capacity)?;
    let end = offset
        .checked_add(bytes)
        .ok_or(TransferDataRejection::Capacity)?;
    if bytes == 0
        || length > TRANSFER_PAGE_BYTES
        || end > manifest.bytes
        || end < manifest.bytes && length != TRANSFER_PAGE_BYTES
        || !offset.is_multiple_of(TRANSFER_PAGE_BYTES as u64)
    {
        return Err(TransferDataRejection::InvalidPlan);
    }
    Ok(end)
}
