use super::*;

impl DataApplication {
    // The runtime supplies the quorum-authorized plan after old source pins have drained.
    pub(super) fn reclaim_source(
        &mut self,
        plan: AuthorityTransfer,
    ) -> Result<TransferOutcome, TransferDataRejection> {
        plan.validate()
            .map_err(|_| TransferDataRejection::InvalidPlan)?;
        if plan.stage < TransferStage::Checkpointed {
            return Err(TransferDataRejection::NotReady);
        }
        let source = self
            .authority
            .sources
            .get(&plan.id)
            .ok_or(TransferDataRejection::NotReady)?;
        if !same_plan(&source.plan, &plan) || plan.frozen_cut != source.cut {
            return Err(TransferDataRejection::Conflict);
        }
        let manifest = plan
            .manifest
            .as_ref()
            .ok_or(TransferDataRejection::InvalidPlan)?;
        let outcome = TransferOutcome::SourceReclaimed {
            plan: plan.id,
            partition: plan.partition,
        };
        if source.phase != SourcePhase::Frozen {
            return if source.manifest.as_ref() == Some(manifest) {
                Ok(outcome)
            } else {
                Err(TransferDataRejection::Conflict)
            };
        }
        let state = self
            .partitions
            .get(&plan.partition)
            .ok_or(TransferDataRejection::InvalidPlan)?;
        let cut = source.cut.ok_or(TransferDataRejection::NotReady)?;
        let payload = view(source, state, cut)?;
        let bytes = encoded_size(&payload, source.capacity.max_bytes)?;
        let mut hash = blake3::Hasher::new();
        postcard::to_io(&payload, &mut hash).map_err(|_| TransferDataRejection::Encoding)?;
        if manifest.bytes != bytes
            || manifest.digest != *hash.finalize().as_bytes()
            || manifest.committed_end != source.committed_end
        {
            return Err(TransferDataRejection::Conflict);
        }
        let hot_bytes = self
            .hot_bytes
            .checked_sub(state.producer.hot_bytes())
            .ok_or(TransferDataRejection::InvalidPlan)?;
        let hot_messages = self
            .hot_messages
            .checked_sub(state.producer.hot_message_count())
            .ok_or(TransferDataRejection::InvalidPlan)?;
        let remaining = self
            .partitions
            .keys()
            .copied()
            .filter(|partition| *partition != plan.partition)
            .collect();
        let lifecycle = self
            .lifecycle
            .without_authority_scope(plan.partition, &remaining)
            .map_err(|_| TransferDataRejection::NotReady)?;
        let mut fences = self
            .ownership
            .clone()
            .ok_or(TransferDataRejection::InvalidPlan)?;
        fences
            .retire_partition(&plan.source)
            .map_err(|_| TransferDataRejection::InvalidPlan)?;
        let source = self
            .authority
            .sources
            .get_mut(&plan.id)
            .ok_or(TransferDataRejection::NotReady)?;
        source.lifecycle = None;
        source.metadata_bytes = 0;
        source.phase = SourcePhase::Reclaimed;
        source.manifest = Some(manifest.clone());
        self.partitions.remove(&plan.partition);
        self.lifecycle = lifecycle;
        self.ownership = Some(fences);
        self.hot_bytes = hot_bytes;
        self.hot_messages = hot_messages;
        Ok(outcome)
    }

    pub(super) fn record_retired_cut(
        &mut self,
        plan: Id,
        cut: CommitPosition,
    ) -> Result<TransferOutcome, TransferDataRejection> {
        let source = self
            .authority
            .sources
            .get_mut(&plan)
            .ok_or(TransferDataRejection::NotReady)?;
        match source.phase {
            SourcePhase::Frozen => return Err(TransferDataRejection::NotReady),
            SourcePhase::Retired(original) => return Ok(TransferOutcome::Cut(original)),
            SourcePhase::Reclaimed => {}
        }
        if !valid_retired_cut(source, cut) {
            return Err(TransferDataRejection::InvalidPlan);
        }
        source.phase = SourcePhase::Retired(cut);
        Ok(TransferOutcome::Cut(cut))
    }

    pub(super) fn validate_source_record(
        &self,
        source: &FrozenSource,
    ) -> Result<(), DataRejection> {
        let invalid = || DataRejection::Transfer(TransferDataRejection::InvalidPlan);
        source.plan.validate().map_err(|_| invalid())?;
        source
            .capacity
            .validate()
            .map_err(DataRejection::Transfer)?;
        if source.plan.stage != TransferStage::Prepare
            || self.config.as_ref().is_none_or(|config| {
                config.cluster_id != source.plan.source.cluster_id
                    || config.group_id != source.plan.source.group_id
            })
            || source.cut.is_some_and(|cut| {
                cut.term == 0
                    || cut.index == 0
                    || !source.plan.source_voters.contains(&cut.leader_node)
            })
        {
            return Err(invalid());
        }
        if source.phase == SourcePhase::Frozen {
            if source.manifest.is_some() {
                return Err(invalid());
            }
            return self.validate_frozen_source(source.plan.partition, source);
        }
        if source.lifecycle.is_some()
            || source.metadata_bytes != 0
            || source.manifest.as_ref().is_none_or(|manifest| {
                source.cut != Some(manifest.source_cut)
                    || manifest.committed_end != source.committed_end
                    || manifest.bytes == 0
                    || manifest.bytes > source.capacity.max_bytes
                    || manifest.digest == [0; 32]
            })
            || self
                .ownership
                .as_ref()
                .and_then(|fences| fences.fence(source.plan.partition))
                .is_some_and(|fence| fence.grant.epoch <= source.plan.target.epoch)
            || matches!(source.phase, SourcePhase::Retired(cut) if !valid_retired_cut(source, cut))
        {
            return Err(invalid());
        }
        Ok(())
    }
}

fn valid_retired_cut(source: &FrozenSource, cut: CommitPosition) -> bool {
    source
        .cut
        .is_some_and(|frozen| cut.index > frozen.index && cut.term >= frozen.term)
        && source.plan.source_voters.contains(&cut.leader_node)
}
