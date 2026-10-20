use super::*;

pub(super) struct RelocationPlan {
    pub segment: Segment,
    original: Vec<Location>,
}
struct RelocationInputs<'a> {
    source: &'a Segment,
    target: Option<&'a Segment>,
    live: u64,
}
impl Writer {
    pub(super) fn plan_relocation(
        &self,
        references: &mut [Location],
    ) -> Result<Option<RelocationPlan>> {
        let threshold = self
            .options
            .max_file_bytes
            .saturating_sub(self.options.control_reserve_bytes)
            .saturating_sub(self.options.segment_target_bytes);
        let segments = self.manifest.sealed.len().saturating_add(1).saturating_add(
            self.retired
                .iter()
                .filter(|file| file.kind == SegmentedFileKind::Segment)
                .count(),
        );
        if self.physical_bytes < threshold && segments < self.options.max_segments.saturating_sub(2)
        {
            return Ok(None);
        }
        // The ordered checkpoint barrier seals the old active before any source frame is copied.
        let count_pressure = segments >= self.options.max_segments.saturating_sub(2);
        let Some(inputs) = self.relocation_inputs(references, count_pressure) else {
            return Ok(None);
        };
        let selected =
            |id| id == inputs.source.id || inputs.target.is_some_and(|target| target.id == id);
        let original = references
            .iter()
            .filter(|r| selected(r.segment))
            .cloned()
            .collect::<Vec<_>>();
        let live = inputs.live;
        let segment = Segment {
            id: new_id()?,
            first_sequence: inputs
                .target
                .map_or(inputs.source.first_sequence, |target| {
                    target.first_sequence.min(inputs.source.first_sequence)
                }),
            reference_only: true,
            records: original.len() as u64,
            byte_len: SEGMENT_HEADER_BYTES.checked_add(live).ok_or(Error::Limit {
                name: "relocation segment bytes",
            })?,
            checksum: [0; 32],
        };
        let mut offset = SEGMENT_HEADER_BYTES;
        for location in references.iter_mut().filter(|r| selected(r.segment)) {
            location.segment = segment.id;
            location.offset = offset;
            offset = offset
                .checked_add(location.frame_bytes)
                .ok_or(Error::Limit {
                    name: "relocation reference offset",
                })?;
        }
        Ok(Some(RelocationPlan { segment, original }))
    }
    fn relocation_inputs<'a>(
        &'a self,
        references: &[Location],
        count_pressure: bool,
    ) -> Option<RelocationInputs<'a>> {
        let candidates = self
            .manifest
            .sealed
            .iter()
            .chain(std::iter::once(&self.manifest.active))
            .filter_map(|segment| {
                let live = references
                    .iter()
                    .filter(|r| r.segment == segment.id)
                    .map(|r| r.frame_bytes)
                    .sum::<u64>();
                let dead = segment
                    .byte_len
                    .saturating_sub(SEGMENT_HEADER_BYTES)
                    .saturating_sub(live);
                (live > 0).then_some((segment, live, dead))
            })
            .collect::<Vec<_>>();
        let paired = |source_id, source_live: u64| {
            candidates
                .iter()
                .filter(|(segment, live, _)| {
                    segment.reference_only
                        && segment.id != source_id
                        && source_live.checked_add(*live).is_some_and(|sum| {
                            sum <= self
                                .options
                                .segment_target_bytes
                                .saturating_sub(SEGMENT_HEADER_BYTES)
                        })
                })
                .max_by_key(|(_, live, _)| *live)
        };
        if count_pressure
            && let Some((source, live, _, target, target_live)) = candidates
                .iter()
                .filter(|(s, _, dead)| *dead > 0 || s.reference_only)
                .filter_map(|(source, live, dead)| {
                    paired(source.id, *live).map(|(target, target_live, _)| {
                        (*source, *live, *dead, *target, *target_live)
                    })
                })
                .max_by_key(|(_, _, dead, _, _)| *dead)
        {
            return Some(RelocationInputs {
                source,
                target: Some(target),
                live: live + target_live,
            });
        }
        let (source, live, _) = candidates
            .iter()
            .filter(|(_, _, dead)| *dead > 0)
            .max_by_key(|(_, _, dead)| *dead)?;
        let target = paired(source.id, *live);
        Some(RelocationInputs {
            source,
            target: target.map(|(target, _, _)| *target),
            live: live + target.map_or(0, |(_, live, _)| *live),
        })
    }
    pub(super) async fn copy_relocation(&mut self, plan: &RelocationPlan) -> Result<Segment> {
        let start = Instant::now();
        let destination = self.directory.join(plan.segment.name());
        let header = segment_header(self.manifest.identity, &plan.segment)?;
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&destination)
            .await
            .map_err(|e| Error::io(&destination, e))?;
        file.write_all(&header)
            .await
            .map_err(|e| Error::io(&destination, e))?;
        let mut hash = blake3::Hasher::new();
        hash.update(&header);
        for location in &plan.original {
            let source = segment_path(&self.directory, location.segment);
            let mut input = File::open(&source)
                .await
                .map_err(|e| Error::io(&source, e))?;
            let end = input
                .metadata()
                .await
                .map_err(|e| Error::io(&source, e))?
                .len();
            let frame = read_frame(&mut input, &source, location.offset, end, &self.options)
                .await?
                .ok_or(corrupt("Relocation source frame is incomplete"))?;
            if frame.sequence != location.sequence
                || frame.bytes != location.frame_bytes
                || record_id(&frame.payload)? != Some(location.id)
            {
                return Err(corrupt("Relocation source identity differs"));
            }
            file.write_all(&frame.header)
                .await
                .map_err(|e| Error::io(&destination, e))?;
            file.write_all(&frame.payload)
                .await
                .map_err(|e| Error::io(&destination, e))?;
            hash.update(&frame.header);
            hash.update(&frame.payload);
        }
        file.flush().await.map_err(|e| Error::io(&destination, e))?;
        file.sync_all()
            .await
            .map_err(|e| Error::io(&destination, e))?;
        #[cfg(test)]
        super::super::fault_tests::boundary("segment_sync");
        sync_directory(&self.directory).await?;
        #[cfg(test)]
        super::super::fault_tests::boundary("segment_directory_sync");
        #[cfg(test)]
        super::super::fault_tests::boundary("relocation_copy_sync");
        let mut segment = plan.segment.clone();
        segment.checksum = *hash.finalize().as_bytes();
        if let Some(metrics) = &self.metrics {
            metrics.observe(Stage::WalRelocate, start.elapsed().as_secs_f64());
        }
        let mut stats = lock(&self.shared.stats);
        stats.relocation_operations = stats.relocation_operations.saturating_add(1);
        stats.relocated_bytes = stats.relocated_bytes.saturating_add(segment.byte_len);
        Ok(segment)
    }
}
