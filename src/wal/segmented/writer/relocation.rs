use super::*;

pub(super) struct RelocationPlan {
    pub segment: Segment,
    original: Vec<Location>,
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
        if self.physical_bytes < threshold {
            return Ok(None);
        }
        let candidate = self
            .manifest
            .sealed
            .iter()
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
                (live > 0 && dead > 0).then_some((segment, live, dead))
            })
            .max_by_key(|(_, _, dead)| *dead);
        let Some((source, live, _)) = candidate else {
            return Ok(None);
        };
        let original = references
            .iter()
            .filter(|r| r.segment == source.id)
            .cloned()
            .collect::<Vec<_>>();
        let segment = Segment {
            id: new_id()?,
            first_sequence: source.first_sequence,
            reference_only: true,
            records: original.len() as u64,
            byte_len: SEGMENT_HEADER_BYTES.checked_add(live).ok_or(Error::Limit {
                name: "relocation segment bytes",
            })?,
            checksum: [0; 32],
        };
        let mut offset = SEGMENT_HEADER_BYTES;
        for location in references.iter_mut().filter(|r| r.segment == source.id) {
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
                || decode_record(frame.payload.clone())?.id != Some(location.id)
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
        file.sync_all()
            .await
            .map_err(|e| Error::io(&destination, e))?;
        #[cfg(test)]
        super::super::fault_tests::boundary("segment_sync");
        sync_directory(&self.directory).await?;
        #[cfg(test)]
        super::super::fault_tests::boundary("segment_directory_sync");
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
