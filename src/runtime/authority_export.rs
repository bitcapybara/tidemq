use std::{
    io::{BufWriter, Seek, Write},
    sync::Arc,
};

use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    sync::Mutex,
};

use super::*;

#[derive(Clone, Default)]
pub(in crate::runtime) struct ExportCache(Arc<Mutex<Option<Artifact>>>);

struct Artifact {
    plan: Id,
    group: u64,
    manifest: TransferManifest,
    file: tokio::fs::File,
}

impl Service {
    pub(super) async fn export_manifest(
        &self,
        group: u64,
        plan: &AuthorityTransfer,
        deadline: Instant,
    ) -> Result<TransferManifest> {
        if plan.stage < TransferStage::Frozen || plan.stage >= TransferStage::Checkpointed {
            return Err(Error::unavailable());
        }
        let mut slot = wire::within(deadline, self.transfer_exports.0.clone().lock_owned()).await?;
        if let Some(artifact) = slot
            .as_ref()
            .filter(|artifact| artifact.plan == plan.id && artifact.group == group)
        {
            return Ok(artifact.manifest.clone());
        }
        *slot = None;
        let repair = self
            .repair
            .acquire(deadline)
            .await
            .map_err(|source| Error::operation("admit shard export repair work", source))?;
        let (export, permit, expected) = self.copy_transfer_export(group, plan.id).await?;
        let file = self.export_file(deadline).await?;
        let id = plan.id;
        let worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _repair = repair;
            let (file, manifest) = write_export(&export, file, expected, deadline)?;
            *slot = Some(Artifact {
                plan: id,
                group,
                manifest: manifest.clone(),
                file: tokio::fs::File::from_std(file),
            });
            Ok(manifest)
        });
        // The worker owns admission, the file and cache guard until its I/O really finishes.
        wire::within(deadline, worker)
            .await?
            .map_err(|source| Error::operation("join bounded shard export worker", source))?
    }

    async fn copy_transfer_export(
        &self,
        group: u64,
        id: Id,
    ) -> Result<(
        crate::broker::AuthorityExport,
        crate::budget::MemoryPermit,
        u64,
    )> {
        let replica = self.data.get(&group).ok_or_else(Error::unavailable)?;
        replica
            .state
            .try_with_application(|state| {
                let size = state.transfer_export_size(id).map_err(transfer_error)?;
                let charge = size
                    .checked_add(TRANSFER_PAGE_BYTES as u64)
                    .and_then(|size| u32::try_from(size).ok())
                    .ok_or_else(Error::unavailable)?;
                let permit = self
                    .budget
                    .try_reserve(TrafficClass::Bulk, charge)
                    .map_err(|source| Error::operation("admit frozen shard projection", source))?;
                Ok((
                    state.transfer_export(id).map_err(transfer_error)?,
                    permit,
                    size,
                ))
            })
            .await
            .map_err(|source| Error::operation("copy one frozen shard projection", source))?
    }

    async fn export_file(&self, deadline: Instant) -> Result<std::fs::File> {
        let id = Id::new_v7()
            .map_err(|source| Error::operation("allocate private export file identity", source))?;
        let path = self
            .config
            .data_dir
            .join(format!("authority-export-{id}.tmp"));
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = wire::within(deadline, options.open(&path))
            .await?
            .map_err(|source| Error::operation("create private shard export file", source))?;
        // Unlink before writing: process failure cannot leave exported payloads on disk.
        tokio::fs::remove_file(&path)
            .await
            .map_err(|source| Error::operation("unlink private shard export file", source))?;
        Ok(file.into_std().await)
    }

    pub(super) async fn export_page(
        &self,
        group: u64,
        plan: &AuthorityTransfer,
        offset: u64,
        deadline: Instant,
    ) -> Result<TransferPage> {
        let manifest = self.export_manifest(group, plan, deadline).await?;
        if offset >= manifest.bytes
            || !offset.is_multiple_of(TRANSFER_PAGE_BYTES as u64)
            || plan.manifest.as_ref() != Some(&manifest)
        {
            return Err(Error::invalid(
                "Export page differs from the durable manifest",
            ));
        }
        self.repair
            .pace(TRANSFER_PAGE_BYTES, deadline)
            .await
            .map_err(|source| Error::operation("pace shard transfer export page", source))?;
        let mut slot = wire::within(deadline, self.transfer_exports.0.lock()).await?;
        let artifact = slot
            .as_mut()
            .filter(|artifact| artifact.plan == plan.id && artifact.group == group)
            .ok_or_else(Error::unavailable)?;
        let length = usize::try_from(
            manifest
                .bytes
                .saturating_sub(offset)
                .min(TRANSFER_PAGE_BYTES as u64),
        )
        .map_err(|_| Error::unavailable())?;
        let _permit = self
            .budget
            .try_reserve(TrafficClass::Bulk, TRANSFER_PAGE_BYTES as u32)
            .map_err(|source| Error::operation("admit shard export page buffer", source))?;
        let mut bytes = vec![0; length];
        wire::within(
            deadline,
            artifact.file.seek(std::io::SeekFrom::Start(offset)),
        )
        .await?
        .map_err(|source| Error::operation("seek private shard export page", source))?;
        wire::within(deadline, artifact.file.read_exact(&mut bytes))
            .await?
            .map_err(|source| Error::operation("read private shard export page", source))?;
        let digest = *blake3::hash(&bytes).as_bytes();
        Ok(TransferPage {
            plan: plan.id,
            offset,
            bytes: bytes.into(),
            digest,
        })
    }

    pub(super) async fn clear_export(&self, id: Id, deadline: Instant) -> Result<()> {
        let mut slot = wire::within(deadline, self.transfer_exports.0.lock()).await?;
        if slot.as_ref().is_some_and(|artifact| artifact.plan == id) {
            *slot = None;
        }
        Ok(())
    }
}

fn write_export(
    export: &crate::broker::AuthorityExport,
    file: std::fs::File,
    expected: u64,
    deadline: Instant,
) -> Result<(std::fs::File, TransferManifest)> {
    let mut writer = ExportWriter {
        file: BufWriter::with_capacity(TRANSFER_PAGE_BYTES, file),
        hasher: blake3::Hasher::new(),
        written: 0,
        expected,
        deadline,
    };
    export.write_to(&mut writer).map_err(transfer_error)?;
    writer
        .flush()
        .map_err(|source| Error::operation("flush private shard export", source))?;
    if writer.written != expected {
        return Err(Error::invalid("Frozen shard export size changed"));
    }
    let manifest = export.manifest(expected, *writer.hasher.finalize().as_bytes());
    let mut file = writer
        .file
        .into_inner()
        .map_err(|source| Error::operation("finish private shard export", source.into_error()))?;
    file.rewind()
        .map_err(|source| Error::operation("rewind private shard export", source))?;
    Ok((file, manifest))
}

struct ExportWriter {
    file: BufWriter<std::fs::File>,
    hasher: blake3::Hasher,
    written: u64,
    expected: u64,
    deadline: Instant,
}

impl Write for ExportWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .written
            .checked_add(bytes.len() as u64)
            .filter(|next| *next <= self.expected)
            .ok_or_else(|| std::io::Error::other("Shard export exceeds its finite byte bound"))?;
        if Instant::now() >= self.deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Shard export deadline elapsed",
            ));
        }
        self.file.write_all(bytes)?;
        self.hasher.update(bytes);
        self.written = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
