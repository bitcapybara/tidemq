use aws_sdk_s3::{
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};
use tokio::{sync::oneshot, task::JoinHandle, time::Instant};

use super::{Admission, Error, Result, S3Store, Uploaded, error::sdk, operations::metadata};
use crate::archive::{Descriptor, Pack, format::view};

/// Dropping this handle signals the owned worker, which aborts an unfinished upload.
pub struct Upload {
    cancel: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<Uploaded>>>,
}

impl Upload {
    pub async fn wait(mut self) -> Result<Uploaded> {
        self.task
            .take()
            .ok_or(Error::Unknown)?
            .await
            .map_err(|source| Error::Worker { source })?
    }

    pub async fn cancel(mut self) -> Result<()> {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        match self
            .task
            .take()
            .ok_or(Error::Unknown)?
            .await
            .map_err(|source| Error::Worker { source })?
        {
            Err(Error::Cancelled) => Ok(()),
            Err(error) => Err(error),
            Ok(_) => Err(Error::Unknown),
        }
    }
}

impl Drop for Upload {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingUpload {
    pub key: String,
    pub upload_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingUploads {
    pub uploads: Vec<PendingUpload>,
    pub next: Option<(String, String)>,
}

impl S3Store {
    pub async fn start_multipart(&self, pack: Pack) -> Result<Upload> {
        let key = self.object_key(pack.descriptor())?;
        let deadline = self.deadline();
        let admission = self
            .admit_with_rate(
                pack.bytes().len(),
                self.0.limits.part_bytes.min(pack.bytes().len()),
                deadline,
            )
            .await?;
        let store = self.clone();
        let (sender, receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            store
                .multipart_worker(pack, key, deadline, admission, receiver)
                .await
        });
        Ok(Upload {
            cancel: Some(sender),
            task: Some(task),
        })
    }

    async fn multipart_worker(
        &self,
        pack: Pack,
        key: String,
        deadline: Instant,
        _admission: Admission,
        mut cancel: oneshot::Receiver<()>,
    ) -> Result<Uploaded> {
        let created = tokio::time::timeout_at(
            deadline,
            self.0
                .client
                .create_multipart_upload()
                .bucket(&self.0.bucket)
                .key(&key)
                .set_metadata(Some(metadata(pack.descriptor())))
                .customize()
                .interceptor(self.observer())
                .send(),
        )
        .await
        .map_err(|_| Error::Unknown)?
        .map_err(|error| {
            let error = anyhow::Error::new(sdk("create multipart", error));
            tracing::warn!(error = %format_args!("{error:#}"), "S3 multipart creation outcome is unknown");
            Error::Unknown
        })?;
        let upload_id = created
            .upload_id()
            .filter(|id| !id.is_empty() && id.len() <= 2048)
            .ok_or(Error::Corrupt {
                reason: "multipart response has no bounded upload identity",
            })?;
        let result = self
            .upload_parts(&pack, &key, upload_id, deadline, &mut cancel)
            .await;
        match result {
            Ok(uploaded) => Ok(uploaded),
            Err(error) => {
                // Keep admission until cleanup finishes, even after the caller has gone away.
                self.abort_inner(&key, upload_id).await?;
                Err(error)
            }
        }
    }

    async fn upload_parts(
        &self,
        pack: &Pack,
        key: &str,
        upload_id: &str,
        deadline: Instant,
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<Uploaded> {
        let bytes = pack.bytes();
        let part_bytes = self.0.limits.part_bytes;
        let count = bytes.len().div_ceil(part_bytes);
        if count == 0 || count > 10_000 {
            return Err(Error::Configuration {
                reason: "multipart part count is invalid",
            });
        }
        let mut completed = Vec::new();
        completed
            .try_reserve_exact(count)
            .map_err(|source| Error::Allocation { source })?;
        let mut start = 0_usize;
        for number in 1..=count {
            let end = start
                .checked_add(part_bytes)
                .ok_or(Error::Range)?
                .min(bytes.len());
            let part = view(&bytes, start, end).map_err(|_| Error::Range)?;
            if number > 1 {
                tokio::select! {
                    biased;
                    _ = &mut *cancel => return Err(Error::Cancelled),
                    result = self.pace(part.len(), deadline) => result?,
                }
            }
            let request = self
                .0
                .client
                .upload_part()
                .bucket(&self.0.bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(i32::try_from(number).map_err(|_| Error::Range)?)
                .body(ByteStream::from(part))
                .customize()
                .interceptor(self.observer())
                .send();
            let response = tokio::select! {
                biased;
                _ = &mut *cancel => return Err(Error::Cancelled),
                response = tokio::time::timeout_at(deadline, request) => response.map_err(|_| Error::Timeout { operation: "upload part" })?.map_err(|error| sdk("upload part", error))?,
            };
            let etag = response
                .e_tag()
                .filter(|value| !value.is_empty() && value.len() <= 1024)
                .ok_or(Error::Corrupt {
                    reason: "multipart part has no bounded ETag",
                })?;
            completed.push(
                CompletedPart::builder()
                    .part_number(number as i32)
                    .e_tag(etag)
                    .set_checksum_crc32(response.checksum_crc32().map(str::to_owned))
                    .set_checksum_crc32_c(response.checksum_crc32_c().map(str::to_owned))
                    .set_checksum_crc64_nvme(response.checksum_crc64_nvme().map(str::to_owned))
                    .set_checksum_sha1(response.checksum_sha1().map(str::to_owned))
                    .set_checksum_sha256(response.checksum_sha256().map(str::to_owned))
                    .build(),
            );
            start = end;
        }
        self.complete_upload(pack, key, upload_id, completed, deadline, cancel)
            .await
    }

    async fn complete_upload(
        &self,
        pack: &Pack,
        key: &str,
        upload_id: &str,
        completed: Vec<CompletedPart>,
        deadline: Instant,
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<Uploaded> {
        let request = self
            .0
            .client
            .complete_multipart_upload()
            .bucket(&self.0.bucket)
            .key(key)
            .upload_id(upload_id)
            .if_none_match("*")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed))
                    .build(),
            )
            .customize()
            .interceptor(self.observer())
            .send();
        match tokio::select! {
            biased;
            _ = &mut *cancel => return Err(Error::Unknown),
            result = tokio::time::timeout_at(deadline, request) => result,
        } {
            Ok(Ok(_)) => {
                self.head_inner(pack.descriptor(), key, deadline).await?;
                Ok(Uploaded::Created)
            }
            Ok(Err(error)) => match sdk("complete multipart", error) {
                Error::Conflict => {
                    self.abort_inner(key, upload_id).await?;
                    self.verify_existing(pack.descriptor(), key, deadline)
                        .await?;
                    Ok(Uploaded::ExistingVerified)
                }
                _ => Err(Error::Unknown),
            },
            Err(_) => Err(Error::Unknown),
        }
    }

    async fn abort_inner(&self, key: &str, upload_id: &str) -> Result<()> {
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        for _ in 0..3 {
            if self.abort_round(key, upload_id, deadline).await? {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Err(Error::Unknown)
    }

    async fn abort_round(&self, key: &str, upload_id: &str, deadline: Instant) -> Result<bool> {
        match tokio::time::timeout_at(
            deadline,
            self.0
                .client
                .abort_multipart_upload()
                .bucket(&self.0.bucket)
                .key(key)
                .upload_id(upload_id)
                .customize()
                .interceptor(self.observer())
                .send(),
        )
        .await
        {
            Ok(Ok(_)) => self.parts_removed(key, upload_id, deadline).await,
            Ok(Err(error)) => match sdk("abort multipart", error) {
                Error::Missing => Ok(true),
                error => Err(error),
            },
            Err(_) => Err(Error::Timeout {
                operation: "abort multipart",
            }),
        }
    }

    async fn parts_removed(&self, key: &str, upload_id: &str, deadline: Instant) -> Result<bool> {
        match tokio::time::timeout_at(
            deadline,
            self.0
                .client
                .list_parts()
                .bucket(&self.0.bucket)
                .key(key)
                .upload_id(upload_id)
                .max_parts(1)
                .customize()
                .interceptor(self.observer())
                .send(),
        )
        .await
        {
            Ok(Ok(parts)) => Ok(parts.parts().is_empty()),
            Ok(Err(error)) => match sdk("verify multipart abort", error) {
                Error::Missing => Ok(true),
                error => Err(error),
            },
            Err(_) => Err(Error::Timeout {
                operation: "verify multipart abort",
            }),
        }
    }

    /// Recovery enumerates at most one bounded page; it never treats the bucket as a read manifest.
    pub async fn pending_uploads(
        &self,
        marker: Option<(&str, &str)>,
        limit: u16,
    ) -> Result<PendingUploads> {
        if !(1..=100).contains(&limit)
            || marker.is_some_and(|(key, id)| {
                !key.starts_with(&self.0.prefix) || key.len() > 1024 || id.len() > 2048
            })
        {
            return Err(Error::Configuration {
                reason: "multipart reconciliation page or marker is invalid",
            });
        }
        let deadline = self.deadline();
        let _admission = self.admit(1024 * 1024, deadline).await?;
        let response = tokio::time::timeout_at(
            deadline,
            self.0
                .client
                .list_multipart_uploads()
                .bucket(&self.0.bucket)
                .prefix(&self.0.prefix)
                .max_uploads(i32::from(limit))
                .set_key_marker(marker.map(|(key, _)| key.to_owned()))
                .set_upload_id_marker(marker.map(|(_, id)| id.to_owned()))
                .customize()
                .interceptor(self.observer())
                .send(),
        )
        .await
        .map_err(|_| Error::Timeout {
            operation: "list multipart",
        })?
        .map_err(|error| sdk("list multipart", error))?;
        if response.uploads().len() > limit as usize {
            return Err(Error::Corrupt {
                reason: "multipart listing exceeds its page limit",
            });
        }
        let mut uploads = Vec::new();
        uploads
            .try_reserve_exact(response.uploads().len())
            .map_err(|source| Error::Allocation { source })?;
        for item in response.uploads() {
            let key = item
                .key()
                .filter(|key| key.starts_with(&self.0.prefix) && key.len() <= 1024)
                .ok_or(Error::Corrupt {
                    reason: "multipart listing returned another prefix",
                })?;
            let id = item
                .upload_id()
                .filter(|id| !id.is_empty() && id.len() <= 2048)
                .ok_or(Error::Corrupt {
                    reason: "multipart listing upload identity is invalid",
                })?;
            uploads.push(PendingUpload {
                key: key.to_owned(),
                upload_id: id.to_owned(),
            });
        }
        let next = if response.is_truncated() == Some(true) {
            let key = response
                .next_key_marker()
                .filter(|key| key.starts_with(&self.0.prefix) && key.len() <= 1024)
                .ok_or(Error::Corrupt {
                    reason: "multipart listing continuation key is invalid",
                })?;
            let id = response
                .next_upload_id_marker()
                .filter(|id| !id.is_empty() && id.len() <= 2048)
                .ok_or(Error::Corrupt {
                    reason: "multipart listing continuation identity is invalid",
                })?;
            if marker == Some((key, id)) {
                return Err(Error::Corrupt {
                    reason: "multipart listing continuation does not advance",
                });
            }
            Some((key.to_owned(), id.to_owned()))
        } else {
            None
        };
        Ok(PendingUploads { uploads, next })
    }

    /// The caller must reconcile the registered lifecycle task before authorizing this cleanup.
    pub async fn abort_pending(
        &self,
        descriptor: &Descriptor,
        upload: &PendingUpload,
    ) -> Result<()> {
        if self.object_key(descriptor)? != upload.key
            || upload.upload_id.is_empty()
            || upload.upload_id.len() > 2048
        {
            return Err(Error::Conflict);
        }
        let deadline = self.deadline();
        let _admission = self.admit(0, deadline).await?;
        self.abort_inner(&upload.key, &upload.upload_id).await
    }
}
