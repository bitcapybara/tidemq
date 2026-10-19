use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    },
};

use aws_sdk_s3::{primitives::ByteStream, types::ChecksumMode};
use bytes::Bytes;
use tokio::time::Instant;

use super::{Admission, Error, ObjectHead, Result, S3Store, Uploaded, error::sdk};
use crate::archive::{Descriptor, Pack};

impl S3Store {
    pub async fn put_create_only(&self, pack: &Pack) -> Result<Uploaded> {
        let descriptor = pack.descriptor();
        let key = self.object_key(descriptor)?;
        let deadline = self.deadline();
        let _admission = self.admit(pack.bytes().len(), deadline).await?;
        let put = self
            .0
            .client
            .put_object()
            .bucket(&self.0.bucket)
            .key(&key)
            .if_none_match("*")
            .set_metadata(Some(metadata(descriptor)))
            .body(ByteStream::from(pack.bytes()))
            .customize()
            .interceptor(self.observer())
            .send();
        match tokio::time::timeout_at(deadline, put).await {
            Ok(Ok(_)) => {
                self.head_inner(descriptor, &key, deadline).await?;
                Ok(Uploaded::Created)
            }
            Ok(Err(error)) => match sdk("put", error) {
                Error::Conflict => {
                    self.verify_existing(descriptor, &key, deadline).await?;
                    Ok(Uploaded::ExistingVerified)
                }
                Error::Sdk { .. } => {
                    self.verify_existing(descriptor, &key, deadline)
                        .await
                        .map_err(|error| match error {
                            Error::Conflict | Error::Corrupt { .. } => Error::Conflict,
                            _ => Error::Unknown,
                        })?;
                    Ok(Uploaded::ExistingVerified)
                }
                Error::Timeout { .. } => Err(Error::Unknown),
                error => Err(error),
            },
            Err(_) => Err(Error::Unknown),
        }
    }

    /// A matching HEAD is identity evidence, not a substitute for a body checksum.
    pub async fn head(&self, descriptor: &Descriptor) -> Result<ObjectHead> {
        let key = self.object_key(descriptor)?;
        let deadline = self.deadline();
        let _admission = self.admit(0, deadline).await?;
        self.head_inner(descriptor, &key, deadline).await
    }

    /// Reconcile an unknown upload by checking metadata and every content byte.
    pub async fn verify_uploaded(&self, descriptor: &Descriptor) -> Result<()> {
        let key = self.object_key(descriptor)?;
        let deadline = self.deadline();
        let _admission = self
            .admit(
                usize::try_from(descriptor.total_bytes).map_err(|_| Error::Range)?,
                deadline,
            )
            .await?;
        self.verify_existing(descriptor, &key, deadline).await
    }

    pub(super) async fn head_inner(
        &self,
        descriptor: &Descriptor,
        key: &str,
        deadline: Instant,
    ) -> Result<ObjectHead> {
        let response = tokio::time::timeout_at(
            deadline,
            self.0
                .client
                .head_object()
                .bucket(&self.0.bucket)
                .key(key)
                .customize()
                .interceptor(self.observer())
                .send(),
        )
        .await
        .map_err(|_| Error::Timeout { operation: "head" })?
        .map_err(|error| sdk("head", error))?;
        validate_metadata(descriptor, response.content_length(), response.metadata())?;
        Ok(ObjectHead {
            total_bytes: descriptor.total_bytes,
            checksum: descriptor.checksum,
        })
    }

    pub(super) async fn verify_existing(
        &self,
        descriptor: &Descriptor,
        key: &str,
        deadline: Instant,
    ) -> Result<()> {
        self.head_inner(descriptor, key, deadline).await?;
        self.get_inner(descriptor, key, 0, descriptor.total_bytes, deadline)
            .await?;
        Ok(())
    }

    pub async fn read_range(
        &self,
        descriptor: &Descriptor,
        start: u64,
        length: u64,
    ) -> Result<Bytes> {
        let key = self.object_key(descriptor)?;
        let end = start.checked_add(length).ok_or(Error::Range)?;
        if length == 0
            || end > descriptor.total_bytes
            || length > self.0.limits.max_range_bytes as u64
        {
            return Err(Error::Range);
        }
        let deadline = self.deadline();
        let admission = self
            .admit(usize::try_from(length).map_err(|_| Error::Range)?, deadline)
            .await?;
        let bytes = self
            .get_inner(descriptor, &key, start, length, deadline)
            .await?;
        Self::owned_bytes(admission, bytes)
    }

    async fn get_inner(
        &self,
        descriptor: &Descriptor,
        key: &str,
        start: u64,
        length: u64,
        deadline: Instant,
    ) -> Result<Vec<u8>> {
        let last = start
            .checked_add(length)
            .and_then(|value| value.checked_sub(1))
            .ok_or(Error::Range)?;
        let header_timer = super::metrics::StageTimer::new(&self.0.metrics.headers);
        let status = Arc::new(AtomicU16::new(0));
        let request = self
            .0
            .client
            .get_object()
            .bucket(&self.0.bucket)
            .key(key)
            .range(format!("bytes={start}-{last}"))
            .checksum_mode(ChecksumMode::Enabled)
            .customize()
            .interceptor(super::metrics::Observer::status(
                self.metrics(),
                status.clone(),
            ))
            .send();
        let response = tokio::time::timeout_at(deadline, request)
            .await
            .map_err(|_| Error::Timeout {
                operation: "get headers",
            })?
            .map_err(|error| sdk("get", error))?;
        drop(header_timer);
        let expected_range = format!("bytes {start}-{last}/{}", descriptor.total_bytes);
        if status.load(Ordering::Acquire) != 206
            || response.content_range() != Some(expected_range.as_str())
            || response
                .content_length()
                .and_then(|value| u64::try_from(value).ok())
                != Some(length)
        {
            return Err(Error::Corrupt {
                reason: "range response does not match the requested interval",
            });
        }
        validate_metadata(
            descriptor,
            Some(descriptor.total_bytes as i64),
            response.metadata(),
        )?;
        let checksum =
            (start == 0 && length == descriptor.total_bytes).then_some(descriptor.checksum);
        self.read_body(response.body, length, deadline, checksum)
            .await
    }

    async fn read_body(
        &self,
        mut body: ByteStream,
        length: u64,
        deadline: Instant,
        expected_checksum: Option<[u8; 32]>,
    ) -> Result<Vec<u8>> {
        let _body_timer = super::metrics::StageTimer::new(&self.0.metrics.body);
        let mut bytes = Vec::new();
        let mut checksum = expected_checksum.map(|_| blake3::Hasher::new());
        bytes
            .try_reserve_exact(usize::try_from(length).map_err(|_| Error::Range)?)
            .map_err(|source| Error::Allocation { source })?;
        loop {
            if Instant::now() >= deadline {
                return Err(Error::Timeout {
                    operation: "get body",
                });
            }
            let chunk = tokio::time::timeout_at(deadline, body.try_next())
                .await
                .map_err(|_| Error::Timeout {
                    operation: "get body",
                })?
                .map_err(|source| Error::Sdk {
                    operation: "get body",
                    source: Box::new(source),
                })?;
            let Some(chunk) = chunk else {
                break;
            };
            if bytes
                .len()
                .checked_add(chunk.len())
                .is_none_or(|size| size as u64 > length)
            {
                return Err(Error::Corrupt {
                    reason: "range body exceeds its declared bound",
                });
            }
            let mut remaining = chunk.as_ref();
            while !remaining.is_empty() {
                let (piece, rest) = remaining
                    .split_at_checked(remaining.len().min(64 * 1024))
                    .ok_or(Error::Range)?;
                if let Some(checksum) = &mut checksum {
                    checksum.update(piece);
                }
                bytes.extend_from_slice(piece);
                remaining = rest;
                tokio::task::yield_now().await;
                if Instant::now() >= deadline {
                    return Err(Error::Timeout {
                        operation: "get body",
                    });
                }
            }
        }
        if bytes.len() as u64 != length {
            return Err(Error::Corrupt {
                reason: "range body is truncated",
            });
        }
        if let (Some(expected), Some(checksum)) = (expected_checksum, checksum)
            && checksum.finalize().as_bytes() != &expected
        {
            return Err(Error::Corrupt {
                reason: "object checksum differs",
            });
        }
        Ok(bytes)
    }

    /// Call only with a deletion decision from the committed lifecycle authority.
    pub async fn delete(&self, descriptor: &Descriptor) -> Result<()> {
        let key = self.object_key(descriptor)?;
        let deadline = self.deadline();
        let _admission: Admission = self.admit(0, deadline).await?;
        tokio::time::timeout_at(
            deadline,
            self.0
                .client
                .delete_object()
                .bucket(&self.0.bucket)
                .key(key)
                .customize()
                .interceptor(self.observer())
                .send(),
        )
        .await
        .map_err(|_| Error::Timeout {
            operation: "delete",
        })?
        .map_err(|error| sdk("delete", error))?;
        Ok(())
    }
}

pub(super) fn metadata(descriptor: &Descriptor) -> HashMap<String, String> {
    HashMap::from([
        ("tidemq-format".into(), "pack-v1".into()),
        (
            "tidemq-cluster".into(),
            descriptor.identity.cluster_id.to_string(),
        ),
        (
            "tidemq-group".into(),
            descriptor.identity.group_id.to_string(),
        ),
        (
            "tidemq-object".into(),
            descriptor.identity.object_id.to_string(),
        ),
        (
            "tidemq-blake3".into(),
            blake3::Hash::from_bytes(descriptor.checksum)
                .to_hex()
                .to_string(),
        ),
    ])
}

fn validate_metadata(
    descriptor: &Descriptor,
    content_length: Option<i64>,
    actual: Option<&HashMap<String, String>>,
) -> Result<()> {
    if content_length.and_then(|value| u64::try_from(value).ok()) != Some(descriptor.total_bytes)
        || actual.is_none_or(|actual| {
            metadata(descriptor)
                .iter()
                .any(|(key, value)| actual.get(key) != Some(value))
        })
    {
        return Err(Error::Conflict);
    }
    Ok(())
}
