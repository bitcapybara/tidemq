//! Bounded S3 access. Successful uploads do not publish manifests or authorize deletion.

mod error;
mod metrics;
mod multipart;
mod operations;

use std::{fmt, sync::Arc, time::Duration};

use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig,
};
use bytes::Bytes;
use tidemq_protocol::{Id, common::TrafficClass};
use tidemq_transport::budget::{MemoryBudget, MemoryPermit};
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

pub use error::{Error, Result};
pub use metrics::S3Metrics;
pub use multipart::{PendingUpload, PendingUploads, Upload};

use crate::archive::{Descriptor, PackLimits};

pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub cluster_id: Id,
    pub credentials: Credentials,
    pub allow_http_loopback: bool,
}

impl fmt::Debug for S3Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("cluster_id", &self.cluster_id)
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_in_flight: u32,
    pub max_object_bytes: usize,
    pub max_range_bytes: usize,
    pub bytes_per_second: u64,
    pub request_timeout: Duration,
    pub part_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_in_flight: 4,
            max_object_bytes: 64 * 1024 * 1024,
            max_range_bytes: 64 * 1024 * 1024,
            bytes_per_second: 64 * 1024 * 1024,
            request_timeout: Duration::from_secs(30),
            part_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Uploaded {
    Created,
    ExistingVerified,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectHead {
    pub total_bytes: u64,
    pub checksum: [u8; 32],
}

#[derive(Clone)]
pub struct S3Store(Arc<Inner>);

struct Inner {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    cluster_id: Id,
    limits: Limits,
    budget: MemoryBudget,
    operations: Arc<Semaphore>,
    decode_slots: Arc<Semaphore>,
    rate_next: Mutex<Instant>,
    metrics: Arc<S3Metrics>,
}

pub(super) struct Admission {
    _operation: OwnedSemaphorePermit,
    memory: Option<MemoryPermit>,
    _timer: metrics::OperationTimer,
}

struct OwnedBody {
    bytes: Vec<u8>,
    _permit: MemoryPermit,
}

impl AsRef<[u8]> for OwnedBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl S3Store {
    pub fn new(config: S3Config, limits: Limits, budget: MemoryBudget) -> Result<Self> {
        validate_config(&config, limits)?;
        let sdk = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(config.endpoint)
            .region(Region::new(config.region))
            .credentials_provider(config.credentials)
            .force_path_style(true)
            .retry_config(RetryConfig::standard().with_max_attempts(3))
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(3))
                    .operation_timeout(limits.request_timeout)
                    .operation_attempt_timeout(limits.request_timeout)
                    .build(),
            )
            .build();
        Ok(Self(Arc::new(Inner {
            client: aws_sdk_s3::Client::from_conf(sdk),
            bucket: config.bucket,
            prefix: format!("{}/{}/", config.prefix, config.cluster_id),
            cluster_id: config.cluster_id,
            limits,
            budget,
            operations: Arc::new(Semaphore::new(limits.max_in_flight as usize)),
            decode_slots: Arc::new(Semaphore::new(limits.max_in_flight as usize)),
            rate_next: Mutex::new(Instant::now()),
            metrics: Arc::new(S3Metrics::default()),
        })))
    }

    pub fn metrics(&self) -> Arc<S3Metrics> {
        self.0.metrics.clone()
    }

    pub(crate) fn budget(&self) -> &MemoryBudget {
        &self.0.budget
    }
    pub(crate) fn decoder_slots(&self) -> Arc<Semaphore> {
        self.0.decode_slots.clone()
    }

    fn observer(&self) -> metrics::Observer {
        metrics::Observer::new(self.metrics())
    }

    pub fn object_key(&self, descriptor: &Descriptor) -> Result<String> {
        self.validate_descriptor(descriptor)?;
        Ok(format!(
            "{}packs/g{}/{}-{}.pack",
            self.0.prefix,
            descriptor.identity.group_id,
            descriptor.identity.object_id,
            blake3::Hash::from_bytes(descriptor.checksum).to_hex()
        ))
    }

    fn validate_descriptor(&self, descriptor: &Descriptor) -> Result<()> {
        if descriptor.identity.cluster_id != self.0.cluster_id {
            return Err(Error::Configuration {
                reason: "object belongs to another cluster",
            });
        }
        descriptor
            .validate(PackLimits {
                max_bytes: self.0.limits.max_object_bytes,
                ..PackLimits::default()
            })
            .map_err(|_| Error::Corrupt {
                reason: "object descriptor is invalid",
            })
    }

    fn deadline(&self) -> Instant {
        Instant::now() + self.0.limits.request_timeout
    }

    async fn admit(&self, bytes: usize, deadline: Instant) -> Result<Admission> {
        self.admit_with_rate(bytes, bytes, deadline).await
    }

    async fn admit_with_rate(
        &self,
        bytes: usize,
        rate_bytes: usize,
        deadline: Instant,
    ) -> Result<Admission> {
        let started = Instant::now();
        let result = self.admit_inner(bytes, rate_bytes, deadline).await;
        self.0
            .metrics
            .admission
            .observe(started.elapsed().as_secs_f64());
        if result.is_err() {
            self.0.metrics.rejected.inc();
        }
        result
    }

    async fn admit_inner(
        &self,
        bytes: usize,
        rate_bytes: usize,
        deadline: Instant,
    ) -> Result<Admission> {
        let timer = metrics::OperationTimer::new(self.metrics());
        let operation = self
            .0
            .operations
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let charge = bytes
            .checked_mul(2)
            .and_then(|value| value.checked_add(64 * 1024))
            .ok_or(Error::Overloaded)?;
        let charge = u32::try_from(charge).map_err(|_| Error::Overloaded)?;
        let memory = self
            .0
            .budget
            .try_reserve(
                if bytes == 0 {
                    TrafficClass::Control
                } else {
                    TrafficClass::Bulk
                },
                charge,
            )
            .map_err(|_| Error::Overloaded)?;
        self.pace(rate_bytes, deadline).await?;
        Ok(Admission {
            _operation: operation,
            memory: Some(memory),
            _timer: timer,
        })
    }

    async fn pace(&self, bytes: usize, deadline: Instant) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let nanoseconds =
            (bytes as u128) * 1_000_000_000 / u128::from(self.0.limits.bytes_per_second);
        let delay =
            Duration::from_nanos(u64::try_from(nanoseconds).map_err(|_| Error::Overloaded)?);
        let ready = {
            let mut next = self.0.rate_next.lock().await;
            let ready = (*next).max(Instant::now());
            if ready >= deadline {
                return Err(Error::Overloaded);
            }
            *next = ready.checked_add(delay).ok_or(Error::Overloaded)?;
            ready
        };
        tokio::time::timeout_at(deadline, tokio::time::sleep_until(ready))
            .await
            .map_err(|_| Error::Timeout {
                operation: "admission",
            })?;
        Ok(())
    }

    fn owned_bytes(mut admission: Admission, bytes: Vec<u8>) -> Result<Bytes> {
        let permit = admission.memory.take().ok_or(Error::Overloaded)?;
        Ok(Bytes::from_owner(OwnedBody {
            bytes,
            _permit: permit,
        }))
    }
}

fn validate_config(config: &S3Config, limits: Limits) -> Result<()> {
    let endpoint = &config.endpoint;
    let valid_https = endpoint.starts_with("https://") && endpoint.len() > 8;
    let valid_http = config.allow_http_loopback
        && endpoint.strip_prefix("http://").is_some_and(|address| {
            address
                .parse::<std::net::SocketAddr>()
                .is_ok_and(|socket| socket.ip().is_loopback() && socket.port() != 0)
        });
    if !(valid_https || valid_http) || endpoint.contains(['@', '?', '#']) || endpoint.len() > 2048 {
        return Err(Error::Configuration {
            reason: "endpoint requires HTTPS or explicit loopback HTTP without credentials or query parameters",
        });
    }
    if !(3..=63).contains(&config.bucket.len())
        || !config
            .bucket
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || !config
            .bucket
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        || !config
            .bucket
            .bytes()
            .last()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(Error::Configuration {
            reason: "bucket requires a fixed DNS-compatible name",
        });
    }
    if config.prefix.is_empty()
        || config.prefix.len() > 128
        || !config
            .prefix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_'))
        || config.prefix.starts_with('/')
        || config.prefix.ends_with('/')
        || config.prefix.contains("//")
        || config.region.is_empty()
        || config.region.len() > 64
    {
        return Err(Error::Configuration {
            reason: "prefix or region is invalid",
        });
    }
    if !(1..=128).contains(&limits.max_in_flight)
        || limits.max_object_bytes == 0
        || limits.max_object_bytes > 512 * 1024 * 1024
        || limits.max_range_bytes == 0
        || limits.max_range_bytes > limits.max_object_bytes
        || limits.bytes_per_second == 0
        || limits.bytes_per_second > 16 * 1024 * 1024 * 1024
        || limits.request_timeout.is_zero()
        || limits.request_timeout > Duration::from_secs(300)
        || !(5 * 1024 * 1024..=64 * 1024 * 1024).contains(&limits.part_bytes)
    {
        return Err(Error::Configuration {
            reason: "operation, byte, rate, timeout, or multipart limits are invalid",
        });
    }
    Ok(())
}
